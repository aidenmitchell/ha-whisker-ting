"""WebSocket client for real-time Whisker Ting data."""

from __future__ import annotations

import asyncio
import logging
import struct
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

import aiohttp
import msgpack

from .const import SIGNALR_URL

_LOGGER = logging.getLogger(__name__)

# SignalR MessagePack message types
MSG_TYPE_INVOCATION = 1
MSG_TYPE_STREAM_ITEM = 2
MSG_TYPE_COMPLETION = 3
MSG_TYPE_STREAM_INVOCATION = 4
MSG_TYPE_CANCEL_INVOCATION = 5
MSG_TYPE_PING = 6
MSG_TYPE_CLOSE = 7


@dataclass
class VoltageData:
    """Real-time voltage data from WebSocket."""

    timestamp: datetime
    voltage: float
    voltage_hi: float
    voltage_lo: float
    average_peaks_max: float


class WhiskerWebSocket:
    """WebSocket client for Whisker Ting SignalR hub."""

    # Consider data stale if no update in 30 seconds (normally updates every ~250ms)
    STALE_DATA_THRESHOLD = 30

    def __init__(
        self,
        session: aiohttp.ClientSession,
        api_key: str,
        user_id: int,
        station_id: str,
        on_voltage_update: Callable[[str, VoltageData], None] | None = None,
        on_disconnect: Callable[[str], None] | None = None,
    ) -> None:
        """Initialize the WebSocket client."""
        self._session = session
        self._api_key = api_key  # The api_key is used as the stream token
        self._user_id = user_id
        self._station_id = station_id
        self._on_voltage_update = on_voltage_update
        self._on_disconnect = on_disconnect
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._connected = False
        self._reconnect_task: asyncio.Task | None = None
        self._ping_task: asyncio.Task | None = None
        self._receive_task: asyncio.Task | None = None
        self._stale_check_task: asyncio.Task | None = None
        self._message_id = 0
        self._first_data_received = asyncio.Event()
        self._last_data_time: datetime | None = None
        self._shutting_down = False

    @property
    def connected(self) -> bool:
        """Return True if connected."""
        return self._connected

    def _encode_invocation(self, method: str, args: list) -> bytes:
        """Encode a SignalR invocation message."""
        self._message_id += 1
        # SignalR MessagePack invocation format:
        # {1: [type, headers, invocationId, target, arguments]}
        # Type 1 = INVOCATION (not streaming)
        message = {
            1: [
                MSG_TYPE_INVOCATION,
                {},  # headers
                str(self._message_id),  # invocationId
                method,
                args,
            ]
        }
        return msgpack.packb(message, use_bin_type=True)

    def _encode_ping(self) -> bytes:
        """Encode a SignalR ping message."""
        # Ping is just {1: [6]}
        return msgpack.packb({1: [MSG_TYPE_PING]}, use_bin_type=True)

    def _decode_voltage_data(self, data: bytes) -> VoltageData | None:
        """Decode voltage data from MessagePack message."""
        try:
            # Find double values in the message (0xcb prefix)
            doubles = []
            pos = 0
            while pos < len(data):
                if data[pos] == 0xCB:  # float64 marker
                    val = struct.unpack(">d", data[pos + 1 : pos + 9])[0]
                    doubles.append(val)
                    pos += 9
                else:
                    pos += 1

            if len(doubles) >= 4:
                voltage = doubles[0]
                peaks = doubles[1]
                voltage_hi = doubles[2]
                voltage_lo = doubles[3]

                # Filter out obviously bad readings
                # Only discard zero/near-zero or clearly garbage values
                if abs(voltage) < 1 or abs(voltage) > 1000:
                    _LOGGER.debug(
                        "Discarding anomalous voltage reading: %.2fV", voltage
                    )
                    return None

                # Find timestamp (uint64 with 0xd7 or 0xcf prefix)
                timestamp = datetime.now()  # Default to now
                pos = 0
                while pos < len(data) - 8:
                    if data[pos] == 0xD7:  # ext8 with type -1 (timestamp)
                        pos += 1
                        if data[pos] == 0xFF:  # timestamp type
                            pos += 1
                            ts_val = struct.unpack(">Q", data[pos : pos + 8])[0]
                            # SignalR uses .NET ticks (100ns since 1/1/0001)
                            # Convert to Unix timestamp
                            try:
                                timestamp = datetime.fromtimestamp(ts_val / 10000000 - 62135596800)
                            except (ValueError, OSError):
                                pass
                            break
                        pos += 7
                    else:
                        pos += 1

                return VoltageData(
                    timestamp=timestamp,
                    voltage=voltage,
                    average_peaks_max=peaks,
                    voltage_hi=voltage_hi,
                    voltage_lo=voltage_lo,
                )
        except Exception as err:
            _LOGGER.debug("Error decoding voltage data: %s", err)

        return None

    async def connect(self) -> bool:
        """Connect to the SignalR hub."""
        try:
            _LOGGER.debug("Connecting to SignalR hub: %s", SIGNALR_URL)

            self._ws = await self._session.ws_connect(
                SIGNALR_URL,
                headers={
                    "Origin": "ionic://localhost",
                },
            )

            # Send protocol negotiation
            handshake = '{"protocol":"messagepack","version":1}\x1e'
            await self._ws.send_str(handshake)

            # Wait for handshake response
            msg = await self._ws.receive(timeout=10)
            if msg.type == aiohttp.WSMsgType.BINARY:
                _LOGGER.debug("Received handshake response (binary)")
            elif msg.type == aiohttp.WSMsgType.TEXT:
                _LOGGER.debug("Received handshake response: %s", msg.data)

            # Subscribe to device stream using api_key as the token
            init_args = [
                {"StationId": self._station_id, "DataElement": "ComboBinaryData"},
                self._api_key,  # Use api_key directly as the stream token
                str(self._user_id),
            ]
            init_msg = self._encode_invocation("InitializeStreaming", init_args)
            await self._ws.send_bytes(init_msg)

            self._connected = True
            self._last_data_time = datetime.now()
            self._connected_at = datetime.now()

            # Start background tasks
            self._receive_task = asyncio.create_task(self._receive_loop())
            self._ping_task = asyncio.create_task(self._ping_loop())
            self._stale_check_task = asyncio.create_task(self._stale_data_check_loop())

            _LOGGER.info("Connected to SignalR hub for station %s", self._station_id)
            return True

        except Exception as err:
            _LOGGER.error("Failed to connect to SignalR hub: %s", err)
            self._connected = False
            # Close the half-open socket so it is not leaked
            if self._ws and not self._ws.closed:
                try:
                    await self._ws.close()
                except Exception:  # noqa: BLE001
                    pass
            self._ws = None
            return False

    async def disconnect(self) -> None:
        """Disconnect from the SignalR hub."""
        self._shutting_down = True
        self._connected = False

        for task in [self._ping_task, self._receive_task, self._stale_check_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._ping_task = None
        self._receive_task = None
        self._stale_check_task = None

        if self._ws and not self._ws.closed:
            await self._ws.close()
            self._ws = None

        _LOGGER.debug("Disconnected from SignalR hub")

    async def wait_for_data(self, timeout: float = 5.0) -> bool:
        """Wait for the first voltage data to be received.

        Returns True if data was received, False if timeout.
        """
        try:
            await asyncio.wait_for(self._first_data_received.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            _LOGGER.debug("Timeout waiting for first voltage data")
            return False

    async def _receive_loop(self) -> None:
        """Receive messages from the WebSocket."""
        while self._connected and self._ws and not self._ws.closed:
            try:
                msg = await asyncio.wait_for(
                    self._ws.receive(),
                    timeout=30,
                )

                if msg.type == aiohttp.WSMsgType.BINARY:
                    # Check if it's a voltage update
                    if b"updateComboBinaryData" in msg.data:
                        voltage_data = self._decode_voltage_data(msg.data)
                        if voltage_data and self._on_voltage_update:
                            self._last_data_time = datetime.now()
                            self._on_voltage_update(self._station_id, voltage_data)
                            # Signal that we've received data
                            if not self._first_data_received.is_set():
                                self._first_data_received.set()
                    elif msg.data == b"\x02\x91\x06":  # Ping response
                        _LOGGER.debug("Received ping response")

                elif msg.type == aiohttp.WSMsgType.TEXT:
                    _LOGGER.debug("Received text message: %s", msg.data)

                elif msg.type in (
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR,
                ):
                    _LOGGER.warning("WebSocket closed or error: %s", msg.type)
                    self._connected = False
                    break

            except asyncio.TimeoutError:
                _LOGGER.debug("WebSocket receive timeout, continuing...")
            except asyncio.CancelledError:
                break
            except Exception as err:
                _LOGGER.error("Error in receive loop: %s", err)
                self._connected = False
                break

        # Notify manager that we disconnected (for reconnection)
        if not self._shutting_down and self._on_disconnect:
            # The Ting cloud closes streams on a fixed session lifetime (~530 s). A close after a
            # healthy session is routine: log quietly and let the manager reconnect immediately.
            lived = (datetime.now() - self._connected_at).total_seconds() if getattr(self, "_connected_at", None) else 0
            if lived > 300:
                _LOGGER.info("WebSocket closed by server after %.0f s for station %s, reconnecting", lived, self._station_id)
            else:
                _LOGGER.warning("WebSocket disconnected for station %s after %.0f s, triggering reconnect", self._station_id, lived)
            self._on_disconnect(self._station_id)

    async def _stale_data_check_loop(self) -> None:
        """Check for stale data and trigger reconnect if needed."""
        while self._connected and not self._shutting_down:
            try:
                await asyncio.sleep(self.STALE_DATA_THRESHOLD)

                if not self._connected or self._shutting_down:
                    break

                if self._last_data_time:
                    time_since_update = (datetime.now() - self._last_data_time).total_seconds()
                    if time_since_update > self.STALE_DATA_THRESHOLD:
                        _LOGGER.error(
                            "WebSocket data stale for station %s (no update in %.0f seconds), reconnecting",
                            self._station_id,
                            time_since_update,
                        )
                        self._connected = False
                        # Trigger reconnect via callback
                        if self._on_disconnect:
                            self._on_disconnect(self._station_id)
                        break

            except asyncio.CancelledError:
                break
            except Exception as err:
                _LOGGER.error("Error in stale data check: %s", err)
                break

    async def _ping_loop(self) -> None:
        """Send periodic pings to keep the connection alive."""
        while self._connected and self._ws and not self._ws.closed:
            try:
                await asyncio.sleep(15)  # Ping every 15 seconds
                if self._connected and self._ws and not self._ws.closed:
                    ping_msg = self._encode_ping()
                    await self._ws.send_bytes(ping_msg)
                    _LOGGER.debug("Sent ping")
            except asyncio.CancelledError:
                break
            except Exception as err:
                _LOGGER.error("Error in ping loop: %s", err)
                break


class WhiskerWebSocketManager:
    """Manages WebSocket connections for multiple devices."""

    # Reconnect settings
    RECONNECT_MIN_DELAY = 5
    RECONNECT_MAX_DELAY = 300  # 5 minutes max
    RECONNECT_BACKOFF_FACTOR = 2

    def __init__(
        self,
        session: aiohttp.ClientSession,
        on_voltage_update: Callable[[str, VoltageData], None] | None = None,
    ) -> None:
        """Initialize the manager."""
        self._session = session
        self._on_voltage_update = on_voltage_update
        self._connections: dict[str, WhiskerWebSocket] = {}
        self._voltage_data: dict[str, VoltageData] = {}
        self._credentials: dict[str, dict] = {}  # Store credentials for reconnect
        self._reconnect_tasks: dict[str, asyncio.Task] = {}
        self._reconnect_attempts: dict[str, int] = {}
        self._shutting_down = False

    def get_voltage_data(self, station_id: str) -> VoltageData | None:
        """Get the latest voltage data for a station."""
        return self._voltage_data.get(station_id)

    def _handle_voltage_update(self, station_id: str, data: VoltageData) -> None:
        """Handle voltage update from WebSocket."""
        self._voltage_data[station_id] = data
        _LOGGER.debug(
            "Voltage update for %s: %.2fV (hi: %.2fV, lo: %.2fV)",
            station_id,
            data.voltage,
            data.voltage_hi,
            data.voltage_lo,
        )
        if self._on_voltage_update:
            self._on_voltage_update(station_id, data)

    def _handle_disconnect(self, station_id: str) -> None:
        """Handle WebSocket disconnect - schedule reconnection."""
        if self._shutting_down:
            return

        # Close the old connection instead of just dropping it (leaked sockets eventually exhaust file descriptors: EMFILE)
        old = self._connections.pop(station_id, None)
        if old is not None:
            connected_at = getattr(old, "_connected_at", None)
            if connected_at and (datetime.now() - connected_at).total_seconds() > 300:
                self._reconnect_attempts[station_id] = 0
            old._on_disconnect = None  # stop its receive loop from triggering a second reconnect
            asyncio.create_task(old.disconnect())

        # Schedule reconnection
        if station_id not in self._reconnect_tasks or self._reconnect_tasks[station_id].done():
            self._reconnect_tasks[station_id] = asyncio.create_task(
                self._reconnect_with_backoff(station_id)
            )

    async def _reconnect_with_backoff(self, station_id: str) -> None:
        """Reconnect to a station with exponential backoff."""
        if station_id not in self._credentials:
            _LOGGER.error("No credentials stored for station %s, cannot reconnect", station_id)
            return

        creds = self._credentials[station_id]
        attempts = self._reconnect_attempts.get(station_id, 0)

        # Calculate delay with exponential backoff. First attempt after a healthy session is
        # near-immediate (the server's routine ~530 s close), so the voltage stream barely gaps.
        if attempts == 0:
            delay = 1
        else:
            delay = min(
                self.RECONNECT_MIN_DELAY * (self.RECONNECT_BACKOFF_FACTOR ** (attempts - 1)),
                self.RECONNECT_MAX_DELAY,
            )

        _LOGGER.debug(
            "Reconnecting to station %s in %.0f seconds (attempt %d)",
            station_id,
            delay,
            attempts + 1,
        )

        await asyncio.sleep(delay)

        if self._shutting_down:
            return

        self._reconnect_attempts[station_id] = attempts + 1

        # Create new connection
        ws = WhiskerWebSocket(
            session=self._session,
            api_key=creds["api_key"],
            user_id=creds["user_id"],
            station_id=station_id,
            on_voltage_update=self._handle_voltage_update,
            on_disconnect=self._handle_disconnect,
        )

        if await ws.connect():
            self._connections[station_id] = ws
            _LOGGER.debug("Reconnected to station %s", station_id)
        else:
            _LOGGER.warning("Reconnection failed for station %s, will retry", station_id)
            # Schedule another reconnect attempt
            if not self._shutting_down:
                self._reconnect_tasks[station_id] = asyncio.create_task(
                    self._reconnect_with_backoff(station_id)
                )

    async def connect_device(
        self,
        api_key: str,
        user_id: int,
        station_id: str,
    ) -> bool:
        """Connect to a device's WebSocket stream."""
        if station_id in self._connections:
            _LOGGER.debug("Already connected to station %s", station_id)
            return True

        # Store credentials for reconnection
        self._credentials[station_id] = {
            "api_key": api_key,
            "user_id": user_id,
        }
        self._reconnect_attempts[station_id] = 0

        ws = WhiskerWebSocket(
            session=self._session,
            api_key=api_key,
            user_id=user_id,
            station_id=station_id,
            on_voltage_update=self._handle_voltage_update,
            on_disconnect=self._handle_disconnect,
        )

        if await ws.connect():
            self._connections[station_id] = ws
            return True
        return False

    async def disconnect_all(self) -> None:
        """Disconnect all WebSocket connections."""
        self._shutting_down = True

        # Cancel any pending reconnect tasks
        for task in self._reconnect_tasks.values():
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._reconnect_tasks.clear()

        # Disconnect all connections
        for station_id, ws in list(self._connections.items()):
            await ws.disconnect()
            del self._connections[station_id]

    async def disconnect_device(self, station_id: str) -> None:
        """Disconnect a specific device."""
        # Cancel any pending reconnect
        if station_id in self._reconnect_tasks:
            task = self._reconnect_tasks[station_id]
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            del self._reconnect_tasks[station_id]

        if station_id in self._connections:
            await self._connections[station_id].disconnect()
            del self._connections[station_id]

    async def wait_for_data(self, station_id: str, timeout: float = 5.0) -> bool:
        """Wait for first voltage data from a specific station.

        Returns True if data was received, False if timeout or not connected.
        """
        ws = self._connections.get(station_id)
        if ws:
            return await ws.wait_for_data(timeout=timeout)
        return False
