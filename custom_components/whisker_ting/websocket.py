"""WebSocket client for real-time Whisker Ting data."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from time import monotonic

import aiohttp

from .const import SIGNALR_URL
from .protocol import (
    MSG_TYPE_CLOSE,
    MSG_TYPE_COMPLETION,
    MSG_TYPE_PING,
    SignalRProtocolError,
    VoltageData,
    close_error,
    completion_error,
    decode_hub_messages,
    decode_voltage_update,
    encode_invocation,
    encode_ping,
)

_LOGGER = logging.getLogger(__name__)


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
        self._last_data_time: float | None = None
        self._shutting_down = False

    @property
    def connected(self) -> bool:
        """Return True if connected."""
        return self._connected

    def _encode_invocation(self, method: str, args: list) -> bytes:
        """Encode a SignalR invocation message."""
        self._message_id += 1
        return encode_invocation(str(self._message_id), method, args)

    def _encode_ping(self) -> bytes:
        """Encode a SignalR ping message."""
        return encode_ping()

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
            if msg.type not in (
                aiohttp.WSMsgType.BINARY,
                aiohttp.WSMsgType.TEXT,
            ):
                raise SignalRProtocolError(
                    f"unexpected handshake response type: {msg.type.name}"
                )

            handshake_data = (
                msg.data.decode("utf-8") if isinstance(msg.data, bytes) else msg.data
            )
            if not handshake_data.endswith("\x1e"):
                raise SignalRProtocolError("unterminated SignalR handshake response")
            handshake_result = json.loads(handshake_data[:-1])
            if handshake_result.get("error"):
                raise SignalRProtocolError(
                    f"SignalR handshake failed: {handshake_result['error']}"
                )

            # Subscribe to device stream using api_key as the token
            init_args = [
                {"StationId": self._station_id, "DataElement": "ComboBinaryData"},
                self._api_key,  # Use api_key directly as the stream token
                str(self._user_id),
            ]
            init_msg = self._encode_invocation("InitializeStreaming", init_args)
            await self._ws.send_bytes(init_msg)

            self._connected = True
            self._last_data_time = monotonic()

            # Start background tasks
            self._receive_task = asyncio.create_task(self._receive_loop())
            self._ping_task = asyncio.create_task(self._ping_loop())
            self._stale_check_task = asyncio.create_task(self._stale_data_check_loop())

            _LOGGER.info("Connected to SignalR hub for station %s", self._station_id)
            return True

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            json.JSONDecodeError,
            SignalRProtocolError,
            TypeError,
            UnicodeDecodeError,
        ) as err:
            _LOGGER.error("Failed to connect to SignalR hub: %s", err)
            self._connected = False
            return False

    async def disconnect(self) -> None:
        """Disconnect from the SignalR hub."""
        self._shutting_down = True
        self._connected = False

        current_task = asyncio.current_task()
        for task in [self._ping_task, self._receive_task, self._stale_check_task]:
            if task and task is not current_task:
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
                    try:
                        messages = decode_hub_messages(msg.data)
                    except SignalRProtocolError as err:
                        _LOGGER.warning(
                            "Invalid SignalR data for station %s: %s",
                            self._station_id,
                            err,
                        )
                        continue

                    for hub_message in messages:
                        voltage_data = decode_voltage_update(hub_message)
                        if voltage_data:
                            self._last_data_time = monotonic()
                            if self._on_voltage_update:
                                self._on_voltage_update(
                                    self._station_id,
                                    voltage_data,
                                )
                            self._first_data_received.set()
                            continue

                        message_type = hub_message[0]
                        if message_type == MSG_TYPE_PING:
                            _LOGGER.debug("Received SignalR ping")
                        elif message_type == MSG_TYPE_COMPLETION:
                            error = completion_error(hub_message)
                            if error:
                                _LOGGER.error(
                                    "SignalR invocation failed for station %s: %s",
                                    self._station_id,
                                    error,
                                )
                        elif message_type == MSG_TYPE_CLOSE:
                            error = close_error(hub_message)
                            if error:
                                _LOGGER.error(
                                    "SignalR server closed station %s: %s",
                                    self._station_id,
                                    error,
                                )
                            self._connected = False
                            break

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
            except Exception as err:  # noqa: BLE001 - background task boundary
                _LOGGER.error("Error in receive loop: %s", err)
                self._connected = False
                break

        # Notify manager that we disconnected (for reconnection)
        if not self._shutting_down and self._on_disconnect:
            _LOGGER.warning(
                "WebSocket disconnected for station %s, triggering reconnect",
                self._station_id,
            )
            self._on_disconnect(self._station_id)

    async def _stale_data_check_loop(self) -> None:
        """Check for stale data and trigger reconnect if needed."""
        while self._connected and not self._shutting_down:
            try:
                await asyncio.sleep(self.STALE_DATA_THRESHOLD)

                if not self._connected or self._shutting_down:
                    break

                if self._last_data_time:
                    time_since_update = monotonic() - self._last_data_time
                    if time_since_update > self.STALE_DATA_THRESHOLD:
                        _LOGGER.error(
                            "WebSocket data stale for station %s (no update in %.0f seconds), reconnecting",
                            self._station_id,
                            time_since_update,
                        )
                        self._connected = False
                        if self._ws and not self._ws.closed:
                            await self._ws.close()
                        break

            except asyncio.CancelledError:
                break
            except Exception as err:  # noqa: BLE001 - background task boundary
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
            except Exception as err:  # noqa: BLE001 - background task boundary
                _LOGGER.error("Error in ping loop: %s", err)
                break


class WhiskerWebSocketManager:
    """Manages WebSocket connections for multiple devices."""

    # Ting sends roughly four readings per second. Publishing every packet would
    # create hundreds of thousands of Home Assistant recorder rows per day.
    PUBLISH_INTERVAL = 5.0

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
        self._last_publish_time: dict[str, float] = {}
        self._shutting_down = False

    def get_voltage_data(self, station_id: str) -> VoltageData | None:
        """Get the latest voltage data for a station."""
        return self._voltage_data.get(station_id)

    def _handle_voltage_update(self, station_id: str, data: VoltageData) -> None:
        """Handle voltage update from WebSocket."""
        self._voltage_data[station_id] = data
        # Reset reconnect attempts on successful data
        self._reconnect_attempts[station_id] = 0

        now = monotonic()
        last_publish = self._last_publish_time.get(station_id)
        if last_publish is not None and now - last_publish < self.PUBLISH_INTERVAL:
            return
        self._last_publish_time[station_id] = now

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

        # Remove old connection
        if station_id in self._connections:
            del self._connections[station_id]

        # Schedule reconnection
        if (
            station_id not in self._reconnect_tasks
            or self._reconnect_tasks[station_id].done()
        ):
            self._reconnect_tasks[station_id] = asyncio.create_task(
                self._reconnect_with_backoff(station_id)
            )

    async def _reconnect_with_backoff(self, station_id: str) -> None:
        """Reconnect to a station with exponential backoff."""
        if station_id not in self._credentials:
            _LOGGER.error(
                "No credentials stored for station %s, cannot reconnect", station_id
            )
            return

        creds = self._credentials[station_id]
        attempts = self._reconnect_attempts.get(station_id, 0)

        # Calculate delay with exponential backoff
        delay = min(
            self.RECONNECT_MIN_DELAY * (self.RECONNECT_BACKOFF_FACTOR**attempts),
            self.RECONNECT_MAX_DELAY,
        )

        _LOGGER.info(
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
            _LOGGER.info("Reconnected to station %s", station_id)
        else:
            _LOGGER.warning(
                "Reconnection failed for station %s, will retry", station_id
            )
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
