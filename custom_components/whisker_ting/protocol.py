"""Minimal ASP.NET Core SignalR MessagePack protocol support for Ting."""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import msgpack

MSG_TYPE_INVOCATION = 1
MSG_TYPE_STREAM_ITEM = 2
MSG_TYPE_COMPLETION = 3
MSG_TYPE_STREAM_INVOCATION = 4
MSG_TYPE_CANCEL_INVOCATION = 5
MSG_TYPE_PING = 6
MSG_TYPE_CLOSE = 7

MAX_LENGTH_PREFIX_BYTES = 5
MAX_MESSAGE_SIZE = 0x7FFFFFFF


class SignalRProtocolError(ValueError):
    """Raised when a SignalR hub message is malformed or incomplete."""


@dataclass(frozen=True)
class VoltageData:
    """One real-time voltage update from the Ting SignalR hub."""

    timestamp: datetime
    voltage: float
    voltage_hi: float
    voltage_lo: float
    average_peaks_max: float


def encode_varint(value: int) -> bytes:
    """Encode a SignalR binary message length as a 7-bit VarInt."""
    if value < 0 or value > MAX_MESSAGE_SIZE:
        raise SignalRProtocolError(
            f"message length must be between 0 and {MAX_MESSAGE_SIZE}"
        )

    encoded = bytearray()
    while True:
        part = value & 0x7F
        value >>= 7
        if value:
            part |= 0x80
        encoded.append(part)
        if not value:
            return bytes(encoded)


def frame_messagepack(payload: Any) -> bytes:
    """MessagePack-encode and length-prefix one SignalR hub message."""
    body = msgpack.packb(payload, use_bin_type=True)
    return encode_varint(len(body)) + body


def encode_invocation(
    invocation_id: str | None,
    target: str,
    arguments: list[Any],
) -> bytes:
    """Encode a SignalR Invocation message.

    SignalR MessagePack v1 requires six fields, including the trailing stream
    IDs array, even when the invocation does not contain client streams.
    """
    return frame_messagepack(
        [
            MSG_TYPE_INVOCATION,
            {},
            invocation_id,
            target,
            arguments,
            [],
        ]
    )


def encode_ping() -> bytes:
    """Encode a SignalR Ping message."""
    return frame_messagepack([MSG_TYPE_PING])


def iter_binary_frames(data: bytes) -> Iterable[bytes]:
    """Yield MessagePack bodies from one SignalR binary transport payload."""
    offset = 0
    while offset < len(data):
        length = 0
        shift = 0

        for _ in range(MAX_LENGTH_PREFIX_BYTES):
            if offset >= len(data):
                raise SignalRProtocolError("incomplete SignalR length prefix")

            part = data[offset]
            offset += 1
            length |= (part & 0x7F) << shift

            if not part & 0x80:
                break
            shift += 7
        else:
            raise SignalRProtocolError("SignalR length prefix exceeds five bytes")

        if length > MAX_MESSAGE_SIZE:
            raise SignalRProtocolError("SignalR message exceeds 2 GiB")

        end = offset + length
        if end > len(data):
            raise SignalRProtocolError(
                f"incomplete SignalR frame: declared={length}, "
                f"available={len(data) - offset}"
            )

        yield data[offset:end]
        offset = end


def decode_hub_messages(data: bytes) -> list[list[Any]]:
    """Decode all SignalR MessagePack hub messages in a transport payload."""
    messages: list[list[Any]] = []
    for frame in iter_binary_frames(data):
        try:
            message = msgpack.unpackb(
                frame,
                raw=False,
                strict_map_key=False,
                timestamp=3,
            )
        except (ValueError, msgpack.UnpackException) as err:
            raise SignalRProtocolError(f"invalid MessagePack payload: {err}") from err

        if not isinstance(message, list) or not message:
            raise SignalRProtocolError("SignalR hub message must be a non-empty array")
        messages.append(message)

    return messages


def decode_voltage_update(message: list[Any]) -> VoltageData | None:
    """Decode Ting's updateComboBinaryData client invocation."""
    if (
        len(message) < 5
        or message[0] != MSG_TYPE_INVOCATION
        or not isinstance(message[3], str)
        or message[3].casefold() != "updatecombobinarydata"
        or not isinstance(message[4], list)
        or not message[4]
        or not isinstance(message[4][0], dict)
    ):
        return None

    update = message[4][0]
    try:
        voltage = _finite_float(update["Voltage"])
        voltage_hi = _finite_float(update["VoltageHi"])
        voltage_lo = _finite_float(update["VoltageLo"])
        average_peaks_max = _finite_float(update["AveragePeaksMax"])
    except (KeyError, TypeError, ValueError):
        return None

    if abs(voltage) < 1 or abs(voltage) > 1000:
        return None

    return VoltageData(
        timestamp=_decode_timestamp(update.get("DataTimeUtc")),
        voltage=voltage,
        voltage_hi=voltage_hi,
        voltage_lo=voltage_lo,
        average_peaks_max=average_peaks_max,
    )


def completion_error(message: list[Any]) -> str | None:
    """Return the error text from a SignalR Completion message, if present."""
    if len(message) >= 5 and message[0] == MSG_TYPE_COMPLETION and message[3] == 1:
        return str(message[4])
    return None


def close_error(message: list[Any]) -> str | None:
    """Return the error text from a SignalR Close message, if present."""
    if len(message) >= 2 and message[0] == MSG_TYPE_CLOSE and message[1]:
        return str(message[1])
    return None


def _finite_float(value: Any) -> float:
    """Convert a numeric value to a finite float."""
    converted = float(value)
    if not math.isfinite(converted):
        raise ValueError("value is not finite")
    return converted


def _decode_timestamp(value: Any) -> datetime:
    """Normalize MessagePack, ISO-8601, or missing timestamps to UTC."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, msgpack.Timestamp):
        return value.to_datetime().astimezone(timezone.utc)

    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            pass

    return datetime.now(timezone.utc)
