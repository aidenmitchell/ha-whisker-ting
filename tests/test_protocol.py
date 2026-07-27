"""Tests for the Ting SignalR MessagePack protocol helpers."""

from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path

import msgpack
import pytest

PROTOCOL_PATH = (
    Path(__file__).parents[1] / "custom_components" / "whisker_ting" / "protocol.py"
)
SPEC = importlib.util.spec_from_file_location("whisker_ting_protocol", PROTOCOL_PATH)
assert SPEC and SPEC.loader
protocol = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = protocol
SPEC.loader.exec_module(protocol)


def test_encode_ping_matches_signalr_specification() -> None:
    """Ping is a two-byte MessagePack body with a one-byte length prefix."""
    assert protocol.encode_ping() == b"\x02\x91\x06"


def test_encode_invocation_has_length_prefix_and_stream_ids() -> None:
    """Invocation uses a VarInt prefix and the required six-field array."""
    arguments = [
        {"StationId": "123456789", "DataElement": "ComboBinaryData"},
        "stream-token",
        "123456",
    ]

    packet = protocol.encode_invocation(
        "1",
        "InitializeStreaming",
        arguments,
    )
    frames = list(protocol.iter_binary_frames(packet))

    assert len(frames) == 1
    assert msgpack.unpackb(frames[0], raw=False) == [
        1,
        {},
        "1",
        "InitializeStreaming",
        arguments,
        [],
    ]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, b"\x00"),
        (127, b"\x7f"),
        (128, b"\x80\x01"),
        (135, b"\x87\x01"),
        (16384, b"\x80\x80\x01"),
    ],
)
def test_encode_varint(value: int, expected: bytes) -> None:
    """Lengths use SignalR's little-endian 7-bit VarInt encoding."""
    assert protocol.encode_varint(value) == expected


def test_decode_multiple_messages_from_one_transport_payload() -> None:
    """A WebSocket payload may contain more than one SignalR message."""
    payload = protocol.encode_ping() + protocol.frame_messagepack([3, {}, "1", 3, None])

    assert protocol.decode_hub_messages(payload) == [
        [6],
        [3, {}, "1", 3, None],
    ]


def test_incomplete_frame_is_rejected() -> None:
    """Declared frame lengths cannot exceed the available payload."""
    with pytest.raises(protocol.SignalRProtocolError, match="incomplete"):
        protocol.decode_hub_messages(b"\x05\x91\x06")


def test_decode_live_voltage_invocation() -> None:
    """Named fields, rather than raw float ordering, determine voltage values."""
    timestamp = datetime(2026, 7, 27, 20, 30, tzinfo=timezone.utc)
    message = [
        1,
        {},
        None,
        "updateComboBinaryData",
        [
            {
                "DataTimeUtc": timestamp,
                "VoltageLo": 114.91,
                "AveragePeaksMax": 8,
                "Voltage": 120.25,
                "VoltageHi": 124.66,
            }
        ],
        [],
    ]

    update = protocol.decode_voltage_update(message)

    assert update == protocol.VoltageData(
        timestamp=timestamp,
        voltage=120.25,
        voltage_hi=124.66,
        voltage_lo=114.91,
        average_peaks_max=8.0,
    )


def test_decode_timestamp_extension_from_wire() -> None:
    """MessagePack timestamps from Ting decode into timezone-aware UTC values."""
    timestamp = datetime(2026, 7, 27, 20, 30, tzinfo=timezone.utc)
    message = [
        1,
        {},
        None,
        "updateComboBinaryData",
        [
            {
                "DataTimeUtc": msgpack.Timestamp.from_datetime(timestamp),
                "Voltage": 120.25,
                "VoltageHi": 124.66,
                "VoltageLo": 114.91,
                "AveragePeaksMax": 8,
            }
        ],
        [],
    ]
    wire_data = protocol.frame_messagepack(message)
    decoded_message = protocol.decode_hub_messages(wire_data)[0]

    assert protocol.decode_voltage_update(decoded_message).timestamp == timestamp


def test_invalid_voltage_is_ignored() -> None:
    """Missing, non-finite, or implausible readings never reach Home Assistant."""
    base = [
        1,
        {},
        None,
        "updateComboBinaryData",
        [
            {
                "DataTimeUtc": None,
                "Voltage": 0,
                "VoltageHi": 124.66,
                "VoltageLo": 114.91,
                "AveragePeaksMax": 8,
            }
        ],
        [],
    ]

    assert protocol.decode_voltage_update(base) is None


def test_completion_and_close_errors() -> None:
    """Protocol errors are extracted without depending on raw byte searches."""
    assert protocol.completion_error([3, {}, "1", 1, "bad invocation"]) == (
        "bad invocation"
    )
    assert protocol.completion_error([3, {}, "1", 3, None]) is None
    assert protocol.close_error([7, "connection failed", True]) == ("connection failed")
