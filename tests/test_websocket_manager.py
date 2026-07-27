"""Tests for WebSocket update publication behavior."""

from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType

PACKAGE_NAME = "whisker_ting_test"
COMPONENT_PATH = Path(__file__).parents[1] / "custom_components" / "whisker_ting"

package = ModuleType(PACKAGE_NAME)
package.__path__ = [str(COMPONENT_PATH)]
sys.modules[PACKAGE_NAME] = package


def load_component_module(name: str):
    """Load one component module without importing Home Assistant."""
    qualified_name = f"{PACKAGE_NAME}.{name}"
    spec = importlib.util.spec_from_file_location(
        qualified_name,
        COMPONENT_PATH / f"{name}.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[qualified_name] = module
    spec.loader.exec_module(module)
    return module


load_component_module("const")
protocol = load_component_module("protocol")
websocket = load_component_module("websocket")


def voltage(value: float) -> protocol.VoltageData:
    """Build a representative voltage update."""
    return protocol.VoltageData(
        timestamp=datetime(2026, 7, 27, tzinfo=timezone.utc),
        voltage=value,
        voltage_hi=125.0,
        voltage_lo=115.0,
        average_peaks_max=8.0,
    )


def test_manager_keeps_latest_sample_but_throttles_publication(
    monkeypatch,
) -> None:
    """Raw samples stay fresh while Home Assistant updates at a safe rate."""
    published = []
    manager = websocket.WhiskerWebSocketManager(
        session=object(),
        on_voltage_update=lambda station_id, data: published.append((station_id, data)),
    )
    times = iter([100.0, 102.0, 106.0])
    monkeypatch.setattr(websocket, "monotonic", times.__next__)

    manager._handle_voltage_update("station", voltage(120.0))
    manager._handle_voltage_update("station", voltage(121.0))
    manager._handle_voltage_update("station", voltage(122.0))

    assert [item.voltage for _, item in published] == [120.0, 122.0]
    assert manager.get_voltage_data("station").voltage == 122.0
