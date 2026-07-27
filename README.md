# Whisker Ting Integration for Home Assistant

[![hacs_badge](https://img.shields.io/badge/HACS-Custom-41BDF5.svg)](https://github.com/hacs/integration)
[![GitHub Release](https://img.shields.io/github/release/aidenmitchell/ha-whisker-ting.svg)](https://github.com/aidenmitchell/ha-whisker-ting/releases)

Home Assistant integration for [Whisker Labs Ting](https://www.tingfire.com/) electrical fire safety sensors.

> [!IMPORTANT]
> This is an unofficial, reverse-engineered integration. Keep the official Ting
> app and Whisker Labs Fire Safety Team notifications enabled and treat them as
> the authoritative safety system. Do not rely on Home Assistant as the sole
> source of electrical hazard alerts.

## Features

- **Real-time voltage monitoring** via WebSocket connection
  - Current voltage
  - Voltage high/low
  - Average peaks
  - Latest readings are published to Home Assistant every five seconds to avoid
    excessive recorder growth
- **Fire hazard status** monitoring
  - Electrical Fire Hazard (EFH) detection
  - Utility Fire Hazard (UFH) detection
  - Learning mode status
- **Device diagnostics**
  - Firmware version
  - WiFi/Bluetooth MAC addresses
  - Serial number

## Installation

### HACS (Recommended)

1. Open HACS in Home Assistant
2. Click the three dots in the top right corner
3. Select "Custom repositories"
4. Add `https://github.com/aidenmitchell/ha-whisker-ting` as an Integration
5. Click "Download" on the Whisker Ting integration
6. Restart Home Assistant

### Manual Installation

1. Download the latest release from [GitHub](https://github.com/aidenmitchell/ha-whisker-ting/releases)
2. Extract and copy the `custom_components/whisker_ting` folder to your Home Assistant `config/custom_components/` directory
3. Restart Home Assistant

## Configuration

1. Go to **Settings** → **Devices & Services**
2. Click **Add Integration**
3. Search for "Whisker Ting"
4. Enter your Whisker Labs account credentials (email and password)

## Sensors

| Sensor | Description |
|--------|-------------|
| Current voltage | Real-time voltage reading (WebSocket) |
| Voltage high | Peak high voltage |
| Voltage low | Peak low voltage |
| Hazard status | Overall hazard status (no_hazards, hazard_detected, learning) |
| EFH status | Electrical Fire Hazard status |
| UFH status | Utility Fire Hazard status |

## Binary Sensors

| Sensor | Description |
|--------|-------------|
| Fire hazard | On when any fire hazard is detected |
| Learning mode | On when the device is in learning mode |

## Requirements

- Home Assistant 2024.1.0 or newer
- Whisker Labs Ting device
- Whisker Labs account

## Troubleshooting

### Voltage shows "Unknown" briefly on startup

This is normal - the integration waits for the WebSocket connection to receive its first data packet before displaying values.

If voltage stays unknown for more than 30 seconds, review the Home Assistant
logs for `custom_components.whisker_ting.websocket` and open an issue with the
error text. Never include your password, API key, or authentication tokens.

### Authentication errors

Ensure you're using the same email and password you use in the Whisker Labs mobile app.

## Credits

This integration is not affiliated with or endorsed by Whisker Labs, Inc.

## License

MIT License - see [LICENSE](LICENSE) for details.
