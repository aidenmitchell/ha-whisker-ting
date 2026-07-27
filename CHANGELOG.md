# Changelog

## 1.1.0

- Repair ASP.NET Core SignalR MessagePack framing for Ting's live-voltage
  stream.
- Decode framed hub messages and named voltage fields instead of scanning raw
  bytes for floating-point markers.
- Validate the SignalR handshake and surface invocation and close errors.
- Send standards-compliant SignalR ping messages.
- Publish the latest voltage reading to Home Assistant every five seconds to
  limit recorder growth while retaining the live WebSocket connection.
- Add protocol, malformed-message, timestamp, and publication-throttling tests.
