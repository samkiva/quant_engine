# Binance API Notes

## WebSocket Endpoints — Spot Testnet
- Base URL: wss://stream.testnet.binance.vision/ws
- Raw stream: /ws/<streamName>  (e.g. /ws/btcusdt@trade)
- Combined: /stream?streams=<name1>/<name2>
- All symbol names must be lowercase
- 24h connection limit — serverShutdown event fires 10min before

## WebSocket Behavior
- Server pings every 20s; must pong within 60s
- websockets library handles ping/pong automatically
- Reconnect required after server-initiated close

## Rate Limits
- Max 5 incoming messages per second per connection

## Common Errors
- HTTP 404: wrong hostname — must be stream.testnet.binance.vision

## Trade Stream Schema (as of 2026-05)
Fields present: e, E, s, t, p, q, T, m, M
Fields removed: b (buyer_order_id), a (seller_order_id)
Note: Use .get() for any field not guaranteed by docs. Never assume schema stability.
