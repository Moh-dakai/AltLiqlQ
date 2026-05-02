# AltLiqIQ — Altcoin Perpetual Liquidation Intelligence

Real-time liquidation cluster maps, squeeze risk classification, and cascade size estimation for altcoin perp markets. BTC/ETH excluded. Built on free public APIs — no API keys required.

## Supported Assets
SOL, DOGE, LINK, AVAX, SUI, ARB, WIF, PEPE, INJ, TIA, JTO, PYTH, STRK, MANTA, ALT, NEAR, FTM, MATIC, OP, ATOM

## Tools

### `get_altcoin_liq_clusters(asset, venues?)`
Returns top liquidation clusters for an altcoin perp:
- `top_clusters[]` — price_level, usd_volume, side, distance_pct
- `dominant_side` — long | short
- `risk_classification` — LOW | MODERATE | HIGH | EXTREME
- `squeeze_type` — LONG_SQUEEZE | SHORT_SQUEEZE | etc
- `funding_rate` — current funding rate %
- `confidence_score` — 0–1

### `compare_altcoin_liq_risk(assets[])`
Side-by-side comparison of up to 8 altcoins ranked by risk severity.

### `estimate_cascade(asset, target_price)`
Estimates total USD liquidated if price reaches target:
- `cascade_size_usd`
- `severity` — LOW | MODERATE | HIGH | EXTREME
- `clusters_in_path`
- `direction` — up | down

## Data Sources
- Binance Futures public API (no key needed)
  - `/fapi/v1/premiumIndex` — mark price
  - `/fapi/v1/openInterest` — OI
  - `/fapi/v1/depth` — order book walls
  - `/fapi/v1/allForceOrders` — liquidation history
  - `/futures/data/topLongShortPositionRatio` — LSR
  - `/fapi/v1/fundingRate` — funding

## Deploy to Railway

1. Push this repo to GitHub
2. Connect repo to Railway
3. Set environment variable: `CTX_SECRET=<your_ctxprotocol_secret>`
4. Railway auto-detects `Procfile` and deploys

## Local Development

```bash
pip install -r requirements.txt
cp .env.example .env
# edit .env with your CTX_SECRET
python main.py
```

Server runs on `http://localhost:8000`

## MCP Endpoints
- `GET /` — service info
- `GET /health` — health check
- `GET /sse` — SSE connection
- `POST /messages` — MCP JSON-RPC (auth enforced on tools/call)

## Must-Win Prompts
1. "Where are the biggest liquidation clusters on SOL perps right now?"
2. "Is DOGE at risk of a long squeeze today based on current liquidation positioning?"
3. "Compare liquidation risk on SOL vs AVAX perps right now"
4. "What altcoins have the most dangerous liquidation clusters right now?"
5. "How large would the cascade be on LINK perps if price drops 5% from here?"
