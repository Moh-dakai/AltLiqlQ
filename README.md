# AltLiqIQ

Altcoin perpetual liquidation intelligence for Binance, Bybit, and OKX.

AltLiqIQ is an MCP server that maps liquidation clusters, classifies squeeze risk, and estimates cascade size for altcoin perpetual markets. It excludes BTC and ETH by design and focuses on altcoins where liquidation positioning can change quickly and free raw exchange endpoints are harder to interpret.

## What It Does

- Aggregates perp market data across Binance, Bybit, and OKX
- Detects liquidation clusters from merged order books and recent liquidation activity
- Scores squeeze risk as `LOW`, `MODERATE`, `HIGH`, or `EXTREME`
- Labels likely setup direction such as `LONG_SQUEEZE` or `SHORT_SQUEEZE`
- Estimates liquidation cascade size at a target price
- Compares up to 8 supported assets in one ranked call

## Supported Assets

`SOL`, `DOGE`, `LINK`, `AVAX`, `SUI`, `ARB`, `WIF`, `PEPE`, `INJ`, `TIA`, `JTO`, `PYTH`, `STRK`, `MANTA`, `ALT`, `NEAR`, `FTM`, `MATIC`, `OP`, `ATOM`

`BTC` and `ETH` are intentionally excluded.

## Data Sources

AltLiqIQ uses public exchange APIs only. No exchange API keys are required.

### Binance Futures

- Mark price
- Open interest
- Order book depth
- Forced liquidation history
- Top trader long/short ratio
- Funding rate

### Bybit

- Mark price
- Open interest
- Order book
- Funding rate
- Account long/short ratio
- Recent trades used as a liquidation proxy for large prints

### OKX

- Mark price
- Open interest
- Order book
- Public liquidation orders
- Funding rate
- Contract long/short account ratio

## How It Works

The server fetches venue data concurrently, then normalizes it into a single analysis layer:

- Mark price is merged using the median of available venues
- Open interest is summed across venues
- Long/short positioning is averaged across available readings
- Funding is averaged across venues
- Order books are merged into one cluster-detection surface
- Liquidations are pooled across venues, with OKX liquidation prints weighted more heavily because they come from a direct public liquidation endpoint

The cluster engine combines:

1. Order book wall density
2. Recent liquidation distribution
3. Synthetic OI-aware cluster estimates

## MCP Tools

### `get_altcoin_liq_clusters`

Returns a real-time liquidation cluster map for one altcoin.

Inputs:

- `asset`: altcoin ticker such as `SOL` or `LINK`
- `venues`: optional array of venues from `binance`, `bybit`, `okx`

Key outputs:

- `mark_price`
- `open_interest_usd`
- `funding_rate`
- `long_short_ratio`
- `top_clusters`
- `dominant_side`
- `risk_classification`
- `squeeze_type`
- `nearby_cluster_usd`
- `funding_bias`
- `confidence_score`
- `venues_used`
- `venue_breakdown`
- `analysis_timestamp`

Rate-limit metadata:

- `60` requests per minute
- `1000ms` cooldown
- `8` max concurrency
- Use `compare_altcoin_liq_risk` instead of repeating this tool across many assets

### `compare_altcoin_liq_risk`

Compares liquidation risk across multiple altcoin perp markets and ranks them by severity.

Inputs:

- `assets`: array of up to 8 altcoin tickers

Key outputs:

- `assets_compared`
- `results`
- `highest_risk_asset`
- `analysis_timestamp`

Rate-limit metadata:

- `20` requests per minute
- `3000ms` cooldown
- `2` max concurrency
- Heavy fan-out tool across 3 venues and up to 8 assets

### `estimate_cascade`

Estimates how much USD liquidation could be triggered if price reaches a target.

Inputs:

- `asset`: altcoin ticker
- `target_price`: target price in USD

Key outputs:

- `mark_price`
- `target_price`
- `direction`
- `pct_move_required`
- `clusters_in_path`
- `cascade_size_usd`
- `severity`
- `confidence_score`
- `analysis_timestamp`

Rate-limit metadata:

- `60` requests per minute
- `1000ms` cooldown
- `8` max concurrency
- Reuses cached cluster data for 30 seconds when possible

## Endpoints

- `GET /` service info, supported assets, pricing, and tool list
- `GET /health` health check
- `GET /sse` MCP SSE transport
- `POST /messages` MCP JSON-RPC message endpoint


## Local Development

```bash
pip install -r requirements.txt
cp .env.example .env
# edit .env and set CTX_SECRET
python main.py
```

The server runs on `http://localhost:8000`.

## Deploying


`Procfile`:

```txt
web: uvicorn main:app --host 0.0.0.0 --port $PORT
```

## Example Queries

- "Where are the biggest liquidation clusters on SOL perps right now?"
- "Is DOGE at risk of a long squeeze based on current liquidation positioning?"
- "Compare liquidation risk on SOL, AVAX, LINK, and ARB right now"
- "Estimate the cascade on LINK if price drops 5% from here"
- "Show PEPE liquidation clusters using only Bybit and OKX"
- "Which supported altcoin has the highest squeeze risk right now?"

## Operational Notes

- Cache TTL is `30` seconds
- The server is query-only and does not place trades
- Multi-asset comparison should go through `compare_altcoin_liq_risk` instead of repeated single-asset calls
- Bybit liquidation detection is proxy-based from recent large trades, so confidence can differ from venues with direct liquidation feeds

## Why This Is Useful

Free exchange endpoints provide raw pieces of the picture, but not the normalized answer traders usually want:

- Where are the biggest liquidation pockets?
- Which side is most vulnerable?
- Is this setup likely to squeeze up or down?
- How large could a cascade be if price moves into those clusters?

AltLiqIQ turns fragmented multi-venue perp data into one consistent response surface that an agent or trader can act on quickly.
