import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
import uvicorn
from ctxprotocol import ContextError, is_protected_mcp_method, verify_context_request
from dotenv import load_dotenv
from mcp import Tool
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

load_dotenv()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BINANCE_FUTURES_BASE = "https://fapi.binance.com"
BYBIT_BASE = "https://api.bybit.com"
OKX_BASE = "https://www.okx.com"

ALL_VENUES = ["binance", "bybit", "okx"]

SUPPORTED_ASSETS = [
    "SOL", "DOGE", "LINK", "AVAX", "SUI", "ARB", "WIF", "PEPE",
    "INJ", "TIA", "JTO", "PYTH", "STRK", "MANTA", "ALT",
    "NEAR", "FTM", "MATIC", "OP", "ATOM",
]

# Cluster detection: group liquidation levels within this % of each other
CLUSTER_MERGE_PCT = 0.005  # 0.5%

# Cascade multiplier assumptions: each cluster liquidated triggers X% more liq pressure
CASCADE_MULTIPLIER = 1.35

CACHE_TTL = 30  # seconds — live data, keep fresh
_CACHE: dict[str, tuple[float, Any]] = {}


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def cache_get(key: str) -> Any | None:
    if key in _CACHE:
        ts, val = _CACHE[key]
        if time.time() - ts < CACHE_TTL:
            return val
    return None


def cache_set(key: str, val: Any) -> None:
    _CACHE[key] = (time.time(), val)


# ---------------------------------------------------------------------------
# Binance API helpers
# ---------------------------------------------------------------------------

async def binance_get(path: str, params: dict = {}) -> Any:
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(f"{BINANCE_FUTURES_BASE}{path}", params=params)
        r.raise_for_status()
        return r.json()


async def fetch_mark_price(symbol: str) -> float:
    """Get current mark price for a perp symbol."""
    cached = cache_get(f"mark_{symbol}")
    if cached:
        return cached
    data = await binance_get("/fapi/v1/premiumIndex", {"symbol": symbol})
    price = float(data["markPrice"])
    cache_set(f"mark_{symbol}", price)
    return price


async def fetch_open_interest(symbol: str) -> float:
    """Get current open interest in USD."""
    cached = cache_get(f"oi_{symbol}")
    if cached:
        return cached
    data = await binance_get("/fapi/v1/openInterest", {"symbol": symbol})
    oi = float(data["openInterest"])
    price = await fetch_mark_price(symbol)
    oi_usd = oi * price
    cache_set(f"oi_{symbol}", oi_usd)
    return oi_usd


async def fetch_long_short_ratio(symbol: str) -> Dict[str, float]:
    """Get top trader long/short ratio."""
    cached = cache_get(f"lsr_{symbol}")
    if cached:
        return cached
    data = await binance_get(
        "/futures/data/topLongShortPositionRatio",
        {"symbol": symbol, "period": "5m", "limit": 1}
    )
    if data and isinstance(data, list):
        ratio = {
            "long_pct": float(data[0]["longAccount"]),
            "short_pct": float(data[0]["shortAccount"]),
            "ratio": float(data[0]["longShortRatio"]),
        }
    else:
        ratio = {"long_pct": 0.5, "short_pct": 0.5, "ratio": 1.0}
    cache_set(f"lsr_{symbol}", ratio)
    return ratio


async def fetch_funding_rate(symbol: str) -> float:
    """Get latest funding rate."""
    cached = cache_get(f"fr_{symbol}")
    if cached:
        return cached
    data = await binance_get(
        "/fapi/v1/fundingRate",
        {"symbol": symbol, "limit": 1}
    )
    rate = float(data[0]["fundingRate"]) if data else 0.0
    cache_set(f"fr_{symbol}", rate)
    return rate


async def fetch_orderbook_depth(symbol: str, limit: int = 500) -> Dict[str, Any]:
    """Fetch order book to infer liquidation cluster zones from wall density."""
    cached = cache_get(f"ob_{symbol}")
    if cached:
        return cached
    data = await binance_get("/fapi/v1/depth", {"symbol": symbol, "limit": limit})
    cache_set(f"ob_{symbol}", data)
    return data


async def fetch_recent_liquidations(symbol: str) -> List[Dict]:
    """Fetch recent forced liquidation orders from Binance."""
    cached = cache_get(f"liq_{symbol}")
    if cached:
        return cached
    data = await binance_get("/fapi/v1/allForceOrders", {"symbol": symbol, "limit": 100})
    cache_set(f"liq_{symbol}", data)
    return data if isinstance(data, list) else []


async def fetch_klines(symbol: str, interval: str = "1h", limit: int = 48) -> List[List]:
    """Fetch recent klines for volatility calculation."""
    cached = cache_get(f"klines_{symbol}_{interval}_{limit}")
    if cached:
        return cached
    data = await binance_get("/fapi/v1/klines", {
        "symbol": symbol, "interval": interval, "limit": limit
    })
    cache_set(f"klines_{symbol}_{interval}_{limit}", data)
    return data if isinstance(data, list) else []


# ---------------------------------------------------------------------------
# Bybit API helpers
# ---------------------------------------------------------------------------

async def bybit_get(path: str, params: dict = {}) -> Any:
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(f"{BYBIT_BASE}{path}", params=params)
        r.raise_for_status()
        return r.json()


def asset_to_bybit_symbol(asset: str) -> str:
    return f"{asset.upper()}USDT"


async def bybit_fetch_mark_price(asset: str) -> float | None:
    """Bybit linear perp mark price."""
    symbol = asset_to_bybit_symbol(asset)
    cached = cache_get(f"bybit_mark_{symbol}")
    if cached is not None:
        return cached
    try:
        data = await bybit_get("/v5/market/tickers", {"category": "linear", "symbol": symbol})
        result = data.get("result", {}).get("list", [])
        if result:
            price = float(result[0]["markPrice"])
            cache_set(f"bybit_mark_{symbol}", price)
            return price
    except Exception:
        pass
    return None


async def bybit_fetch_open_interest(asset: str) -> float | None:
    """Bybit OI in USD."""
    symbol = asset_to_bybit_symbol(asset)
    cached = cache_get(f"bybit_oi_{symbol}")
    if cached is not None:
        return cached
    try:
        data = await bybit_get("/v5/market/open-interest", {
            "category": "linear", "symbol": symbol, "intervalTime": "5min", "limit": 1
        })
        result = data.get("result", {}).get("list", [])
        if result:
            oi = float(result[0]["openInterest"])
            mark = await bybit_fetch_mark_price(asset)
            oi_usd = oi * mark if mark else 0.0
            cache_set(f"bybit_oi_{symbol}", oi_usd)
            return oi_usd
    except Exception:
        pass
    return None


async def bybit_fetch_orderbook(asset: str, limit: int = 500) -> Dict[str, Any]:
    """Bybit linear perp order book."""
    symbol = asset_to_bybit_symbol(asset)
    cached = cache_get(f"bybit_ob_{symbol}")
    if cached is not None:
        return cached
    try:
        data = await bybit_get("/v5/market/orderbook", {
            "category": "linear", "symbol": symbol, "limit": limit
        })
        result = data.get("result", {})
        # Bybit returns {"b": [[price, qty]...], "a": [[price, qty]...]}
        ob = {
            "bids": result.get("b", []),
            "asks": result.get("a", []),
        }
        cache_set(f"bybit_ob_{symbol}", ob)
        return ob
    except Exception:
        return {"bids": [], "asks": []}


async def bybit_fetch_liquidations(asset: str) -> List[Dict]:
    """
    Bybit recent liquidations via /v5/market/recent-trade filtered to liquidation-like large trades.
    Bybit doesn't expose a direct liq endpoint publicly, so we use insurance fund data
    and large trade feed as a proxy.
    """
    symbol = asset_to_bybit_symbol(asset)
    cached = cache_get(f"bybit_liq_{symbol}")
    if cached is not None:
        return cached
    try:
        # Use recent trades with large size as liquidation proxy
        data = await bybit_get("/v5/market/recent-trade", {
            "category": "linear", "symbol": symbol, "limit": 200
        })
        trades = data.get("result", {}).get("list", [])
        # Filter to large trades only (likely liquidations)
        liqs = []
        for t in trades:
            qty = float(t.get("size", 0))
            price = float(t.get("price", 0))
            usd = qty * price
            if usd >= 10_000:  # $10k+ trades as liquidation proxy
                liqs.append({
                    "price": t["price"],
                    "origQty": t["size"],
                    "side": "SELL" if t.get("side") == "Sell" else "BUY",
                    "venue": "bybit",
                })
        cache_set(f"bybit_liq_{symbol}", liqs)
        return liqs
    except Exception:
        return []


async def bybit_fetch_funding_rate(asset: str) -> float | None:
    """Bybit current funding rate."""
    symbol = asset_to_bybit_symbol(asset)
    cached = cache_get(f"bybit_fr_{symbol}")
    if cached is not None:
        return cached
    try:
        data = await bybit_get("/v5/market/tickers", {"category": "linear", "symbol": symbol})
        result = data.get("result", {}).get("list", [])
        if result:
            rate = float(result[0].get("fundingRate", 0))
            cache_set(f"bybit_fr_{symbol}", rate)
            return rate
    except Exception:
        pass
    return None


async def bybit_fetch_long_short_ratio(asset: str) -> Dict[str, float] | None:
    """Bybit long/short ratio."""
    symbol = asset_to_bybit_symbol(asset)
    cached = cache_get(f"bybit_lsr_{symbol}")
    if cached is not None:
        return cached
    try:
        data = await bybit_get("/v5/market/account-ratio", {
            "category": "linear", "symbol": symbol, "period": "5min", "limit": 1
        })
        result = data.get("result", {}).get("list", [])
        if result:
            buy_ratio = float(result[0].get("buyRatio", 0.5))
            sell_ratio = float(result[0].get("sellRatio", 0.5))
            ratio = {
                "long_pct": buy_ratio,
                "short_pct": sell_ratio,
                "ratio": buy_ratio / sell_ratio if sell_ratio > 0 else 1.0,
            }
            cache_set(f"bybit_lsr_{symbol}", ratio)
            return ratio
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# OKX API helpers
# ---------------------------------------------------------------------------

async def okx_get(path: str, params: dict = {}) -> Any:
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(f"{OKX_BASE}{path}", params=params)
        r.raise_for_status()
        return r.json()


def asset_to_okx_symbol(asset: str) -> str:
    return f"{asset.upper()}-USDT-SWAP"


async def okx_fetch_mark_price(asset: str) -> float | None:
    """OKX perp mark price."""
    inst_id = asset_to_okx_symbol(asset)
    cached = cache_get(f"okx_mark_{inst_id}")
    if cached is not None:
        return cached
    try:
        data = await okx_get("/api/v5/public/mark-price", {"instType": "SWAP", "instId": inst_id})
        result = data.get("data", [])
        if result:
            price = float(result[0]["markPx"])
            cache_set(f"okx_mark_{inst_id}", price)
            return price
    except Exception:
        pass
    return None


async def okx_fetch_open_interest(asset: str) -> float | None:
    """OKX OI in USD."""
    inst_id = asset_to_okx_symbol(asset)
    cached = cache_get(f"okx_oi_{inst_id}")
    if cached is not None:
        return cached
    try:
        data = await okx_get("/api/v5/public/open-interest", {"instType": "SWAP", "instId": inst_id})
        result = data.get("data", [])
        if result:
            oi_contracts = float(result[0].get("oiCcy", result[0].get("oi", 0)))
            mark = await okx_fetch_mark_price(asset)
            oi_usd = oi_contracts * mark if mark else 0.0
            cache_set(f"okx_oi_{inst_id}", oi_usd)
            return oi_usd
    except Exception:
        pass
    return None


async def okx_fetch_orderbook(asset: str, depth: int = 400) -> Dict[str, Any]:
    """OKX perp order book."""
    inst_id = asset_to_okx_symbol(asset)
    cached = cache_get(f"okx_ob_{inst_id}")
    if cached is not None:
        return cached
    try:
        data = await okx_get("/api/v5/market/books", {"instId": inst_id, "sz": depth})
        result = data.get("data", [{}])[0]
        # OKX returns [[price, qty, liquidated_orders, order_count], ...]
        ob = {
            "bids": [[lvl[0], lvl[1]] for lvl in result.get("bids", [])],
            "asks": [[lvl[0], lvl[1]] for lvl in result.get("asks", [])],
        }
        cache_set(f"okx_ob_{inst_id}", ob)
        return ob
    except Exception:
        return {"bids": [], "asks": []}


async def okx_fetch_liquidations(asset: str) -> List[Dict]:
    """OKX recent liquidation orders — OKX has a real public liq endpoint."""
    inst_id = asset_to_okx_symbol(asset)
    cached = cache_get(f"okx_liq_{inst_id}")
    if cached is not None:
        return cached
    try:
        # OKX public liquidation endpoint.
        # NOTE: instId is documented as "only applicable to MARGIN" for this
        # endpoint. For SWAP/FUTURES/OPTION, OKX filters on instFamily (or uly)
        # instead — passing instId here was silently matching nothing for
        # SWAP requests, which is why every altcoin returned liq_count=0
        # regardless of real market activity.
        inst_family = inst_id.rsplit("-", 1)[0] if inst_id.endswith("-SWAP") else inst_id
        data = await okx_get("/api/v5/public/liquidation-orders", {
            "instType": "SWAP",
            "instFamily": inst_family,
            "state": "filled",
            "limit": "100",
        })
        raw = data.get("data", [])
        liqs = []
        for item in raw:
            for detail in item.get("details", []):
                try:
                    liqs.append({
                        "price": detail.get("bkPx", detail.get("px", "0")),
                        "origQty": detail.get("sz", "0"),
                        "side": "SELL" if detail.get("side") == "sell" else "BUY",
                        "venue": "okx",
                    })
                except Exception:
                    continue
        cache_set(f"okx_liq_{inst_id}", liqs)
        return liqs
    except Exception:
        return []


async def okx_fetch_funding_rate(asset: str) -> float | None:
    """OKX current funding rate."""
    inst_id = asset_to_okx_symbol(asset)
    cached = cache_get(f"okx_fr_{inst_id}")
    if cached is not None:
        return cached
    try:
        data = await okx_get("/api/v5/public/funding-rate", {"instId": inst_id})
        result = data.get("data", [])
        if result:
            rate = float(result[0].get("fundingRate", 0))
            cache_set(f"okx_fr_{inst_id}", rate)
            return rate
    except Exception:
        pass
    return None


async def okx_fetch_long_short_ratio(asset: str) -> Dict[str, float] | None:
    """OKX long/short ratio."""
    inst_id = asset_to_okx_symbol(asset)
    cached = cache_get(f"okx_lsr_{inst_id}")
    if cached is not None:
        return cached
    try:
        data = await okx_get("/api/v5/rubik/stat/contracts/long-short-account-ratio-contract", {
            "ccy": asset.upper(), "period": "5m"
        })
        result = data.get("data", [])
        if result:
            # Returns [[timestamp, long_ratio, short_ratio], ...]
            latest = result[-1]
            long_r = float(latest[1])
            short_r = float(latest[2])
            ratio = {
                "long_pct": long_r,
                "short_pct": short_r,
                "ratio": long_r / short_r if short_r > 0 else 1.0,
            }
            cache_set(f"okx_lsr_{inst_id}", ratio)
            return ratio
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Multi-venue aggregation
# ---------------------------------------------------------------------------

async def fetch_all_venues(
    asset: str,
    venues: List[str],
) -> Dict[str, Any]:
    """
    Fetch data from all requested venues concurrently.
    Returns merged/aggregated data with per-venue breakdown.
    """
    tasks = {}

    if "binance" in venues:
        symbol = asset_to_symbol(asset)
        tasks["binance"] = {
            "mark_price":    fetch_mark_price(symbol),
            "oi_usd":        fetch_open_interest(symbol),
            "long_short":    fetch_long_short_ratio(symbol),
            "funding_rate":  fetch_funding_rate(symbol),
            "orderbook":     fetch_orderbook_depth(symbol),
            "liquidations":  fetch_recent_liquidations(symbol),
        }

    if "bybit" in venues:
        tasks["bybit"] = {
            "mark_price":   bybit_fetch_mark_price(asset),
            "oi_usd":       bybit_fetch_open_interest(asset),
            "long_short":   bybit_fetch_long_short_ratio(asset),
            "funding_rate": bybit_fetch_funding_rate(asset),
            "orderbook":    bybit_fetch_orderbook(asset),
            "liquidations": bybit_fetch_liquidations(asset),
        }

    if "okx" in venues:
        tasks["okx"] = {
            "mark_price":   okx_fetch_mark_price(asset),
            "oi_usd":       okx_fetch_open_interest(asset),
            "long_short":   okx_fetch_long_short_ratio(asset),
            "funding_rate": okx_fetch_funding_rate(asset),
            "orderbook":    okx_fetch_orderbook(asset),
            "liquidations": okx_fetch_liquidations(asset),
        }

    # Fire all coroutines concurrently
    venue_results: Dict[str, Dict[str, Any]] = {}
    for venue, coros in tasks.items():
        keys = list(coros.keys())
        coro_list = list(coros.values())
        raw = await asyncio.gather(*coro_list, return_exceptions=True)
        venue_results[venue] = {
            k: (None if isinstance(v, Exception) else v)
            for k, v in zip(keys, raw)
        }

    # --- Merge mark price: take median of successful venues ---
    mark_prices = [
        v["mark_price"] for v in venue_results.values()
        if v.get("mark_price") is not None
    ]
    if not mark_prices:
        raise ValueError(f"No mark price available for {asset} from any venue")
    mark_price = sorted(mark_prices)[len(mark_prices) // 2]  # median

    # --- Merge OI: sum across venues ---
    total_oi_usd = sum(
        v["oi_usd"] for v in venue_results.values()
        if v.get("oi_usd") is not None
    ) or 0.0

    # --- Merge long/short: weighted average by OI ---
    ls_readings = [
        v["long_short"] for v in venue_results.values()
        if v.get("long_short") is not None
    ]
    if ls_readings:
        avg_long = sum(r["long_pct"] for r in ls_readings) / len(ls_readings)
        avg_short = sum(r["short_pct"] for r in ls_readings) / len(ls_readings)
        merged_ls = {
            "long_pct": round(avg_long, 4),
            "short_pct": round(avg_short, 4),
            "ratio": round(avg_long / avg_short, 3) if avg_short > 0 else 1.0,
            "venues": len(ls_readings),
        }
    else:
        merged_ls = {"long_pct": 0.5, "short_pct": 0.5, "ratio": 1.0, "venues": 0}

    # --- Merge funding rate: average across venues ---
    funding_rates = [
        v["funding_rate"] for v in venue_results.values()
        if v.get("funding_rate") is not None
    ]
    merged_funding = sum(funding_rates) / len(funding_rates) if funding_rates else 0.0

    # --- Merge orderbooks: tag each level with venue ---
    merged_orderbook: Dict[str, List] = {"bids": [], "asks": []}
    for venue, vdata in venue_results.items():
        ob = vdata.get("orderbook") or {}
        for side in ("bids", "asks"):
            for level in ob.get(side, []):
                merged_orderbook[side].append(level)

    # --- Merge liquidations: pool all venues ---
    merged_liqs: List[Dict] = []
    for venue, vdata in venue_results.items():
        liqs = vdata.get("liquidations") or []
        for liq in liqs:
            liq.setdefault("venue", venue)
            merged_liqs.append(liq)

    # Build per-venue summary
    venue_summary = {}
    for venue, vdata in venue_results.items():
        venue_summary[venue] = {
            "mark_price": vdata.get("mark_price"),
            "oi_usd": vdata.get("oi_usd"),
            "funding_rate": round(vdata["funding_rate"] * 100, 4) if vdata.get("funding_rate") is not None else None,
            "liq_count": len(vdata.get("liquidations") or []),
            "ob_bids": len((vdata.get("orderbook") or {}).get("bids", [])),
            "ob_asks": len((vdata.get("orderbook") or {}).get("asks", [])),
        }

    return {
        "mark_price": mark_price,
        "total_oi_usd": total_oi_usd,
        "long_short": merged_ls,
        "funding_rate": merged_funding,
        "orderbook": merged_orderbook,
        "liquidations": merged_liqs,
        "venue_summary": venue_summary,
        "venues_used": list(venue_results.keys()),
    }


# ---------------------------------------------------------------------------
# Liquidation cluster engine
# ---------------------------------------------------------------------------

def asset_to_symbol(asset: str) -> str:
    return f"{asset.upper()}USDT"


def compute_liquidation_clusters(
    mark_price: float,
    orderbook: Dict[str, Any],
    recent_liqs: List[Dict],
    oi_usd: float,
    long_short: Dict[str, float],
) -> List[Dict[str, Any]]:
    """
    Build liquidation cluster map from:
    1. Order book wall density (proxy for stop/liq clusters) — all venues merged
    2. Recent forced liquidation price distribution — all venues, OKX weighted higher (real liq data)
    3. OI-weighted distance from mark price

    Returns clusters sorted by USD volume descending.
    """
    price_volumes: Dict[float, Dict] = {}

    def snap(price: float) -> float:
        return round(price / (mark_price * CLUSTER_MERGE_PCT)) * (mark_price * CLUSTER_MERGE_PCT)

    # --- Layer 1: Orderbook walls (all venues merged) ---
    for side, direction in [("bids", "long"), ("asks", "short")]:
        levels = orderbook.get(side, [])
        for level in levels:
            try:
                price = float(level[0])
                qty = float(level[1])
                usd_size = qty * price
                if usd_size < 50_000:
                    continue
                bucket = snap(price)
                if bucket not in price_volumes:
                    price_volumes[bucket] = {"usd_volume": 0.0, "side": direction, "sources": set()}
                price_volumes[bucket]["usd_volume"] += usd_size
                price_volumes[bucket]["sources"].add("orderbook")
            except Exception:
                continue

    # --- Layer 2: Recent liquidation history (multi-venue) ---
    for liq in recent_liqs:
        try:
            price = float(liq.get("price", 0))
            qty = float(liq.get("origQty", 0))
            side = "long" if liq.get("side") == "SELL" else "short"
            usd_size = price * qty
            venue = liq.get("venue", "binance")
            if price == 0 or usd_size < 1_000:
                continue
            # OKX has real liquidation data — weight it higher
            # Bybit large-trade proxy weighted normal
            weight = 4.0 if venue == "okx" else 3.0
            bucket = snap(price)
            if bucket not in price_volumes:
                price_volumes[bucket] = {"usd_volume": 0.0, "side": side, "sources": set()}
            price_volumes[bucket]["usd_volume"] += usd_size * weight
            price_volumes[bucket]["sources"].add(f"liquidation_{venue}")
        except Exception:
            continue

    # --- Layer 3: OI-based synthetic cluster estimate ---
    long_pct = long_short.get("long_pct", 0.5)
    short_pct = long_short.get("short_pct", 0.5)

    for leverage in [5, 10, 20]:  # Model 3 leverage tiers
        long_liq_price = mark_price * (1 - 1 / leverage)
        short_liq_price = mark_price * (1 + 1 / leverage)
        weight = {5: 0.05, 10: 0.15, 20: 0.10}[leverage]  # 20x is thinner

        for est_price, est_side, est_pct in [
            (long_liq_price, "long", long_pct),
            (short_liq_price, "short", short_pct),
        ]:
            bucket = snap(est_price)
            usd_estimate = oi_usd * est_pct * weight
            if bucket not in price_volumes:
                price_volumes[bucket] = {"usd_volume": 0.0, "side": est_side, "sources": set()}
            price_volumes[bucket]["usd_volume"] += usd_estimate
            price_volumes[bucket]["sources"].add("oi_estimate")

    # Build cluster list. Each cluster is tagged is_synthetic=True only when
    # its ENTIRE usd_volume comes from the Layer 3 OI/leverage-tier model
    # with no order book or real liquidation confirmation. This is a hard
    # boolean an agent can branch on directly, not something it has to infer
    # by parsing the sources array itself.
    clusters = []
    for price, data in price_volumes.items():
        if data["usd_volume"] < 100_000:
            continue
        pct_from_mark = (price - mark_price) / mark_price * 100
        sources = list(data["sources"])
        is_synthetic = sources == ["oi_estimate"]
        clusters.append({
            "price_level": round(price, 6),
            "usd_volume": round(data["usd_volume"]),
            "side": data["side"],
            "distance_pct": round(pct_from_mark, 2),
            "sources": sources,
            "is_synthetic": is_synthetic,
        })

    clusters.sort(key=lambda x: x["usd_volume"], reverse=True)
    return clusters[:20]


def classify_risk(
    clusters: List[Dict],
    mark_price: float,
    long_short: Dict[str, float],
    funding_rate: float,
) -> Dict[str, Any]:
    """
    Classify overall squeeze risk based on:
    - Proximity of largest clusters to mark price
    - Long/short ratio imbalance
    - Funding rate direction & magnitude
    """
    if not clusters:
        return {"risk_classification": "UNKNOWN", "dominant_side": "unknown", "reasoning": "No cluster data"}

    # Find closest cluster within 5%
    nearby = [c for c in clusters if abs(c["distance_pct"]) <= 5.0]
    nearby_usd = sum(c["usd_volume"] for c in nearby)

    # Dominant side
    long_vol = sum(c["usd_volume"] for c in clusters if c["side"] == "long")
    short_vol = sum(c["usd_volume"] for c in clusters if c["side"] == "short")
    dominant_side = "long" if long_vol > short_vol else "short"

    # Funding rate signal: positive = longs pay shorts (overextended longs)
    funding_bias = "long_heavy" if funding_rate > 0.0005 else (
        "short_heavy" if funding_rate < -0.0005 else "neutral"
    )

    # Long/short ratio
    ls_ratio = long_short.get("ratio", 1.0)
    ls_bias = "long_crowded" if ls_ratio > 1.5 else ("short_crowded" if ls_ratio < 0.67 else "balanced")

    # Risk score
    risk_score = 0
    if nearby_usd > 5_000_000:
        risk_score += 3
    elif nearby_usd > 1_000_000:
        risk_score += 2
    elif nearby_usd > 250_000:
        risk_score += 1

    if funding_bias == "long_heavy":
        risk_score += 2
    elif funding_bias == "short_heavy":
        risk_score += 1

    if ls_bias == "long_crowded":
        risk_score += 1
    elif ls_bias == "short_crowded":
        risk_score += 1

    # Classify
    if risk_score >= 5:
        risk_level = "EXTREME"
    elif risk_score >= 3:
        risk_level = "HIGH"
    elif risk_score >= 2:
        risk_level = "MODERATE"
    else:
        risk_level = "LOW"

    # Squeeze direction: if longs crowded + large long clusters nearby = long squeeze risk
    if dominant_side == "long" and (funding_bias == "long_heavy" or ls_bias == "long_crowded"):
        squeeze_type = "LONG_SQUEEZE"
    elif dominant_side == "short" and (funding_bias == "short_heavy" or ls_bias == "short_crowded"):
        squeeze_type = "SHORT_SQUEEZE"
    else:
        squeeze_type = f"{dominant_side.upper()}_SIDE_RISK"

    return {
        "risk_classification": risk_level,
        "squeeze_type": squeeze_type,
        "dominant_side": dominant_side,
        "nearby_cluster_usd": nearby_usd,
        "funding_bias": funding_bias,
        "ls_bias": ls_bias,
        "risk_score": risk_score,
    }


def estimate_cascade_size(
    clusters: List[Dict],
    mark_price: float,
    target_price: float,
    oi_usd: float,
) -> Dict[str, Any]:
    """
    Estimate total cascade liquidation USD if price reaches target_price.
    Uses cluster data + cascade multiplier for secondary liquidations.
    """
    direction = "down" if target_price < mark_price else "up"
    pct_move = abs(target_price - mark_price) / mark_price * 100

    # Clusters that would be hit
    if direction == "down":
        hit_clusters = [c for c in clusters if c["price_level"] <= mark_price and c["price_level"] >= target_price]
        hit_clusters += [c for c in clusters if c["side"] == "long" and c["price_level"] >= target_price and c["price_level"] <= mark_price]
    else:
        hit_clusters = [c for c in clusters if c["price_level"] >= mark_price and c["price_level"] <= target_price]
        hit_clusters += [c for c in clusters if c["side"] == "short" and c["price_level"] <= target_price and c["price_level"] >= mark_price]

    # Deduplicate
    seen = set()
    unique_clusters = []
    for c in hit_clusters:
        key = c["price_level"]
        if key not in seen:
            seen.add(key)
            unique_clusters.append(c)

    direct_liq_usd = sum(c["usd_volume"] for c in unique_clusters)

    # OI-based estimate: % of OI that would liquidate given leverage assumptions
    avg_leverage = 10
    oi_hit_pct = min(pct_move / (100 / avg_leverage), 1.0)
    oi_based_estimate = oi_usd * oi_hit_pct * 0.4  # 40% of at-risk OI actually liquidates

    # Take max of cluster-based and OI-based
    base_cascade = max(direct_liq_usd, oi_based_estimate)

    # Apply cascade multiplier (secondary liquidations)
    total_cascade = base_cascade * CASCADE_MULTIPLIER

    # Severity classification
    if total_cascade > 100_000_000:
        severity = "EXTREME"
    elif total_cascade > 25_000_000:
        severity = "HIGH"
    elif total_cascade > 5_000_000:
        severity = "MODERATE"
    else:
        severity = "LOW"

    return {
        "target_price": round(target_price, 6),
        "direction": direction,
        "pct_move_required": round(pct_move, 2),
        "clusters_in_path": len(unique_clusters),
        "direct_cluster_usd": round(direct_liq_usd),
        "cascade_size_usd": round(total_cascade),
        "cascade_multiplier_applied": CASCADE_MULTIPLIER,
        "severity": severity,
        "clusters_hit": unique_clusters[:5],  # top 5 clusters in path
    }


def compute_confidence_score(
    cluster_count: int,
    has_liq_history: bool,
    oi_usd: float,
    sample_liqs: int,
) -> float:
    """Confidence in the cluster map based on data richness."""
    score = 0.0
    score += min(cluster_count / 10, 0.4)         # up to 0.4 from cluster density
    score += 0.2 if has_liq_history else 0.0       # 0.2 for real liq history
    score += min(oi_usd / 500_000_000, 0.2)        # 0.2 for high OI (liquid market)
    score += min(sample_liqs / 50, 0.2)            # 0.2 for liq sample size
    return round(min(score, 1.0), 2)


# ---------------------------------------------------------------------------
# Core tool implementations
# ---------------------------------------------------------------------------

class AltLiqIQServer:

    async def get_altcoin_liq_clusters(
        self,
        asset: str,
        venues: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        asset = asset.upper()
        if asset in ("BTC", "ETH"):
            return {
                "success": False,
                "error": "BTC and ETH are excluded. AltLiqIQ covers altcoin perp markets only.",
            }
        if asset not in SUPPORTED_ASSETS:
            return {
                "success": False,
                "error": f"Asset '{asset}' not supported. Supported: {SUPPORTED_ASSETS}",
            }

        # Default to all venues; caller can restrict
        active_venues = [v.lower() for v in (venues or ALL_VENUES) if v.lower() in ALL_VENUES]
        if not active_venues:
            active_venues = ALL_VENUES

        try:
            agg = await fetch_all_venues(asset, active_venues)
        except Exception as e:
            return {"success": False, "error": f"Data fetch failed: {str(e)}", "asset": asset}

        mark_price  = agg["mark_price"]
        oi_usd      = agg["total_oi_usd"]
        long_short  = agg["long_short"]
        funding_rate = agg["funding_rate"]
        orderbook   = agg["orderbook"]
        liquidations = agg["liquidations"]

        clusters = compute_liquidation_clusters(
            mark_price, orderbook, liquidations, oi_usd, long_short
        )

        # Observed clusters: backed by real order book depth and/or a real
        # venue liquidation print. This is now what drives top_clusters and
        # risk classification — the synthetic OI/leverage-tier layer no
        # longer competes for the same ranking, so it can't crowd out real
        # signal just because the modeled USD size is larger.
        observed = [c for c in clusters if not c["is_synthetic"]]
        estimated = [c for c in clusters if c["is_synthetic"]]

        risk = classify_risk(observed, mark_price, long_short, funding_rate)
        confidence = compute_confidence_score(
            len(observed), len(liquidations) > 0, oi_usd, len(liquidations)
        )

        def _public_fields(c):
            return {
                "price_level": c["price_level"],
                "usd_volume": c["usd_volume"],
                "side": c["side"],
                "distance_pct": c["distance_pct"],
                "sources": c["sources"],
                "is_synthetic": c["is_synthetic"],
            }

        top_clusters = [_public_fields(c) for c in observed[:8]]
        estimated_clusters = [_public_fields(c) for c in estimated[:5]]

        nearby_estimated_usd = sum(
            c["usd_volume"] for c in estimated if abs(c["distance_pct"]) <= 5.0
        )

        return {
            "success": True,
            "asset": asset,
            "mark_price": round(mark_price, 6),
            "open_interest_usd": round(oi_usd),
            "funding_rate": round(funding_rate * 100, 4),
            "long_short_ratio": long_short,
            # top_clusters: real, observed clusters only (order book depth
            # and/or a real venue liquidation print). This is the primary
            # signal and drives risk_classification / squeeze_type below.
            "top_clusters": top_clusters,
            # estimated_clusters: the 5x/10x/20x OI/leverage-tier projection.
            # A useful secondary view, but never blended into top_clusters
            # ranking or risk scoring — see is_synthetic on each cluster.
            "estimated_clusters": estimated_clusters,
            "dominant_side": risk["dominant_side"],
            "risk_classification": risk["risk_classification"],
            "squeeze_type": risk["squeeze_type"],
            "nearby_cluster_usd": risk["nearby_cluster_usd"],
            "nearby_estimated_cluster_usd": round(nearby_estimated_usd),
            "funding_bias": risk["funding_bias"],
            "confidence_score": confidence,
            "venues_used": agg["venues_used"],
            "venue_breakdown": agg["venue_summary"],
            "analysis_timestamp": datetime.utcnow().isoformat() + "Z",
        }

    async def compare_altcoin_liq_risk(
        self,
        assets: List[str],
    ) -> Dict[str, Any]:
        assets = [a.upper() for a in assets if a.upper() not in ("BTC", "ETH")]
        if not assets:
            return {"success": False, "error": "No valid altcoin assets provided (BTC/ETH excluded)."}
        if len(assets) > 8:
            assets = assets[:8]

        results = await asyncio.gather(
            *[self.get_altcoin_liq_clusters(asset) for asset in assets],
            return_exceptions=True
        )

        comparison = []
        for asset, result in zip(assets, results):
            if isinstance(result, Exception):
                comparison.append({"asset": asset, "error": str(result)})
            elif not result.get("success"):
                comparison.append({"asset": asset, "error": result.get("error", "Unknown error")})
            else:
                comparison.append({
                    "asset": asset,
                    "mark_price": result["mark_price"],
                    "risk_classification": result["risk_classification"],
                    "squeeze_type": result["squeeze_type"],
                    "dominant_side": result["dominant_side"],
                    "funding_rate_pct": result["funding_rate"],
                    "open_interest_usd": result["open_interest_usd"],
                    "nearby_cluster_usd": result["nearby_cluster_usd"],
                    "confidence_score": result["confidence_score"],
                    "top_cluster": result["top_clusters"][0] if result["top_clusters"] else None,
                })

        # Sort by risk severity
        risk_order = {"EXTREME": 0, "HIGH": 1, "MODERATE": 2, "LOW": 3, "UNKNOWN": 4}
        valid = [r for r in comparison if "risk_classification" in r]
        errors = [r for r in comparison if "risk_classification" not in r]
        valid.sort(key=lambda x: risk_order.get(x["risk_classification"], 4))

        return {
            "success": True,
            "assets_compared": assets,
            "results": valid + errors,
            "highest_risk_asset": valid[0]["asset"] if valid else None,
            "analysis_timestamp": datetime.utcnow().isoformat() + "Z",
        }

    async def estimate_cascade(
        self,
        asset: str,
        target_price: float,
    ) -> Dict[str, Any]:
        asset = asset.upper()
        if asset in ("BTC", "ETH"):
            return {"success": False, "error": "BTC and ETH are excluded from AltLiqIQ."}

        # Get full cluster data first
        cluster_data = await self.get_altcoin_liq_clusters(asset)
        if not cluster_data.get("success"):
            return {"success": False, "asset": asset, "error": cluster_data.get("error")}

        mark_price = cluster_data["mark_price"]
        oi_usd = cluster_data["open_interest_usd"]
        clusters = cluster_data["top_clusters"]

        cascade = estimate_cascade_size(clusters, mark_price, target_price, oi_usd)

        return {
            "success": True,
            "asset": asset,
            "mark_price": mark_price,
            "open_interest_usd": oi_usd,
            "current_risk": cluster_data["risk_classification"],
            **cascade,
            "confidence_score": cluster_data["confidence_score"],
            "analysis_timestamp": datetime.utcnow().isoformat() + "Z",
        }


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

mcp_server = Server("altliqiq")


@mcp_server.list_tools()
async def handle_list_tools() -> list[Tool]:
    return [
        Tool(
            name="get_altcoin_liq_clusters",
            description=(
                "Get real-time liquidation cluster map for an altcoin perp market. "
                "Returns top_clusters (observed: real order book depth and/or real "
                "venue liquidation prints only) separately from estimated_clusters "
                "(synthetic 5x/10x/20x OI-leverage-tier projections) — check each "
                "cluster's is_synthetic boolean rather than assuming top_clusters is "
                "all real. Also returns dominant_side, risk_classification, "
                "squeeze_type, funding_rate, and confidence_score (risk/squeeze are "
                "derived from observed clusters only). "
                "BTC and ETH are excluded — altcoins only (SOL, DOGE, LINK, AVAX, SUI, ARB, WIF, PEPE, etc)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "asset": {
                        "type": "string",
                        "description": "Altcoin ticker e.g. SOL, DOGE, LINK, AVAX, SUI, ARB, WIF, PEPE",
                    },
                    "venues": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Exchanges to include. Options: 'binance', 'bybit', 'okx'. Default: all three.",
                        "default": ["binance", "bybit", "okx"],
                    },
                },
                "required": ["asset"],
                "_meta": {
                    "rateLimit": {
                        "maxRequestsPerMinute": 60,
                        "cooldownMs": 1000,
                        "maxConcurrency": 8,
                        "supportsBulk": False,
                        "recommendedBatchTools": ["compare_altcoin_liq_risk"],
                        "notes": (
                            "Each call fans out to 3 exchanges concurrently (Binance, Bybit, OKX). "
                            "For multi-asset queries use compare_altcoin_liq_risk — "
                            "batches up to 8 assets in a single call."
                        ),
                    }
                },
            },
            outputSchema={
                "type": "object",
                "properties": {
                    "success": {"type": "boolean"},
                    "asset": {"type": "string"},
                    "mark_price": {"type": "number"},
                    "open_interest_usd": {"type": "number"},
                    "funding_rate": {"type": "number"},
                    "long_short_ratio": {"type": "object"},
                    "top_clusters": {
                        "type": "array",
                        "description": (
                            "Observed clusters only — backed by real order book depth "
                            "and/or a real venue liquidation print. Each item has "
                            "is_synthetic=false. This drives risk_classification and "
                            "squeeze_type below."
                        ),
                    },
                    "estimated_clusters": {
                        "type": "array",
                        "description": (
                            "Synthetic clusters from the 5x/10x/20x OI/leverage-tier "
                            "model only. Each item has is_synthetic=true. A secondary "
                            "view — never blended into top_clusters ranking or risk "
                            "scoring."
                        ),
                    },
                    "dominant_side": {"type": "string"},
                    "risk_classification": {"type": "string"},
                    "squeeze_type": {"type": "string"},
                    "nearby_cluster_usd": {
                        "type": "number",
                        "description": "USD volume within 5% of mark, observed clusters only.",
                    },
                    "nearby_estimated_cluster_usd": {
                        "type": "number",
                        "description": "USD volume within 5% of mark, synthetic clusters only.",
                    },
                    "funding_bias": {"type": "string"},
                    "confidence_score": {"type": "number"},
                    "venues_used": {"type": "array"},
                    "venue_breakdown": {"type": "object"},
                    "analysis_timestamp": {"type": "string"},
                },
            },
        ),
        Tool(
            name="compare_altcoin_liq_risk",
            description=(
                "Compare liquidation risk across multiple altcoin perp markets simultaneously. "
                "Returns ranked list by risk severity with cluster density, squeeze type, "
                "funding rate, and OI for each asset. Max 8 assets per call."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "assets": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of altcoin tickers e.g. ['SOL', 'AVAX', 'LINK']",
                        "examples": [["SOL", "AVAX", "LINK", "DOGE"]],
                    },
                },
                "required": ["assets"],
                "_meta": {
                    "rateLimit": {
                        "maxRequestsPerMinute": 20,
                        "cooldownMs": 3000,
                        "maxConcurrency": 2,
                        "supportsBulk": True,
                        "maxBulkAssets": 8,
                        "notes": (
                            "Heavy fan-out: fetches 3 exchanges x up to 8 assets concurrently. "
                            "Limit to 2 concurrent calls. Prefer over 8x get_altcoin_liq_clusters."
                        ),
                    }
                },
            },
            outputSchema={
                "type": "object",
                "properties": {
                    "success": {"type": "boolean"},
                    "assets_compared": {"type": "array"},
                    "results": {"type": "array"},
                    "highest_risk_asset": {"type": "string"},
                    "analysis_timestamp": {"type": "string"},
                },
            },
        ),
        Tool(
            name="estimate_cascade",
            description=(
                "Estimate the total liquidation cascade size (USD) if an altcoin perp "
                "reaches a specific price target. Returns cascade_size_usd, severity, "
                "clusters_in_path, and direction. Use for 'what if price drops X%' analysis."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "asset": {
                        "type": "string",
                        "description": "Altcoin ticker e.g. LINK, SOL, PEPE",
                    },
                    "target_price": {
                        "type": "number",
                        "description": "Price level to simulate cascade from (in USD)",
                    },
                },
                "required": ["asset", "target_price"],
                "_meta": {
                    "rateLimit": {
                        "maxRequestsPerMinute": 60,
                        "cooldownMs": 1000,
                        "maxConcurrency": 8,
                        "supportsBulk": False,
                        "notes": (
                            "Internally calls get_altcoin_liq_clusters first then runs cascade math. "
                            "Counts as one billable call. Cache TTL 30s — repeated calls for the same "
                            "asset within 30s return cached cluster data."
                        ),
                    }
                },
            },
            outputSchema={
                "type": "object",
                "properties": {
                    "success": {"type": "boolean"},
                    "asset": {"type": "string"},
                    "mark_price": {"type": "number"},
                    "target_price": {"type": "number"},
                    "direction": {"type": "string"},
                    "pct_move_required": {"type": "number"},
                    "clusters_in_path": {"type": "integer"},
                    "cascade_size_usd": {"type": "number"},
                    "severity": {"type": "string"},
                    "confidence_score": {"type": "number"},
                    "analysis_timestamp": {"type": "string"},
                },
            },
        ),
    ]


@mcp_server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> dict:
    try:
        server = AltLiqIQServer()

        if name == "get_altcoin_liq_clusters":
            return await server.get_altcoin_liq_clusters(
                asset=arguments["asset"],
                venues=arguments.get("venues"),
            )

        if name == "compare_altcoin_liq_risk":
            return await server.compare_altcoin_liq_risk(
                assets=arguments["assets"],
            )

        if name == "estimate_cascade":
            return await server.estimate_cascade(
                asset=arguments["asset"],
                target_price=float(arguments["target_price"]),
            )

        return {"error": f"Unknown tool: {name}"}

    except Exception as exc:
        return {
            "success": False,
            "error": f"Internal server error: {str(exc)}",
            "tool": name,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }


# ---------------------------------------------------------------------------
# Starlette app — same pattern as EventVol
# ---------------------------------------------------------------------------

sse_transport = SseServerTransport("/messages")


async def keepalive():
    await asyncio.sleep(30)
    while True:
        try:
            port = int(os.environ.get("PORT", 8000))
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.get(f"http://127.0.0.1:{port}/health")
        except Exception:
            pass
        await asyncio.sleep(240)


@asynccontextmanager
async def lifespan(app: Starlette):
    keepalive_task = asyncio.create_task(keepalive())
    try:
        yield
    finally:
        keepalive_task.cancel()
        try:
            await keepalive_task
        except asyncio.CancelledError:
            pass


class AlreadySentResponse(Response):
    async def __call__(self, scope, receive, send) -> None:
        return


async def handle_root(request: Request):
    return JSONResponse({
        "name": "AltLiqIQ",
        "description": (
            "Altcoin Perpetual Liquidation Intelligence. Real-time liquidation cluster maps, "
            "squeeze risk classification, and cascade size estimation for altcoin perp markets. "
            "BTC/ETH excluded. Covers SOL, DOGE, LINK, AVAX, SUI, ARB, WIF, PEPE and 12+ more."
        ),
        "version": "1.0.0",
        "tools": [
            "get_altcoin_liq_clusters",
            "compare_altcoin_liq_risk",
            "estimate_cascade",
        ],
        "supported_assets": SUPPORTED_ASSETS,
        "pricing": "$0.10 per response",
        "author": "AltLiqIQ",
        "_meta": {
            "rateLimit": {
                "maxRequestsPerMinute": 60,
                "cooldownMs": 1000,
                "maxConcurrency": 8,
                "supportsBulk": True,
                "recommendedBatchTools": ["compare_altcoin_liq_risk"],
                "notes": (
                    "compare_altcoin_liq_risk batches up to 8 assets in one call — "
                    "prefer over repeated get_altcoin_liq_clusters calls. "
                    "estimate_cascade reuses cached cluster data (30s TTL) when called "
                    "immediately after get_altcoin_liq_clusters for the same asset."
                ),
            }
        },
    })


async def handle_health(request: Request):
    return JSONResponse({"status": "ok"})


async def handle_sse(request: Request):
    async with sse_transport.connect_sse(
        request.scope, request.receive, request._send
    ) as streams:
        await mcp_server.run(
            streams[0], streams[1],
            mcp_server.create_initialization_options()
        )
    return Response()


async def handle_messages(request: Request):
    body_bytes = await request.body()

    try:
        body_json = json.loads(body_bytes)
        method = body_json.get("method", "")
    except Exception:
        body_json = {}
        method = ""

    if is_protected_mcp_method(method):
        try:
            await verify_context_request(
                authorization_header=request.headers.get("authorization", "")
            )
        except ContextError as e:
            return JSONResponse(
                {
                    "jsonrpc": "2.0",
                    "error": {"code": -32001, "message": f"Unauthorized: {e.message}"},
                    "id": body_json.get("id"),
                },
                status_code=401,
            )

    async def receive():
        return {"type": "http.request", "body": body_bytes, "more_body": False}

    await sse_transport.handle_post_message(request.scope, receive, request._send)
    return AlreadySentResponse()


async def handle_debug_binance_liq(request: Request) -> JSONResponse:
    """
    TEMPORARY debug endpoint — runs raw, unwrapped calls to Binance's
    allForceOrders endpoint (SOL + BTC control) plus a klines control check,
    directly from THIS server's network path. This exists because local
    machines often can't reach Binance at all (regional blocks, ISP
    filtering), which makes local testing useless for diagnosing what the
    deployed server itself sees.

    Remove this route once the Binance liquidation gap is resolved — it's
    diagnostic only, not part of the product surface.
    """
    async def raw_get(url: str, params: dict) -> Dict[str, Any]:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.get(url, params=params)
                body_preview = r.text[:500]
                parsed_len = None
                try:
                    parsed = r.json()
                    if isinstance(parsed, list):
                        parsed_len = len(parsed)
                except Exception:
                    pass
                return {
                    "status_code": r.status_code,
                    "body_preview": body_preview,
                    "parsed_list_length": parsed_len,
                }
        except Exception as e:
            return {"exception": f"{type(e).__name__}: {e}"}

    results = {
        "sol_force_orders": await raw_get(
            f"{BINANCE_FUTURES_BASE}/fapi/v1/allForceOrders",
            {"symbol": "SOLUSDT", "limit": 100},
        ),
        "btc_force_orders_control": await raw_get(
            f"{BINANCE_FUTURES_BASE}/fapi/v1/allForceOrders",
            {"symbol": "BTCUSDT", "limit": 100},
        ),
        "klines_connectivity_control": await raw_get(
            f"{BINANCE_FUTURES_BASE}/fapi/v1/klines",
            {"symbol": "SOLUSDT", "interval": "1h", "limit": 2},
        ),
    }
    return JSONResponse(results)


app = Starlette(
    routes=[
        Route("/", handle_root),
        Route("/health", handle_health),
        Route("/sse", handle_sse),
        Route("/messages", handle_messages, methods=["POST"]),
        Route("/debug/binance-liq", handle_debug_binance_liq),
    ],
    lifespan=lifespan,
)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)