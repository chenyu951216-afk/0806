from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import logging
import math
import os
import random
import sqlite3
import statistics
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

# ============================================================
# Configuration
# ============================================================

APP_NAME = "Gate 爆發前掃幣雷達"
VERSION = "2.1.0"
PORT = int(os.getenv("PORT", "8080"))
TZ_OFFSET_HOURS = int(os.getenv("TZ_OFFSET_HOURS", "8"))
LOCAL_TZ = timezone(timedelta(hours=TZ_OFFSET_HOURS))
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
except PermissionError:
    DATA_DIR = Path("./data")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "scanner_v2.db"

GATE_BASE_URL = os.getenv("GATE_BASE_URL", "https://api.gateio.ws/api/v4").rstrip("/")
COINGLASS_BASE_URL = os.getenv("COINGLASS_BASE_URL", "https://open-api-v4.coinglass.com").rstrip("/")
COINGLASS_API_KEY = os.getenv("COINGLASS_API_KEY", "").strip()
COINGLASS_EXCHANGE = os.getenv("COINGLASS_EXCHANGE", "Gate").strip()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

DEMO_MODE = os.getenv("DEMO_MODE", "false").lower() == "true"
AUTO_SCAN = os.getenv("AUTO_SCAN", "true").lower() == "true"
SCAN_INTERVAL_SECONDS = max(60, int(os.getenv("SCAN_INTERVAL_SECONDS", "300")))
LIVE_PRICE_INTERVAL_SECONDS = max(2, int(os.getenv("LIVE_PRICE_INTERVAL_SECONDS", "5")))
TRACK_INTERVAL_SECONDS = max(10, int(os.getenv("TRACK_INTERVAL_SECONDS", "20")))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))
HTTP_CONCURRENCY = max(2, int(os.getenv("HTTP_CONCURRENCY", "8")))

MAX_UNIVERSE = max(20, int(os.getenv("MAX_UNIVERSE", "120")))
DEEP_SCAN_LIMIT = max(10, int(os.getenv("DEEP_SCAN_LIMIT", "55")))
ORDERBOOK_FINALISTS = max(3, int(os.getenv("ORDERBOOK_FINALISTS", "18")))
MAX_SIGNALS = max(5, int(os.getenv("MAX_SIGNALS", "35")))
COINGLASS_MARKET_PAGES = max(1, int(os.getenv("COINGLASS_MARKET_PAGES", "4")))
COINGLASS_PER_PAGE = max(20, int(os.getenv("COINGLASS_PER_PAGE", "100")))

MIN_SPOT_QUOTE_VOLUME_USDT = float(os.getenv("MIN_SPOT_QUOTE_VOLUME_USDT", "1000000"))
MIN_FUTURES_QUOTE_VOLUME_USDT = float(os.getenv("MIN_FUTURES_QUOTE_VOLUME_USDT", "1000000"))
MIN_MARKET_CAP_USD = float(os.getenv("MIN_MARKET_CAP_USD", "12000000"))
MAX_MARKET_CAP_USD = float(os.getenv("MAX_MARKET_CAP_USD", "3500000000"))
MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "0.35"))
MAX_24H_PUMP_PCT = float(os.getenv("MAX_24H_PUMP_PCT", "16"))
MAX_4H_PUMP_PCT = float(os.getenv("MAX_4H_PUMP_PCT", "8"))
MIN_SETUP_SCORE = float(os.getenv("MIN_SETUP_SCORE", "64"))
ENTRY_READY_SCORE = float(os.getenv("ENTRY_READY_SCORE", "76"))
ALERT_SCORE = float(os.getenv("ALERT_SCORE", "82"))
MIN_STOP_PCT = float(os.getenv("MIN_STOP_PCT", "1.4"))
MAX_STOP_PCT = float(os.getenv("MAX_STOP_PCT", "7"))
MAX_EXPECTED_MOVE_PCT = float(os.getenv("MAX_EXPECTED_MOVE_PCT", "55"))
PLAN_TTL_CANDLES = max(8, int(os.getenv("PLAN_TTL_CANDLES", "32")))
DEFAULT_NOTIONAL_USDT = float(os.getenv("DEFAULT_NOTIONAL_USDT", "100"))
DEFAULT_ACCOUNT_BALANCE_USDT = float(os.getenv("DEFAULT_ACCOUNT_BALANCE_USDT", "1000"))
RISK_PER_TRADE_PCT = min(20.0, max(0.1, float(os.getenv("RISK_PER_TRADE_PCT", "2"))))
INTRABAR_POLICY = os.getenv("INTRABAR_POLICY", "conservative").lower()

EXCLUDED_BASES = {
    x.strip().upper()
    for x in os.getenv(
        "EXCLUDED_BASES",
        "BTC,ETH,BNB,SOL,XRP,DOGE,ADA,TRX,BCH,LTC,TON,DOT,USDC,USDE,DAI,FDUSD,TUSD,USDD,EUR,USD1,PAXG,XAUT,GT,WBTC,WETH,STETH",
    ).split(",")
    if x.strip()
}
INTERVAL_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400, "1d": 86400}

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("breakout-scanner-v2.1")

HTTP_SEMAPHORE = asyncio.Semaphore(HTTP_CONCURRENCY)
LIVE_PRICES: dict[str, dict[str, float]] = {}
STATE: dict[str, Any] = {
    "scan_running": False,
    "last_scan_started": None,
    "last_scan_finished": None,
    "last_scan_error": None,
    "last_live_update": None,
    "last_universe_size": 0,
    "last_deep_scanned": 0,
    "request_errors": 0,
}

# ============================================================
# Helpers
# ============================================================


def now_ts() -> int:
    return int(time.time())


def local_iso(ts: int | float | None = None) -> str:
    ts = time.time() if ts is None else float(ts)
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(LOCAL_TZ).isoformat(timespec="seconds")


def f(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def pct_change(new: float, old: float) -> float:
    return (new / old - 1.0) * 100.0 if old else 0.0


def mean(values: Iterable[float]) -> float:
    vals = list(values)
    return sum(vals) / len(vals) if vals else 0.0


def median(values: Iterable[float]) -> float:
    vals = list(values)
    return statistics.median(vals) if vals else 0.0


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    vals = sorted(values)
    idx = clamp(q, 0, 1) * (len(vals) - 1)
    lo, hi = math.floor(idx), math.ceil(idx)
    if lo == hi:
        return vals[lo]
    return vals[lo] + (vals[hi] - vals[lo]) * (idx - lo)


def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    out = [values[0]]
    for value in values[1:]:
        out.append(alpha * value + (1 - alpha) * out[-1])
    return out


def rsi(values: list[float], period: int = 14) -> float:
    if len(values) <= period:
        return 50.0
    gains, losses = [], []
    for old, new in zip(values[-period - 1 : -1], values[-period:]):
        diff = new - old
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain, avg_loss = mean(gains), mean(losses)
    if avg_loss <= 1e-12:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - 100 / (1 + rs)


def atr(candles: list[dict[str, float]], period: int = 14) -> float:
    if len(candles) < 2:
        return 0.0
    trs: list[float] = []
    for prev, cur in zip(candles[-period - 1 : -1], candles[-period:]):
        trs.append(max(cur["high"] - cur["low"], abs(cur["high"] - prev["close"]), abs(cur["low"] - prev["close"])))
    return mean(trs)


def safe_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def parse_json(value: str | None, default: Any) -> Any:
    try:
        return json.loads(value) if value else default
    except json.JSONDecodeError:
        return default


def strip_open_candle(candles: list[dict[str, float]], interval: str) -> list[dict[str, float]]:
    seconds = INTERVAL_SECONDS[interval]
    cutoff = now_ts()
    return [c for c in candles if int(c["time"]) + seconds <= cutoff]


def normalize_candles(raw: Any, interval: str) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    if not isinstance(raw, list):
        return rows
    for item in raw:
        try:
            if isinstance(item, dict):
                row = {
                    "time": int(f(item.get("t", item.get("time")))),
                    "volume": f(item.get("v", item.get("volume"))),
                    "close": f(item.get("c", item.get("close"))),
                    "high": f(item.get("h", item.get("high"))),
                    "low": f(item.get("l", item.get("low"))),
                    "open": f(item.get("o", item.get("open"))),
                    "quote_volume": f(item.get("sum", item.get("amount", item.get("quote_volume")))),
                }
            elif isinstance(item, list) and len(item) >= 6:
                row = {
                    "time": int(f(item[0])),
                    "volume": f(item[1]),
                    "close": f(item[2]),
                    "high": f(item[3]),
                    "low": f(item[4]),
                    "open": f(item[5]),
                    "quote_volume": f(item[6]) if len(item) > 6 else 0.0,
                }
            else:
                continue
            if min(row["open"], row["high"], row["low"], row["close"]) > 0:
                rows.append(row)
        except (TypeError, ValueError, IndexError):
            continue
    rows.sort(key=lambda x: x["time"])
    return strip_open_candle(rows, interval)


def normalize_oi(raw: Any) -> list[dict[str, float]]:
    if isinstance(raw, dict):
        raw = raw.get("data", [])
    rows: list[dict[str, float]] = []
    if not isinstance(raw, list):
        return rows
    for item in raw:
        if not isinstance(item, dict):
            continue
        ts = int(f(item.get("time", item.get("t"))))
        if ts > 10_000_000_000:
            ts //= 1000
        oi = f(item.get("open_interest", item.get("close", item.get("oi"))))
        if ts and oi > 0:
            rows.append({
                "time": ts,
                "oi": oi,
                "lsr_taker": f(item.get("lsr_taker")),
                "lsr_account": f(item.get("lsr_account")),
                "top_lsr_account": f(item.get("top_lsr_account")),
                "top_lsr_size": f(item.get("top_lsr_size")),
                "long_liq_usd": f(item.get("long_liq_usd_new", item.get("long_liq_usd"))),
                "short_liq_usd": f(item.get("short_liq_usd_new", item.get("short_liq_usd"))),
            })
    rows.sort(key=lambda x: x["time"])
    return rows


def ticker_quote_volume(item: dict[str, Any]) -> float:
    for key in ("quote_volume", "volume_24h_quote", "volume_24h_usd", "volume_24h_usdt", "volume_24h_settle", "volume_usd_24h"):
        if f(item.get(key)) > 0:
            return f(item.get(key))
    last = f(item.get("last", item.get("mark_price")))
    return last * f(item.get("base_volume", item.get("volume_24h", item.get("volume"))))


def ticker_change_pct(item: dict[str, Any]) -> float:
    for key in ("change_percentage", "change_24h", "price_change_percent_24h"):
        if key in item:
            return f(item.get(key))
    value = f(item.get("change_utc8", item.get("change")))
    return value * 100 if abs(value) <= 2 else value


def is_bad_base(base: str) -> bool:
    base = base.upper()
    if base in EXCLUDED_BASES or base.startswith("1000"):
        return True
    return any(base.endswith(x) for x in ("3L", "3S", "5L", "5S", "BULL", "BEAR", "UP", "DOWN"))


def plan_hash(symbol: str, trigger: float, base_start: int, created: int) -> str:
    raw = f"{symbol}|{trigger:.12g}|{base_start}|{created}"
    return hashlib.sha1(raw.encode()).hexdigest()[:10].upper()

# ============================================================
# Database
# ============================================================


def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    with db_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS setups_v2 (
                symbol TEXT PRIMARY KEY,
                plan_id TEXT NOT NULL,
                setup_status TEXT NOT NULL,
                score REAL NOT NULL,
                action_code TEXT NOT NULL,
                plan_created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                last_closed_15m INTEGER NOT NULL,
                payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_setups_score ON setups_v2(score DESC);

            CREATE TABLE IF NOT EXISTS positions_v2 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                plan_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                entry_price REAL NOT NULL,
                notional REAL NOT NULL,
                original_qty REAL NOT NULL,
                remaining_qty REAL NOT NULL,
                original_stop REAL NOT NULL,
                active_stop REAL NOT NULL,
                structural_invalidation REAL NOT NULL,
                tps_json TEXT NOT NULL,
                realized_pnl REAL NOT NULL DEFAULT 0,
                current_price REAL NOT NULL,
                highest_live REAL NOT NULL,
                highest_closed_15m REAL NOT NULL,
                last_processed_1m INTEGER NOT NULL DEFAULT 0,
                last_trend_15m INTEGER NOT NULL DEFAULT 0,
                trend_status TEXT NOT NULL,
                trailing_enabled INTEGER NOT NULL DEFAULT 0,
                override_warning TEXT,
                opened_at INTEGER NOT NULL,
                closed_at INTEGER,
                close_reason TEXT,
                setup_snapshot TEXT NOT NULL,
                account_balance_at_entry REAL NOT NULL DEFAULT 0,
                planned_risk_usdt REAL NOT NULL DEFAULT 0,
                planned_risk_pct REAL NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_positions_status ON positions_v2(status, opened_at DESC);

            CREATE TABLE IF NOT EXISTS position_events_v2 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                position_id INTEGER NOT NULL,
                event_time INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                price REAL NOT NULL,
                qty REAL NOT NULL DEFAULT 0,
                pnl REAL NOT NULL DEFAULT 0,
                detail TEXT,
                FOREIGN KEY(position_id) REFERENCES positions_v2(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS alerts_v2 (
                alert_key TEXT PRIMARY KEY,
                sent_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS app_settings_v2 (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
            """
        )
        # Safe in-place migration for users reusing the v2 persistent volume.
        columns = {row[1] for row in conn.execute("PRAGMA table_info(positions_v2)").fetchall()}
        for name, ddl in (
            ("account_balance_at_entry", "REAL NOT NULL DEFAULT 0"),
            ("planned_risk_usdt", "REAL NOT NULL DEFAULT 0"),
            ("planned_risk_pct", "REAL NOT NULL DEFAULT 0"),
        ):
            if name not in columns:
                conn.execute(f"ALTER TABLE positions_v2 ADD COLUMN {name} {ddl}")
        conn.execute(
            "INSERT OR IGNORE INTO app_settings_v2(key,value,updated_at) VALUES('account_balance_usdt',?,?)",
            (str(max(DEFAULT_ACCOUNT_BALANCE_USDT, 0.01)), now_ts()),
        )

# ============================================================
# Account risk sizing
# ============================================================


def get_account_balance() -> float:
    with db_conn() as conn:
        row = conn.execute("SELECT value FROM app_settings_v2 WHERE key='account_balance_usdt'").fetchone()
    return max(0.01, f(row["value"] if row else DEFAULT_ACCOUNT_BALANCE_USDT, DEFAULT_ACCOUNT_BALANCE_USDT))


def save_account_balance(balance: float) -> float:
    balance = max(0.01, float(balance))
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO app_settings_v2(key,value,updated_at) VALUES('account_balance_usdt',?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
            (str(balance), now_ts()),
        )
    return balance


def calculate_risk_sizing(account_balance: float, entry_price: float, stop_price: float) -> dict[str, Any]:
    account_balance = max(0.0, f(account_balance))
    entry_price = f(entry_price)
    stop_price = f(stop_price)
    risk_budget = account_balance * RISK_PER_TRADE_PCT / 100
    if account_balance <= 0 or entry_price <= 0 or stop_price <= 0 or entry_price <= stop_price:
        return {
            "valid": False,
            "account_balance_usdt": account_balance,
            "risk_pct": RISK_PER_TRADE_PCT,
            "risk_budget_usdt": risk_budget,
            "entry_price": entry_price,
            "stop_price": stop_price,
            "stop_distance_pct": 0,
            "theoretical_notional_usdt": 0,
            "recommended_notional_usdt": 0,
            "quantity": 0,
            "actual_risk_usdt": 0,
            "actual_risk_pct_balance": 0,
            "exceeds_1x_balance": False,
            "required_leverage": 0,
            "one_x_notional_usdt": 0,
            "one_x_quantity": 0,
            "one_x_actual_risk_usdt": 0,
            "one_x_actual_risk_pct_balance": 0,
            "capped_by_1x_balance": False,
        }
    stop_fraction = (entry_price - stop_price) / entry_price
    theoretical = risk_budget / stop_fraction
    recommended = theoretical
    one_x_notional = min(theoretical, account_balance)
    actual_risk = recommended * stop_fraction
    one_x_actual_risk = one_x_notional * stop_fraction
    return {
        "valid": True,
        "account_balance_usdt": round(account_balance, 4),
        "risk_pct": round(RISK_PER_TRADE_PCT, 4),
        "risk_budget_usdt": round(risk_budget, 4),
        "entry_price": entry_price,
        "stop_price": stop_price,
        "stop_distance_pct": round(stop_fraction * 100, 4),
        "theoretical_notional_usdt": round(theoretical, 4),
        "recommended_notional_usdt": round(recommended, 4),
        "quantity": round(recommended / entry_price, 12),
        "actual_risk_usdt": round(actual_risk, 4),
        "actual_risk_pct_balance": round(actual_risk / account_balance * 100, 4),
        "exceeds_1x_balance": theoretical > account_balance + 1e-9,
        "required_leverage": round(theoretical / account_balance, 4),
        "one_x_notional_usdt": round(one_x_notional, 4),
        "one_x_quantity": round(one_x_notional / entry_price, 12),
        "one_x_actual_risk_usdt": round(one_x_actual_risk, 4),
        "one_x_actual_risk_pct_balance": round(one_x_actual_risk / account_balance * 100, 4),
        "capped_by_1x_balance": theoretical > account_balance + 1e-9,
    }


def setup_risk_sizing(plan: dict[str, Any], account_balance: float, action_code: str) -> dict[str, Any]:
    early_entry = (f(plan.get("early_entry_low")) + f(plan.get("early_entry_high"))) / 2
    retest_entry = (f(plan.get("retest_entry_low")) + f(plan.get("retest_entry_high"))) / 2
    reference_entry = f(plan.get("reference_entry"), early_entry)
    mode = "retest" if action_code in {"BREAKOUT_RETEST_READY", "WAIT_RETEST", "WAIT_RECLAIM"} else "early"
    scenarios = {
        "early": calculate_risk_sizing(account_balance, early_entry, f(plan.get("stop"))),
        "retest": calculate_risk_sizing(account_balance, retest_entry, f(plan.get("stop"))),
        "reference": calculate_risk_sizing(account_balance, reference_entry, f(plan.get("stop"))),
    }
    return {
        "account_balance_usdt": round(account_balance, 4),
        "risk_pct": round(RISK_PER_TRADE_PCT, 4),
        "risk_budget_usdt": round(account_balance * RISK_PER_TRADE_PCT / 100, 4),
        "selected_mode": mode,
        "selected": scenarios[mode],
        **scenarios,
    }


# ============================================================
# HTTP client
# ============================================================


class MarketClient:
    def __init__(self) -> None:
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(REQUEST_TIMEOUT_SECONDS),
            limits=httpx.Limits(max_connections=max(12, HTTP_CONCURRENCY * 2), max_keepalive_connections=HTTP_CONCURRENCY),
            headers={"User-Agent": f"gate-breakout-scanner/{VERSION}"},
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def get_json(self, url: str, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None, retries: int = 3) -> Any:
        error: Exception | None = None
        for attempt in range(retries):
            try:
                async with HTTP_SEMAPHORE:
                    response = await self.client.get(url, params=params, headers=headers)
                if response.status_code == 429:
                    await asyncio.sleep(1.2 * (attempt + 1))
                    continue
                response.raise_for_status()
                return response.json()
            except Exception as exc:  # noqa: BLE001
                error = exc
                STATE["request_errors"] += 1
                await asyncio.sleep(0.4 * (2**attempt))
        raise RuntimeError(f"GET failed {url}: {error}")

    async def gate(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return await self.get_json(f"{GATE_BASE_URL}{path}", params=params)

    async def coinglass(self, path: str, params: dict[str, Any]) -> Any:
        if not COINGLASS_API_KEY:
            return None
        return await self.get_json(
            f"{COINGLASS_BASE_URL}{path}",
            params=params,
            headers={"CG-API-KEY": COINGLASS_API_KEY},
            retries=2,
        )

    async def post_discord(self, content: str) -> None:
        if not DISCORD_WEBHOOK_URL:
            return
        try:
            response = await self.client.post(DISCORD_WEBHOOK_URL, json={"content": content[:1900]})
            response.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Discord push failed: %s", exc)


CLIENT: MarketClient | None = None

# ============================================================
# Demo data
# ============================================================


def demo_candles(symbol: str, interval: str, count: int = 200) -> list[dict[str, float]]:
    seed = int(hashlib.md5(f"{symbol}:{interval}".encode()).hexdigest()[:8], 16)
    rnd = random.Random(seed)
    seconds = INTERVAL_SECONDS[interval]
    end = (now_ts() // seconds) * seconds - seconds
    base = 0.35 + (seed % 9000) / 10000
    rows: list[dict[str, float]] = []
    price = base
    for i in range(count):
        compression = 0.0032 if i < count - 55 else 0.0011
        drift = 0.00018 if i > count - 80 else 0.00004
        if i > count - 12:
            drift += 0.00022
        ret = drift + rnd.gauss(0, compression)
        open_ = price
        close = max(0.000001, open_ * (1 + ret))
        wick = abs(rnd.gauss(0, compression * 0.7))
        high = max(open_, close) * (1 + wick)
        low = min(open_, close) * (1 - wick)
        vol = 220_000 * (0.75 + rnd.random() * 0.5)
        if i >= count - 3:
            vol *= 2.0 + 0.35 * (i - (count - 3))
        rows.append({
            "time": end - (count - 1 - i) * seconds,
            "open": open_, "high": high, "low": low, "close": close,
            "volume": vol / close, "quote_volume": vol,
        })
        price = close
    return rows


def demo_oi(symbol: str, count: int = 100) -> list[dict[str, float]]:
    seed = int(hashlib.sha1(symbol.encode()).hexdigest()[:8], 16)
    rnd = random.Random(seed)
    end = (now_ts() // 900) * 900 - 900
    oi = 7_000_000 + seed % 3_000_000
    rows = []
    for i in range(count):
        oi *= 1 + (0.0007 if i < count - 20 else 0.0025) + rnd.gauss(0, 0.001)
        rows.append({
            "time": end - (count - 1 - i) * 900,
            "oi": oi,
            "lsr_account": 1.12,
            "top_lsr_account": 1.35,
            "top_lsr_size": 1.28,
            "long_liq_usd": 900,
            "short_liq_usd": 2400,
        })
    return rows

# ============================================================
# Scanner model
# ============================================================


@dataclass
class UniverseItem:
    symbol: str
    base: str
    spot_volume: float
    futures_volume: float
    change_24h: float
    last: float
    funding_rate: float
    futures_ticker: dict[str, Any]


def compute_btc_regime(candles: list[dict[str, float]]) -> dict[str, Any]:
    if len(candles) < 60:
        return {"label": "資料不足", "score": -2, "bearish": False, "change_4h": 0}
    closes = [c["close"] for c in candles]
    e20, e50 = ema(closes, 20)[-1], ema(closes, 50)[-1]
    ch4 = pct_change(closes[-1], closes[-5])
    if closes[-1] < e50 and ch4 < -2.2:
        return {"label": "BTC風險偏高", "score": -9, "bearish": True, "change_4h": ch4}
    if closes[-1] > e20 > e50 and ch4 > -1.0:
        return {"label": "BTC環境偏多", "score": 5, "bearish": False, "change_4h": ch4}
    return {"label": "BTC中性", "score": 1, "bearish": False, "change_4h": ch4}


def quote_volumes(candles: list[dict[str, float]]) -> list[float]:
    return [max(c["quote_volume"], c["volume"] * c["close"]) for c in candles]


def cg_metrics(row: dict[str, Any] | None) -> dict[str, float]:
    row = row or {}
    return {
        "market_cap_usd": f(row.get("market_cap_usd")),
        "oi_market_cap_ratio": f(row.get("open_interest_market_cap_ratio")),
        "cg_oi_1h_pct": f(row.get("open_interest_change_percent_1h")),
        "cg_oi_4h_pct": f(row.get("open_interest_change_percent_4h")),
        "cg_volume_1h_pct": f(row.get("volume_change_percent_1h")),
        "cg_volume_4h_pct": f(row.get("volume_change_percent_4h")),
        "long_short_ratio_1h": f(row.get("long_short_ratio_1h")),
        "liquidation_usd_1h": f(row.get("liquidation_usd_1h")),
        "long_liquidation_usd_1h": f(row.get("long_liquidation_usd_1h")),
        "short_liquidation_usd_1h": f(row.get("short_liquidation_usd_1h")),
        "avg_funding_rate_by_oi": f(row.get("avg_funding_rate_by_oi")),
    }


def build_signal(
    item: UniverseItem,
    spot15: list[dict[str, float]],
    spot1h: list[dict[str, float]],
    spot4h: list[dict[str, float]],
    fut15: list[dict[str, float]],
    oi_rows: list[dict[str, float]],
    btc: dict[str, Any],
    cg_row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if len(spot15) < 130 or len(spot1h) < 70 or len(spot4h) < 35:
        return None

    closes = [c["close"] for c in spot15]
    highs = [c["high"] for c in spot15]
    lows = [c["low"] for c in spot15]
    vols = quote_volumes(spot15)
    last = closes[-1]
    atr15 = atr(spot15, 14)
    if last <= 0 or atr15 <= 0:
        return None

    e20 = ema(closes, 20)[-1]
    e50 = ema(closes, 50)[-1]
    h_closes = [c["close"] for c in spot1h]
    h_e20, h_e50 = ema(h_closes, 20)[-1], ema(h_closes, 50)[-1]
    h4_closes = [c["close"] for c in spot4h]
    h4_e20 = ema(h4_closes, 20)[-1]

    base_slice = spot15[-49:-1]
    base_start = int(base_slice[0]["time"])
    resistance = max(c["high"] for c in base_slice)
    range_low = min(c["low"] for c in base_slice)
    base_height = resistance - range_low
    base_height_pct = base_height / last * 100
    base_quote = sum(max(c["quote_volume"], c["volume"] * c["close"]) for c in base_slice)
    base_vwap = (
        sum(((c["high"] + c["low"] + c["close"]) / 3) * max(c["quote_volume"], c["volume"] * c["close"]) for c in base_slice)
        / max(base_quote, 1e-12)
    )

    prior_vol = vols[-70:-6]
    recent_vol = vols[-3:]
    volume_ratio = mean(recent_vol) / max(median(prior_vol), 1e-12)
    last_volume_ratio = vols[-1] / max(median(prior_vol), 1e-12)
    max_recent_volume_ratio = max(recent_vol) / max(median(prior_vol), 1e-12)

    ranges = [(c["high"] - c["low"]) / c["close"] for c in spot15]
    compression_now = median(ranges[-20:])
    compression_old = median(ranges[-90:-35])
    compression_ratio = compression_now / max(compression_old, 1e-12)

    change_1h = pct_change(last, closes[-5])
    change_4h = pct_change(last, closes[-17])
    change_24h = pct_change(last, closes[-97])
    distance_to_trigger_atr = (resistance - last) / atr15
    range_position = (last - range_low) / max(base_height, 1e-12)
    candle_body_atr = abs(spot15[-1]["close"] - spot15[-1]["open"]) / atr15
    already_pumped = item.change_24h > MAX_24H_PUMP_PCT or change_4h > MAX_4H_PUMP_PCT or candle_body_atr > 2.4

    signed_flow = 0.0
    for c, qv in zip(spot15[-12:], vols[-12:]):
        signed_flow += qv if c["close"] >= c["open"] else -qv
    flow_ratio = signed_flow / max(sum(vols[-12:]), 1e-12)

    fut_vols = quote_volumes(fut15) if fut15 else []
    fut_volume_ratio = 0.0
    spot_led_ratio = 1.0
    if len(fut_vols) >= 70:
        fut_volume_ratio = mean(fut_vols[-3:]) / max(median(fut_vols[-70:-6]), 1e-12)
        spot_led_ratio = volume_ratio / max(fut_volume_ratio, 0.15)

    oi_15m = oi_1h = oi_4h = 0.0
    lsr_account = top_lsr_account = top_lsr_size = 0.0
    long_liq_1h = short_liq_1h = 0.0
    if len(oi_rows) >= 17:
        oi_15m = pct_change(oi_rows[-1]["oi"], oi_rows[-2]["oi"])
        oi_1h = pct_change(oi_rows[-1]["oi"], oi_rows[-5]["oi"])
        oi_4h = pct_change(oi_rows[-1]["oi"], oi_rows[-17]["oi"])
        latest_oi = oi_rows[-1]
        lsr_account = f(latest_oi.get("lsr_account"))
        top_lsr_account = f(latest_oi.get("top_lsr_account"))
        top_lsr_size = f(latest_oi.get("top_lsr_size"))
        long_liq_1h = sum(f(x.get("long_liq_usd")) for x in oi_rows[-4:])
        short_liq_1h = sum(f(x.get("short_liq_usd")) for x in oi_rows[-4:])

    funding_pct = item.funding_rate * 100
    rsi15 = rsi(closes, 14)
    cg = cg_metrics(cg_row)

    components: dict[str, float] = {}
    reasons: list[str] = []
    warnings: list[str] = []

    # 1) Liquidity and small-cap suitability (0..12)
    liquidity = clamp((math.log10(max(item.spot_volume, 1)) - 5.8) * 4.2, 0, 7)
    liquidity += clamp((math.log10(max(item.futures_volume, 1)) - 5.8) * 2.8, 0, 5)
    components["流動性"] = liquidity

    mcap_score = 0.0
    mcap = cg["market_cap_usd"]
    if mcap > 0:
        if MIN_MARKET_CAP_USD <= mcap <= 900_000_000:
            mcap_score = 6
            reasons.append(f"市值約 {mcap / 1e6:.0f}M，符合小幣爆發區間")
        elif mcap <= MAX_MARKET_CAP_USD:
            mcap_score = 3
        elif mcap > MAX_MARKET_CAP_USD:
            mcap_score = -5
            warnings.append("市值高於設定的小幣上限")
        else:
            mcap_score = -6
            warnings.append("市值過小，操縱與滑點風險高")
    components["小幣適配"] = mcap_score

    # 2) Base / compression (0..21)
    base_score = 0.0
    if compression_ratio <= 0.72:
        base_score += 9
        reasons.append("15m 波動明顯收斂，具蓄勢特徵")
    elif compression_ratio <= 0.88:
        base_score += 6
    elif compression_ratio <= 1.0:
        base_score += 3
    if 2.5 <= base_height_pct <= 13:
        base_score += 5
    elif base_height_pct > 18:
        base_score -= 4
        warnings.append("基底過寬，不屬於乾淨蓄勢")
    if 0.48 <= range_position <= 0.94:
        base_score += 4
    if -0.25 <= distance_to_trigger_atr <= 1.6:
        base_score += 3
        reasons.append("價格接近基底上緣但尚未明顯追價")
    components["基底收斂"] = base_score

    # 3) Stealth spot-volume build (0..23)
    volume_score = 0.0
    if 1.45 <= volume_ratio <= 4.5:
        volume_score += clamp((volume_ratio - 1.35) * 8.5, 5, 15)
        reasons.append(f"近 3 根已收 15m 現貨量為基準 {volume_ratio:.2f} 倍")
    elif volume_ratio > 6:
        volume_score -= 6
        warnings.append("現貨量能已過度爆量，可能已進入追價段")
    else:
        warnings.append("現貨提前放量仍不夠明顯")
    if 1.2 <= last_volume_ratio <= 5:
        volume_score += 3
    if max_recent_volume_ratio > 7:
        volume_score -= 5
        warnings.append("單根成交量異常尖峰，需防拉高出貨")
    if flow_ratio > 0.18:
        volume_score += 4
        reasons.append("近 12 根主動成交方向偏買方")
    elif flow_ratio < -0.12:
        volume_score -= 4
    if spot_led_ratio >= 1.12:
        volume_score += 3
        reasons.append("現貨放量強於合約，較不像純槓桿拉盤")
    components["現貨提前放量"] = volume_score

    # 4) Healthy OI build-up (0..18)
    oi_score = 0.0
    if oi_rows:
        if 0 <= oi_15m <= 4:
            oi_score += 3
        elif oi_15m > 7:
            oi_score -= 4
        if 0.5 <= oi_1h <= 8:
            oi_score += 6
        elif oi_1h > 12:
            oi_score -= 5
        if 1.2 <= oi_4h <= 18:
            oi_score += 7
            reasons.append(f"OI 健康累積：1h {oi_1h:+.2f}%／4h {oi_4h:+.2f}%")
        elif oi_4h > 28:
            oi_score -= 7
            warnings.append("OI 增速過快，槓桿擁擠風險高")
        if oi_4h > 2 and abs(change_4h) < 4.5:
            oi_score += 2
            reasons.append("價格尚未大漲但 OI 已開始累積")
    else:
        warnings.append("Gate OI 歷史資料暫缺")
    components["OI健康累積"] = oi_score

    # 5) Trend / accumulation structure (0..16)
    trend_score = 0.0
    if last > e20 > e50:
        trend_score += 7
    elif last > e20:
        trend_score += 4
    if h_closes[-1] > h_e20 >= h_e50:
        trend_score += 6
    elif h_closes[-1] > h_e20:
        trend_score += 3
    if h4_closes[-1] >= h4_e20 * 0.97:
        trend_score += 3
    components["多週期結構"] = trend_score

    # 6) Crowding / derivatives quality (-14..9)
    crowd = 5.0
    if abs(funding_pct) <= 0.025:
        crowd += 2
    elif abs(funding_pct) > 0.08:
        crowd -= 7
        warnings.append(f"資金費率偏熱 {funding_pct:.4f}%")
    elif abs(funding_pct) > 0.045:
        crowd -= 3
    if 0.72 <= lsr_account <= 1.85 or lsr_account == 0:
        crowd += 1
    elif lsr_account > 2.6:
        crowd -= 4
        warnings.append(f"Gate 多空帳戶比過度偏多 {lsr_account:.2f}")
    if top_lsr_account > 3 or top_lsr_size > 3:
        crowd -= 3
        warnings.append("大戶多空比過度擁擠")
    if short_liq_1h > long_liq_1h * 1.5 and short_liq_1h > 0:
        crowd += 1
    if cg["oi_market_cap_ratio"] > 0.35:
        crowd -= 4
        warnings.append("CoinGlass OI／市值比過高")
    elif 0.01 <= cg["oi_market_cap_ratio"] <= 0.18:
        crowd += 1
    components["擁擠風險"] = crowd

    # 7) BTC regime
    components["BTC環境"] = f(btc.get("score"))
    if btc.get("bearish"):
        warnings.append("BTC 1h 環境偏弱，山寨爆發成功率下修")

    score = sum(components.values())
    if rsi15 > 76:
        score -= 6
        warnings.append("15m RSI 過熱")
    if change_24h < -14:
        score -= 7
        warnings.append("24h 跌幅過深，可能只是反抽")
    if already_pumped:
        score -= 25
        warnings.append("已超過未起漲條件，列為不追價")
    score = clamp(score, 0, 100)

    # Fixed setup plan. This plan is frozen in DB until expiry/invalidation.
    trigger = resistance + atr15 * 0.08
    early_center = max(base_vwap, e20)
    early_low = early_center - atr15 * 0.25
    early_high = min(resistance - atr15 * 0.18, early_center + atr15 * 0.22)
    if early_high <= early_low:
        early_low = trigger - atr15 * 0.55
        early_high = trigger - atr15 * 0.22
    retest_low = trigger - atr15 * 0.20
    retest_high = trigger + atr15 * 0.12
    chase_cap = trigger + atr15 * 0.65

    recent_pivot = min(lows[-16:])
    raw_stop = min(recent_pivot, base_vwap - atr15 * 0.80) - atr15 * 0.12
    reference_entry = (early_low + early_high) / 2 if last <= trigger else (retest_low + retest_high) / 2
    if raw_stop >= reference_entry:
        raw_stop = reference_entry * (1 - MIN_STOP_PCT / 100)
    stop_pct = (reference_entry - raw_stop) / reference_entry * 100
    stop = reference_entry * (1 - MIN_STOP_PCT / 100) if stop_pct < MIN_STOP_PCT else raw_stop
    stop_pct = (reference_entry - stop) / reference_entry * 100
    structural_invalidation = min(range_low - atr15 * 0.15, stop - atr15 * 0.55)
    risk = reference_entry - stop
    if risk <= 0:
        return None

    measured_move = max(base_height * 0.95, atr15 * 4.2, risk * 5.0)
    max_target = reference_entry * (1 + MAX_EXPECTED_MOVE_PCT / 100)
    tp1 = max(reference_entry + 1.5 * risk, trigger + 0.28 * measured_move)
    tp2 = max(reference_entry + 3.0 * risk, trigger + 0.62 * measured_move)
    tp3 = max(reference_entry + 5.0 * risk, trigger + measured_move)
    tp1, tp2, tp3 = min(tp1, max_target), min(tp2, max_target), min(tp3, max_target)
    if tp2 <= tp1:
        tp2 = min(max_target, tp1 + risk * 1.5)
    if tp3 <= tp2:
        tp3 = min(max_target, tp2 + risk * 2.0)

    rr1 = (tp1 - reference_entry) / risk
    rr2 = (tp2 - reference_entry) / risk
    rr3 = (tp3 - reference_entry) / risk
    plan_valid = score >= MIN_SETUP_SCORE and stop_pct <= MAX_STOP_PCT and rr2 >= 2.2 and not already_pumped
    if stop_pct > MAX_STOP_PCT:
        warnings.append(f"結構停損需 {stop_pct:.2f}%，超出上限")
    if rr2 < 2.2:
        warnings.append("到中段目標的盈虧比不足")

    closed_breakout = last > trigger and spot15[-1]["close"] > spot15[-1]["open"] and last_volume_ratio >= 1.25
    created = now_ts()
    plan = {
        "setup_key": f"{item.symbol}:{base_start}:{trigger:.10g}",
        "reference_entry": reference_entry,
        "early_entry_low": early_low,
        "early_entry_high": early_high,
        "breakout_trigger": trigger,
        "retest_entry_low": retest_low,
        "retest_entry_high": retest_high,
        "chase_cap": chase_cap,
        "stop": stop,
        "stop_pct": round(stop_pct, 2),
        "structural_invalidation": structural_invalidation,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "tp1_pct": round(pct_change(tp1, reference_entry), 2),
        "tp2_pct": round(pct_change(tp2, reference_entry), 2),
        "tp3_pct": round(pct_change(tp3, reference_entry), 2),
        "rr1": round(rr1, 2),
        "rr2": round(rr2, 2),
        "rr3": round(rr3, 2),
        "expected_move_low_pct": round(pct_change(tp2, reference_entry), 2),
        "expected_move_high_pct": round(pct_change(tp3, reference_entry), 2),
        "base_vwap": base_vwap,
        "base_low": range_low,
        "base_high": resistance,
        "atr15": atr15,
        "plan_valid": plan_valid,
        "closed_breakout_confirmed": closed_breakout,
    }

    initial_status = "ACTIVE" if plan_valid else "WATCH"
    trend_status = "趨勢確認初期" if closed_breakout else ("趨勢尚未改變" if last >= e20 else "初級破壞")
    return {
        "symbol": item.symbol,
        "base": item.base,
        "score": round(score, 2),
        "setup_status": initial_status,
        "trend_status": trend_status,
        "closed_price": last,
        "last_closed_15m": int(spot15[-1]["time"]),
        "last_closed_15m_iso": local_iso(int(spot15[-1]["time"]) + 900),
        "change_1h_pct": round(change_1h, 2),
        "change_4h_pct": round(change_4h, 2),
        "change_24h_pct": round(change_24h, 2),
        "spot_quote_volume_24h": item.spot_volume,
        "futures_quote_volume_24h": item.futures_volume,
        "funding_rate_pct": round(funding_pct, 5),
        "volume_ratio": round(volume_ratio, 2),
        "futures_volume_ratio": round(fut_volume_ratio, 2),
        "spot_led_ratio": round(spot_led_ratio, 2),
        "oi_15m_pct": round(oi_15m, 2),
        "oi_1h_pct": round(oi_1h, 2),
        "oi_4h_pct": round(oi_4h, 2),
        "rsi15": round(rsi15, 1),
        "compression_ratio": round(compression_ratio, 3),
        "btc_regime": btc,
        "coinglass": cg,
        "orderbook": {},
        "components": {k: round(v, 2) for k, v in components.items()},
        "reasons": reasons,
        "warnings": warnings,
        "plan": plan,
        "computed_at": created,
    }


def classify_action(payload: dict[str, Any], live_price: float | None = None) -> dict[str, str]:
    plan = payload.get("plan", {})
    live = f(live_price, f(payload.get("closed_price")))
    closed = f(payload.get("closed_price"))
    status = str(payload.get("setup_status", "WATCH"))
    score = f(payload.get("score"))

    if status == "INVALIDATED" or closed <= f(plan.get("structural_invalidation")):
        return {"code": "INVALIDATED", "label": "計畫失效", "instruction": "結構已破壞，不應掛單；若你仍已成交，可手動標記並以原計畫追蹤。"}
    if status == "EXPIRED":
        return {"code": "EXPIRED", "label": "計畫到期", "instruction": "固定計畫已到期，等待下一個新基底計畫，不沿用舊價位追單。"}
    if live > f(plan.get("chase_cap")):
        return {"code": "DO_NOT_CHASE", "label": "已超追價上限", "instruction": "不要市價追入；只等回踩至突破回測區，否則放棄這一輪。"}
    if closed > f(plan.get("breakout_trigger")):
        if f(plan.get("retest_entry_low")) <= live <= f(plan.get("retest_entry_high")):
            return {"code": "BREAKOUT_RETEST_READY", "label": "突破回踩可掛單", "instruction": "15m 已收線突破，現價位於回測區；優先使用限價單，不用市價追。"}
        if live > f(plan.get("retest_entry_high")):
            return {"code": "WAIT_RETEST", "label": "等待回踩", "instruction": "突破已確認但現價偏高；把限價單放在回測區，未回踩就不成交。"}
        return {"code": "WAIT_RECLAIM", "label": "跌回突破下方", "instruction": "價格低於回測區，先等重新站回觸發位，不急著接。"}
    if score >= ENTRY_READY_SCORE and f(plan.get("early_entry_low")) <= live <= f(plan.get("early_entry_high")):
        return {"code": "EARLY_LIMIT_READY", "label": "爆發前可小倉掛單", "instruction": "尚未突破、價格在提前佈局區；建議只掛預定倉位 40%，其餘等突破回踩。"}
    if live < f(plan.get("early_entry_low")):
        return {"code": "WAIT_STABILIZE", "label": "等待止跌", "instruction": "現價低於理想提前區，先等 15m 收線重新穩住，不往下追接。"}
    if live <= f(plan.get("chase_cap")):
        return {"code": "WAIT_ENTRY", "label": "等待掛單區", "instruction": "條件仍在，但現價不在理想區；只預掛固定區間，不追市價。"}
    return {"code": "OBSERVE", "label": "只觀察", "instruction": "尚未形成可執行條件。"}


async def fetch_universe() -> list[UniverseItem]:
    if DEMO_MODE:
        symbols = ["SIREN_USDT", "RAVE_USDT", "LAB_USDT", "ARKM_USDT", "WIF_USDT", "ENA_USDT", "ONDO_USDT", "ZORA_USDT"]
        return [
            UniverseItem(s, s.split("_")[0], 4_500_000 + i * 950_000, 8_000_000 + i * 1_250_000, 2.0 + i * 0.45, 0.5 + i * 0.12, 0.0001, {})
            for i, s in enumerate(symbols)
        ]
    assert CLIENT
    spot_raw, fut_raw = await asyncio.gather(CLIENT.gate("/spot/tickers"), CLIENT.gate("/futures/usdt/tickers"))
    spot_map = {str(x.get("currency_pair")): x for x in spot_raw if isinstance(x, dict) and str(x.get("currency_pair", "")).endswith("_USDT")}
    fut_map = {str(x.get("contract")): x for x in fut_raw if isinstance(x, dict) and str(x.get("contract", "")).endswith("_USDT")}
    items: list[UniverseItem] = []
    for symbol, spot in spot_map.items():
        fut = fut_map.get(symbol)
        if not fut:
            continue
        base = symbol.removesuffix("_USDT")
        if is_bad_base(base):
            continue
        spot_vol, fut_vol = ticker_quote_volume(spot), ticker_quote_volume(fut)
        last, change = f(spot.get("last")), ticker_change_pct(spot)
        if last <= 0 or spot_vol < MIN_SPOT_QUOTE_VOLUME_USDT or fut_vol < MIN_FUTURES_QUOTE_VOLUME_USDT:
            continue
        if change > MAX_24H_PUMP_PCT * 1.5 or change < -35:
            continue
        items.append(UniverseItem(symbol, base, spot_vol, fut_vol, change, last, f(fut.get("funding_rate", fut.get("funding_rate_indicative"))), fut))
    items.sort(
        key=lambda x: math.log10(x.spot_volume + 1) + 0.65 * math.log10(x.futures_volume + 1) - max(x.change_24h - 6, 0) * 0.11,
        reverse=True,
    )
    return items[:MAX_UNIVERSE]


async def fetch_symbol_data(item: UniverseItem) -> tuple[list[dict[str, float]], list[dict[str, float]], list[dict[str, float]], list[dict[str, float]], list[dict[str, float]]]:
    if DEMO_MODE:
        return (
            demo_candles(item.symbol, "15m", 200),
            demo_candles(item.symbol, "1h", 120),
            demo_candles(item.symbol, "4h", 80),
            demo_candles(item.symbol + "F", "15m", 200),
            demo_oi(item.symbol, 110),
        )
    assert CLIENT
    raw = await asyncio.gather(
        CLIENT.gate("/spot/candlesticks", {"currency_pair": item.symbol, "interval": "15m", "limit": 200}),
        CLIENT.gate("/spot/candlesticks", {"currency_pair": item.symbol, "interval": "1h", "limit": 120}),
        CLIENT.gate("/spot/candlesticks", {"currency_pair": item.symbol, "interval": "4h", "limit": 80}),
        CLIENT.gate("/futures/usdt/candlesticks", {"contract": item.symbol, "interval": "15m", "limit": 200}),
        CLIENT.gate("/futures/usdt/contract_stats", {"contract": item.symbol, "interval": "15m", "limit": 110}),
        return_exceptions=True,
    )
    vals = [[] if isinstance(x, Exception) else x for x in raw]
    return normalize_candles(vals[0], "15m"), normalize_candles(vals[1], "1h"), normalize_candles(vals[2], "4h"), normalize_candles(vals[3], "15m"), normalize_oi(vals[4])


async def fetch_coinglass_market_map() -> dict[str, dict[str, Any]]:
    if not COINGLASS_API_KEY or DEMO_MODE:
        return {}
    assert CLIENT
    out: dict[str, dict[str, Any]] = {}
    for page in range(1, COINGLASS_MARKET_PAGES + 1):
        try:
            raw = await CLIENT.coinglass(
                "/api/futures/coins-markets",
                {"exchange_list": COINGLASS_EXCHANGE.lower(), "page": page, "per_page": COINGLASS_PER_PAGE},
            )
            data = raw.get("data", []) if isinstance(raw, dict) else []
            if not data:
                break
            for row in data:
                if isinstance(row, dict) and row.get("symbol"):
                    out[str(row["symbol"]).upper()] = row
            if len(data) < COINGLASS_PER_PAGE:
                break
        except Exception as exc:  # noqa: BLE001
            logger.info("CoinGlass coins-markets unavailable, Gate-only fallback: %s", exc)
            break
    return out


async def fetch_btc_regime() -> dict[str, Any]:
    if DEMO_MODE:
        return {"label": "BTC環境偏多", "score": 5, "bearish": False, "change_4h": 1.1}
    assert CLIENT
    raw = await CLIENT.gate("/spot/candlesticks", {"currency_pair": "BTC_USDT", "interval": "1h", "limit": 120})
    return compute_btc_regime(normalize_candles(raw, "1h"))


async def enrich_orderbook(signal: dict[str, Any]) -> None:
    if DEMO_MODE:
        signal["orderbook"] = {"spread_pct": 0.07, "imbalance": 1.34, "bid_depth_usdt": 82_000, "ask_depth_usdt": 61_000}
        signal["score"] = round(clamp(f(signal["score"]) + 4, 0, 100), 2)
        return
    assert CLIENT
    try:
        raw = await CLIENT.gate("/spot/order_book", {"currency_pair": signal["symbol"], "limit": 30, "with_id": "false"})
        bids = raw.get("bids", []) if isinstance(raw, dict) else []
        asks = raw.get("asks", []) if isinstance(raw, dict) else []
        bid_depth = sum(f(x[0]) * f(x[1]) for x in bids if len(x) >= 2)
        ask_depth = sum(f(x[0]) * f(x[1]) for x in asks if len(x) >= 2)
        best_bid = f(bids[0][0]) if bids else 0
        best_ask = f(asks[0][0]) if asks else 0
        mid = (best_bid + best_ask) / 2 if best_bid and best_ask else 0
        spread = (best_ask - best_bid) / mid * 100 if mid else 99
        imbalance = bid_depth / max(ask_depth, 1e-12)
        signal["orderbook"] = {"spread_pct": round(spread, 4), "imbalance": round(imbalance, 3), "bid_depth_usdt": bid_depth, "ask_depth_usdt": ask_depth}
        bonus = 0.0
        if spread <= 0.12:
            bonus += 2
        elif spread > MAX_SPREAD_PCT:
            bonus -= 10
            signal["warnings"].append(f"買賣價差 {spread:.3f}% 過大")
        if 1.12 <= imbalance <= 2.8:
            bonus += 3
            signal["reasons"].append(f"訂單簿買方深度約為賣方 {imbalance:.2f} 倍")
        elif imbalance < 0.62:
            bonus -= 3
        signal["components"]["訂單簿"] = round(bonus, 2)
        signal["score"] = round(clamp(f(signal["score"]) + bonus, 0, 100), 2)
        if f(signal["score"]) < MIN_SETUP_SCORE:
            signal["setup_status"] = "WATCH"
            signal["plan"]["plan_valid"] = False
    except Exception as exc:  # noqa: BLE001
        signal["orderbook"] = {"error": str(exc)}


def lifecycle_for(payload: dict[str, Any], expires_at: int) -> str:
    plan = payload.get("plan", {})
    closed = f(payload.get("closed_price"))
    if closed <= f(plan.get("structural_invalidation")):
        return "INVALIDATED"
    if now_ts() >= expires_at:
        return "EXPIRED"
    return "ACTIVE" if bool(plan.get("plan_valid")) else "WATCH"


def merge_and_save_setups(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    saved: list[dict[str, Any]] = []
    current = now_ts()
    with db_conn() as conn:
        existing_rows = {r["symbol"]: r for r in conn.execute("SELECT * FROM setups_v2").fetchall()}
        for fresh in signals:
            symbol = fresh["symbol"]
            old = existing_rows.get(symbol)
            preserve = False
            if old:
                old_payload = parse_json(old["payload"], {})
                old_status = lifecycle_for(old_payload, int(old["expires_at"]))
                preserve = old_status in {"ACTIVE", "WATCH"} and current < int(old["expires_at"]) and f(old_payload.get("closed_price")) > f(old_payload.get("plan", {}).get("structural_invalidation"))
            if preserve:
                # Metrics may update, but the entire trade plan remains frozen.
                old_plan = old_payload.get("plan", {})
                fresh["plan"] = old_plan
                fresh["plan_id"] = old["plan_id"]
                fresh["plan_created_at"] = int(old["plan_created_at"])
                fresh["expires_at"] = int(old["expires_at"])
                fresh["setup_status"] = lifecycle_for(fresh, int(old["expires_at"]))
            else:
                created = current
                expires = int(fresh["last_closed_15m"]) + 900 + PLAN_TTL_CANDLES * 900
                fresh["plan_id"] = plan_hash(symbol, f(fresh["plan"].get("breakout_trigger")), int(fresh["last_closed_15m"]), created)
                fresh["plan_created_at"] = created
                fresh["expires_at"] = expires
                fresh["setup_status"] = lifecycle_for(fresh, expires)

            live = LIVE_PRICES.get(symbol, {}).get("price", f(fresh.get("closed_price")))
            action = classify_action(fresh, live)
            fresh["action_code"] = action["code"]
            fresh["action_label"] = action["label"]
            fresh["trade_instruction"] = action["instruction"]
            fresh["updated_at"] = current
            fresh["plan_created_at_iso"] = local_iso(fresh["plan_created_at"])
            fresh["expires_at_iso"] = local_iso(fresh["expires_at"])
            conn.execute(
                """INSERT INTO setups_v2(symbol,plan_id,setup_status,score,action_code,plan_created_at,expires_at,updated_at,last_closed_15m,payload)
                   VALUES(?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(symbol) DO UPDATE SET plan_id=excluded.plan_id,setup_status=excluded.setup_status,score=excluded.score,
                   action_code=excluded.action_code,plan_created_at=excluded.plan_created_at,expires_at=excluded.expires_at,
                   updated_at=excluded.updated_at,last_closed_15m=excluded.last_closed_15m,payload=excluded.payload""",
                (symbol, fresh["plan_id"], fresh["setup_status"], fresh["score"], fresh["action_code"], fresh["plan_created_at"], fresh["expires_at"], current, fresh["last_closed_15m"], safe_json(fresh)),
            )
            saved.append(fresh)

        # Expire or invalidate older rows even if they are absent from this scan.
        for symbol, row in existing_rows.items():
            if symbol in {s["symbol"] for s in signals}:
                continue
            payload = parse_json(row["payload"], {})
            status = lifecycle_for(payload, int(row["expires_at"]))
            if status != row["setup_status"]:
                payload["setup_status"] = status
                action = classify_action(payload, LIVE_PRICES.get(symbol, {}).get("price"))
                payload.update({"action_code": action["code"], "action_label": action["label"], "trade_instruction": action["instruction"]})
                conn.execute("UPDATE setups_v2 SET setup_status=?,action_code=?,updated_at=?,payload=? WHERE symbol=?", (status, action["code"], current, safe_json(payload), symbol))
    return saved


async def maybe_alert(signal: dict[str, Any]) -> None:
    if f(signal.get("score")) < ALERT_SCORE or signal.get("action_code") not in {"EARLY_LIMIT_READY", "BREAKOUT_RETEST_READY"}:
        return
    key = f"{signal['plan_id']}:{signal['action_code']}"
    with db_conn() as conn:
        if conn.execute("SELECT 1 FROM alerts_v2 WHERE alert_key=?", (key,)).fetchone():
            return
        conn.execute("INSERT INTO alerts_v2(alert_key,sent_at) VALUES(?,?)", (key, now_ts()))
    p = signal["plan"]
    assert CLIENT
    await CLIENT.post_discord(
        f"🚀 **{signal['symbol']} 爆發前候選**｜{signal['action_label']}｜分數 {signal['score']}\n"
        f"固定計畫 {signal['plan_id']}｜提前區 {p['early_entry_low']:.8g}–{p['early_entry_high']:.8g}｜回測區 {p['retest_entry_low']:.8g}–{p['retest_entry_high']:.8g}\n"
        f"SL {p['stop']:.8g}｜TP {p['tp1']:.8g}/{p['tp2']:.8g}/{p['tp3']:.8g}｜追價上限 {p['chase_cap']:.8g}"
    )


async def run_scan(force: bool = False) -> dict[str, Any]:
    if STATE["scan_running"]:
        return {"message": "掃描已在執行中"}
    STATE["scan_running"] = True
    STATE["last_scan_started"] = now_ts()
    STATE["last_scan_error"] = None
    try:
        universe, btc, cg_map = await asyncio.gather(fetch_universe(), fetch_btc_regime(), fetch_coinglass_market_map())
        STATE["last_universe_size"] = len(universe)
        deep = universe[:DEEP_SCAN_LIMIT]
        STATE["last_deep_scanned"] = len(deep)

        async def one(item: UniverseItem) -> dict[str, Any] | None:
            try:
                data = await fetch_symbol_data(item)
                return build_signal(item, *data, btc, cg_map.get(item.base))
            except Exception as exc:  # noqa: BLE001
                logger.debug("Deep scan failed %s: %s", item.symbol, exc)
                return None

        results = await asyncio.gather(*(one(item) for item in deep))
        signals = [x for x in results if x is not None]
        signals.sort(key=lambda x: f(x.get("score")), reverse=True)
        for signal in signals[:ORDERBOOK_FINALISTS]:
            await enrich_orderbook(signal)
        signals.sort(key=lambda x: f(x.get("score")), reverse=True)
        signals = signals[:MAX_SIGNALS]
        saved = merge_and_save_setups(signals)
        for signal in saved:
            await maybe_alert(signal)
        STATE["last_scan_finished"] = now_ts()
        return {"message": f"掃描完成：Universe {len(universe)}，深度分析 {len(deep)}，保存 {len(saved)} 個固定計畫"}
    except Exception as exc:  # noqa: BLE001
        STATE["last_scan_error"] = str(exc)
        logger.exception("Scan failed")
        raise
    finally:
        STATE["scan_running"] = False

# ============================================================
# Live prices
# ============================================================


async def refresh_live_prices() -> None:
    current = now_ts()
    if DEMO_MODE:
        with db_conn() as conn:
            rows = conn.execute("SELECT symbol,payload FROM setups_v2").fetchall()
        for row in rows:
            payload = parse_json(row["payload"], {})
            base = f(payload.get("closed_price"), 1)
            old = LIVE_PRICES.get(row["symbol"], {}).get("price", base)
            move = random.uniform(-0.0008, 0.0012)
            LIVE_PRICES[row["symbol"]] = {"price": max(base * 0.96, old * (1 + move)), "ts": current}
    else:
        assert CLIENT
        raw = await CLIENT.gate("/spot/tickers")
        for item in raw if isinstance(raw, list) else []:
            symbol = str(item.get("currency_pair", ""))
            price = f(item.get("last"))
            if symbol.endswith("_USDT") and price > 0:
                LIVE_PRICES[symbol] = {"price": price, "ts": current}
    STATE["last_live_update"] = current
    with db_conn() as conn:
        open_rows = conn.execute("SELECT id,symbol,current_price,highest_live FROM positions_v2 WHERE status='OPEN'").fetchall()
        for row in open_rows:
            live = LIVE_PRICES.get(row["symbol"], {}).get("price")
            if live:
                conn.execute("UPDATE positions_v2 SET current_price=?,highest_live=? WHERE id=?", (live, max(f(row["highest_live"]), live), row["id"]))

# ============================================================
# Position tracking
# ============================================================


class CreatePositionRequest(BaseModel):
    symbol: str
    entry_price: float = Field(gt=0)
    notional: float = Field(default=DEFAULT_NOTIONAL_USDT, gt=0)


class ManualCloseRequest(BaseModel):
    exit_price: float | None = Field(default=None, gt=0)
    reason: str = "手動平倉"


class AccountBalanceRequest(BaseModel):
    balance_usdt: float = Field(gt=0, le=1_000_000_000)


def log_event(conn: sqlite3.Connection, position_id: int, event_type: str, price: float, qty: float = 0, pnl: float = 0, detail: str = "") -> None:
    conn.execute(
        "INSERT INTO position_events_v2(position_id,event_time,event_type,price,qty,pnl,detail) VALUES(?,?,?,?,?,?,?)",
        (position_id, now_ts(), event_type, price, qty, pnl, detail),
    )


def close_position_qty(conn: sqlite3.Connection, row: sqlite3.Row, qty: float, price: float, reason: str, event_type: str) -> None:
    qty = min(max(qty, 0), f(row["remaining_qty"]))
    if qty <= 0:
        return
    pnl = (price - f(row["entry_price"])) * qty
    remaining = max(0.0, f(row["remaining_qty"]) - qty)
    realized = f(row["realized_pnl"]) + pnl
    if remaining <= f(row["original_qty"]) * 1e-8:
        conn.execute(
            "UPDATE positions_v2 SET remaining_qty=0,realized_pnl=?,status='CLOSED',closed_at=?,close_reason=?,current_price=? WHERE id=?",
            (realized, now_ts(), reason, price, row["id"]),
        )
    else:
        conn.execute("UPDATE positions_v2 SET remaining_qty=?,realized_pnl=?,current_price=? WHERE id=?", (remaining, realized, price, row["id"]))
    log_event(conn, row["id"], event_type, price, qty, pnl, reason)


def row_position(row: sqlite3.Row) -> dict[str, Any]:
    tps = parse_json(row["tps_json"], [])
    current_price = LIVE_PRICES.get(row["symbol"], {}).get("price", f(row["current_price"]))
    unrealized = (current_price - f(row["entry_price"])) * f(row["remaining_qty"]) if row["status"] == "OPEN" else 0.0
    total = f(row["realized_pnl"]) + unrealized
    return {
        "id": row["id"], "symbol": row["symbol"], "plan_id": row["plan_id"], "status": row["status"],
        "entry_price": row["entry_price"], "notional": row["notional"], "current_price": current_price,
        "original_stop": row["original_stop"], "active_stop": row["active_stop"], "structural_invalidation": row["structural_invalidation"],
        "tps": tps, "realized_pnl": round(f(row["realized_pnl"]), 6), "unrealized_pnl": round(unrealized, 6),
        "total_pnl": round(total, 6), "pnl_pct": round(total / max(f(row["notional"]), 1e-12) * 100, 3),
        "remaining_qty": f(row["remaining_qty"]), "original_qty": f(row["original_qty"]),
        "remaining_pct": round(f(row["remaining_qty"]) / max(f(row["original_qty"]), 1e-12) * 100, 2),
        "trend_status": row["trend_status"], "trailing_enabled": bool(row["trailing_enabled"]),
        "override_warning": row["override_warning"], "opened_at": row["opened_at"], "opened_at_iso": local_iso(row["opened_at"]),
        "closed_at": row["closed_at"], "closed_at_iso": local_iso(row["closed_at"]) if row["closed_at"] else None,
        "close_reason": row["close_reason"], "last_processed_1m": row["last_processed_1m"],
        "account_balance_at_entry": f(row["account_balance_at_entry"]),
        "planned_risk_usdt": f(row["planned_risk_usdt"]),
        "planned_risk_pct": f(row["planned_risk_pct"]),
    }


async def fetch_tracking_candles(symbol: str, interval: str, limit: int) -> list[dict[str, float]]:
    if DEMO_MODE:
        return demo_candles(symbol + str(now_ts() // INTERVAL_SECONDS[interval]), interval, limit)
    assert CLIENT
    raw = await CLIENT.gate("/spot/candlesticks", {"currency_pair": symbol, "interval": interval, "limit": limit})
    return normalize_candles(raw, interval)


def position_trend(candles: list[dict[str, float]], snapshot: dict[str, Any]) -> str:
    if len(candles) < 25:
        return "趨勢尚未改變"
    plan = snapshot.get("plan", {})
    closes = [c["close"] for c in candles]
    last = closes[-1]
    e20 = ema(closes, 20)[-1]
    atr15 = atr(candles, 14)
    if last <= f(plan.get("structural_invalidation")):
        return "嚴重破壞"
    if last <= f(plan.get("stop")) + 0.35 * atr15:
        return "中級破壞"
    if last < min(e20, f(plan.get("base_vwap"))):
        return "初級破壞"
    if last < f(plan.get("breakout_trigger")):
        return "趨勢尚未改變"
    if last < f(plan.get("breakout_trigger")) + 0.35 * atr15:
        return "趨勢確認初期"
    if last < f(plan.get("tp1")):
        return "趨勢確認中級"
    return "完整趨勢確認"


async def process_position(position_id: int) -> None:
    with db_conn() as conn:
        row = conn.execute("SELECT * FROM positions_v2 WHERE id=? AND status='OPEN'", (position_id,)).fetchone()
    if not row:
        return
    symbol = row["symbol"]
    one_min, fifteen = await asyncio.gather(fetch_tracking_candles(symbol, "1m", 25), fetch_tracking_candles(symbol, "15m", 80))
    if not one_min:
        return

    with db_conn() as conn:
        current = conn.execute("SELECT * FROM positions_v2 WHERE id=? AND status='OPEN'", (position_id,)).fetchone()
        if not current:
            return
        tps = parse_json(current["tps_json"], [])
        for candle in one_min:
            if int(candle["time"]) <= int(current["last_processed_1m"]):
                continue
            current = conn.execute("SELECT * FROM positions_v2 WHERE id=? AND status='OPEN'", (position_id,)).fetchone()
            if not current:
                break
            low, high = f(candle["low"]), f(candle["high"])
            active_stop = f(current["active_stop"])
            pending = [tp for tp in tps if not tp.get("hit")]
            target_touched = any(high >= f(tp.get("price")) for tp in pending)
            stop_touched = low <= active_stop
            if stop_touched and (INTRABAR_POLICY == "conservative" or not target_touched):
                close_position_qty(conn, current, f(current["remaining_qty"]), active_stop, "已收 1m K 觸及保護停損", "STOP")
                break

            for idx, tp in enumerate(tps):
                if tp.get("hit") or high < f(tp.get("price")):
                    continue
                current = conn.execute("SELECT * FROM positions_v2 WHERE id=? AND status='OPEN'", (position_id,)).fetchone()
                if not current:
                    break
                qty = f(current["original_qty"]) * f(tp.get("fraction"))
                close_position_qty(conn, current, qty, f(tp.get("price")), f"{tp['name']} 達標", tp["name"])
                tp["hit"] = True
                tp["hit_time"] = int(candle["time"])
                risk = f(current["entry_price"]) - f(current["original_stop"])
                new_stop = f(current["active_stop"])
                if idx == 0:
                    new_stop = max(new_stop, f(current["entry_price"]))
                elif idx == 1:
                    new_stop = max(new_stop, f(current["entry_price"]) + risk)
                elif idx == 2:
                    conn.execute("UPDATE positions_v2 SET trailing_enabled=1 WHERE id=?", (position_id,))
                conn.execute("UPDATE positions_v2 SET active_stop=?,tps_json=? WHERE id=?", (new_stop, safe_json(tps), position_id))
                log_event(conn, position_id, "STOP_RAISED", new_stop, 0, 0, "依固定持倉規則上移；永不下移")

            conn.execute("UPDATE positions_v2 SET last_processed_1m=? WHERE id=?", (int(candle["time"]), position_id))

        current = conn.execute("SELECT * FROM positions_v2 WHERE id=? AND status='OPEN'", (position_id,)).fetchone()
        if current and fifteen:
            snapshot = parse_json(current["setup_snapshot"], {})
            last15 = int(fifteen[-1]["time"])
            trend = position_trend(fifteen, snapshot)
            highest15 = max(f(current["highest_closed_15m"]), f(fifteen[-1]["close"]))
            new_stop = f(current["active_stop"])
            if bool(current["trailing_enabled"]):
                closes = [c["close"] for c in fifteen]
                atr15 = atr(fifteen, 14)
                trail = max(ema(closes, 20)[-1] - 0.45 * atr15, highest15 - 2.0 * atr15)
                new_stop = max(new_stop, trail)
            conn.execute(
                "UPDATE positions_v2 SET trend_status=?,last_trend_15m=?,highest_closed_15m=?,active_stop=? WHERE id=?",
                (trend, last15, highest15, new_stop, position_id),
            )


async def track_all_positions() -> None:
    with db_conn() as conn:
        ids = [r["id"] for r in conn.execute("SELECT id FROM positions_v2 WHERE status='OPEN'").fetchall()]
    await asyncio.gather(*(process_position(pid) for pid in ids), return_exceptions=True)

# ============================================================
# App lifecycle
# ============================================================


@asynccontextmanager
async def lifespan(_: FastAPI):
    global CLIENT
    init_db()
    CLIENT = MarketClient()

    async def scan_loop() -> None:
        await asyncio.sleep(1)
        if AUTO_SCAN:
            try:
                await run_scan()
            except Exception:
                pass
        while True:
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)
            if AUTO_SCAN:
                try:
                    await run_scan()
                except Exception:
                    pass

    async def price_loop() -> None:
        await asyncio.sleep(0.5)
        while True:
            try:
                await refresh_live_prices()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Live price refresh failed: %s", exc)
            await asyncio.sleep(LIVE_PRICE_INTERVAL_SECONDS)

    async def tracker_loop() -> None:
        await asyncio.sleep(4)
        while True:
            try:
                await track_all_positions()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Position tracker failed: %s", exc)
            await asyncio.sleep(TRACK_INTERVAL_SECONDS)

    tasks = [asyncio.create_task(scan_loop()), asyncio.create_task(price_loop()), asyncio.create_task(tracker_loop())]
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if CLIENT:
            await CLIENT.close()


app = FastAPI(title=APP_NAME, version=VERSION, lifespan=lifespan)

# ============================================================
# API
# ============================================================


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse(HTML_PAGE)


@app.get("/health")
async def health() -> dict[str, Any]:
    return {**STATE, "app": APP_NAME, "version": VERSION, "db": str(DB_PATH), "demo": DEMO_MODE, "server_time": now_ts(),
            "risk_per_trade_pct": RISK_PER_TRADE_PCT, "account_balance_usdt": get_account_balance()}


@app.get("/api/live")
async def live_prices(symbols: str = "") -> dict[str, Any]:
    requested = {x.strip().upper() for x in symbols.split(",") if x.strip()}
    data = {k: v for k, v in LIVE_PRICES.items() if not requested or k in requested}
    return {"updated_at": STATE["last_live_update"], "prices": data}


@app.get("/api/account")
async def account_settings() -> dict[str, Any]:
    balance = get_account_balance()
    return {
        "balance_usdt": balance,
        "risk_per_trade_pct": RISK_PER_TRADE_PCT,
        "risk_budget_usdt": round(balance * RISK_PER_TRADE_PCT / 100, 4),
    }


@app.post("/api/account")
async def update_account_settings(req: AccountBalanceRequest) -> dict[str, Any]:
    balance = save_account_balance(req.balance_usdt)
    return {
        "balance_usdt": balance,
        "risk_per_trade_pct": RISK_PER_TRADE_PCT,
        "risk_budget_usdt": round(balance * RISK_PER_TRADE_PCT / 100, 4),
    }


@app.get("/api/setups")
async def list_setups(min_score: float = Query(default=0, ge=0, le=100)) -> list[dict[str, Any]]:
    with db_conn() as conn:
        rows = conn.execute("SELECT * FROM setups_v2 WHERE score>=? ORDER BY score DESC,updated_at DESC", (min_score,)).fetchall()
    output: list[dict[str, Any]] = []
    account_balance = get_account_balance()
    for row in rows:
        payload = parse_json(row["payload"], {})
        payload["setup_status"] = lifecycle_for(payload, int(row["expires_at"]))
        live_row = LIVE_PRICES.get(row["symbol"], {})
        live = f(live_row.get("price"), f(payload.get("closed_price")))
        action = classify_action(payload, live)
        payload.update({
            "live_price": live,
            "live_price_ts": int(f(live_row.get("ts"))),
            "live_price_iso": local_iso(f(live_row.get("ts"))) if live_row.get("ts") else None,
            "plan_id": row["plan_id"], "plan_created_at": row["plan_created_at"], "plan_created_at_iso": local_iso(row["plan_created_at"]),
            "expires_at": row["expires_at"], "expires_at_iso": local_iso(row["expires_at"]),
            "action_code": action["code"], "action_label": action["label"], "trade_instruction": action["instruction"],
            "order_button_enabled": True,
            "risk_sizing": setup_risk_sizing(payload.get("plan", {}), account_balance, action["code"]),
        })
        output.append(payload)
    priority = {"EARLY_LIMIT_READY": 0, "BREAKOUT_RETEST_READY": 0, "WAIT_ENTRY": 1, "WAIT_RETEST": 1, "WAIT_STABILIZE": 2, "DO_NOT_CHASE": 3, "INVALIDATED": 4, "EXPIRED": 5}
    output.sort(key=lambda x: (priority.get(x.get("action_code"), 2), -f(x.get("score"))))
    return output


@app.post("/api/scan")
async def scan_now() -> dict[str, Any]:
    try:
        return await run_scan(force=True)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"掃描失敗：{exc}") from exc


@app.get("/api/positions")
async def list_positions(status: str = "ALL") -> list[dict[str, Any]]:
    status = status.upper()
    with db_conn() as conn:
        if status in {"OPEN", "CLOSED"}:
            rows = conn.execute("SELECT * FROM positions_v2 WHERE status=? ORDER BY opened_at DESC", (status,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM positions_v2 ORDER BY opened_at DESC").fetchall()
    return [row_position(r) for r in rows]


@app.post("/api/positions")
async def create_position(req: CreatePositionRequest) -> dict[str, Any]:
    symbol = req.symbol.upper().strip()
    with db_conn() as conn:
        setup_row = conn.execute("SELECT * FROM setups_v2 WHERE symbol=?", (symbol,)).fetchone()
        if not setup_row:
            raise HTTPException(status_code=404, detail="找不到此幣的固定掃描計畫")
        snapshot = parse_json(setup_row["payload"], {})
        plan = snapshot.get("plan", {})
        stop = f(plan.get("stop"))
        if stop <= 0 or req.entry_price <= stop:
            raise HTTPException(status_code=400, detail="實際成交價必須高於固定結構停損")
        risk = req.entry_price - stop
        warnings: list[str] = []
        action = classify_action(snapshot, req.entry_price)
        if action["code"] not in {"EARLY_LIMIT_READY", "BREAKOUT_RETEST_READY"}:
            warnings.append(f"此成交屬手動覆寫：{action['label']}")
        if req.entry_price > f(plan.get("chase_cap")):
            warnings.append("成交價高於追價上限；系統仍允許紀錄，但風險明顯較高")
        risk_pct = risk / req.entry_price * 100
        if risk_pct > MAX_STOP_PCT:
            warnings.append(f"實際成交後停損距離 {risk_pct:.2f}% 超過建議上限")
        account_balance = get_account_balance()
        sizing = calculate_risk_sizing(account_balance, req.entry_price, stop)
        actual_risk_usdt = req.notional * risk / req.entry_price
        actual_risk_pct_balance = actual_risk_usdt / max(account_balance, 1e-12) * 100
        if f(sizing.get("exceeds_1x_balance")):
            warnings.append(
                f"要讓固定 SL 剛好虧帳戶 {RISK_PER_TRADE_PCT:.2f}%，名目金額超過帳戶餘額；至少需要約 {f(sizing.get('required_leverage')):.2f} 倍。若堅持 1 倍，最多下 {account_balance:.2f} U。"
            )
        if req.notional > f(sizing.get("recommended_notional_usdt")) * 1.01:
            warnings.append(
                f"此名目金額在固定 SL 的預計虧損約 {actual_risk_usdt:.2f} U（帳戶 {actual_risk_pct_balance:.2f}%），高於單筆 {RISK_PER_TRADE_PCT:.2f}% 風險設定"
            )
        elif req.notional < f(sizing.get("recommended_notional_usdt")) * 0.99:
            warnings.append(f"名目金額低於 2% 風險建議值；此單實際最大風險約 {actual_risk_pct_balance:.2f}% 帳戶資金")

        # Freeze the execution plan once, using actual fill price while respecting the setup's absolute structure.
        tp1 = max(f(plan.get("tp1")), req.entry_price + 1.5 * risk)
        tp2 = max(f(plan.get("tp2")), req.entry_price + 3.0 * risk)
        tp3 = max(f(plan.get("tp3")), req.entry_price + 5.0 * risk)
        tps = [
            {"name": "TP1", "price": tp1, "fraction": 0.25, "hit": False, "rule_after": "停損移至成本"},
            {"name": "TP2", "price": tp2, "fraction": 0.30, "hit": False, "rule_after": "停損移至 +1R"},
            {"name": "TP3", "price": tp3, "fraction": 0.30, "hit": False, "rule_after": "剩餘 15% 啟動 15m 追蹤停損"},
        ]
        qty = req.notional / req.entry_price
        live = LIVE_PRICES.get(symbol, {}).get("price", req.entry_price)
        cursor = conn.execute(
            """INSERT INTO positions_v2(symbol,plan_id,status,entry_price,notional,original_qty,remaining_qty,original_stop,active_stop,
               structural_invalidation,tps_json,realized_pnl,current_price,highest_live,highest_closed_15m,last_processed_1m,last_trend_15m,
               trend_status,trailing_enabled,override_warning,opened_at,setup_snapshot,account_balance_at_entry,planned_risk_usdt,planned_risk_pct)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (symbol, setup_row["plan_id"], "OPEN", req.entry_price, req.notional, qty, qty, stop, stop,
             f(plan.get("structural_invalidation")), safe_json(tps), 0, live, max(live, req.entry_price), req.entry_price, 0, 0,
             snapshot.get("trend_status", "趨勢尚未改變"), 0, "；".join(warnings), now_ts(), safe_json(snapshot),
             account_balance, actual_risk_usdt, actual_risk_pct_balance),
        )
        pid = int(cursor.lastrowid)
        log_event(conn, pid, "OPEN", req.entry_price, qty, 0, "固定執行計畫已鎖定；1 倍槓桿計算")
        row = conn.execute("SELECT * FROM positions_v2 WHERE id=?", (pid,)).fetchone()
    return {"position": row_position(row), "warnings": warnings, "risk_sizing": sizing}


@app.post("/api/positions/{position_id}/close")
async def manual_close(position_id: int, req: ManualCloseRequest) -> dict[str, Any]:
    with db_conn() as conn:
        row = conn.execute("SELECT * FROM positions_v2 WHERE id=? AND status='OPEN'", (position_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="找不到未平倉部位")
        price = req.exit_price or LIVE_PRICES.get(row["symbol"], {}).get("price") or f(row["current_price"])
        close_position_qty(conn, row, f(row["remaining_qty"]), f(price), req.reason, "MANUAL_CLOSE")
        updated = conn.execute("SELECT * FROM positions_v2 WHERE id=?", (position_id,)).fetchone()
    return row_position(updated)


@app.get("/api/positions/{position_id}/events")
async def position_events(position_id: int) -> list[dict[str, Any]]:
    with db_conn() as conn:
        rows = conn.execute("SELECT * FROM position_events_v2 WHERE position_id=? ORDER BY event_time", (position_id,)).fetchall()
    return [{**dict(r), "event_time_iso": local_iso(r["event_time"])} for r in rows]


@app.get("/api/stats")
async def stats() -> dict[str, Any]:
    positions = await list_positions()
    closed = [x for x in positions if x["status"] == "CLOSED"]
    opened = [x for x in positions if x["status"] == "OPEN"]
    wins = [x for x in closed if x["total_pnl"] > 0]
    gross_win = sum(max(x["total_pnl"], 0) for x in closed)
    gross_loss = abs(sum(min(x["total_pnl"], 0) for x in closed))
    return {
        "open_count": len(opened), "closed_count": len(closed),
        "win_rate_pct": len(wins) / len(closed) * 100 if closed else 0,
        "profit_factor": gross_win / gross_loss if gross_loss else (999 if gross_win else 0),
        "realized_pnl": sum(x["realized_pnl"] for x in positions),
        "open_pnl": sum(x["unrealized_pnl"] for x in opened),
    }


@app.get("/api/export.csv")
async def export_csv() -> StreamingResponse:
    positions = await list_positions()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "symbol", "plan_id", "status", "entry_price", "notional", "account_balance_at_entry", "planned_risk_usdt", "planned_risk_pct", "current_price", "remaining_pct", "realized_pnl", "unrealized_pnl", "total_pnl", "pnl_pct", "trend_status", "original_stop", "active_stop", "opened_at", "closed_at", "close_reason", "override_warning"])
    for x in positions:
        writer.writerow([x["id"], x["symbol"], x["plan_id"], x["status"], x["entry_price"], x["notional"], x["account_balance_at_entry"], x["planned_risk_usdt"], x["planned_risk_pct"], x["current_price"], x["remaining_pct"], x["realized_pnl"], x["unrealized_pnl"], x["total_pnl"], x["pnl_pct"], x["trend_status"], x["original_stop"], x["active_stop"], x["opened_at_iso"], x["closed_at_iso"], x["close_reason"], x["override_warning"]])
    output.seek(0)
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=gate_breakout_positions_v2_1.csv"})

# ============================================================
# UI
# ============================================================


HTML_PAGE = r"""
<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Gate 爆發前掃幣雷達 v2.1</title><style>
:root{--bg:#07101d;--panel:#0d1a2a;--panel2:#101f32;--line:#203650;--text:#e7f0fb;--muted:#8fa6bf;--green:#5ee0a2;--red:#ff7a8d;--yellow:#ffd36a;--blue:#67b8ff;--purple:#bd9cff}
*{box-sizing:border-box}body{margin:0;background:linear-gradient(145deg,#06101d,#091524 45%,#07101d);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans TC",sans-serif}.wrap{max-width:1680px;margin:auto;padding:18px}.top{display:flex;justify-content:space-between;gap:16px;align-items:flex-start;margin-bottom:14px}.title h1{font-size:25px;margin:0}.title p{color:var(--muted);margin:6px 0 0}.actions{display:flex;gap:8px;flex-wrap:wrap}button,.btn{border:1px solid var(--line);background:#13253a;color:var(--text);padding:9px 13px;border-radius:9px;cursor:pointer;text-decoration:none;font-weight:700}button:hover,.btn:hover{border-color:#3f6a99}.primary{background:#145d4b;border-color:#237c65}.danger{background:#5b2230;border-color:#8d3448}.riskbar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;background:#0d2337;border:1px solid #285174;border-radius:12px;padding:11px 13px;margin-bottom:13px}.riskbar input{width:150px;background:#071321;color:var(--text);border:1px solid var(--line);padding:8px;border-radius:8px}.riskmetric{background:#081522;border:1px solid var(--line);border-radius:9px;padding:7px 10px}.notice{background:#28210f;border:1px solid #554819;color:#ffe290;padding:11px 13px;border-radius:11px;margin-bottom:13px}.stats{display:grid;grid-template-columns:repeat(6,1fr);gap:9px;margin-bottom:13px}.stat{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:11px}.stat small{color:var(--muted)}.stat b{display:block;font-size:19px;margin-top:4px}.tabs{display:flex;gap:6px;margin-bottom:9px}.tab.active{background:#1c4f7b;border-color:#3175ac}.panel{background:rgba(13,26,42,.94);border:1px solid var(--line);border-radius:14px;overflow:hidden}.hidden{display:none}.toolbar{display:flex;align-items:center;gap:9px;padding:11px;border-bottom:1px solid var(--line);flex-wrap:wrap}.toolbar input{width:74px;background:#071321;color:var(--text);border:1px solid var(--line);padding:7px;border-radius:8px}.tablewrap{overflow:auto}table{width:100%;border-collapse:collapse;min-width:1650px}th,td{padding:10px 9px;border-bottom:1px solid #1c3047;text-align:left;vertical-align:top;font-size:13px}th{position:sticky;top:0;background:#101f32;color:#adc0d6;z-index:2}.sym{font-size:15px;font-weight:900}.muted{color:var(--muted)}.green{color:var(--green)}.red{color:var(--red)}.yellow{color:var(--yellow)}.purple{color:var(--purple)}.money{white-space:nowrap}.badge{display:inline-block;margin-top:4px;padding:4px 8px;border-radius:999px;background:#18304a;border:1px solid #2b5075;font-size:12px}.badge.ready{background:#123d32;border-color:#28745e;color:#76e7bd}.badge.warn{background:#443517;border-color:#745b26;color:#ffd978}.badge.bad{background:#45202a;border-color:#783447;color:#ff9bad}.planbox{border-left:3px solid #376b9a;padding-left:8px}.fixed{font-size:11px;color:#92b8d9}.instruction{max-width:245px;line-height:1.45}.detail{max-width:300px}.detail summary{cursor:pointer;color:var(--blue)}.detail ul{padding-left:17px;margin:6px 0}.live{font-size:16px;font-weight:900}.pulse{animation:pulse .45s ease}@keyframes pulse{50%{opacity:.35}}.modal{position:fixed;inset:0;background:rgba(0,0,0,.72);display:none;align-items:center;justify-content:center;padding:15px;z-index:20}.modal.show{display:flex}.modalbox{width:min(620px,100%);background:#0e1d2e;border:1px solid var(--line);border-radius:15px;padding:18px}.modalbox label{display:block;margin-top:11px;color:var(--muted)}.modalbox input{width:100%;margin-top:5px;padding:11px;border-radius:8px;border:1px solid var(--line);background:#071321;color:var(--text)}.preview{background:#081522;border:1px solid var(--line);border-radius:10px;padding:10px;margin-top:12px;line-height:1.55}.modalactions{display:flex;justify-content:flex-end;gap:8px;margin-top:14px}.footer{color:var(--muted);font-size:12px;margin:12px 2px}@media(max-width:900px){.stats{grid-template-columns:repeat(2,1fr)}.top{flex-direction:column}.wrap{padding:10px}}
</style></head><body><div class="wrap">
<div class="top"><div class="title"><h1>Gate 爆發前掃幣雷達 v2.1</h1><p>目標：未起漲小幣｜不追價｜策略只用已收 K｜即時價格獨立更新｜交易計畫固定鎖定</p></div><div class="actions"><button class="primary" onclick="scanNow()">立即掃描</button><a class="btn" href="/api/export.csv">匯出交易 CSV</a></div></div>
<div class="riskbar"><b>帳戶餘額</b><input id="accountBalance" type="number" min="0.01" step="any" placeholder="輸入 USDT"><button class="primary" onclick="saveAccount()">儲存餘額</button><span class="riskmetric">每筆固定風險 <b id="riskPct">2%</b></span><span class="riskmetric">每筆可承受虧損 <b id="riskBudget">—</b></span><span class="muted">每個訊號會依固定 SL 反推「到 SL 剛好虧帳戶 2%」的目標名目，並標示 1 倍是否足夠。</span></div>
<div class="notice">「即時價」只用來顯示、計算即時損益與判斷是否位於掛單區；掃幣條件只使用已收 15m／1h／4h K。自動止盈止損只使用新收完的 1m K。固定計畫在到期或結構失效前不會因重新掃描而改價。</div>
<div class="stats" id="stats"></div>
<div class="tabs"><button class="tab active" data-tab="setups" onclick="tab('setups')">爆發前候選</button><button class="tab" data-tab="open" onclick="tab('open')">持倉追蹤</button><button class="tab" data-tab="closed" onclick="tab('closed')">已平倉</button></div>
<section id="setups" class="panel"><div class="toolbar">最低評分 <input id="minScore" type="number" min="0" max="100" value="60" onchange="loadSetups()"><span id="scanState" class="muted"></span></div><div class="tablewrap"><table><thead><tr><th>幣種／固定計畫</th><th>目前該做什麼</th><th>即時價／已收價</th><th>未起漲證據</th><th>掛單區</th><th>2%風險倉位</th><th>固定止損／破壞</th><th>固定止盈／預計漲幅</th><th>詳細資料</th><th>操作</th></tr></thead><tbody id="setupRows"></tbody></table></div></section>
<section id="open" class="panel hidden"><div class="tablewrap"><table><thead><tr><th>幣種／狀態</th><th>進場／本金</th><th>即時價</th><th>總損益</th><th>剩餘</th><th>固定原始 SL</th><th>目前保護 SL</th><th>固定 TP</th><th>操作</th></tr></thead><tbody id="openRows"></tbody></table></div></section>
<section id="closed" class="panel hidden"><div class="tablewrap"><table><thead><tr><th>幣種</th><th>計畫</th><th>進場</th><th>損益</th><th>損益率</th><th>平倉原因</th><th>時間</th></tr></thead><tbody id="closedRows"></tbody></table></div></section>
<div class="footer" id="health"></div></div>
<div class="modal" id="orderModal"><div class="modalbox"><h3 id="modalTitle">標記已下單</h3><div id="modalPlan" class="preview"></div><label>實際成交價<input id="entryPrice" type="number" step="any" oninput="renderOrderPreview(true)"></label><label>名目金額（USDT，1 倍）<input id="notional" type="number" step="any" value="100" oninput="renderOrderPreview(false)"></label><button style="margin-top:8px" onclick="applySuggestedNotional()">套用 2% 風險建議名目</button><div id="orderWarning" class="preview"></div><div class="modalactions"><button onclick="closeModal()">取消</button><button class="primary" onclick="confirmOrder()">確認已下單並鎖定計畫</button></div></div></div>
<script>
let setups=[],selected=null,account={balance_usdt:0,risk_per_trade_pct:2,risk_budget_usdt:0};const fmt=(n,d=4)=>{n=Number(n);if(!isFinite(n))return'-';if(Math.abs(n)>=1000)return n.toLocaleString(undefined,{maximumFractionDigits:2});if(Math.abs(n)>=1)return n.toFixed(d);return n.toPrecision(6)};const pc=n=>`${Number(n)>=0?'+':''}${Number(n).toFixed(2)}%`;const cls=n=>Number(n)>=0?'green':'red';
async function j(url,opt){const r=await fetch(url,opt);const x=await r.json();if(!r.ok)throw new Error(x.detail||'請求失敗');return x}
function calcRisk(entry,stop){entry=Number(entry);stop=Number(stop);const bal=Number(account.balance_usdt),pct=Number(account.risk_per_trade_pct),budget=bal*pct/100;if(!(entry>stop&&stop>0&&bal>0))return{valid:false,recommended_notional_usdt:0};const frac=(entry-stop)/entry,theoretical=budget/frac,recommended=theoretical,oneX=Math.min(theoretical,bal),actual=recommended*frac,oneXRisk=oneX*frac;return{valid:true,stop_distance_pct:frac*100,risk_budget_usdt:budget,theoretical_notional_usdt:theoretical,recommended_notional_usdt:recommended,quantity:recommended/entry,actual_risk_usdt:actual,actual_risk_pct_balance:actual/bal*100,exceeds_1x_balance:theoretical>bal+1e-9,required_leverage:theoretical/bal,one_x_notional_usdt:oneX,one_x_quantity:oneX/entry,one_x_actual_risk_usdt:oneXRisk,one_x_actual_risk_pct_balance:oneXRisk/bal*100,capped_by_1x_balance:theoretical>bal+1e-9}}
async function loadAccount(){try{account=await j('/api/account');document.getElementById('accountBalance').value=account.balance_usdt;document.getElementById('riskPct').textContent=Number(account.risk_per_trade_pct).toFixed(2)+'%';document.getElementById('riskBudget').textContent=fmt(account.risk_budget_usdt,2)+' U'}catch(e){console.error(e)}}
async function saveAccount(){const balance=Number(document.getElementById('accountBalance').value);if(!(balance>0)){alert('請輸入有效的帳戶餘額');return}try{account=await j('/api/account',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({balance_usdt:balance})});document.getElementById('riskBudget').textContent=fmt(account.risk_budget_usdt,2)+' U';await loadSetups();alert('帳戶餘額已儲存，每個訊號已重新計算 2% 風險名目金額。')}catch(e){alert(e.message)}}
function sizingCell(s){const z=s.risk_sizing?.selected||{},mode=s.risk_sizing?.selected_mode==='retest'?'回踩參考':'提前參考';if(!z.valid)return '<span class="red">無法計算</span>';return `<b>${mode}</b> ${fmt(z.entry_price)}<br><b class="green">2%目標名目 ${fmt(z.recommended_notional_usdt,2)} U</b><br>到SL虧損 ${fmt(z.actual_risk_usdt,2)} U (${Number(z.actual_risk_pct_balance).toFixed(2)}%)<br>下單數量 ${fmt(z.quantity,6)}<br>${z.exceeds_1x_balance?`<span class="yellow">需至少 ${Number(z.required_leverage).toFixed(2)}x；1倍最多 ${fmt(z.one_x_notional_usdt,2)} U，風險 ${Number(z.one_x_actual_risk_pct_balance).toFixed(2)}%</span>`:`<span class="muted">1倍可執行｜SL距離 ${Number(z.stop_distance_pct).toFixed(2)}%</span>`}`}
function tab(id){document.querySelectorAll('section.panel').forEach(x=>x.classList.add('hidden'));document.getElementById(id).classList.remove('hidden');document.querySelectorAll('.tab').forEach(x=>x.classList.toggle('active',x.dataset.tab===id));if(id!=='setups')loadPositions()}
function badge(action){const ready=['EARLY_LIMIT_READY','BREAKOUT_RETEST_READY'].includes(action.action_code),bad=['INVALIDATED','EXPIRED','DO_NOT_CHASE'].includes(action.action_code);return `<span class="badge ${ready?'ready':bad?'bad':'warn'}">${action.action_label}</span>`}
function liveCell(s){return `<span class="live live-price" data-symbol="${s.symbol}">${fmt(s.live_price)}</span><div class="muted">已收15m ${fmt(s.closed_price)}</div><div class="muted">${s.last_closed_15m_iso}</div><div class="${cls(s.change_4h_pct)}">4h ${pc(s.change_4h_pct)}｜24h ${pc(s.change_24h_pct)}</div>`}
async function loadSetups(){try{setups=await j('/api/setups?min_score='+document.getElementById('minScore').value);document.getElementById('setupRows').innerHTML=setups.map(s=>{const p=s.plan,o=s.orderbook||{},cg=s.coinglass||{};return `<tr><td><div class="sym">${s.symbol}</div><div class="score ${s.score>=82?'green':s.score>=72?'yellow':''}">評分 ${s.score}</div><div class="fixed">Plan ${s.plan_id}<br>建立 ${s.plan_created_at_iso}<br>到期 ${s.expires_at_iso}</div></td><td id="action-${s.symbol}">${badge(s)}<div class="instruction">${s.trade_instruction}</div></td><td>${liveCell(s)}</td><td>現貨量 ${s.volume_ratio}x<br>現貨/合約 ${s.spot_led_ratio}x<br>OI 1h ${pc(s.oi_1h_pct)}<br>OI 4h ${pc(s.oi_4h_pct)}<br>費率 ${s.funding_rate_pct}%<br><span class="muted">市值 ${cg.market_cap_usd?fmt(cg.market_cap_usd/1e6,1)+'M':'-'}</span></td><td class="money planbox"><b>提前 40%</b><br>${fmt(p.early_entry_low)}–${fmt(p.early_entry_high)}<br><b>突破觸發</b> ${fmt(p.breakout_trigger)}<br><b>回踩區</b><br>${fmt(p.retest_entry_low)}–${fmt(p.retest_entry_high)}<br><span class="red">追價上限 ${fmt(p.chase_cap)}</span></td><td class="money">${sizingCell(s)}</td><td class="money">固定 SL ${fmt(p.stop)} <span class="red">-${p.stop_pct}%</span><br>嚴重破壞 ${fmt(p.structural_invalidation)}</td><td class="money">TP1 ${fmt(p.tp1)} <span class="green">+${p.tp1_pct}%</span><br>TP2 ${fmt(p.tp2)} <span class="green">+${p.tp2_pct}%</span><br>TP3 ${fmt(p.tp3)} <span class="green">+${p.tp3_pct}%</span><br>預計主波 ${p.expected_move_low_pct}%–${p.expected_move_high_pct}%<br>RR ${p.rr1}/${p.rr2}/${p.rr3}</td><td><details class="detail"><summary>查看依據</summary><b>優勢</b><ul>${(s.reasons||[]).map(x=>`<li>${x}</li>`).join('')}</ul><b>風險</b><ul>${(s.warnings||[]).map(x=>`<li>${x}</li>`).join('')}</ul><div>價差 ${o.spread_pct??'-'}%｜簿差 ${o.imbalance??'-'}x</div><div>BTC ${s.btc_regime?.label||'-'}</div></details></td><td><button onclick="openOrder('${s.symbol}')">已下單</button><div class="muted">按鈕永遠可用；非建議區會顯示覆寫警告</div></td></tr>`}).join('')||'<tr><td colspan="10" class="muted">尚無候選，請等待首次掃描。</td></tr>'}catch(e){console.error(e)}}
function openOrder(sym){selected=setups.find(x=>x.symbol===sym);if(!selected)return;document.getElementById('modalTitle').textContent=sym+'｜標記已下單';document.getElementById('entryPrice').value=selected.live_price;document.getElementById('modalPlan').innerHTML=`固定計畫 <b>${selected.plan_id}</b><br>建議動作：${selected.action_label}<br>提前區 ${fmt(selected.plan.early_entry_low)}–${fmt(selected.plan.early_entry_high)}｜回踩區 ${fmt(selected.plan.retest_entry_low)}–${fmt(selected.plan.retest_entry_high)}<br>SL ${fmt(selected.plan.stop)}｜追價上限 ${fmt(selected.plan.chase_cap)}`;renderOrderPreview(true);document.getElementById('orderModal').classList.add('show')}
function closeModal(){document.getElementById('orderModal').classList.remove('show')}
function applySuggestedNotional(){renderOrderPreview(true)}
function renderOrderPreview(apply=false){if(!selected)return;const e=Number(document.getElementById('entryPrice').value),p=selected.plan,z=calcRisk(e,p.stop);if(apply&&z.valid)document.getElementById('notional').value=z.recommended_notional_usdt.toFixed(2);const n=Number(document.getElementById('notional').value),actualRisk=z.valid?n*(e-p.stop)/e:0,actualPct=actualRisk/Math.max(Number(account.balance_usdt),1e-12)*100;let w=[];if(e>p.chase_cap)w.push('成交價高於追價上限');if(!(e>=p.early_entry_low&&e<=p.early_entry_high)&&!(e>=p.retest_entry_low&&e<=p.retest_entry_high))w.push('成交價不在建議掛單區');if(e<=p.stop)w.push('成交價低於或等於停損，無法建立');if(z.valid&&n>z.recommended_notional_usdt*1.01)w.push(`目前名目在 SL 虧損約 ${actualPct.toFixed(2)}% 帳戶資金，超過 2%`);document.getElementById('orderWarning').innerHTML=z.valid?`帳戶餘額：${fmt(account.balance_usdt,2)} U｜2%風險額：${fmt(z.risk_budget_usdt,2)} U<br>固定 SL 距離：${z.stop_distance_pct.toFixed(2)}%｜<b class="green">建議名目：${fmt(z.recommended_notional_usdt,2)} U</b>｜數量：${fmt(z.quantity,6)}<br>${z.exceeds_1x_balance?`<span class="yellow">此 2% 目標名目超過帳戶餘額，至少需要約 ${z.required_leverage.toFixed(2)}x；若堅持 1 倍，最多下 ${fmt(z.one_x_notional_usdt,2)} U，到 SL 約虧 ${fmt(z.one_x_actual_risk_usdt,2)} U（${z.one_x_actual_risk_pct_balance.toFixed(2)}%）</span><br>`:''}目前輸入名目到 SL 預計虧損：${fmt(actualRisk,2)} U（帳戶 ${actualPct.toFixed(2)}%）<br>${w.length?'<span class="red">手動覆寫提醒：'+w.join('；')+'</span>':'<span class="green">成交價與倉位風險可接受</span>'}`:'<span class="red">成交價必須高於固定停損，才能計算倉位。</span>'}
async function confirmOrder(){try{const x=await j('/api/positions',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbol:selected.symbol,entry_price:Number(document.getElementById('entryPrice').value),notional:Number(document.getElementById('notional').value)})});if(x.warnings?.length)alert(x.warnings.join('\n'));closeModal();tab('open')}catch(e){alert(e.message)}}
async function loadPositions(){const a=await j('/api/positions');const open=a.filter(x=>x.status==='OPEN'),closed=a.filter(x=>x.status==='CLOSED');document.getElementById('openRows').innerHTML=open.map(x=>`<tr><td><div class="sym">${x.symbol}</div><span class="badge">${x.trend_status}</span><div class="fixed">${x.plan_id}</div>${x.override_warning?`<div class="red">${x.override_warning}</div>`:''}</td><td>${fmt(x.entry_price)}<br><b>${fmt(x.notional,2)} U</b><br><span class="muted">進場帳戶 ${fmt(x.account_balance_at_entry,2)} U<br>SL風險 ${fmt(x.planned_risk_usdt,2)} U (${Number(x.planned_risk_pct).toFixed(2)}%)</span></td><td><span class="live live-price" data-symbol="${x.symbol}">${fmt(x.current_price)}</span></td><td class="${cls(x.total_pnl)} position-pnl" data-symbol="${x.symbol}" data-entry="${x.entry_price}" data-remqty="${x.remaining_qty}" data-realized="${x.realized_pnl}" data-notional="${x.notional}"><b>${fmt(x.total_pnl,2)} U</b><br>${pc(x.pnl_pct)}</td><td>${x.remaining_pct.toFixed(1)}%</td><td>${fmt(x.original_stop)}</td><td>${fmt(x.active_stop)}<br><span class="muted">只依固定規則上移，不下移</span></td><td>${x.tps.map(t=>`${t.name} ${t.hit?'✅':'○'} ${fmt(t.price)}`).join('<br>')}<br><span class="muted">TP3 後尾倉追蹤：${x.trailing_enabled?'已啟動':'未啟動'}</span></td><td><button class="danger" onclick="manualClose(${x.id},${x.current_price})">已平倉</button></td></tr>`).join('')||'<tr><td colspan="9" class="muted">目前沒有持倉。</td></tr>';document.getElementById('closedRows').innerHTML=closed.map(x=>`<tr><td class="sym">${x.symbol}</td><td>${x.plan_id}</td><td>${fmt(x.entry_price)}</td><td class="${cls(x.total_pnl)}">${fmt(x.total_pnl,2)} U</td><td class="${cls(x.pnl_pct)}">${pc(x.pnl_pct)}</td><td>${x.close_reason||'-'}</td><td>${x.opened_at_iso}<br>${x.closed_at_iso||'-'}</td></tr>`).join('')||'<tr><td colspan="7" class="muted">尚無已平倉紀錄。</td></tr>';loadStats()}
async function manualClose(id,price){const p=prompt('輸入實際平倉價；留空使用即時價',price);if(p===null)return;try{await j(`/api/positions/${id}/close`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({exit_price:p===''?null:Number(p),reason:'使用者提前手動平倉'})});loadPositions()}catch(e){alert(e.message)}}
async function loadStats(){const s=await j('/api/stats');document.getElementById('stats').innerHTML=[['未平倉',s.open_count],['已平倉',s.closed_count],['勝率',s.win_rate_pct.toFixed(1)+'%'],['Profit Factor',s.profit_factor>100?'—':s.profit_factor.toFixed(2)],['已實現',fmt(s.realized_pnl,2)+' U'],['未實現',fmt(s.open_pnl,2)+' U']].map(x=>`<div class="stat"><small>${x[0]}</small><b>${x[1]}</b></div>`).join('')}
function actionFor(s,live){const p=s.plan||{},closed=Number(s.closed_price),score=Number(s.score);if(s.setup_status==='INVALIDATED'||closed<=Number(p.structural_invalidation))return{action_code:'INVALIDATED',action_label:'計畫失效',trade_instruction:'結構已破壞，不應掛單；若你仍已成交，可手動標記並以原計畫追蹤。'};if(s.setup_status==='EXPIRED')return{action_code:'EXPIRED',action_label:'計畫到期',trade_instruction:'固定計畫已到期，等待下一個新基底計畫，不沿用舊價位追單。'};if(live>Number(p.chase_cap))return{action_code:'DO_NOT_CHASE',action_label:'已超追價上限',trade_instruction:'不要市價追入；只等回踩至突破回測區，否則放棄這一輪。'};if(closed>Number(p.breakout_trigger)){if(live>=Number(p.retest_entry_low)&&live<=Number(p.retest_entry_high))return{action_code:'BREAKOUT_RETEST_READY',action_label:'突破回踩可掛單',trade_instruction:'15m 已收線突破，現價位於回測區；優先使用限價單，不用市價追。'};if(live>Number(p.retest_entry_high))return{action_code:'WAIT_RETEST',action_label:'等待回踩',trade_instruction:'突破已確認但現價偏高；把限價單放在回測區，未回踩就不成交。'};return{action_code:'WAIT_RECLAIM',action_label:'跌回突破下方',trade_instruction:'價格低於回測區，先等重新站回觸發位，不急著接。'}}if(score>=76&&live>=Number(p.early_entry_low)&&live<=Number(p.early_entry_high))return{action_code:'EARLY_LIMIT_READY',action_label:'爆發前可小倉掛單',trade_instruction:'尚未突破、價格在提前佈局區；建議只掛預定倉位 40%，其餘等突破回踩。'};if(live<Number(p.early_entry_low))return{action_code:'WAIT_STABILIZE',action_label:'等待止跌',trade_instruction:'現價低於理想提前區，先等 15m 收線重新穩住，不往下追接。'};return{action_code:'WAIT_ENTRY',action_label:'等待掛單區',trade_instruction:'條件仍在，但現價不在理想區；只預掛固定區間，不追市價。'}}
async function updateLive(){try{const x=await j('/api/live');document.querySelectorAll('.live-price').forEach(el=>{const r=x.prices[el.dataset.symbol];if(r){const old=el.textContent,n=fmt(r.price);if(old!==n){el.textContent=n;el.classList.add('pulse');setTimeout(()=>el.classList.remove('pulse'),450)}}});setups.forEach(s=>{const r=x.prices[s.symbol];if(!r)return;s.live_price=Number(r.price);const a=actionFor(s,s.live_price),cell=document.getElementById('action-'+s.symbol);if(cell)cell.innerHTML=badge(a)+`<div class="instruction">${a.trade_instruction}</div>`});document.querySelectorAll('.position-pnl').forEach(el=>{const r=x.prices[el.dataset.symbol];if(!r)return;const pnl=Number(el.dataset.realized)+(Number(r.price)-Number(el.dataset.entry))*Number(el.dataset.remqty),pct=pnl/Number(el.dataset.notional)*100;el.className=(pnl>=0?'green':'red')+' position-pnl';el.innerHTML=`<b>${fmt(pnl,2)} U</b><br>${pc(pct)}`})}catch(e){}}
async function scanNow(){try{const x=await j('/api/scan',{method:'POST'});alert(x.message);loadSetups()}catch(e){alert(e.message)}}
async function health(){try{const h=await j('/health');document.getElementById('scanState').textContent=h.scan_running?'掃描中…':`上次掃描：${h.last_scan_finished?new Date(h.last_scan_finished*1000).toLocaleString():'尚未'}`;document.getElementById('health').textContent=`v${h.version}｜即時價 ${h.last_live_update?new Date(h.last_live_update*1000).toLocaleTimeString():'尚未'}｜Universe ${h.last_universe_size}｜Deep ${h.last_deep_scanned}｜API errors ${h.request_errors}${h.demo?'｜DEMO MODE':''}`}catch(e){}}
function refreshSlow(){loadSetups();loadPositions();loadStats();health()}loadAccount().then(refreshSlow);setInterval(updateLive,3000);setInterval(refreshSlow,30000);
</script></body></html>
"""


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, reload=False)
