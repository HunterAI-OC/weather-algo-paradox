#!/usr/bin/env python3
"""
journal.py — Paper trading journal for weather markets.
Tracks every entry, exit, and resolution in a Parquet file.
Supports restart recovery: re-loads open positions on startup.
"""
import uuid
import requests
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ── Constants ─────────────────────────────────────────────────────────────────

JOURNAL_PATH = Path("/home/ubuntu/polymarket/weather-algo-paradox/paper_trades.parquet")
JOURNAL_AVAILABLE = True  # journal.py is always available when imported directly
PARTITION_COL = "city"

# Trade/states
DIRECTION_BUY = "BUY"
EXIT_REASON_SCULP = "SCALP"
EXIT_REASON_STOP = "STOP_LOSS"
EXIT_REASON_TAKE = "TAKE_PROFIT"
EXIT_REASON_RESOLUTION = "RESOLUTION"
EXIT_REASON_MARKET_CLOSE = "MARKET_CLOSE"

MARKET_TYPE_HIGHEST = "HIGHEST_TEMP"
MARKET_TYPE_LOWEST = "LOWEST_TEMP"

# ── Schema definition ──────────────────────────────────────────────────────────

SCHEMA = pa.schema([
    # Unique identifier for this trade
    pa.field("trade_id",       pa.string()),
    # Market identity
    pa.field("slug",           pa.string()),
    pa.field("city",           pa.string()),
    pa.field("market_date",   pa.string()),   # e.g. "2026-04-26"
    pa.field("market_type",    pa.string()),   # HIGHEST_TEMP or LOWEST_TEMP
    pa.field("market_url",     pa.string()),
    pa.field("market_resolved", pa.bool_()),
    # Bucket identity
    pa.field("bucket_question", pa.string()),
    pa.field("clob_token",    pa.string()),
    # Entry
    pa.field("direction",     pa.string()),   # BUY
    pa.field("entry_price",   pa.float64()),
    pa.field("entry_price_market", pa.float64()),  # market price when we entered
    pa.field("entry_time_utc", pa.string()),  # ISO 8601
    pa.field("position_size", pa.float64()),  # notional in $ (1 contract = $1)
    pa.field("ecmwf_estimate", pa.float64(), nullable=True),  # model temp at entry
    pa.field("spread_buckets", pa.string(), nullable=True),   # comma sep bucket qs if paradox spread
    # Exit
    pa.field("exit_price",         pa.float64(), nullable=True),
    pa.field("exit_time_utc",       pa.string(), nullable=True),
    pa.field("exit_reason",         pa.string(), nullable=True),
    pa.field("realized_pnl",        pa.float64(), nullable=True),
    # Resolution
    pa.field("resolved_bucket",    pa.string(), nullable=True),
    pa.field("actual_temperature", pa.string(), nullable=True),  # e.g. "18.8°C"
    pa.field("resolution_time_utc", pa.string(), nullable=True),
    pa.field("contract_payout",    pa.float64(), nullable=True),  # 0.0 or 1.0
    pa.field("trade_pnl",         pa.float64(), nullable=True),  # (payout - entry) × size
    # Meta
    pa.field("updated_at_utc",    pa.string()),
    pa.field("notes",             pa.string(), nullable=True),
])


# ── Helpers ────────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _city_from_slug(slug: str) -> str:
    """Extract city name from a Polymarket slug like 'highest-temperature-in-munich-on-april-26-2026'."""
    parts = slug.replace("-", " ").split()
    if "in" in parts:
        idx = parts.index("in")
        return " ".join(parts[idx+1:]).title()
    return slug


def _date_from_slug(slug: str) -> str:
    """Extract date string from slug, e.g. 'april-26-2026'."""
    parts = slug.replace("-", " ").split()
    months = ["january","february","march","april","may","june",
              "july","august","september","october","november","december"]
    date_parts = [p for p in parts if p.lower() in months or p.isdigit()]
    return "-".join(date_parts[-3:]).title()


def _market_type_from_slug(slug: str) -> str:
    if "highest" in slug.lower():
        return MARKET_TYPE_HIGHEST
    elif "lowest" in slug.lower():
        return MARKET_TYPE_LOWEST
    return "UNKNOWN"


def _load_df() -> pd.DataFrame:
    """Load existing journal or return empty DataFrame with schema."""
    if JOURNAL_PATH.exists():
        try:
            return pq.read_table(JOURNAL_PATH).to_pandas()
        except Exception:
            pass
    return pd.DataFrame(columns=[f.name for f in SCHEMA])


def _save_df(df: pd.DataFrame()):
    """Write DataFrame to Parquet, partitioned by city."""
    table = pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)
    # Write partitioned
    pq.write_to_dataset(
        table,
        root_path=str(JOURNAL_PATH.parent / JOURNAL_PATH.stem),
        partition_cols=[PARTITION_COL] if PARTITION_COL in df.columns else None,
        existing_data_behavior="overwrite_or_ignore",
    )


def _write_full(df: pd.DataFrame()):
    """Write full Parquet without partitioning (simpler for small datasets)."""
    table = pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)
    writer = pq.ParquetWriter(str(JOURNAL_PATH), SCHEMA)
    writer.write_table(table)
    writer.close()


# ── Public API ─────────────────────────────────────────────────────────────────

def open_trade(
    slug: str,
    bucket_question: str,
    clob_token: str,
    entry_price: float,
    entry_price_market: float,
    position_size: float,
    direction: str = DIRECTION_BUY,
    ecmwf_estimate: float = None,
    spread_buckets: str = None,
    market_url: str = None,
    notes: str = None,
) -> str:
    """
    Record a new paper trade entry.
    Returns trade_id.
    """
    df = _load_df()
    trade_id = str(uuid.uuid4())[:8]

    city = _city_from_slug(slug)
    market_date = _date_from_slug(slug)
    mtype = _market_type_from_slug(slug)
    url = market_url or f"https://polymarket.com/event/{slug}"

    now_ts = _now()

    row = {
        "trade_id": trade_id,
        "slug": slug,
        "city": city,
        "market_date": market_date,
        "market_type": mtype,
        "market_url": url,
        "market_resolved": False,
        "bucket_question": bucket_question,
        "clob_token": clob_token,
        "direction": direction,
        "entry_price": entry_price,
        "entry_price_market": entry_price_market,
        "entry_time_utc": now_ts,
        "position_size": position_size,
        "ecmwf_estimate": ecmwf_estimate,
        "spread_buckets": spread_buckets,
        "exit_price": None,
        "exit_time_utc": None,
        "exit_reason": None,
        "realized_pnl": None,
        "resolved_bucket": None,
        "actual_temperature": None,
        "resolution_time_utc": None,
        "contract_payout": None,
        "trade_pnl": None,
        "updated_at_utc": now_ts,
        "notes": notes or "",
    }

    new_df = pd.DataFrame([row])
    df = pd.concat([df, new_df], ignore_index=True)
    _write_full(df)
    return trade_id


def close_trade(
    trade_id: str,
    exit_price: float,
    reason: str,
) -> bool:
    """
    Close/reduce an open trade. Calculates realized P&L.
    Returns True if trade found and updated.
    """
    df = _load_df()
    mask = df["trade_id"] == trade_id

    if not mask.any():
        return False

    idx = df[mask].index[0]
    now_ts = _now()

    entry_price = float(df.loc[idx, "entry_price"])
    position_size = float(df.loc[idx, "position_size"])
    # Realized P&L = (exit - entry) × position_size
    # For a YES contract at price p, payout is 1.0 on win
    realized_pnl = (exit_price - entry_price) * position_size

    df.loc[idx, "exit_price"] = exit_price
    df.loc[idx, "exit_time_utc"] = now_ts
    df.loc[idx, "exit_reason"] = reason
    df.loc[idx, "realized_pnl"] = realized_pnl
    df.loc[idx, "updated_at_utc"] = now_ts

    _write_full(df)
    return True


def resolve_trade(
    trade_id: str,
    resolved_bucket: str,
    actual_temperature: str,
    contract_payout: float,
    resolution_time_utc: str = None,
) -> bool:
    """
    Fill in resolution fields for a trade after market resolves.
    contract_payout: 0.0 (lost) or 1.0 (won)
    """
    df = _load_df()
    mask = df["trade_id"] == trade_id

    if not mask.any():
        return False

    idx = df[mask].index[0]
    now_ts = _now()

    entry_price = float(df.loc[idx, "entry_price"])
    position_size = float(df.loc[idx, "position_size"])

    # Total P&L = (payout - entry_price) × position_size
    trade_pnl = (contract_payout - entry_price) * position_size

    df.loc[idx, "resolved_bucket"] = resolved_bucket
    df.loc[idx, "actual_temperature"] = actual_temperature
    df.loc[idx, "contract_payout"] = contract_payout
    df.loc[idx, "trade_pnl"] = trade_pnl
    df.loc[idx, "market_resolved"] = True
    df.loc[idx, "resolution_time_utc"] = resolution_time_utc or now_ts
    df.loc[idx, "updated_at_utc"] = now_ts

    _write_full(df)
    return True


def get_open_trades(slug: str = None) -> pd.DataFrame:
    """Return all open (unresolved) trades, optionally filtered by slug."""
    df = _load_df()
    open_mask = ~df["market_resolved"].astype(bool)
    if slug:
        open_mask &= df["slug"] == slug
    return df[open_mask].copy()


def get_closed_trades() -> pd.DataFrame:
    """Return all resolved trades."""
    df = _load_df()
    return df[df["market_resolved"].astype(bool)].copy()


def get_trade(trade_id: str) -> dict | None:
    """Return a single trade as a dict."""
    df = _load_df()
    row = df[df["trade_id"] == trade_id]
    if row.empty:
        return None
    return row.iloc[0].to_dict()


def get_unresolved_slugs() -> list[str]:
    """Return slugs of open markets — used by daemon to check resolutions."""
    df = _load_df()
    open_trades = df[~df["market_resolved"].astype(bool)]
    return open_trades["slug"].unique().tolist()


def mark_slug_resolved(slug: str):
    """Mark all trades for a slug as resolved (market closed)."""
    df = _load_df()
    mask = (df["slug"] == slug) & (~df["market_resolved"].astype(bool))
    df.loc[mask, "market_resolved"] = True
    df.loc[mask, "updated_at_utc"] = _now()
    _write_full(df)


def get_summary() -> dict:
    """Quick performance summary."""
    df = _load_df()
    closed = df[df["market_resolved"].astype(bool)]
    if closed.empty:
        return {
            "total_trades": 0,
            "open_trades": len(df),
            "total_pnl": 0.0,
            "win_rate": None,
        }

    total_pnl = closed["trade_pnl"].sum()
    wins = (closed["contract_payout"] > 0).sum()
    total = len(closed)
    win_rate = wins / total if total > 0 else 0.0

    return {
        "total_trades": total,
        "open_trades": len(df) - total,
        "total_pnl": round(total_pnl, 4),
        "win_rate": round(win_rate, 4),
        "wins": int(wins),
        "losses": int(total - wins),
    }


def resolve_market_from_polymarket(slug: str) -> dict | None:
    """
    Fetch resolution info from Polymarket API for a given slug.
    Returns dict with resolved_bucket, actual_temperature, resolution_time if resolved,
    or None if not yet resolved.
    """
    try:
        resp = requests.get(
            "https://gamma-api.polymarket.com/events",
            params={"slug": slug},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and data:
            ev = data[0]
        elif isinstance(data, dict):
            ev = data
        else:
            return None
    except Exception:
        return None

    # Check if market is resolved
    markets = ev.get("markets", [])
    if not markets:
        return None

    # Get winner info
    resolved_markets = [m for m in markets if m.get("winner")]
    if not resolved_markets:
        return None  # not resolved yet

    winner = resolved_markets[0]
    winner_question = winner.get("question", "")

    # Resolution time
    end_date = ev.get("endDate", "") or ""

    return {
        "resolved_bucket": winner_question,
        "resolution_time_utc": end_date,
    }


if __name__ == "__main__":
    import requests

    # Smoke test
    print("Schema fields:", [f.name for f in SCHEMA])
    print("Journal path:", JOURNAL_PATH)

    # Demo entry
    tid = open_trade(
        slug="highest-temperature-in-munich-on-april-26-2026",
        bucket_question="Will the highest temperature in Munich be 18°C on April 26?",
        clob_token="30682892606341727429138868175827456846969686999716218105258194732173700184314",
        entry_price=0.33,
        entry_price_market=0.31,
        position_size=1.0,
        ecmwf_estimate=18.2,
    )
    print("Opened trade:", tid)

    # Demo close
    close_trade(tid, exit_price=0.69, reason=EXIT_REASON_SCULP)
    print("Closed trade:", tid)

    print("Summary:", get_summary())
