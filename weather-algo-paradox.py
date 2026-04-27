#!/usr/bin/env python3
"""
weather-algo-paradox.py — Gambler's Paradox strategy for Polymarket weather markets.

SCAN    → Fetch all active weather markets from Polymarket
ANALYSIS→ ECMWF point estimate → nearest integer → ±1 bucket range
         → sum(3 bucket YES prices) = spread cost
         → only enter if spread cost < $1.00
ENTRY   → BUY YES on all 3 adjacent buckets simultaneously
         → one entry per slug (no re-trades)
EXIT    → Hold to resolution OR scalp if price moves before resolve
         → fill contract_payout, actual_temp, trade_pnl on resolve

Strategy ID: PARADOX
Paper trading only (journal.py integration)
"""
import json
import os
import re
import requests
import time
import sys
from datetime import datetime, timezone
from pathlib import Path

import journal

# ── Config ─────────────────────────────────────────────────────────────────────
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
OPEN_METEO_BASE     = "https://api.open-meteo.com/v1/forecast"
SCAN_INTERVAL       = 300          # seconds between full scans (5 min)
STRATEGY_ID         = "PARADOX"
METEO_SIGMA         = 1.2         # °C — typical daily temp σ for city-level forecast error

# ── City → ICAO + coordinates ──────────────────────────────────────────────────
CITY_DATA = {
    # city_lower: (icao, lat, lon)
    "munich":        ("EDDM",    48.3537,  11.7750),
    "münchen":       ("EDDM",    48.3537,  11.7750),
    "seoul":         ("RKSI",    37.4602,  126.4407),
    "tokyo":         ("RJTT",    35.5494,  139.7798),
    "london":        ("EGLC",    51.5048,   0.0553),
    "paris":         ("LFPG",    49.0097,   2.5479),
    "new york":      ("KLGA",    40.7769, -73.8742),
    "nyc":           ("KLGA",    40.7769, -73.8742),
    "chicago":       ("KORD",    41.9742, -87.9073),
    "miami":         ("KMIA",    25.7959, -80.2870),
    "dallas":        ("KDAL",    32.7767, -96.7970),
    "seattle":        ("KSEA",    47.4502,-122.3088),
    "atlanta":       ("KATL",    33.6407, -84.4277),
    "shanghai":      ("ZSPD",    31.1443, 121.8083),
    "singapore":     ("WSSS",     1.3644,  103.9915),
    "toronto":       ("CYYZ",    43.6777, -79.6248),
    "sao paulo":     ("SBGR",   -23.4356, -46.4731),
    "buenos aires":   ("SAEZ",   -34.8228, -58.5358),
    "wellington":     ("NZWN",   -41.3255, 174.8050),
    "denver":        ("KDEN",    39.8561,-104.6737),
    "moscow":        ("UUEE",    55.9726,  37.4133),
    "dubai":         ("OMDB",    25.2528,  55.3644),
    "mexico city":   ("MMMX",    19.4363, -99.0721),
    "hong kong":     ("VHHH",    22.3080, 113.9185),
    "beijing":       ("ZBAA",    40.0799, 116.6031),
    "bangkok":       ("VTBS",    13.6900, 100.7501),
    "milan":         ("LIMC",    45.6306,   8.7281),
    "amsterdam":      ("EHAM",    52.3105,   4.7683),
    "madrid":        ("LEMD",    40.4719,  -3.5626),
    "rome":          ("LIRF",    41.8003,  12.2389),
    "sydney":        ("YSSY",   -33.9399, 151.1753),
    "lagos":         ("DNPO",    6.4541,    3.3947),
    "jakarta":       ("WIII",    -6.1256, 106.6569),
    "manila":        ("RPLL",    14.5086,  121.0186),
    "kuala lumpur":  ("WMKK",     3.1295, 101.7141),
    "warsaw":        ("EPWA",    52.1657,  20.9671),
    "ankara":        ("LTAC",    39.9561,  32.8897),
    "tel aviv":      ("LLBG",    32.0114,  34.8864),
    "cap town":      ("FACT",   -33.9715,  18.6021),
    "cape town":     ("FACT",   -33.9715,  18.6021),
    "karachi":       ("OPKC",    24.8938,  67.1682),
    "delhi":         ("VIDP",    28.5665,  77.1031),
    "mumbai":        ("VABB",    19.0883,  72.8639),
    "chennai":       ("VOMM",    12.9716,  80.1619),
    "kolkata":       ("VECC",    22.6547,  88.4467),
    "bogota":        ("SKBO",     4.7016, -74.1467),
    "santiago":      ("SCEL",   -33.3930, -70.7858),
    " lima":         ("SPIM",   -12.0219, -77.1143),
    "jeddah":        ("OEJN",    21.6796,  39.1565),
    "riyadh":        ("OERK",    24.6577,  46.7219),
    "tehran":        ("OIIE",    35.4149,  51.4203),
    "hanoi":         ("VVNB",    21.2188, 105.8064),
    "taipei":        ("RCSS",    25.0806, 121.2233),
    "baghdad":       ("ORBI",    33.2595,  44.3028),
    "athens":        ("LGAV",    37.9364,  23.9445),
    "vienna":        ("LOWW",    48.1103,  16.5694),
    "budapest":      ("LHBP",    47.4361,  19.2556),
    "prague":        ("LKPR",    50.1061,  14.2622),
    "stockholm":      ("ESSA",    59.6519,  17.9186),
    "oslo":           ("ENGM",    60.1939,  11.1004),
    "helsinki":       ("EFHK",    60.3172,  24.9683),
    "zurich":         ("LSZH",    47.4643,   8.5490),
    "brussels":       ("EBBR",    50.9039,   4.4840),
    "lisbon":         ("LPPT",    38.7756,  -9.1354),
    "dublin":         ("EIDW",    53.4264,  -6.2499),
    "busan":          ("RKPK",    35.1794,  129.0756),
    "chongqing":      ("ZUCK",    29.5332,  106.5344),
    "guangzhou":      ("ZGGG",    23.1693,  113.3255),
    "shenzhen":       ("ZGSZ",    22.6393,  113.8102),
    "wuhan":          ("ZHHH",    30.7836,  114.2094),
    "houston":        ("KIAH",    29.9902,  -95.3368),
    "san francisco":  ("KSFO",    37.6213,-122.3790),
    "los angeles":    ("KLAX",    33.9425,-118.4081),
    "austin":         ("KAUS",    30.1944,  -97.7352),
    "panama city":    ("MPTO",     8.9734,  -79.5001),
    "lucknow":        ("VILK",    26.7603,  80.8898),
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def city_from_slug(slug: str) -> str | None:
    """Extract city name from slug like 'highest-temperature-in-munich-on-april-26-2026'."""
    m = re.search(r'-in-([a-z\s]+)-on-', slug.lower())
    if m:
        return m.group(1).replace("-", " ").strip().title()
    return None


def extract_date_from_slug(slug: str) -> str | None:
    """Extract date string from slug e.g. 'april-26-2026'."""
    m = re.search(
        r'(january|february|march|april|may|june|july|august|september|october|november|december)-(\d{2})-(\d{4})',
        slug.lower(),
    )
    if not m:
        return None
    months_str = (
        "january february march april may june july august "
        "september october november december"
    )
    month_map = {m: f"{i+1:02d}" for i, m in enumerate(months_str.split())}
    month_name, day, year = m.groups()
    return f"{year}-{month_map[month_name]}-{day}"


def get_weather_data(city: str, date: str) -> dict | None:
    """
    Fetch ECMWF forecast from Open-Meteo for a city + target date.
    Returns dict with keys: ecmwf_peak, ecmwf_min, ecmwf_max, model_date
    or None if fetch fails.
    """
    city_key = city.lower()
    if city_key not in CITY_DATA:
        return None
    icao, lat, lon = CITY_DATA[city_key]

    params = {
        "latitude":       lat,
        "longitude":      lon,
        "hourly":         "temperature_2m",
        "daily":          "temperature_2m_max,temperature_2m_min",
        "timezone":       "UTC",
        "forecast_days":  10,
    }
    try:
        resp = requests.get(OPEN_METEO_BASE, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[{ts()}] Open-Meteo error for {city}: {e}")
        return None

    daily = data.get("daily", {})
    dates = daily.get("time", [])
    if date in dates:
        idx = dates.index(date)
        return {
            "ecmwf_max": daily["temperature_2m_max"][idx],
            "ecmwf_min": daily["temperature_2m_min"][idx],
            "ecmwf_peak": daily["temperature_2m_max"][idx],
        }

    # If target date not in forecast, return today's best estimate
    if dates:
        return {
            "ecmwf_max":  daily["temperature_2m_max"][0],
            "ecmwf_min":  daily["temperature_2m_min"][0],
            "ecmwf_peak": daily["temperature_2m_max"][0],
        }
    return None


def parse_buckets(event: dict) -> list[dict]:
    """
    Return tradeable buckets sorted by temperature.
    All temperatures are normalized to Celsius regardless of market unit.
    Attaches 'unit' field: 'F' or 'C'.
    """
    buckets = []
    market_unit = 'C'  # default


    for b in event.get("markets", []):
        if not b.get("acceptingOrders"):
            continue
        raw = b.get("outcomePrices", "[]")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                continue
        yes = float(raw[0]) if raw and len(raw) > 0 else None
        if yes is None or yes <= 0.001 or yes >= 0.999:
            continue
        q = b.get("question", "")
        temp_raw = extract_temp_from_question(q)
        if temp_raw is None:
            continue
        # Detect unit from first bucket that has it
        if market_unit == 'C':
            detected = detect_question_unit(q)
            if detected != 'C':
                market_unit = detected
        # Normalize to Celsius
        if market_unit == 'F':
            temp_norm = fahrenheit_to_celsius(temp_raw)
        else:
            temp_norm = temp_raw
        buckets.append({
            "question":    q,
            "yes_price":   yes,
            "no_price":   float(raw[1]) if len(raw) > 1 else 1.0,
            "temp":        temp_norm,   # always Celsius
            "temp_raw":    temp_raw,    # original unit value (for display)
            "unit":        market_unit,
            "clob_token":  _clob_token(b),
            "market_id":   b.get("id", ""),
        })
    buckets = [b for b in buckets if b["temp"] is not None]
    return sorted(buckets, key=lambda x: x["temp"])


def fahrenheit_to_celsius(f: float) -> float:
    """Convert Fahrenheit to Celsius."""
    return (f - 32.0) * 5.0 / 9.0


def celsius_to_fahrenheit(c: float) -> float:
    """Convert Celsius to Fahrenheit."""
    return (c * 9.0 / 5.0) + 32.0


def detect_question_unit(q: str) -> str:
    """
    Detect temperature unit from a bucket question string.
    Returns 'F' for Fahrenheit, 'C' for Celsius, or 'C' as default.
    """
    if re.search(r'°?\s*F\b', q, re.IGNORECASE):
        return 'F'
    if re.search(r'°?\s*C\b', q, re.IGNORECASE):
        return 'C'
    return 'C'  # default to Celsius



def extract_temp_from_question(q: str) -> float | None:
    """
    Extract temperature numeric value from bucket question string.
    Also returns the detected unit as a second value via a named tuple,
    or None if no temperature found.
    """
    # Try "X°C" pattern first
    m = re.search(r'(\d+(?:\.\d+)?)\s*°?\s*C', q, re.IGNORECASE)
    if m:
        return float(m.group(1))
    # Try "X-Y°F" range → return midpoint
    m = re.search(r'(\d+)-(\d+)\s*°?F', q, re.IGNORECASE)
    if m:
        return (int(m.group(1)) + int(m.group(2))) / 2.0
    # Try "X°F or below" / "X°F or higher" / "X°F or above"
    m = re.search(r'(\d+(?:\.\d+)?)\s*°?F\s+(?:or below|or higher|or above)', q, re.IGNORECASE)
    if m:
        return float(m.group(1))
    # Try raw "X°F" pattern without unit symbol
    m = re.search(r'(\d+(?:\.\d+)?)\s*°?F(?!\s+(?:or|and))', q, re.IGNORECASE)
    if m:
        return float(m.group(1))
    # Try plain number when unit is implicit (detect from other buckets or default)
    m = re.search(r'(\d+(?:\.\d+)?)\s*(?:°|\s)(?=\s|$|\.)', q)
    if m:
        return float(m.group(1))
    return None


def _clob_token(market: dict) -> str:
    raw = market.get("clobTokenIds", "[]")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return "?"
    if isinstance(raw, list) and raw:
        return raw[0]
    return "?"


def find_adjacent_buckets(buckets: list[dict], target_temp: float) -> tuple[list[dict], float]:
    """
    Find the 3 buckets closest to target_temp (±1 from rounded estimate).
    Returns (adjacent_buckets, spread_cost).
    """
    if not buckets:
        return [], 0.0

    rounded = round(target_temp)
    lo = rounded - 1
    hi = rounded + 1

    # Find buckets within [lo, hi] range
    adjacent = [b for b in buckets if lo <= b["temp"] <= hi]
    spread_cost = sum(b["yes_price"] for b in adjacent)

    return adjacent, spread_cost


def discord_post(message: str) -> bool:
    if not DISCORD_WEBHOOK_URL:
        print(f"[{ts()}] No DISCORD_WEBHOOK_URL — printing:")
        print(message, flush=True)
        return False
    payload = {"content": message}
    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        resp.raise_for_status()
        print(f"[{ts()}] Discord: {resp.status_code}")
        return True
    except Exception as e:
        print(f"[{ts()}] Discord error: {e}")
        return False


def fmt_trade_alert(slug: str, city: str, target: int, buckets: list[dict],
                    spread_cost: float, ecmwf_peak: float, position_size: float,
                    trade_ids: list[str]) -> str:
    bucket_lines = []
    for b in buckets:
        bucket_lines.append(
            f"  {b['temp']:.0f}°C @ YES={b['yes_price']:.4f} | {b['question'][:50]}"
        )
    return (
        f"**🎰 PARADOX ENTRY — {city}**\n"
        f"<https://polymarket.com/event/{slug}>\n"
        f">>> ECMWF: {ecmwf_peak}°C → target {target}°C  |  Spread cost: ${spread_cost:.4f}\n"
        f">>> Position size: ${position_size:.2f}  |  Trade IDs: {', '.join(trade_ids)}\n"
        + "\n".join(bucket_lines)
    )


def get_open_trades_for_slug(slug: str) -> list[dict]:
    """Check if we already have an open trade for this slug (any bucket)."""
    try:
        return journal.get_open_trades(slug=slug)
    except Exception:
        return []


def already_traded(slug: str) -> bool:
    return len(get_open_trades_for_slug(slug)) > 0


# ── Strategy: evaluate one event ────────────────────────────────────────────────

def evaluate_event(event: dict) -> bool:
    """
    Evaluate one Polymarket event for paradox trade opportunity.
    Returns True if a trade was opened.
    """
    slug = event.get("slug", "")
    city = city_from_slug(slug)
    market_date = extract_date_from_slug(slug)

    if not city or not market_date:
        return False

    # Skip if already traded this slug
    if already_traded(slug):
        print(f"[{ts()}] Already traded: {slug}")
        return False

    # Get weather forecast
    weather = get_weather_data(city, market_date)
    if not weather:
        print(f"[{ts()}] No weather data for {city}")
        return False

    ecmwf_peak = weather.get("ecmwf_peak")
    if ecmwf_peak is None:
        return False
    target = round(ecmwf_peak)   # nearest integer

    # Parse buckets
    buckets = parse_buckets(event)
    if not buckets:
        return False

    # Find 3 adjacent buckets around target
    adjacent, spread_cost = find_adjacent_buckets(buckets, ecmwf_peak)

    # Paradox condition: spread cost must be < $1.00
    # Paradox gate: spread must cost < $0.769 for ≥30% ROI
    # ROI = ($1 - spread_cost) / spread_cost  →  ≥30% requires spread_cost < $0.769
    ROI_THRESHOLD_COST = 0.769
    if spread_cost >= ROI_THRESHOLD_COST:
        print(f"[{ts()}] {city} {market_date}: spread cost ${spread_cost:.4f} → ROI < 30% — skip")
        return False
    if len(adjacent) < 2:
        print(f"[{ts()}] {city} {market_date}: fewer than 2 adjacent buckets — skip")
        return False

    # Position sizing — uniform across 3 buckets
    # Kelly-ish: f = (payout * p - cost) / payout  — for now use flat $10 per bucket
    position_size = 10.0
    notional_per_bucket = position_size / len(adjacent)

    # Open paper trade for each bucket
    trade_ids = []
    for b in adjacent:
        tid = journal.open_trade(
            slug=slug,
            bucket_question=b["question"],
            clob_token=b["clob_token"],
            entry_price=b["yes_price"],
            entry_price_market=b["yes_price"],
            position_size=notional_per_bucket,
            direction="BUY",
            ecmwf_estimate=ecmwf_peak,
            market_url=f"https://polymarket.com/event/{slug}",
            notes=f"PARADOX spread_cost=${spread_cost:.4f} target={target}°C sigma={METEO_SIGMA}°C",
        )
        trade_ids.append(tid)
        print(f"[{ts()}] Opened trade {tid}: {b['question'][:50]} @ {b['yes_price']}")

    # Post to Discord
    msg = fmt_trade_alert(
        slug=slug, city=city, target=target,
        buckets=adjacent, spread_cost=spread_cost,
        ecmwf_peak=ecmwf_peak, position_size=position_size,
        trade_ids=trade_ids,
    )
    discord_post(msg)
    return True


# ── Fetch all weather events ───────────────────────────────────────────────────

def fetch_all_weather_events() -> list[dict]:
    all_events = []
    for query in ["highest temperature", "lowest temperature"]:
        page = 1
        while True:
            params = {
                "q": query,
                "limit_per_type": 50,
                "events_status": "active",
                "page": page,
            }
            try:
                resp = requests.get(
                    "https://gamma-api.polymarket.com/public-search",
                    params=params, timeout=15,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"[{ts()}] Fetch error ({query} page {page}): {e}")
                break

            if not data:
                break
            events = data.get("events", []) if isinstance(data, dict) else data
            if not events:
                break
            all_events.extend(events)
            if not data.get("pagination", {}).get("hasMore", False):
                break
            page += 1
    return all_events


# ── Resolution checker ────────────────────────────────────────────────────────

def check_resolutions():
    """Check all open trades for resolved markets."""
    try:
        open_trades = journal.get_open_trades()
    except Exception:
        return

    slugs_seen = set()
    for _, row in open_trades.iterrows():
        slug = row.get("slug")
        if slug in slugs_seen:
            continue
        slugs_seen.add(slug)

        try:
            resp = requests.get(
                "https://gamma-api.polymarket.com/events",
                params={"slug": slug},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            ev = data[0] if isinstance(data, list) else data
        except Exception as e:
            print(f"[{ts()}] Resolution check error for {slug}: {e}")
            continue

        markets = ev.get("markets", []) if ev else []
        resolved = [m for m in markets if m.get("winner")]
        if not resolved:
            continue

        winner = resolved[0]
        winner_q = winner.get("question", "")
        end_date = ev.get("endDate", "") or ""

        # Find the bucket that matches our trade
        for _, trade in open_trades.iterrows():
            if trade.get("slug") != slug:
                continue

            # Determine payout
            if winner_q == trade.get("bucket_question"):
                payout = 1.0
            else:
                payout = 0.0

            # Get actual temperature from winner question
            actual_temp = extract_temp_from_question(winner_q)
            actual_str = f"{actual_temp}°C" if actual_temp else winner_q

            journal.resolve_trade(
                trade_id=trade.get("trade_id"),
                resolved_bucket=winner_q,
                actual_temperature=actual_str,
                contract_payout=payout,
                resolution_time_utc=end_date,
            )
            pnl = trade.get("position_size", 0) * (payout - trade.get("entry_price", 0))
            outcome = "WIN" if payout > 0 else "LOSS"
            print(f"[{ts()}] RESOLVED {outcome} {trade.get('trade_id')}: {actual_str} → payout {payout}")

        journal.mark_slug_resolved(slug)


# ── Main loop ─────────────────────────────────────────────────────────────────

def run():
    print(f"[{ts()}] Paradox strategy starting")
    print(f"  Strategy ID: {STRATEGY_ID}")
    print(f"  Scan interval: {SCAN_INTERVAL}s")
    print(f"  Webhook: {'YES' if DISCORD_WEBHOOK_URL else 'NO'}")
    print(f"  Paper trading: {'YES' if journal.JOURNAL_AVAILABLE else 'NO'}")

    while True:
        try:
            # Check for resolutions first
            check_resolutions()

            # Scan all markets
            events = fetch_all_weather_events()
            total = len(events)
            new_trades = 0

            for ev in events:
                if evaluate_event(ev):
                    new_trades += 1

            print(f"[{ts()}] Scan complete: {total} markets, {new_trades} new trades")

        except Exception as e:
            print(f"[{ts()}] Error: {e}")

        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    run()
