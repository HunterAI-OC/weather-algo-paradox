#!/usr/bin/env python3
"""
weather-algo-paradox.py — Gambler's Paradox strategy for Polymarket weather markets.

SCAN    → Fetch all active weather markets from Polymarket
ANALYSIS→ ECMWF point estimate → nearest integer → ±1 bucket range
         → sum(3 bucket YES prices) = spread cost
         → only enter if total cost < $1.00 (paradox condition)
         → Calculate model probability and Kelly fraction for each bucket
ENTRY   → BUY YES on all 3 adjacent buckets simultaneously
         → one entry per slug (no re-trades)
EXIT    → Hold to resolution OR scalp if price moves before resolve
         → fill contract_payout, actual_temp, trade_pnl on resolve

Strategy ID: PARADOX
Paper trading only (journal.py integration)
"""
import json
import math
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
KELLY_MULTIPLIER    = 0.5         # fractional Kelly (half-Kelly for risk management)
EV_THRESHOLD        = 0.02        # minimum EV per contract to enter ($)
MAX_SPREAD_COST     = 1.0         # paradox spread must cost < $1.00

# ── City → ICAO + coordinates ──────────────────────────────────────────────────
CITY_DATA = {
    # city_lower: (icao, lat, lon)
    "munich":        ("EDDM",    48.3537,  11.7750),
    "münchen":       ("EDDM",    48.3537,  11.7750),
    "seoul":         ("RKSI",    37.4602, 126.4407),
    "tokyo":         ("RJTT",    35.5494, 139.7798),
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
    "singapore":     ("WSSS",     1.3644, 103.9915),
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
    "busan":          ("RKPK",    35.1794, 129.0756),
    "chongqing":      ("ZUCK",    29.5332, 106.5344),
    "guangzhou":      ("ZGGG",    23.1693, 113.3255),
    "shenzhen":       ("ZGSZ",    22.6393, 113.8102),
    "wuhan":          ("ZHHH",    30.7836, 114.2094),
    "houston":        ("KIAH",    29.9902, -95.3368),
    "san francisco":  ("KSFO",    37.6213,-122.3790),
    "los angeles":    ("KLAX",    33.9425,-118.4081),
    "austin":         ("KAUS",    30.1944, -97.7352),
    "panama city":    ("MPTO",     8.9734, -79.5001),
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
    Returns dict with keys: ecmwf_peak, ecmwf_min, ecmwf_max
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
            "ecmwf_max":  daily["temperature_2m_max"][idx],
            "ecmwf_min":  daily["temperature_2m_min"][idx],
            "ecmwf_peak": daily["temperature_2m_max"][idx],
        }

    # Target date not in forecast range — return None (don't guess)
    return None


def parse_buckets(event: dict) -> list[dict]:
    """Return tradeable buckets sorted by temperature."""
    buckets = []
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
        temp = extract_temp_from_question(q)
        buckets.append({
            "question":    q,
            "yes_price":   yes,
            "no_price":   float(raw[1]) if len(raw) > 1 else 1.0,
            "temp":        temp,
            "clob_token":  _clob_token(b),
            "market_id":   b.get("id", ""),
        })
    buckets = [b for b in buckets if b["temp"] is not None]
    return sorted(buckets, key=lambda x: x["temp"])


def extract_temp_from_question(q: str) -> float | None:
    """Extract temperature value from bucket question string."""
    # Try "X°C" pattern first
    m = re.search(r'(\d+(?:\.\d+)?)\s*°?\s*C', q, re.IGNORECASE)
    if m:
        return float(m.group(1))
    # Try "X-Y°F" range → return midpoint
    m = re.search(r'(\d+)-(\d+)\s*°?F', q, re.IGNORECASE)
    if m:
        return (int(m.group(1)) + int(m.group(2))) / 2.0
    # Try "X°F or below"
    m = re.search(r'(\d+)\s*°?F.*below', q, re.IGNORECASE)
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


# ── Kelly + EV calculation ────────────────────────────────────────────────────

def normal_cdf(x: float) -> float:
    """Standard normal CDF using error function approximation."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def model_probability(ecmwf_peak: float, bucket_temp: float, sigma: float = METEO_SIGMA) -> float:
    """
    Probability that the high temperature equals bucket_temp given ECMWF point
    estimate ecmwf_peak, using a normal distribution with city-level sigma.

    Uses the midpoint of the bucket interval as the distribution center.
    Bucket width is 1°C, so P(bucket) = P(X ∈ [temp-0.5, temp+0.5]).
    """
    half_width = 0.5
    z_lo = (bucket_temp - ecmwf_peak - half_width) / sigma
    z_hi = (bucket_temp - ecmwf_peak + half_width) / sigma
    return max(0.0, min(1.0, normal_cdf(z_hi) - normal_cdf(z_lo)))


def kelly_fraction(p: float, market_price: float, fraction: float = KELLY_MULTIPLIER) -> float:
    """
    Fractional Kelly for a binary YES contract.
    You pay market_price per contract.
    Win → receive $1.00 (net profit = 1 - market_price)
    Lose → lose stake = market_price
    Net odds b = (1 - market_price) / market_price.
    Full Kelly: f* = p - (1-p)/b
    Returns fractional Kelly, capped at 0 if negative edge.
    """
    if p <= 0.0 or market_price <= 0.0 or market_price >= 1.0:
        return 0.0
    b = (1.0 - market_price) / market_price   # net odds received on $1 invested
    if b <= 0.0:
        return 0.0
    full_kelly = p - (1.0 - p) / b
    return max(0.0, min(fraction, full_kelly * fraction))


def bucket_ev(p: float, market_price: float) -> float:
    """
    Expected value per dollar invested in one YES contract.
    EV = P(win) × $1.00 - cost = p × 1 - market_price
    """
    return max(-market_price, p - market_price)


# ── Paradox spread analysis ───────────────────────────────────────────────────

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
    adjacent = [b for b in buckets if lo <= b["temp"] <= hi]
    spread_cost = sum(b["yes_price"] for b in adjacent)
    return adjacent, spread_cost


# ── Discord ────────────────────────────────────────────────────────────────────

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


def fmt_trade_alert(
    slug: str, city: str, target: int, buckets: list[dict],
    spread_cost: float, ecmwf_peak: float, kelly_total: float,
    position_size: float, trade_ids: list[str]
) -> str:
    bucket_lines = []
    for b in buckets:
        p = b.get("model_prob", 0.0)
        ev = b.get("bucket_ev", 0.0)
        kelly = b.get("kelly_frac", 0.0)
        bucket_lines.append(
            f"  {b['temp']:.0f}°C | YES={b['yes_price']:.4f} | "
            f"model={p:.3f} | EV/share=${ev:.4f} | Kelly×={kelly:.2f}"
        )
    return (
        f"**🎰 PARADOX ENTRY — {city}**\n"
        f"<https://polymarket.com/event/{slug}>\n"
        f">>> ECMWF: {ecmwf_peak}°C → target {target}°C  |  "
        f"Spread: ${spread_cost:.4f}  |  Total Kelly f: {kelly_total:.3f}\n"
        f">>> Size: ${position_size:.2f}  |  IDs: {', '.join(trade_ids)}\n"
        + "\n".join(bucket_lines)
    )


# ── Journal helpers ─────────────────────────────────────────────────────────────

def already_traded(slug: str) -> bool:
    try:
        df = journal.get_open_trades(slug=slug)
        return len(df) > 0
    except Exception:
        return False


# ── Strategy: evaluate one event ────────────────────────────────────────────────

def evaluate_event(event: dict) -> bool:
    """
    Evaluate one Polymarket event for paradox trade opportunity.
    Returns True if trades were opened.
    """
    slug = event.get("slug", "")
    city = city_from_slug(slug)
    market_date = extract_date_from_slug(slug)

    if not city or not market_date:
        return False

    if already_traded(slug):
        print(f"[{ts()}] Already traded: {slug}")
        return False

    weather = get_weather_data(city, market_date)
    if not weather:
        print(f"[{ts()}] No weather data for {city}")
        return False

    ecmwf_peak = weather.get("ecmwf_peak")
    if ecmwf_peak is None:
        print(f"[{ts()}] {city} {market_date}: no ECMWF peak data")
        return False

    target = round(ecmwf_peak)
    buckets = parse_buckets(event)
    if not buckets:
        return False

    adjacent, spread_cost = find_adjacent_buckets(buckets, ecmwf_peak)

    # Paradox gate: spread must cost < $1.00
    if spread_cost is None or spread_cost >= MAX_SPREAD_COST:
        print(f"[{ts()}] {city} {market_date}: spread ${spread_cost:.4f} >= $1.00 — skip")
        return False

    if len(adjacent) < 2:
        print(f"[{ts()}] {city} {market_date}: fewer than 2 adjacent buckets — skip")
        return False

    # ── Kelly + EV scoring per bucket ─────────────────────────────────────────
    total_spread = spread_cost
    total_kelly_f = 0.0

    for b in adjacent:
        p = model_probability(ecmwf_peak, b["temp"])
        ev = bucket_ev(p, b["yes_price"])
        kelly = kelly_fraction(p, b["yes_price"])  # fractional Kelly for this bucket
        b["model_prob"] = p
        b["bucket_ev"] = ev
        b["kelly_frac"] = kelly
        total_kelly_f += kelly

    # ── Paradox spread EV (the spread is one trade, one payout) ───────────────
    # Buy 3 buckets. Payout on any win = $1.00 (the winning bucket pays $1, the other 2 are losses)
    # total_p = P(any adjacent bucket wins) = sum of individual P(bucket)
    # spread_cost = sum of YES prices for all adjacent buckets
    # Spread EV = total_p × $1.00 - spread_cost
    # Individual bucket EV is only meaningful for single-bucket trades.
    total_model_prob = sum(b["model_prob"] for b in adjacent)
    spread_ev = total_model_prob - spread_cost  # EV in $ per $1 invested in the spread

    if spread_ev < EV_THRESHOLD:
        print(f"[{ts()}] {city} {market_date}: spread EV ${spread_ev:.4f} < ${EV_THRESHOLD} — skip")
        return False

    # ── Kelly for the spread as a whole ─────────────────────────────────────────
    # Treat the paradox spread as one "bet": cost = spread_cost, payout = $1.00
    kelly_total = kelly_fraction(total_model_prob, spread_cost)  # already fractional

    # ── Position sizing ──────────────────────────────────────────────────────────
    bankroll = 1000.0
    position_size = min(bankroll * kelly_total, 50.0)  # cap at $50 per spread
    notional_per_bucket = position_size / len(adjacent)

    # ── Open trades ────────────────────────────────────────────────────────────
    trade_ids = []
    bucket_temps = [b["temp"] for b in adjacent]
    spread_str = ",".join(str(t) for t in bucket_temps)

    for b in adjacent:
        tid = journal.open_trade(
            slug=slug,
            bucket_question=b["question"],
            clob_token=b["clob_token"],
            entry_price=b["yes_price"],
            entry_price_market=b["yes_price"],
            position_size=notional_per_bucket,
            direction=journal.DIRECTION_BUY,
            ecmwf_estimate=ecmwf_peak,
            spread_buckets=spread_str,
            market_url=f"https://polymarket.com/event/{slug}",
            notes=(
                f"PARADOX sigma={METEO_SIGMA}°C "
                f"model_p={b['model_prob']:.3f} "
                f"EV=${b['bucket_ev']:.4f} "
                f"Kelly={b['kelly_frac']:.3f}"
            ),
        )
        trade_ids.append(tid)
        print(
            f"[{ts()}] {city}: open {tid} — {b['temp']:.0f}°C "
            f"@ {b['yes_price']} | p={b['model_prob']:.3f} "
            f"EV=${b['bucket_ev']:.4f} Kelly={b['kelly_frac']:.3f}"
        )

    # ── Discord alert ─────────────────────────────────────────────────────────
    msg = fmt_trade_alert(
        slug=slug, city=city, target=target,
        buckets=adjacent, spread_cost=spread_cost,
        ecmwf_peak=ecmwf_peak, kelly_total=kelly_total,
        position_size=position_size, trade_ids=trade_ids,
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
                print(f"[{ts()}] Fetch error ({query} p{page}): {e}")
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
    """Check open trades for resolved markets and record outcomes."""
    try:
        open_trades = journal.get_open_trades()
    except Exception:
        return

    slugs_seen = set()
    for _, row in open_trades.iterrows():
        slug = row.get("slug")
        if not slug or slug in slugs_seen:
            continue
        slugs_seen.add(slug)

        try:
            resp = requests.get(
                "https://gamma-api.polymarket.com/events",
                params={"slug": slug}, timeout=15,
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

        # Resolve all trades for this slug
        for _, trade in open_trades.iterrows():
            if trade.get("slug") != slug:
                continue

            payout = 1.0 if winner_q == trade.get("bucket_question") else 0.0
            actual_temp = extract_temp_from_question(winner_q)
            actual_str = f"{actual_temp}°C" if actual_temp else winner_q

            journal.resolve_trade(
                trade_id=trade.get("trade_id"),
                resolved_bucket=winner_q,
                actual_temperature=actual_str,
                contract_payout=payout,
                resolution_time_utc=end_date,
            )
            outcome = "WIN" if payout > 0 else "LOSS"
            pnl = trade.get("position_size", 0) * (payout - trade.get("entry_price", 0))
            print(
                f"[{ts()}] RESOLVED {outcome} {trade.get('trade_id')} "
                f"{trade.get('bucket_question','')[:40]}: {actual_str}"
            )

        journal.mark_slug_resolved(slug)


# ── Main loop ─────────────────────────────────────────────────────────────────

def run():
    print(f"[{ts()}] Paradox strategy starting")
    print(f"  Strategy ID: {STRATEGY_ID}")
    print(f"  Scan interval: {SCAN_INTERVAL}s")
    print(f"  Kelly multiplier: {KELLY_MULTIPLIER}×")
    print(f"  EV threshold: ${EV_THRESHOLD}")
    print(f"  Max spread cost: ${MAX_SPREAD_COST}")
    print(f"  Webhook: {'YES' if DISCORD_WEBHOOK_URL else 'NO'}")
    print(f"  Paper trading: {'YES' if journal.JOURNAL_AVAILABLE else 'NO'}")

    while True:
        try:
            check_resolutions()
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
