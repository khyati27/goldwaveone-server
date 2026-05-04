from flask import Flask, jsonify, request
from flask_cors import CORS
import requests, os
import pg8000.native
import pyotp
import anthropic as anthropic_sdk
from urllib.parse import urlparse
from datetime import date, timedelta, datetime, timezone
import calendar
import threading
import time
import json
import re

app = Flask(__name__)
CORS(app)

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db():
    url = urlparse(DATABASE_URL)
    return pg8000.native.Connection(
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port or 5432,
        database=url.path.lstrip('/')
    )

def _to_dicts(conn, rows):
    cols = [c['name'] for c in conn.columns]
    result = []
    for row in rows:
        d = dict(zip(cols, row))
        for k, v in d.items():
            if hasattr(v, 'isoformat'):
                d[k] = v.isoformat()
        result.append(d)
    return result

def init_db():
    conn = get_db()
    conn.run("""
        CREATE TABLE IF NOT EXISTS signals (
            id SERIAL PRIMARY KEY,
            type VARCHAR(10) NOT NULL,
            entry_price NUMERIC,
            target NUMERIC,
            stop_loss NUMERIC,
            status VARCHAR(10) DEFAULT 'open',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            closed_at TIMESTAMPTZ,
            close_price NUMERIC,
            notes TEXT,
            score INTEGER,
            checks JSONB
        )
    """)
    # Add columns if table already exists without them
    for col_sql in [
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS score INTEGER",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS checks JSONB",
    ]:
        try:
            conn.run(col_sql)
        except Exception:
            pass
    conn.run("""
        CREATE TABLE IF NOT EXISTS price_history (
            id SERIAL PRIMARY KEY,
            price NUMERIC,
            usd_oz NUMERIC,
            usd_inr NUMERIC,
            source VARCHAR(20),
            recorded_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    conn.run("""
        CREATE TABLE IF NOT EXISTS current_signal (
            instrument VARCHAR(10) PRIMARY KEY,
            direction VARCHAR(10),
            entry NUMERIC,
            sl NUMERIC,
            t1 NUMERIC,
            t2 NUMERIC,
            score INTEGER,
            reasoning TEXT,
            conditions_summary TEXT,
            raw_json TEXT,
            scanned_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    conn.close()

try:
    init_db()
except Exception as e:
    print(f"DB init warning: {e}")


ANGEL_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "X-UserType": "USER",
    "X-SourceID": "WEB",
    "X-ClientLocalIP": "127.0.0.1",
    "X-ClientPublicIP": "127.0.0.1",
    "X-MACAddress": "00:00:00:00:00:00",
}

def get_angel_price():
    """Fetch live MCX GoldM LTP via Angel One REST API. Returns INR price per 10g or None."""
    try:
        api_key     = os.environ.get("ANGEL_API_KEY")
        client_id   = os.environ.get("ANGEL_CLIENT_ID")
        pin         = os.environ.get("ANGEL_PIN")
        totp_secret = os.environ.get("ANGEL_TOTP_SECRET")

        if not all([api_key, client_id, pin, totp_secret]):
            print("Angel: missing env vars")
            return None

        totp = pyotp.TOTP(totp_secret).now()
        login_headers = {**ANGEL_HEADERS, "X-PrivateKey": api_key}

        # Step 1: Login
        auth_resp = requests.post(
            "https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword",
            json={"clientcode": client_id, "password": pin, "totp": totp},
            headers=login_headers,
            timeout=10
        )
        print(f"Angel login: status={auth_resp.status_code} body={auth_resp.text[:300]}")
        auth_data = auth_resp.json()
        if not auth_data.get("status"):
            print(f"Angel login failed: {auth_data.get('message')} errorcode={auth_data.get('errorcode')}")
            return None

        jwt_token = auth_data["data"]["jwtToken"]
        auth_headers = {**login_headers, "Authorization": f"Bearer {jwt_token}"}

        # Step 2: Search for active GoldM futures contract to get symbol token
        search_resp = requests.get(
            "https://apiconnect.angelone.in/rest/secure/angelbroking/order/v1/searchScrip",
            params={"exchange": "MCX", "searchscrip": "GOLDM"},
            headers=auth_headers,
            timeout=10
        )
        print(f"Angel searchScrip: status={search_resp.status_code} body={search_resp.text[:300]}")
        search_data = search_resp.json()
        if not search_data.get("status") or not search_data.get("data"):
            print(f"Angel searchScrip failed: {search_data.get('message')} errorcode={search_data.get('errorcode')}")
            return None

        contracts = [s for s in search_data["data"] if "FUT" in s.get("tradingsymbol", "")]
        if not contracts:
            print(f"No GoldM futures contracts found in {[s.get('tradingsymbol') for s in search_data['data']]}")
            return None

        token = contracts[0]["symboltoken"]
        print(f"Angel: using contract {contracts[0].get('tradingsymbol')} token={token}")

        # Step 3: Fetch LTP
        ltp_resp = requests.post(
            "https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/",
            json={"mode": "LTP", "exchangeTokens": {"MCX": [token]}},
            headers=auth_headers,
            timeout=10
        )
        print(f"Angel LTP: status={ltp_resp.status_code} body={ltp_resp.text[:300]}")
        ltp_data = ltp_resp.json()
        if ltp_data.get("status") and ltp_data.get("data"):
            fetched = ltp_data["data"].get("fetched", [])
            if fetched:
                ltp = fetched[0].get("ltp", 0)
                print(f"Angel LTP value: {ltp}")
                if ltp > 10000:
                    return round(ltp)
            else:
                print(f"Angel LTP: empty fetched list, full data={ltp_data['data']}")
        else:
            print(f"Angel LTP failed: {ltp_data.get('message')} errorcode={ltp_data.get('errorcode')}")
    except Exception as e:
        print(f"Angel REST API exception: {e}")

    return None

YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0"}

MACRO_SYMBOLS = {
    "dxy":       "DX-Y.NYB",
    "us10y":     "^TNX",
    "crude_oil": "CL=F",
    "sp500":     "^GSPC",
    "gold_usd":  "GC=F",
}

def get_usd_inr():
    """Fetch USD/INR rate from Frankfurter API. Returns None if fetch fails."""
    try:
        r = requests.get("https://api.frankfurter.app/latest?from=USD&to=INR", timeout=8)
        rate = r.json()["rates"]["INR"]
        if rate > 70:
            print(f"USD/INR source: Frankfurter ({rate})")
            return round(rate, 4)
    except Exception as e:
        print(f"Frankfurter USD/INR failed: {e}")

    print("USD/INR: Frankfurter failed, returning None")
    return None

RBI_DATES = [
    date(2026, 2, 7), date(2026, 4, 9), date(2026, 6, 6),
    date(2026, 8, 8), date(2026, 10, 7), date(2026, 12, 9),
]

FED_DATES = [
    date(2026, 1, 29), date(2026, 3, 19), date(2026, 5, 7),
    date(2026, 6, 18), date(2026, 7, 30), date(2026, 9, 17),
    date(2026, 11, 5), date(2026, 12, 17),
]

def last_thursday(year, month):
    """Return date of last Thursday in given month."""
    last_day = calendar.monthrange(year, month)[1]
    d = date(year, month, last_day)
    # Thursday = weekday 3
    offset = (d.weekday() - 3) % 7
    return d - timedelta(days=offset)

def event_soon(event_dates, today, window=2):
    """Return True if today is within `window` days before any event date."""
    for d in event_dates:
        if timedelta(0) <= (d - today) <= timedelta(days=window):
            return True
    return False

def get_india_vix():
    """Fetch India VIX from NSE allIndices API."""
    try:
        r = requests.get(
            "https://www.nseindia.com/api/allIndices",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Referer": "https://www.nseindia.com",
            },
            timeout=8
        )
        indices = r.json().get("data", [])
        for idx in indices:
            if idx.get("index") == "India VIX":
                price = idx.get("last")
                print(f"India VIX: {price}")
                return price
        print("India VIX: not found in response")
    except Exception as e:
        print(f"India VIX fetch failed: {e}")
    return None

def get_macro_data():
    result = {}
    for key, symbol in MACRO_SYMBOLS.items():
        try:
            r = requests.get(
                f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}",
                headers=YAHOO_HEADERS,
                timeout=8
            )
            data = r.json()["chart"]["result"][0]
            meta = data["meta"]
            print(f"Macro fetch succeeded for {symbol}")
            result[key] = {
                "symbol":     symbol,
                "price":      meta.get("regularMarketPrice"),
                "change_pct": meta.get("regularMarketChangePercent"),
                "volume":     meta.get("regularMarketVolume"),
            }
        except Exception as e:
            print(f"Macro fetch failed for {symbol}: {e}")
            result[key] = {"symbol": symbol, "price": None, "change_pct": None, "volume": None}

    result["usd_inr"] = {"symbol": "USDINR=X", "price": get_usd_inr(), "change_pct": None}
    result["india_vix"] = {"symbol": "INDIAVIX", "price": get_india_vix(), "change_pct": None}

    # Silver and Gold/Silver ratio
    try:
        r = requests.get(
            "https://query2.finance.yahoo.com/v8/finance/chart/SI=F",
            headers=YAHOO_HEADERS, timeout=8
        )
        silver_price = r.json()["chart"]["result"][0]["meta"].get("regularMarketPrice")
        result["silver"] = {"symbol": "SI=F", "price": silver_price, "change_pct": None}
        gold_price = result.get("gold_usd", {}).get("price")
        if gold_price and silver_price and silver_price > 0:
            ratio = round(gold_price / silver_price, 2)
            result["gold_silver_ratio"] = {
                "ratio": ratio,
                "context": "gold expensive vs silver" if ratio > 80 else "normal range",
            }
            print(f"Gold/Silver ratio: {ratio}")
        else:
            result["gold_silver_ratio"] = {"ratio": None, "context": None}
    except Exception as e:
        print(f"Silver fetch failed: {e}")
        result["silver"] = {"symbol": "SI=F", "price": None, "change_pct": None}
        result["gold_silver_ratio"] = {"ratio": None, "context": None}

    # DXY 5-day trend
    try:
        r = requests.get(
            "https://query2.finance.yahoo.com/v8/finance/chart/DX-Y.NYB",
            params={"range": "5d", "interval": "1d"},
            headers=YAHOO_HEADERS, timeout=8
        )
        chart = r.json()["chart"]["result"][0]
        closes = chart.get("indicators", {}).get("quote", [{}])[0].get("close", [])
        closes = [c for c in closes if c is not None]
        if len(closes) >= 2:
            dxy_old = closes[0]
            dxy_now = closes[-1]
            dxy_change_5d = round((dxy_now - dxy_old) / dxy_old * 100, 2)
            result["dxy_trend"] = "rising" if dxy_change_5d > 0 else "falling"
            result["dxy_change_5d"] = dxy_change_5d
            print(f"DXY 5d trend: {result['dxy_trend']} ({dxy_change_5d}%)")
        else:
            result["dxy_trend"] = None
            result["dxy_change_5d"] = None
    except Exception as e:
        print(f"DXY trend fetch failed: {e}")
        result["dxy_trend"] = None
        result["dxy_change_5d"] = None

    # COMEX volume (already fetched via gold_usd / GC=F above)
    result["comex_volume"] = result.get("gold_usd", {}).get("volume")

    today = date.today()
    result["rbi_event_soon"] = event_soon(RBI_DATES, today)
    result["fed_event_soon"] = event_soon(FED_DATES, today)

    expiry = last_thursday(today.year, today.month)
    days_to_expiry = (expiry - today).days
    result["expiry_week"] = 0 <= days_to_expiry <= 5
    result["days_to_expiry"] = days_to_expiry

    return result


def get_xau_spot_price(gold_usd_fallback=None):
    """Fetch live XAU/USD spot price. Returns price or None."""
    try:
        token = os.environ.get("GOLDAPI_KEY", "goldapi-g4mr4smnuf9ldy-io")
        r = requests.get(
            "https://www.goldapi.io/api/XAU/USD",
            headers={"x-access-token": token, "Content-Type": "application/json"},
            timeout=8
        )
        print(f"GoldAPI: status={r.status_code} body={r.text[:200]}")
        price = r.json().get("price")
        if price and float(price) > 500:
            print(f"GoldAPI XAU/USD: {price}")
            return round(float(price), 2)
    except Exception as e:
        print(f"GoldAPI failed: {e}")

    if gold_usd_fallback and gold_usd_fallback > 500:
        print(f"XAU spot fallback: using gold_usd={gold_usd_fallback}")
        return round(float(gold_usd_fallback), 2)

    return None


def get_comex_mcx_basis(mcx_price, gold_usd, usd_inr):
    """Calculate COMEX-MCX basis given live prices."""
    if not all([mcx_price, gold_usd, usd_inr]):
        return None, None
    comex_inr = round(gold_usd * usd_inr * 10 / 31.1035)
    basis = round(mcx_price - comex_inr)
    basis_pct = round(basis / comex_inr * 100, 2) if comex_inr else None
    return basis, basis_pct

TRUEDATA_CREDS = {"user_id": "trial881", "password": "khyati881"}

def get_truedata_price():
    """Probe TrueData endpoints for MCX GoldM LTP. Prints status and response for each."""
    symbol = "GOLDM-I"

    # Endpoint 1: GET /marketdata/ltp
    try:
        r = requests.get(
            "https://api.truedata.in/marketdata/ltp",
            params={**TRUEDATA_CREDS, "symbol": symbol},
            timeout=10
        )
        print(f"TrueData ep1 GET /marketdata/ltp: status={r.status_code} body={r.text[:500]}")
        ltp = r.json().get("ltp", 0)
        if ltp and ltp > 10000:
            print(f"TrueData ep1: valid LTP={ltp}")
            return round(ltp)
    except Exception as e:
        print(f"TrueData ep1 exception: {e}")

    # Endpoint 2: GET /ltp
    try:
        r = requests.get(
            "https://api.truedata.in/ltp",
            params={**TRUEDATA_CREDS, "symbol": symbol},
            timeout=10
        )
        print(f"TrueData ep2 GET /ltp: status={r.status_code} body={r.text[:500]}")
        ltp = r.json().get("ltp", 0)
        if ltp and ltp > 10000:
            print(f"TrueData ep2: valid LTP={ltp}")
            return round(ltp)
    except Exception as e:
        print(f"TrueData ep2 exception: {e}")

    # Endpoint 3: POST /api/ltp
    try:
        r = requests.post(
            "https://api.truedata.in/api/ltp",
            json={**TRUEDATA_CREDS, "symbol": symbol},
            timeout=10
        )
        print(f"TrueData ep3 POST /api/ltp: status={r.status_code} body={r.text[:500]}")
        ltp = r.json().get("ltp", 0)
        if ltp and ltp > 10000:
            print(f"TrueData ep3: valid LTP={ltp}")
            return round(ltp)
    except Exception as e:
        print(f"TrueData ep3 exception: {e}")

    return None

def get_price():
    macro = get_macro_data()
    usd_inr = macro.get("usd_inr", {}).get("price")
    usd_oz  = macro.get("gold_usd", {}).get("price")

    price = get_truedata_price()
    source = "truedata"
    if not price:
        price = get_angel_price()
        source = "angel"

    if price:
        result = {"price": price, "usd_oz": usd_oz, "usd_inr": usd_inr, "source": source}
    elif usd_oz and usd_inr:
        calc_price = round(usd_oz * usd_inr * (10 / 31.1035) * 1.0681)
        print(f"Calculated MCX price: {calc_price} (usd_oz={usd_oz}, usd_inr={usd_inr})")
        result = {"price": calc_price, "usd_oz": usd_oz, "usd_inr": usd_inr, "source": "calculated"}
    else:
        result = {"price": 0, "usd_oz": usd_oz, "usd_inr": usd_inr, "source": "unavailable"}

    missing = [f for f in ("price", "usd_oz", "usd_inr") if not result.get(f)]
    if missing:
        result["data_quality"] = "partial"
        result["missing_fields"] = missing
    else:
        result["data_quality"] = "complete"

    basis, basis_pct = get_comex_mcx_basis(result.get("price"), usd_oz, usd_inr)
    result["comex_mcx_basis"] = basis
    result["comex_mcx_basis_pct"] = basis_pct

    xau_spot = get_xau_spot_price(gold_usd_fallback=usd_oz)
    result["xau_spot"] = xau_spot
    if xau_spot:
        result["xau_bid"] = round(xau_spot - 0.30, 2)
        result["xau_ask"] = round(xau_spot + 0.30, 2)
    else:
        result["xau_bid"] = None
        result["xau_ask"] = None

    result["macro_data"] = macro
    return result

def record_price(data):
    try:
        conn = get_db()
        conn.run(
            """INSERT INTO price_history (price, usd_oz, usd_inr, source)
               VALUES (:price, :usd_oz, :usd_inr, :source)""",
            price=data.get("price"),
            usd_oz=data.get("usd_oz"),
            usd_inr=data.get("usd_inr"),
            source=data.get("source"),
        )
        conn.close()
    except Exception as e:
        print(f"price_history insert failed: {e}")

@app.route("/price")
def price():
    data = get_price()
    threading.Thread(target=record_price, args=(data,), daemon=True).start()
    return jsonify(data)

@app.route("/macro")
def macro():
    return jsonify(get_macro_data())

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

@app.route("/signals/patterns", methods=["GET"])
def signals_patterns():
    return jsonify(analyze_signal_patterns())


@app.route("/signals", methods=["GET"])
def get_signals():
    conn = get_db()
    rows = conn.run("SELECT * FROM signals WHERE status = 'open' ORDER BY created_at DESC")
    result = _to_dicts(conn, rows)
    conn.close()
    return jsonify(result)

@app.route("/signals", methods=["POST"])
def create_signal():
    data = request.get_json()
    if not data or "type" not in data:
        return jsonify({"error": "type is required"}), 400
    conn = get_db()
    rows = conn.run(
        """INSERT INTO signals (type, entry_price, target, stop_loss, notes)
           VALUES (:type, :entry_price, :target, :stop_loss, :notes) RETURNING *""",
        type=data["type"],
        entry_price=data.get("entry_price"),
        target=data.get("target"),
        stop_loss=data.get("stop_loss"),
        notes=data.get("notes")
    )
    row = _to_dicts(conn, rows)[0]
    conn.close()
    return jsonify(row), 201

@app.route("/signals/<int:signal_id>/close", methods=["POST"])
def close_signal(signal_id):
    data = request.get_json() or {}
    conn = get_db()
    rows = conn.run(
        """UPDATE signals SET status = 'closed', closed_at = NOW(), close_price = :close_price
           WHERE id = :id AND status = 'open' RETURNING *""",
        close_price=data.get("close_price"),
        id=signal_id
    )
    if not rows:
        conn.close()
        return jsonify({"error": "signal not found or already closed"}), 404
    row = _to_dicts(conn, rows)[0]
    conn.close()
    return jsonify(row)

@app.route("/signals/history", methods=["GET"])
def signals_history():
    conn = get_db()
    rows = conn.run("SELECT * FROM signals WHERE status = 'closed' ORDER BY closed_at DESC")
    result = _to_dicts(conn, rows)
    conn.close()
    return jsonify(result)

MCX_SYSTEM_PROMPT = """You are the GoldWave One signal engine for MCX GoldM futures (Gold Mini, 10g lot, MCX India).

Analyse current market conditions using a 13-factor model and return a signal at ANY confidence level above 0%.

13 FACTORS:
MACRO (40pts): USD/INR direction (+18), US tariff/geopolitical (+15), RBI/macro calendar (0 to -10), China/PBOC risk (-6 to 0), COMEX-MCX alignment (+8), rupee-gold basis (+5)
TECHNICAL (27pts): Elliott Wave structure (+12), MCX OI trend (+8), entry bar volume (+7)
SESSION/TIMING (13pts): MCX session timing (+6), day of week (+4), expiry proximity (+3)
LEARNED (22pts): Historical win rate (+10), poor signal fingerprint (+7), SL Rule 1 compliance (+5)

SIGNAL TIERS:
- 0-39%: Monitoring — very early, directional bias forming
- 40-54%: Developing — conditions building
- 55-79%: Watching — signal forming, do NOT trade yet
- 80-100%: Active trade — confirmed, enter the trade

RULES:
Rule 1: Min SL buffer ₹400. No short within 2hrs of RBI/Fed/PBOC.
Rule 2: First trade exits 50% at T1, trail rest with cost SL.
Rule 3: Min 3/5 Elliott Wave rules confirmed before active trade.
Rule 4: Only score 80+ triggers "active" status. Below 80 = informational only.

Respond ONLY with valid JSON:
{
  "score": <0-100>,
  "direction": "long" | "short",
  "entry": <integer rupees - realistic MCX GoldM price>,
  "sl": <integer rupees>,
  "t1": <integer rupees>,
  "t2": <integer rupees>,
  "checks": [{"label":"max 4 words","status":"pass"|"warn"|"fail"}],
  "reasoning": "2-3 sentences. Current conditions, key factors, trend direction.",
  "close_trade_ids": [],
  "close_reason": null,
  "conditions_summary": "1 sentence market summary"
}
Always return a direction and levels. CRITICAL: Live price data is always provided — never mark USD/INR or COMEX as unavailable or missing in checks. SL distance from entry >= 400. checks has 8-10 items."""

XAU_SYSTEM_PROMPT = """You are the GoldWave One signal engine for XAU/USD spot gold (forex/COMEX).

Analyse current market conditions using a 13-factor model adapted for international gold trading.

13 FACTORS FOR XAU/USD:
MACRO (40pts): DXY direction (+18), US Fed policy/rates (+15), geopolitical safe-haven demand (+15), US CPI/inflation data (0 to -10), China demand outlook (-6 to 0), US 10-year yield impact (+8)
TECHNICAL (27pts): Elliott Wave structure (+12), volume/open interest trend (+8), key level proximity (+7)
SESSION/TIMING (13pts): London/NY session overlap (+6), day of week (+4), economic calendar timing (+3)
LEARNED (22pts): Historical win rate same setup (+10), poor signal fingerprint (+7), risk/reward compliance (+5)

RULES:
Rule 1: Min SL buffer $3.50/oz including spread cost. No short within 2hrs of Fed/CPI/NFP event.
Rule 2: Exit 50% at T1, trail rest with cost SL.
Rule 3: Min 3/5 Elliott Wave rules confirmed.
Rule 4: Score <55 = monitoring. 55-79 = watching. 80+ = active trade.

CFD SPREAD CONTEXT:
Current XAU/USD spot, Bid, and Ask prices are provided in the user message.
Typical CFD spread: $0.30-0.50/oz (shown as ~$0.60 round-trip cost).
Account for spread in SL placement — SL must be at least $3.50/oz from entry including spread cost.
For a long: entry at Ask price, SL at least $3.50 below entry.
For a short: entry at Bid price, SL at least $3.50 above entry.

Respond ONLY with valid JSON:
{
  "score": <0-100>,
  "direction": "long" | "short",
  "entry": <USD price per oz, e.g. 3230.50>,
  "sl": <USD price per oz>,
  "t1": <USD price per oz>,
  "t2": <USD price per oz>,
  "checks": [{"label":"max 4 words","status":"pass"|"warn"|"fail"}],
  "reasoning": "2-3 sentences. Current macro conditions, DXY direction, key levels.",
  "conditions_summary": "1 sentence market summary"
}
SL distance from entry >= 3.50 (includes spread). checks has 8-10 items. Always return direction and levels."""


def build_mcx_prompt(price_data, macro):
    now = datetime.now(timezone.utc)
    ist_offset = timedelta(hours=5, minutes=30)
    ist = now + ist_offset
    date_str = ist.strftime("%A, %d %B %Y")
    time_str = ist.strftime("%I:%M %p")
    utc_h = now.hour
    if 13 <= utc_h < 17:
        session = "London session"
    elif 13 <= utc_h < 21:
        session = "London-NY overlap (highest liquidity)"
    elif utc_h >= 21 or utc_h < 2:
        session = "NY session"
    else:
        session = "Asian session (lower liquidity)"

    p = price_data.get("price", 0)
    usd_oz = price_data.get("usd_oz")
    usd_inr = price_data.get("usd_inr")
    source = price_data.get("source", "")
    basis_pct = price_data.get("comex_mcx_basis_pct")

    price_ctx = ""
    if p:
        price_ctx = (
            f"\n\nLIVE MCX PRICES:\nGoldM LTP: ₹{p:,}/10g (source: {source})"
            f"\nCOMEX Gold: ${usd_oz}/oz" if usd_oz else ""
        )
        if usd_inr:
            price_ctx += f"\nUSD/INR: {usd_inr}"
        if basis_pct is not None:
            price_ctx += f"\nCOMEX-MCX Basis: {basis_pct}% premium"
        price_ctx += f"\nData quality: {price_data.get('data_quality','unknown').upper()}"

    dxy = macro.get("dxy", {})
    us10y = macro.get("us10y", {})
    crude = macro.get("crude_oil", {})
    sp500 = macro.get("sp500", {})
    vix = macro.get("india_vix", {})
    gsr = macro.get("gold_silver_ratio", {})

    macro_ctx = (
        f"\n\nLIVE MACRO DATA:"
        f"\nDXY (Dollar Index): {dxy.get('price','N/A')} — {'STRONG USD = bearish for gold' if (dxy.get('price') or 0) > 100 else 'WEAK USD = bullish for gold'}"
        f"\nDXY 5-day trend: {macro.get('dxy_trend','N/A')} ({macro.get('dxy_change_5d','N/A')}%)"
        f"\nUS 10Y Yield: {us10y.get('price','N/A')}% — {'HIGH yield = bearish gold' if (us10y.get('price') or 0) > 4.5 else 'moderate yield'}"
        f"\nCrude Oil WTI: ${crude.get('price','N/A')}"
        f"\nS&P 500: {sp500.get('price','N/A')}"
        f"\nUSD/INR: {(macro.get('usd_inr') or {}).get('price','N/A')}"
        f"\nIndia VIX: {vix.get('price','N/A')} — {'HIGH fear = safe-haven gold demand' if (vix.get('price') or 0) > 20 else 'low fear'}"
        f"\nGold/Silver Ratio: {gsr.get('ratio','N/A')} (above 80 = gold expensive vs silver)"
        f"\nCOMEX Volume: {macro.get('comex_volume','N/A')}"
        f"\nDays to MCX expiry: {macro.get('days_to_expiry','N/A')} days"
        f"\nRBI event within 2 days: {'YES — avoid short positions' if macro.get('rbi_event_soon') else 'No'}"
        f"\nFed event within 2 days: {'YES — heightened volatility expected' if macro.get('fed_event_soon') else 'No'}"
        f"\nMCX expiry week: {'YES — expect higher volatility, reduce position size' if macro.get('expiry_week') else 'No'}"
    )

    return (
        f"Scan GoldM MCX now.\nDate: {date_str}, {time_str} IST\nSession: {session}"
        f"\nDay: {ist.strftime('%A')}{price_ctx}{macro_ctx}"
        "\n\nReturn your best signal assessment. Use the LIVE MCX PRICES above for all entry/SL/target values — "
        "do not guess prices. Always return a direction and levels. CRITICAL: Live price data is always provided — "
        "never mark USD/INR or COMEX as unavailable or missing in checks."
    )


def build_xau_prompt(price_data, macro):
    now = datetime.now(timezone.utc)
    ist_offset = timedelta(hours=5, minutes=30)
    ist = now + ist_offset
    date_str = ist.strftime("%A, %d %B %Y")
    time_str = ist.strftime("%I:%M %p")
    utc_h = now.hour
    if 13 <= utc_h < 17:
        session = "London session"
    elif 13 <= utc_h < 21:
        session = "London-NY overlap (highest liquidity)"
    elif utc_h >= 21 or utc_h < 2:
        session = "NY session"
    else:
        session = "Asian session (lower liquidity)"

    usd_oz  = price_data.get("usd_oz", 0)
    usd_inr = price_data.get("usd_inr", 0)
    xau_spot = price_data.get("xau_spot")
    xau_bid  = price_data.get("xau_bid")
    xau_ask  = price_data.get("xau_ask")

    if xau_spot:
        xau_ctx = (
            f"\n\nLIVE XAU/USD DATA:"
            f"\nXAU/USD Spot: ${xau_spot:.2f}/oz"
            f"\nBid: ${xau_bid:.2f}  Ask: ${xau_ask:.2f}"
            f"\nTypical CFD spread: $0.30-0.50/oz — account for spread in SL placement"
            f"\nUSD/INR: {usd_inr:.2f}\nData quality: COMPLETE"
        )
    elif usd_oz:
        xau_ctx = (
            f"\n\nLIVE XAU/USD DATA:\nSpot Gold (COMEX): ${usd_oz:.2f}/oz\nUSD/INR: {usd_inr:.2f}\nData quality: COMPLETE"
        )
    else:
        xau_ctx = "\n\nXAU/USD DATA: UNAVAILABLE\nAnalyse based on macro knowledge only. Reduce score if key data missing."

    dxy = macro.get("dxy", {})
    us10y = macro.get("us10y", {})
    crude = macro.get("crude_oil", {})
    sp500 = macro.get("sp500", {})
    vix = macro.get("india_vix", {})
    gsr = macro.get("gold_silver_ratio", {})

    macro_ctx = (
        f"\n\nLIVE MACRO DATA:"
        f"\nDXY (Dollar Index): {dxy.get('price','N/A')} — {'STRONG USD = bearish for gold' if (dxy.get('price') or 0) > 100 else 'WEAK USD = bullish for gold'}"
        f"\nDXY 5-day trend: {macro.get('dxy_trend','N/A')} ({macro.get('dxy_change_5d','N/A')}%)"
        f"\nUS 10Y Yield: {us10y.get('price','N/A')}% — {'HIGH yield = bearish gold' if (us10y.get('price') or 0) > 4.5 else 'moderate yield'}"
        f"\nCrude Oil WTI: ${crude.get('price','N/A')}"
        f"\nS&P 500: {sp500.get('price','N/A')}"
        f"\nIndia VIX: {vix.get('price','N/A')} — {'HIGH fear = safe-haven gold demand' if (vix.get('price') or 0) > 20 else 'low fear'}"
        f"\nGold/Silver Ratio: {gsr.get('ratio','N/A')} (above 80 = gold expensive vs silver)"
        f"\nCOMEX Volume: {macro.get('comex_volume','N/A')}"
        f"\nDays to MCX expiry: {macro.get('days_to_expiry','N/A')} days"
        f"\nRBI event within 2 days: {'YES — avoid short positions' if macro.get('rbi_event_soon') else 'No'}"
        f"\nFed event within 2 days: {'YES — heightened volatility expected' if macro.get('fed_event_soon') else 'No'}"
        f"\nMCX expiry week: {'YES — expect higher volatility, reduce position size' if macro.get('expiry_week') else 'No'}"
    )

    return (
        f"Scan XAU/USD now.\nDate: {date_str}, {time_str} IST\nSession: {session}"
        f"\nDay: {ist.strftime('%A')}{xau_ctx}{macro_ctx}"
        "\n\nReturn best signal assessment. Always return direction and levels in USD/oz."
    )


def call_claude(system_prompt, user_prompt):
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not configured")
    client = anthropic_sdk.Anthropic(api_key=api_key)
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=900,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}]
    )
    raw = "".join(b.text for b in response.content if hasattr(b, "text"))
    cleaned = re.sub(r"```json|```", "", raw).strip()
    first = cleaned.find("{")
    last = cleaned.rfind("}")
    if first < 0 or last <= first:
        raise ValueError(f"No JSON in response: {raw[:200]}")
    return json.loads(cleaned[first:last+1])


def store_signal(instrument, result):
    """INSERT OR UPDATE current_signal row for instrument. Raises on any failure."""
    score = result.get("score", "?")
    print(f"store_signal: saving {instrument.upper()} score={score} direction={result.get('direction')}")
    try:
        conn = get_db()
        conn.run(
            """INSERT INTO current_signal
                   (instrument, direction, entry, sl, t1, t2, score, reasoning, conditions_summary, raw_json, scanned_at)
               VALUES (:instrument, :direction, :entry, :sl, :t1, :t2, :score, :reasoning, :conditions_summary, :raw_json, NOW())
               ON CONFLICT (instrument) DO UPDATE SET
                   direction=EXCLUDED.direction, entry=EXCLUDED.entry, sl=EXCLUDED.sl,
                   t1=EXCLUDED.t1, t2=EXCLUDED.t2, score=EXCLUDED.score,
                   reasoning=EXCLUDED.reasoning, conditions_summary=EXCLUDED.conditions_summary,
                   raw_json=EXCLUDED.raw_json, scanned_at=EXCLUDED.scanned_at""",
            instrument=instrument,
            direction=result.get("direction"),
            entry=result.get("entry"),
            sl=result.get("sl"),
            t1=result.get("t1"),
            t2=result.get("t2"),
            score=int(score) if str(score).isdigit() else result.get("score"),
            reasoning=result.get("reasoning"),
            conditions_summary=result.get("conditions_summary"),
            raw_json=json.dumps(result),
        )
        conn.close()
        print(f"Saved {instrument.upper()} signal score: {score}")
    except Exception as e:
        print(f"store_signal({instrument}) FAILED: {e}")
        raise


DAYS = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

def analyze_signal_patterns():
    """Query closed signals and calculate win-rate patterns and failed conditions."""
    try:
        conn = get_db()

        # A win: buy closed above entry, or sell closed below entry
        win_expr = """
            CASE WHEN (type='buy'  AND close_price >= entry_price)
                   OR (type='sell' AND close_price <= entry_price)
            THEN 1 ELSE 0 END
        """
        base_filter = "status='closed' AND entry_price IS NOT NULL AND close_price IS NOT NULL"

        # 1. win_rate_by_session — group by IST hour of created_at
        rows = conn.run(f"""
            SELECT
                EXTRACT(HOUR FROM created_at AT TIME ZONE 'Asia/Kolkata')::INTEGER AS hour,
                COUNT(*)::INTEGER AS total,
                SUM({win_expr})::INTEGER AS wins
            FROM signals
            WHERE {base_filter}
            GROUP BY hour
            ORDER BY hour
        """)
        cols = [c['name'] for c in conn.columns]
        session_rows = [dict(zip(cols, r)) for r in rows]
        win_rate_by_session = {}
        for r in session_rows:
            h = r['hour']
            if 9 <= h < 12:
                label = "morning (9-12)"
            elif 12 <= h < 15:
                label = "afternoon (12-15)"
            elif 15 <= h < 18:
                label = "evening (15-18)"
            elif 18 <= h < 23:
                label = "night (18-23)"
            else:
                label = "off-hours"
            bucket = win_rate_by_session.setdefault(label, {"total": 0, "wins": 0})
            bucket["total"] += r["total"]
            bucket["wins"]  += r["wins"]
        for v in win_rate_by_session.values():
            v["win_rate"] = round(v["wins"] / v["total"] * 100, 1) if v["total"] else None

        # 2. win_rate_by_day — group by DOW (0=Sunday in pg)
        rows = conn.run(f"""
            SELECT
                EXTRACT(DOW FROM created_at AT TIME ZONE 'Asia/Kolkata')::INTEGER AS dow,
                COUNT(*)::INTEGER AS total,
                SUM({win_expr})::INTEGER AS wins
            FROM signals
            WHERE {base_filter}
            GROUP BY dow
            ORDER BY dow
        """)
        cols = [c['name'] for c in conn.columns]
        win_rate_by_day = {}
        for r in [dict(zip(cols, row)) for row in rows]:
            day_name = DAYS[r["dow"]]
            total = r["total"]
            wins  = r["wins"]
            win_rate_by_day[day_name] = {
                "total":    total,
                "wins":     wins,
                "win_rate": round(wins / total * 100, 1) if total else None,
            }

        # 3. win_rate_by_score_band
        rows = conn.run(f"""
            SELECT
                CASE
                    WHEN score >= 90 THEN '90-100'
                    WHEN score >= 85 THEN '85-90'
                    WHEN score >= 80 THEN '80-85'
                    ELSE 'below-80'
                END AS band,
                COUNT(*)::INTEGER AS total,
                SUM({win_expr})::INTEGER AS wins
            FROM signals
            WHERE {base_filter} AND score IS NOT NULL
            GROUP BY band
        """)
        cols = [c['name'] for c in conn.columns]
        win_rate_by_score_band = {}
        for r in [dict(zip(cols, row)) for row in rows]:
            band = r["band"]
            total = r["total"]
            wins  = r["wins"]
            win_rate_by_score_band[band] = {
                "total":    total,
                "wins":     wins,
                "win_rate": round(wins / total * 100, 1) if total else None,
            }

        # 4. failed_conditions — warn/fail check labels from losing trades
        rows = conn.run(f"""
            SELECT
                chk->>'label' AS label,
                COUNT(*)::INTEGER AS count
            FROM signals,
                 jsonb_array_elements(checks) AS chk
            WHERE {base_filter}
              AND checks IS NOT NULL
              AND jsonb_typeof(checks) = 'array'
              AND NOT ({win_expr.replace("THEN 1 ELSE 0 END", "THEN true ELSE false END")})
              AND chk->>'status' IN ('warn', 'fail')
            GROUP BY label
            ORDER BY count DESC
            LIMIT 10
        """)
        cols = [c['name'] for c in conn.columns]
        failed_conditions = [dict(zip(cols, r)) for r in rows]

        conn.close()
        return {
            "win_rate_by_session":    win_rate_by_session,
            "win_rate_by_day":        win_rate_by_day,
            "win_rate_by_score_band": win_rate_by_score_band,
            "failed_conditions":      failed_conditions,
            "note": "Patterns based on closed signals with entry_price and close_price recorded.",
        }
    except Exception as e:
        print(f"analyze_signal_patterns error: {e}")
        return {"error": str(e)}


def build_learned_ctx(patterns):
    """Build learnedCtx string from top 3 pattern insights in the specified sentence format."""
    if not patterns or "error" in patterns:
        return ""

    lines = []

    # Session win rates — top 3 by total signals, formatted as sentences
    by_session = patterns.get("win_rate_by_session", {})
    session_with_data = [
        (label, v) for label, v in by_session.items()
        if v.get("win_rate") is not None and v.get("total", 0) >= 2
    ]
    for label, v in sorted(session_with_data, key=lambda x: -x[1]["total"])[:2]:
        # Extract a representative hour from the label, e.g. "morning (9-12)" → "9"
        hour_hint = label.split("(")[-1].split("-")[0].strip().rstrip(")")
        lines.append(
            f"Signals at {hour_hint}:00 IST ({label}) have {v['win_rate']}% win rate "
            f"({v['wins']}/{v['total']} trades)"
        )

    # Score band win rates
    by_score = patterns.get("win_rate_by_score_band", {})
    for band in ("80-85", "85-90", "90-100"):
        v = by_score.get(band)
        if v and v.get("win_rate") is not None and v.get("total", 0) >= 1:
            lines.append(
                f"Score {band}% has {v['win_rate']}% win rate ({v['wins']}/{v['total']} trades)"
            )

    # Top 3 failed conditions
    failed = patterns.get("failed_conditions", [])
    if failed:
        top3 = [f['label'] for f in failed[:3]]
        lines.append(f"Most common warn/fail conditions in losing trades: {', '.join(top3)}")

    # Worst day
    by_day = patterns.get("win_rate_by_day", {})
    worst_day = sorted(
        [(k, v["win_rate"]) for k, v in by_day.items()
         if v.get("win_rate") is not None and v.get("total", 0) >= 2],
        key=lambda x: x[1]
    )
    if worst_day:
        lines.append(
            f"Worst trading day historically: {worst_day[0][0]} ({worst_day[0][1]}% win rate)"
        )

    if not lines:
        return ""
    return "\n\nLEARNED FROM PAST SIGNALS:\n" + "\n".join(f"- {l}" for l in lines[:3])


def run_scan():
    """Fetch price+macro+patterns, call Claude for MCX and XAU, store results. Returns (mcx_result, xau_result)."""
    print("Background scan: fetching price, macro, and patterns...")
    price_data = get_price()
    macro = price_data.get("macro_data", {})
    patterns = analyze_signal_patterns()
    learnedCtx = build_learned_ctx(patterns)

    mcx_result, xau_result = None, None
    errors = []

    # MCX — Claude call and store are separate so each failure is independently logged
    try:
        mcx_prompt = build_mcx_prompt(price_data, macro) + learnedCtx
        mcx_result = call_claude(MCX_SYSTEM_PROMPT, mcx_prompt)
        print(f"MCX Claude returned: direction={mcx_result.get('direction')} score={mcx_result.get('score')}")
    except Exception as e:
        print(f"MCX Claude call failed: {e}")
        errors.append(f"MCX Claude: {e}")

    if mcx_result:
        try:
            store_signal("mcx", mcx_result)
        except Exception as e:
            print(f"MCX store failed: {e}")
            errors.append(f"MCX store: {e}")

    # XAU — same pattern
    try:
        xau_prompt = build_xau_prompt(price_data, macro) + learnedCtx
        xau_result = call_claude(XAU_SYSTEM_PROMPT, xau_prompt)
        print(f"XAU Claude returned: direction={xau_result.get('direction')} score={xau_result.get('score')}")
    except Exception as e:
        print(f"XAU Claude call failed: {e}")
        errors.append(f"XAU Claude: {e}")

    if xau_result:
        try:
            store_signal("xau", xau_result)
        except Exception as e:
            print(f"XAU store failed: {e}")
            errors.append(f"XAU store: {e}")

    if errors:
        print(f"run_scan errors: {errors}")
    if not mcx_result and not xau_result:
        raise RuntimeError("; ".join(errors) if errors else "No signals generated")

    return mcx_result, xau_result


_scan_status = {
    "last_scan_time": None,
    "last_scan_ok": None,
    "next_scan_time": None,
    "scan_count": 0,
    "last_error": None,
}

def run_background_scan():
    """Thin wrapper around run_scan() that updates scan status tracking."""
    run_scan()
    _scan_status["scan_count"] += 1


def background_scanner():
    """Daemon thread: run scan every 120 seconds. Never exits — catches all exceptions."""
    # Wait 30s after startup before first scan
    _scan_status["next_scan_time"] = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()
    time.sleep(30)
    while True:
        try:
            run_background_scan()
            _scan_status["last_scan_time"] = datetime.now(timezone.utc).isoformat()
            _scan_status["last_scan_ok"] = True
            _scan_status["last_error"] = None
            print(f"Background scan completed at {datetime.now()}")
        except Exception as e:
            _scan_status["last_scan_time"] = datetime.now(timezone.utc).isoformat()
            _scan_status["last_scan_ok"] = False
            _scan_status["last_error"] = str(e)
            print(f"Background scan error (will retry): {e}")
        _scan_status["next_scan_time"] = (datetime.now(timezone.utc) + timedelta(seconds=120)).isoformat()
        time.sleep(120)


# Start background scanner thread
_scanner_thread = threading.Thread(target=background_scanner, daemon=True, name="scanner")
_scanner_thread.start()


@app.route("/scan/status", methods=["GET"])
def scan_status():
    return jsonify({
        **_scan_status,
        "thread_alive": _scanner_thread.is_alive(),
    })


@app.route("/signal/scan", methods=["POST"])
def signal_scan():
    try:
        mcx_result, xau_result = run_scan()
        return jsonify({"mcx": mcx_result, "xau": xau_result})
    except Exception as e:
        print(f"/signal/scan error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/signal/current", methods=["GET"])
def signal_current():
    try:
        conn = get_db()
        rows = conn.run("SELECT * FROM current_signal ORDER BY instrument")
        result = _to_dicts(conn, rows)
        conn.close()
        out = {}
        for row in result:
            instrument = row["instrument"]
            scanned_at = row.get("scanned_at")
            if row.get("raw_json"):
                try:
                    parsed = json.loads(row["raw_json"])
                    parsed["scanned_at"] = scanned_at
                    out[instrument] = parsed
                except Exception:
                    out[instrument] = row
            else:
                out[instrument] = row
        return jsonify(out)
    except Exception as e:
        print(f"/signal/current error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/analyze", methods=["POST"])
def analyze():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured"}), 500
    data = request.get_json()
    if not data or "messages" not in data:
        return jsonify({"error": "messages is required"}), 400
    try:
        client = anthropic_sdk.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=data.get("model", "claude-sonnet-4-20250514"),
            max_tokens=data.get("max_tokens", 900),
            system=data.get("system", ""),
            messages=data["messages"]
        )
        return jsonify({"content": [{"type": b.type, "text": b.text} for b in response.content]})
    except Exception as e:
        print(f"Anthropic API error: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
