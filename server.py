from flask import Flask, jsonify, request
from flask_cors import CORS
import requests, os
import pg8000.native
import pyotp
import anthropic as anthropic_sdk
from urllib.parse import urlparse
from datetime import date, timedelta
import calendar

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
            notes TEXT
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

    result["macro_data"] = macro
    return result

@app.route("/price")
def price():
    return jsonify(get_price())

@app.route("/macro")
def macro():
    return jsonify(get_macro_data())

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

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
