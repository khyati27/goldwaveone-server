from flask import Flask, jsonify, request
from flask_cors import CORS
import requests, os
import pg8000.native
import pyotp
from urllib.parse import urlparse

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
        auth_data = auth_resp.json()
        if not auth_data.get("status"):
            print(f"Angel login failed: {auth_data.get('message')}")
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
        search_data = search_resp.json()
        if not search_data.get("status") or not search_data.get("data"):
            print("Angel searchScrip returned no data")
            return None

        contracts = [s for s in search_data["data"] if "FUT" in s.get("tradingsymbol", "")]
        if not contracts:
            print("No GoldM futures contracts found")
            return None

        token = contracts[0]["symboltoken"]

        # Step 3: Fetch LTP
        ltp_resp = requests.post(
            "https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/",
            json={"mode": "LTP", "exchangeTokens": {"MCX": [token]}},
            headers=auth_headers,
            timeout=10
        )
        ltp_data = ltp_resp.json()
        if ltp_data.get("status") and ltp_data.get("data"):
            fetched = ltp_data["data"].get("fetched", [])
            if fetched:
                ltp = fetched[0].get("ltp", 0)
                if ltp > 10000:
                    return round(ltp)
    except Exception as e:
        print(f"Angel REST API failed: {e}")

    return None

def get_price():
    angel_price = get_angel_price()
    if angel_price:
        return {"price": angel_price, "usd_oz": 0, "usd_inr": 0, "source": "angel"}
    return {"price": 0, "usd_oz": 0, "usd_inr": 0, "source": "unavailable"}

@app.route("/price")
def price():
    return jsonify(get_price())

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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
