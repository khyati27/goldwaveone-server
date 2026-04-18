from flask import Flask, jsonify, request
from flask_cors import CORS
import requests, os, json, re
import pg8000.native
from urllib.parse import urlparse

app = Flask(__name__)
CORS(app)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0"}

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

def get_price():
    # Method 1: GoldAPI.io - direct INR price per gram
    try:
        r = requests.get(
            "https://www.goldapi.io/api/XAU/INR",
            headers={"x-access-token": "goldapi-g4mr4smnuf9ldy-io", "Content-Type": "application/json"},
            timeout=8
        )
        d = r.json()
        price_per_gram = d.get("price_gram_24k", 0)
        if price_per_gram > 1000:
            mcx_price = round(price_per_gram * 10 * 1.09)
            return {"price": mcx_price, "usd_oz": d.get("price", 0), "usd_inr": round(d.get("price", 0) / (price_per_gram / 31.1035), 2), "source": "goldapi.io"}
    except Exception as e:
        print(f"GoldAPI failed: {e}")

    # Fallback: Yahoo + MCX duty factor
    try:
        r1 = requests.get("https://query2.finance.yahoo.com/v8/finance/chart/GC=F", headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        usd_oz = r1.json()["chart"]["result"][0]["meta"]["regularMarketPrice"]
        r2 = requests.get("https://query2.finance.yahoo.com/v8/finance/chart/USDINR=X", headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        usd_inr = r2.json()["chart"]["result"][0]["meta"]["regularMarketPrice"]
        if usd_oz > 1000 and usd_inr > 70:
            mcx_price = round(usd_oz * usd_inr * (10 / 31.1035) * 1.09)
            if 100000 < mcx_price < 300000:
                return {"price": mcx_price, "usd_oz": round(usd_oz,2), "usd_inr": round(usd_inr,4), "source": "Yahoo+duty"}
    except Exception as e:
        print(f"Yahoo failed: {e}")

    return {"price": 155222, "usd_oz": 0, "usd_inr": 0, "source": "cached"}

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
