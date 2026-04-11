from flask import Flask, jsonify
from flask_cors import CORS
import requests, os

app = Flask(__name__)
CORS(app)  # allow all origins — needed for browser requests

def get_mcx_goldm_price():
    """Try multiple sources to get live MCX GoldM price (INR per 10g)."""

    # Source 1: Yahoo Finance (works server-side, no CORS issue)
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        r1 = requests.get("https://query1.finance.yahoo.com/v8/finance/chart/GC=F?interval=1m&range=1d",
                          headers=headers, timeout=8)
        d1 = r1.json()
        usd_oz = d1["chart"]["result"][0]["meta"]["regularMarketPrice"]

        r2 = requests.get("https://query1.finance.yahoo.com/v8/finance/chart/USDINR=X?interval=1m&range=1d",
                          headers=headers, timeout=8)
        d2 = r2.json()
        usd_inr = d2["chart"]["result"][0]["meta"]["regularMarketPrice"]

        price = round(usd_oz * usd_inr * (10 / 31.1035))
        return {"price": price, "usd_oz": round(usd_oz, 2), "usd_inr": round(usd_inr, 4), "source": "Yahoo Finance"}
    except Exception as e:
        print(f"Yahoo failed: {e}")

    # Source 2: Metals API
    try:
        r = requests.get("https://api.metals.live/v1/spot/gold", timeout=8)
        d = r.json()
        usd_oz = d["price"]
        r2 = requests.get("https://open.er-api.com/v6/latest/USD", timeout=8)
        d2 = r2.json()
        usd_inr = d2["rates"]["INR"]
        price = round(usd_oz * usd_inr * (10 / 31.1035))
        return {"price": price, "usd_oz": round(usd_oz, 2), "usd_inr": round(usd_inr, 4), "source": "metals.live"}
    except Exception as e:
        print(f"metals.live failed: {e}")

    # Fallback
    return {"price": 155222, "usd_oz": 3230, "usd_inr": 84.2, "source": "cached", "error": "All sources failed"}


@app.route("/price")
def price():
    data = get_mcx_goldm_price()
    return jsonify(data)

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    print(f"GoldWave price server starting on port {port}")
    app.run(host="0.0.0.0", port=port)
