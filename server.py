from flask import Flask, jsonify
from flask_cors import CORS
import requests, os, json, re

app = Flask(__name__)
CORS(app)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0"}

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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
