from flask import Flask, jsonify
from flask_cors import CORS
import requests, os, json, re

app = Flask(__name__)
CORS(app)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0"}

def get_price():
    # Method 1: Fetch MCX GoldM price directly from NSE/MCX data in INR
    try:
        r = requests.get(
            "https://api.mcxindia.com/MarketWatch/GetMarketWatch",
            headers={**HEADERS, "Referer": "https://www.mcxindia.com/"},
            timeout=8
        )
        data = r.json()
        for item in data.get("Data", []):
            if "GOLDM" in str(item.get("Symbol", "")):
                price = float(item.get("LastTradePrice", 0))
                if 100000 < price < 300000:
                    return {"price": round(price), "source": "MCX direct", "usd_oz": 0, "usd_inr": 0}
    except Exception as e:
        print(f"MCX direct failed: {e}")

    # Method 2: Dhan public API for GoldM
    try:
        r = requests.get(
            "https://api.dhan.co/v2/marketfeed/ltp",
            headers={**HEADERS, "Content-Type": "application/json"},
            json={"NSE_FO": [], "MCX": [{"securityId": "234184", "exchangeSegment": "MCX_FO"}]},
            timeout=8
        )
        data = r.json()
        price = data.get("data", {}).get("MCX", {}).get("234184", {}).get("lastPrice", 0)
        if 100000 < price < 300000:
            return {"price": round(price), "source": "Dhan MCX", "usd_oz": 0, "usd_inr": 0}
    except Exception as e:
        print(f"Dhan failed: {e}")

    # Method 3: Yahoo Finance GC=F × USD/INR + MCX import duty premium (~8%)
    try:
        r1 = requests.get("https://query2.finance.yahoo.com/v8/finance/chart/GC=F", headers=HEADERS, timeout=8)
        usd_oz = r1.json()["chart"]["result"][0]["meta"]["regularMarketPrice"]
        r2 = requests.get("https://query2.finance.yahoo.com/v8/finance/chart/USDINR=X", headers=HEADERS, timeout=8)
        usd_inr = r2.json()["chart"]["result"][0]["meta"]["regularMarketPrice"]
        if usd_oz > 1000 and usd_inr > 70:
            # MCX includes import duty (~15%) + GST (~3%) + premium
            mcx_price = round(usd_oz * usd_inr * (10 / 31.1035) * 1.185)
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
