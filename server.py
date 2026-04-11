from flask import Flask, jsonify
from flask_cors import CORS
import requests, os

app = Flask(__name__)
CORS(app)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

def get_price():

    # Source 1: Yahoo Finance query2 (more reliable endpoint)
    try:
        r1 = requests.get("https://query2.finance.yahoo.com/v8/finance/chart/GC=F",
                          params={"interval":"1m","range":"1d"}, headers=HEADERS, timeout=8)
        usd_oz = r1.json()["chart"]["result"][0]["meta"]["regularMarketPrice"]

        r2 = requests.get("https://query2.finance.yahoo.com/v8/finance/chart/USDINR=X",
                          params={"interval":"1m","range":"1d"}, headers=HEADERS, timeout=8)
        usd_inr = r2.json()["chart"]["result"][0]["meta"]["regularMarketPrice"]

        # Sanity check — gold should be $2,000–$5,000, INR should be 75–90
        if 2000 < usd_oz < 5000 and 75 < usd_inr < 90:
            price = round(usd_oz * usd_inr * (10 / 31.1035))
            return {"price": price, "usd_oz": round(usd_oz,2), "usd_inr": round(usd_inr,4), "source": "Yahoo Finance"}
        else:
            print(f"Yahoo sanity check failed: usd_oz={usd_oz} usd_inr={usd_inr}")
    except Exception as e:
        print(f"Yahoo failed: {e}")

    # Source 2: Open Exchange Rates for USD/INR + metals-api for gold
    try:
        r1 = requests.get("https://api.metals.live/v1/spot/gold", timeout=8)
        usd_oz = r1.json()["price"]

        r2 = requests.get("https://open.er-api.com/v6/latest/USD", timeout=8)
        usd_inr = r2.json()["rates"]["INR"]

        if 2000 < usd_oz < 5000 and 75 < usd_inr < 90:
            price = round(usd_oz * usd_inr * (10 / 31.1035))
            return {"price": price, "usd_oz": round(usd_oz,2), "usd_inr": round(usd_inr,4), "source": "metals.live"}
    except Exception as e:
        print(f"metals.live failed: {e}")

    # Source 3: Frankfurter for USD/INR + hardcoded gold fallback
    try:
        r = requests.get("https://api.frankfurter.app/latest?from=USD&to=INR", timeout=8)
        usd_inr = r.json()["rates"]["INR"]
        # Use last known gold price if we at least have INR rate
        usd_oz = 3230  # last known approximate
        price = round(usd_oz * usd_inr * (10 / 31.1035))
        return {"price": price, "usd_oz": usd_oz, "usd_inr": round(usd_inr,4), "source": "partial (INR live, gold cached)"}
    except Exception as e:
        print(f"Frankfurter failed: {e}")

    # Final fallback
    return {"price": 155222, "usd_oz": 3230, "usd_inr": 84.2, "source": "cached", "error": "all sources failed"}


@app.route("/price")
def price():
    return jsonify(get_price())

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
