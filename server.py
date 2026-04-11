from flask import Flask, jsonify
from flask_cors import CORS
import requests, os

app = Flask(__name__)
CORS(app)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

def get_price():
    # Source 1: Yahoo Finance query2 v7/finance/quote (returns live market prices)
    try:
        r = requests.get(
            "https://query2.finance.yahoo.com/v7/finance/quote",
            params={"symbols": "GC=F,USDINR=X"},
            headers=HEADERS, timeout=8
        )
        results = r.json()["quoteResponse"]["result"]
        gold_data = next(x for x in results if x["symbol"] == "GC=F")
        inr_data  = next(x for x in results if x["symbol"] == "USDINR=X")
        usd_oz  = gold_data["regularMarketPrice"]
        usd_inr = inr_data["regularMarketPrice"]

        if 2000 < usd_oz < 5000 and 75 < usd_inr < 90:
            price = round(usd_oz * usd_inr * (10 / 31.1035))
            return {"price": price, "usd_oz": round(usd_oz, 2), "usd_inr": round(usd_inr, 4), "source": "Yahoo Finance"}
        else:
            print(f"Yahoo sanity check failed: usd_oz={usd_oz} usd_inr={usd_inr}")
    except Exception as e:
        print(f"Yahoo failed: {e}")

    # Source 2: metals.live (gold) + open.er-api.com (USD/INR)
    try:
        r1 = requests.get("https://api.metals.live/v1/spot/gold", timeout=8)
        data = r1.json()
        # metals.live returns either a list [{"gold": ...}] or a dict {"price": ...}
        usd_oz = data[0]["gold"] if isinstance(data, list) else data["price"]

        r2 = requests.get("https://open.er-api.com/v6/latest/USD", timeout=8)
        usd_inr = r2.json()["rates"]["INR"]

        if 2000 < usd_oz < 5000 and 75 < usd_inr < 90:
            price = round(usd_oz * usd_inr * (10 / 31.1035))
            return {"price": price, "usd_oz": round(usd_oz, 2), "usd_inr": round(usd_inr, 4), "source": "metals.live"}
        else:
            print(f"metals.live sanity check failed: usd_oz={usd_oz} usd_inr={usd_inr}")
    except Exception as e:
        print(f"metals.live failed: {e}")

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
