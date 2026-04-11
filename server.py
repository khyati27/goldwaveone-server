from flask import Flask, jsonify
from flask_cors import CORS
import requests, os

app = Flask(__name__)
CORS(app)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

def get_price():
    # --- Gold price ---
    usd_oz = None

    # Gold source 1: Yahoo Finance v8 chart for GC=F
    try:
        r = requests.get(
            "https://query2.finance.yahoo.com/v8/finance/chart/GC=F",
            params={"interval": "1m", "range": "1d"},
            headers=HEADERS, timeout=8
        )
        val = r.json()["chart"]["result"][0]["meta"]["regularMarketPrice"]
        print(f"Yahoo gold raw value: {val}")
        if 2000 < val < 5000:
            usd_oz = val
        else:
            print(f"Yahoo gold sanity check failed: {val}")
    except Exception as e:
        print(f"Yahoo gold failed: {e}")

    # Gold source 2: metals.live fallback
    if usd_oz is None:
        try:
            r = requests.get("https://api.metals.live/v1/spot/gold", timeout=8)
            data = r.json()
            val = data[0]["gold"] if isinstance(data, list) else data["price"]
            print(f"metals.live gold raw value: {val}")
            if 2000 < val < 5000:
                usd_oz = val
            else:
                print(f"metals.live gold sanity check failed: {val}")
        except Exception as e:
            print(f"metals.live gold failed: {e}")

    # --- USD/INR rate ---
    usd_inr = None

    # INR source: open.er-api.com (reliable)
    try:
        r = requests.get("https://open.er-api.com/v6/latest/USD", timeout=8)
        val = r.json()["rates"]["INR"]
        print(f"open.er-api USD/INR raw value: {val}")
        if 80 < val < 90:
            usd_inr = val
        else:
            print(f"open.er-api INR sanity check failed: {val}")
    except Exception as e:
        print(f"open.er-api INR failed: {e}")

    # INR fallback
    if usd_inr is None:
        usd_inr = 84.2
        print(f"Using INR fallback: {usd_inr}")

    # --- Final calculation ---
    if usd_oz is not None:
        price = round(usd_oz * usd_inr * 10 / 31.1035)
        return {
            "price": price,
            "usd_oz": round(usd_oz, 2),
            "usd_inr": round(usd_inr, 4),
            "source": "live"
        }

    # All gold sources failed — hardcoded fallback
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
