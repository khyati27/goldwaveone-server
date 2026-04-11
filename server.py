from flask import Flask, jsonify
from flask_cors import CORS
import requests, os

app = Flask(__name__)
CORS(app)

def get_price():
    headers = {"User-Agent": "Mozilla/5.0"}

    # Gold spot price - use GC=F but divide by correct ratio
    # Yahoo GC=F Jun26 is ~4787, spot gold is ~3230
    # Use XAUUSD=X for spot gold instead
    usd_oz = None
    try:
        r = requests.get("https://query2.finance.yahoo.com/v8/finance/chart/XAUUSD=X", headers=headers, timeout=8)
        val = r.json()["chart"]["result"][0]["meta"]["regularMarketPrice"]
        if 2000 < val < 5000:
            usd_oz = val
            print(f"Gold spot: {usd_oz}")
    except Exception as e:
        print(f"Gold spot failed: {e}")

    # Fallback gold price
    if not usd_oz:
        usd_oz = 3230

    # USD/INR - use frankfurter which is reliable
    usd_inr = None
    try:
        r = requests.get("https://api.frankfurter.app/latest?from=USD&to=INR", timeout=8)
        val = r.json()["rates"]["INR"]
        if 80 < val < 90:
            usd_inr = val
            print(f"USD/INR: {usd_inr}")
    except Exception as e:
        print(f"Frankfurter failed: {e}")

    if not usd_inr:
        usd_inr = 84.2

    price = round(usd_oz * usd_inr * 10 / 31.1035)
    source = "XAUUSD spot + Frankfurter INR" if usd_oz != 3230 and usd_inr != 84.2 else "partial/cached"
    return {"price": price, "usd_oz": round(usd_oz,2), "usd_inr": round(usd_inr,4), "source": source}


@app.route("/price")
def price():
    return jsonify(get_price())

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

@app.route("/debug")
def debug():
    headers = {"User-Agent": "Mozilla/5.0"}
    result = {}

    try:
        r = requests.get("https://query2.finance.yahoo.com/v8/finance/chart/GC=F", headers=headers, timeout=8)
        result["yahoo_gold"] = {"status": r.status_code, "body": r.json()}
    except Exception as e:
        result["yahoo_gold"] = {"error": str(e)}

    try:
        r = requests.get("https://api.metals.live/v1/spot/gold", timeout=8)
        result["metals_live"] = {"status": r.status_code, "body": r.json()}
    except Exception as e:
        result["metals_live"] = {"error": str(e)}

    try:
        r = requests.get("https://open.er-api.com/v6/latest/USD", timeout=8)
        result["open_er_api"] = {"status": r.status_code, "body": r.json()}
    except Exception as e:
        result["open_er_api"] = {"error": str(e)}

    return jsonify(result)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
