from flask import Flask, jsonify
from flask_cors import CORS
import requests, os

app = Flask(__name__)
CORS(app)

def get_price():
    headers = {"User-Agent": "Mozilla/5.0"}

    usd_oz = None
    usd_inr = None

    # Get gold price
    try:
        r = requests.get("https://query2.finance.yahoo.com/v8/finance/chart/GC=F", headers=headers, timeout=8)
        val = r.json()["chart"]["result"][0]["meta"]["regularMarketPrice"]
        if 2000 < val < 5000:
            usd_oz = val
        else:
            print(f"Yahoo gold sanity check failed: {val}")
    except Exception as e:
        print(f"Yahoo gold error: {e}")

    if not usd_oz:
        try:
            r = requests.get("https://api.metals.live/v1/spot/gold", timeout=8)
            val = r.json()["price"]
            if 2000 < val < 5000:
                usd_oz = val
            else:
                print(f"metals.live sanity check failed: {val}")
        except Exception as e:
            print(f"metals.live error: {e}")

    # Get USD/INR - use open.er-api only, not Yahoo
    try:
        r = requests.get("https://open.er-api.com/v6/latest/USD", timeout=8)
        val = r.json()["rates"]["INR"]
        if 80 < val < 90:
            usd_inr = val
        else:
            print(f"open.er-api INR sanity check failed: {val}")
    except Exception as e:
        print(f"open.er-api error: {e}")

    if usd_oz and usd_inr:
        price = round(usd_oz * usd_inr * 10 / 31.1035)
        return {"price": price, "usd_oz": round(usd_oz,2), "usd_inr": round(usd_inr,4), "source": "live"}

    return {"price": 155222, "usd_oz": 3230, "usd_inr": 84.2, "source": "cached"}


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
