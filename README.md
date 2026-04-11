# GoldWave Price Server

Tiny proxy that fetches live MCX GoldM price and returns it CORS-free to the browser app.

## Deploy to Railway (free, 5 mins)

1. railway.app → New Project → Deploy from GitHub
2. Upload these 4 files
3. Set env var: PORT=3000
4. Your price API will be at: https://YOUR-APP.railway.app/price

## API Response
GET /price
{
  "price": 155222,        ← MCX GoldM INR per 10g
  "usd_oz": 3230.50,      ← COMEX gold USD/oz
  "usd_inr": 84.20,       ← USD/INR rate
  "source": "Yahoo Finance"
}
