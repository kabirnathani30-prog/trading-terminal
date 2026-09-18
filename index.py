import asyncio
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import yfinance as yf
import urllib.parse
from aiohttp import ClientSession

app = FastAPI(title="Pro Trading Terminal Cloud API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPSTOX_ACCESS_TOKEN = ""

def resolve_ticker(symbol: str, exchange: str) -> str:
    s = symbol.strip().upper()
    if exchange == "INDEX":
        if s in ["NIFTY 50", "NIFTY"]: return "^NSEI"
        if s in ["BANK NIFTY", "BANKNIFTY"]: return "^NSEBANK"
        if s in ["SENSEX", "BSE SENSEX"]: return "^BSESN"
        if s == "FINNIFTY": return "NIFTY_FIN_SERVICE.NS"
        return f"^{s}"
    if exchange == "BSE": return f"{s}.BO" if not s.endswith(".BO") else s
    if exchange == "NSE": return f"{s}.NS" if not s.endswith(".NS") else s
    return s

@app.get("/api/history")
async def get_history(symbol: str, exchange: str, timeframe: str):
    if exchange == "F&O":
        if not UPSTOX_ACCESS_TOKEN:
            raise HTTPException(status_code=400, detail="F&O requires Upstox Token")
        url = f"https://api.upstox.com/v2/historical-candle/intraday/{urllib.parse.quote(symbol)}/1minute"
        headers = {'Accept': 'application/json', 'Authorization': f'Bearer {UPSTOX_ACCESS_TOKEN}'}
        async with ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    candles = []
                    import dateutil.parser
                    for row in reversed(data["data"]["candles"]):
                        t_val = int(dateutil.parser.isoparse(row[0]).timestamp())
                        candles.append({"time": t_val, "open": row[1], "high": row[2], "low": row[3], "close": row[4]})
                    return {"symbol": symbol, "candles": candles}
        raise HTTPException(status_code=404, detail="F&O Data not found.")

    ticker = resolve_ticker(symbol, exchange)
    tf_mapping = {"1m": ("5d", "1m"), "5m": ("5d", "5m"), "15m": ("30d", "15m"), "1h": ("1mo", "60m"), "1d": ("2y", "1d"), "1w": ("5y", "1wk")}
    period, interval = tf_mapping.get(timeframe, ("30d", "15m"))
    
    try:
        loop = asyncio.get_event_loop()
        df = await loop.run_in_executor(None, lambda: yf.Ticker(ticker).history(period=period, interval=interval))
        if df.empty: raise HTTPException(status_code=404, detail="Symbol not found")
        
        candles = []
        df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])
        df = df[~df.index.duplicated(keep='first')]
        for idx, row in df.iterrows():
            candles.append({"time": int(idx.timestamp()), "open": round(float(row["Open"]), 2), "high": round(float(row["High"]), 2), "low": round(float(row["Low"]), 2), "close": round(float(row["Close"]), 2)})
        return {"symbol": symbol, "candles": candles}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/quotes")
async def get_quotes(symbols: str, exchange: str):
    sym_list = [s.strip() for s in symbols.split(",") if s.strip()]
    results = {}
    loop = asyncio.get_event_loop()
    
    def fetch_q(s):
        try:
            info = yf.Ticker(resolve_ticker(s, exchange)).fast_info
            p = info.last_price
            prv = info.previous_close
            return {"price": p, "change": p - prv, "change_pct": ((p - prv)/prv)*100 if prv else 0}
        except: return None

    for s in sym_list:
        res = await loop.run_in_executor(None, fetch_q, s)
        if res: results[s] = res
    return results

# Mount the static folder at the very end so it serves the frontend properly
app.mount("/", StaticFiles(directory="public", html=True), name="static")
