import asyncio
import re
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import yfinance as yf
import urllib.parse
from aiohttp import ClientSession

app = FastAPI(title="Pro Trading Terminal Cloud API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

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

async def fetch_df(ticker: str, period: str, interval: str):
    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, lambda: yf.Ticker(ticker).history(period=period, interval=interval))
    if not df.empty:
        df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])
        df = df[~df.index.duplicated(keep='first')]
    return df

@app.get("/api/history")
async def get_history(symbol: str, exchange: str, timeframe: str):
    clean_sym = re.sub(r'[\+\-\*\/\s]+$', '', symbol.strip()).upper()

    try:
        if exchange == "F&O":
            if not UPSTOX_ACCESS_TOKEN: raise HTTPException(status_code=400, detail="F&O requires Upstox Token")
            url = f"https://api.upstox.com/v2/historical-candle/intraday/{urllib.parse.quote(clean_sym)}/1minute"
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
                        return {"symbol": clean_sym, "candles": candles}
            raise HTTPException(status_code=404, detail="F&O Data not found.")

        tf_mapping = {"1m": ("5d", "1m"), "5m": ("5d", "5m"), "15m": ("30d", "15m"), "1h": ("1mo", "60m"), "1d": ("2y", "1d"), "1w": ("5y", "1wk"), "1M": ("10y", "1mo")}
        period, interval = tf_mapping.get(timeframe, ("30d", "15m"))
        
        # Basket Engine
        if any(op in clean_sym for op in ['+', '-', '*', '/']):
            tokens = [t for t in re.findall(r'[A-Za-z0-9_\^]+', clean_sym) if not t.isdigit()]
            if not tokens: raise HTTPException(status_code=400, detail="Invalid Basket")
            dfs = await asyncio.gather(*[fetch_df(resolve_ticker(t, exchange), period, interval) for t in tokens])
            ticker_dfs = {}
            for idx, t in enumerate(tokens):
                if dfs[idx].empty: raise HTTPException(status_code=404, detail=f"Data missing for {t}")
                ticker_dfs[t] = dfs[idx]
            common_idx = ticker_dfs[tokens[0]].index
            for t in tokens[1:]: common_idx = common_idx.intersection(ticker_dfs[t].index)
            if len(common_idx) == 0: raise HTTPException(status_code=400, detail="No overlapping timeframes")
            
            combined = pd.DataFrame(index=common_idx)
            for t in tokens:
                combined[f"{t}_o"], combined[f"{t}_h"], combined[f"{t}_l"], combined[f"{t}_c"] = ticker_dfs[t].loc[common_idx, 'Open'], ticker_dfs[t].loc[common_idx, 'High'], ticker_dfs[t].loc[common_idx, 'Low'], ticker_dfs[t].loc[common_idx, 'Close']
            
            def repl(m): return m.group(0) if m.group(0).isdigit() else f"__SYM__{m.group(0)}"
            tok_str = re.sub(r'[A-Za-z0-9_\^]+', repl, clean_sym)
            ex_o, ex_h, ex_l, ex_c = [tok_str.replace("__SYM__", "")] * 4
            for t in tokens:
                ex_o, ex_h, ex_l, ex_c = re.sub(rf'\b{t}\b', f"{t}_o", ex_o), re.sub(rf'\b{t}\b', f"{t}_h", ex_h), re.sub(rf'\b{t}\b', f"{t}_l", ex_l), re.sub(rf'\b{t}\b', f"{t}_c", ex_c)
            
            # Using engine='python' to prevent Vercel C-extension crashes
            res_o = combined.eval(ex_o, engine='python')
            res_h = combined.eval(ex_h, engine='python')
            res_l = combined.eval(ex_l, engine='python')
            res_c = combined.eval(ex_c, engine='python')
            
            candles = [{"time": int(idx.timestamp()), "open": round(float(res_o.loc[idx]), 2), "high": round(float(max(res_h.loc[idx], res_o.loc[idx], res_c.loc[idx])), 2), "low": round(float(min(res_l.loc[idx], res_o.loc[idx], res_c.loc[idx])), 2), "close": round(float(res_c.loc[idx]), 2)} for idx in common_idx]
            return {"symbol": clean_sym, "candles": candles}

        # Single Asset
        df = await fetch_df(resolve_ticker(clean_sym, exchange), period, interval)
        if df.empty: raise HTTPException(status_code=404, detail="Symbol not found")
        return {"symbol": clean_sym, "candles": [{"time": int(idx.timestamp()), "open": round(float(row["Open"]), 2), "high": round(float(row["High"]), 2), "low": round(float(row["Low"]), 2), "close": round(float(row["Close"]), 2)} for idx, row in df.iterrows()]}
    
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
            p, prv = info.last_price, info.previous_close
            return {"price": p, "change": p - prv, "change_pct": ((p - prv)/prv)*100 if prv else 0}
        except: return None
        
    all_tks = set()
    for sym in sym_list:
        clean = re.sub(r'[\+\-\*\/\s]+$', '', sym).upper()
        all_tks.update([t for t in re.findall(r'[A-Za-z0-9_\^]+', clean) if not t.isdigit()])
    t_quotes = {}
    for t in all_tks:
        r = await loop.run_in_executor(None, fetch_q, t)
        if r: t_quotes[t] = r
        
    for sym in sym_list:
        clean = re.sub(r'[\+\-\*\/\s]+$', '', sym).upper()
        if any(op in clean for op in ['+', '-', '*', '/']):
            tokens = [t for t in re.findall(r'[A-Za-z0-9_\^]+', clean) if not t.isdigit()]
            if all(t in t_quotes for t in tokens):
                ep, eprv = clean, clean
                for t in tokens:
                    ep, eprv = re.sub(rf'\b{t}\b', str(t_quotes[t]['price']), ep), re.sub(rf'\b{t}\b', str(t_quotes[t]['price'] - t_quotes[t]['change']), eprv)
                try:
                    cp, pp = float(pd.eval(ep, engine='python')), float(pd.eval(eprv, engine='python'))
                    results[sym] = {"price": cp, "change": cp - pp, "change_pct": ((cp - pp)/pp)*100 if pp else 0}
                except: pass
        else:
            if clean in t_quotes: results[sym] = t_quotes[clean]
    return results

app.mount("/", StaticFiles(directory="public", html=True), name="static")
