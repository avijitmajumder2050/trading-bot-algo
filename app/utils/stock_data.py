import yfinance as yf

def get_stock_data(symbol, period="60d", interval="1d"):
    df = yf.download(f"{symbol}.NS", period=period, interval=interval)
    if df.empty:
        return None
    df["EMA10"] = df["Close"].ewm(span=10).mean()
    df["EMA20"] = df["Close"].ewm(span=20).mean()
    df["EMA50"] = df["Close"].ewm(span=50).mean()
    return df
