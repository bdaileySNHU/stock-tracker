import sys
import os
sys.path.append(os.path.dirname(__file__))

import yfinance as yf
from db import engine
from sqlalchemy import text

def fetch_prices(ticker, period="1mo"):
    data = yf.download(ticker, period=period, auto_adjust=True, progress=False)
    data.columns = data.columns.get_level_values(0)
    data = data.reset_index()

    with engine.connect() as conn:
        # Look up asset_id
        result = conn.execute(
            text("SELECT id FROM assets WHERE ticker = :ticker"),
            {"ticker": ticker}
        )
        asset = result.fetchone()
        if not asset:
            print(f"Ticker {ticker} not found in assets table")
            return
        asset_id = asset[0]

        # Loop and insert — all inside the same connection
        for _, row in data.iterrows():
            conn.execute(
                text("""
                    INSERT INTO price_history (asset_id, date, open, high, low, close, volume)
                    VALUES (:asset_id, :date, :open, :high, :low, :close, :volume)
                    ON CONFLICT (asset_id, date) DO UPDATE
                    SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                        close = EXCLUDED.close, volume = EXCLUDED.volume
                """),
                {"asset_id": asset_id, "date": row["Date"], "open": float(row["Open"]),
                 "high": float(row["High"]), "low": float(row["Low"]),
                 "close": float(row["Close"]), "volume": int(row["Volume"])}
            )
        conn.commit() 

if __name__ == "__main__":
    with engine.connect() as conn:
        result = conn.execute(text("SELECT ticker FROM assets"))
        tickers = [row[0] for row in result.fetchall()]
    
    for ticker in tickers:
        print(f"Fetching {ticker}...")
        fetch_prices(ticker)
