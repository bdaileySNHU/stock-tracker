import time
from db import engine
from fetch_stocks import fetch_prices
from sqlalchemy import text 

def get_tickers():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT ticker FROM assets"))
        return [row[0] for row in result.fetchall()]
    
def run():
    print("Running stock ingestion...")
    tickers = get_tickers()
    for ticker in tickers:
        print(f"Fetching {ticker}...")
        fetch_prices(ticker)
    print("Ingestion complete!")

if __name__ == "__main__":

    run()

    while True:
        time.sleep(86400)  
        run()