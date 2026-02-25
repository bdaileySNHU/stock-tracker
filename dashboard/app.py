import os
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'ingestion'))
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from fetch_stocks import fetch_prices

# Load environment variables from .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

# Initialize database connection
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

st.title("Stock Tracker")

## Fetch and display all assets from the database
#with engine.connect() as conn:
#    df = pd.read_sql(text("SELECT * FROM assets"), conn)
#
#st.dataframe(df)
##

# Add a stock to the table
newTicker = st.text_input("New Stock Ticker")
newName = st.text_input("New Stock Name")

if st.button("Add Stock"):
    with engine.connect() as conn:
            conn.execute(
                text("INSERT INTO assets (ticker, name) VALUES (:ticker, :name)"),
                {"ticker": newTicker.upper(), "name": newName}
          )
            conn.commit()
    fetch_prices(newTicker)  # Fetch prices for the new stock immediately after adding
    st.success(f"{newTicker.upper()} added and price history loaded!")

# Fetch available tickers for the selector
with engine.connect() as conn:
    assets_df = pd.read_sql(text("SELECT ticker, name FROM assets"), conn)

# Create a dropdown to select a specific stock
ticker = st.selectbox(
    "Select a stock",
    assets_df["ticker"].tolist(),
    format_func=lambda t: f"{t} — {assets_df.loc[assets_df['ticker']==t, 'name'].values[0]}"
)

# Fetch price history for the selected ticker
with engine.connect() as conn:
    prices_df = pd.read_sql(
        text("""
            SELECT ph.date, ph.open, ph.high, ph.low, ph.close, ph.volume
            FROM price_history ph
            JOIN assets a ON a.id = ph.asset_id
            WHERE a.ticker = :ticker
            ORDER BY ph.date
        """),
        conn,
        params={"ticker": ticker}
    )

st.subheader(f"{ticker} Price History")
st.dataframe(prices_df, width="stretch", hide_index=True)

# Create a Candlestick chart for the selected ticker
fig = go.Figure(data=[
    go.Candlestick(
        x=prices_df["date"],
        open=prices_df["open"],
        high=prices_df["high"],
        low=prices_df["low"],
        close=prices_df["close"],
        name=ticker
    )
])

# Customize the chart layout
fig.update_layout(
    title=f"{ticker} Price Chart",
    xaxis_title="Date",
    yaxis_title="Price (USD)",
    xaxis_rangeslider_visible=False,
    template="plotly_dark",
    height=450
)

# Display the chart
st.plotly_chart(fig, width="stretch")

