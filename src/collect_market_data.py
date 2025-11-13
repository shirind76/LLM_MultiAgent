"""
collect_market_data.py
----------------------
Downloads S&P 500, market index, gold, risk-free rate (T-bill),
and any extra tickers. Keeps structure identical to your notebook,
and automatically saves results in the /data folder:
  • data/sp500_data.csv  (raw MultiIndex)
  • data/market_data.csv (flattened columns)
"""

# Importing necessary libraries
import yfinance as yf
import pandas as pd
import numpy as np
import requests
from datetime import datetime
import time
import os


# ---------------------------------------------------------------------
# Get S&P 500 tickers
# ---------------------------------------------------------------------
def get_sp500_tickers():
    """
    Fetch S&P 500 tickers. Fix Yahoo format: '.' -> '-'.
    Fallback to DataHub if Wikipedia is blocked.
    """
    try:
        print("🔍 fetching S&P 500 from Wikipedia…")
        headers = {"User-Agent": "Mozilla/5.0"}
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        html = requests.get(url, headers=headers, timeout=20).text
        tables = pd.read_html(html)
        tickers = tables[0]["Symbol"].tolist()
    except Exception as e:
        print(f"⚠️ wikipedia failed ({e}); using DataHub fallback…")
        url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
        tickers = pd.read_csv(url)["Symbol"].tolist()

    tickers = [t.replace(".", "-").strip().upper() for t in tickers]
    print(f"✅ got {len(tickers)} tickers")
    return tickers


# ---------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------
if __name__ == "__main__":
    # Define data folder path
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_path = os.path.join(project_root, "data")
    os.makedirs(data_path, exist_ok=True)

    # Get tickers
    sp500 = get_sp500_tickers()

    # Adding extra tickers and options
    extra_tickers = ["BRK-B", "ARKK"]   # add anything you want here
    include_index = True
    include_gold = True
    include_rf = True

    tickers = sp500.copy()
    if include_index:
        tickers.append("^GSPC")
    if include_gold:
        tickers.append("GC=F")
    if include_rf:
        tickers.append("^IRX")
    tickers += extra_tickers

    print("📊 total to fetch:", len(tickers))

    # Download historical data
    start = "2020-01-01"
    end = datetime.today().strftime("%Y-%m-%d")
    print(f"⏳ downloading {start} → {end}")

    t0 = time.time()
    data = yf.download(
        tickers,
        start=start,
        end=end,
        progress=True,
        group_by="ticker",   # MultiIndex: ('Ticker','Price')
        threads=False,       # more reliable on many networks
        auto_adjust=False,   # consistent OHLCV
        timeout=30
    )
    print(f"⏱️ bulk download took {(time.time() - t0):.1f}s")

    if data.empty:
        raise RuntimeError("Download returned empty DataFrame.")

    print("Type:", type(data))
    print("Index:", type(data.index))
    print("Col level names:", getattr(data.columns, "names", None))
    print("Sample columns:", data.columns[:10])

    # Save raw DataFrame to CSV
    raw_path = os.path.join(data_path, "sp500_data.csv")
    data.to_csv(raw_path)
    print(f"💾 Raw data saved to {raw_path}")

    # Flatten MultiIndex columns to ticker_field
    data.columns = [
        f"{ticker}_{field}".replace(" ", "").replace("/", "")
        for ticker, field in data.columns
    ]

    # Sort columns alphabetically
    data = data.reindex(sorted(data.columns), axis=1)

    # Save cleaned data
    clean_path = os.path.join(data_path, "market_data.csv")
    data.to_csv(clean_path)

    print(f"✅ Cleaned and saved as {clean_path}")
