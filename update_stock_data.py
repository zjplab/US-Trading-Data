#!/usr/bin/env python3
import os
import pandas as pd
import logging
import time
from datetime import datetime

# Configure logging to display timestamped messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def update_stock_data(ticker: str, folder: str, period: str = "max", interval: str = "1d"):
    """
    Fetch historical data for the given ticker and save it as a CSV file in the specified folder.
    Each ticker uses its own cache directory to avoid race conditions.
    Includes a retry loop and a custom requests session to help mitigate SSL/connection errors.
    """
    import yfinance as yf
    import requests
    import platformdirs

    # --- Set a unique cache for this ticker ---
    base_cache = os.path.join(platformdirs.user_cache_dir(), "py-yfinance")
    # Create a subfolder unique to this ticker. (Sanitize the ticker if necessary.)
    ticker_cache = os.path.join(base_cache, f"ticker-{ticker}")
    os.makedirs(ticker_cache, exist_ok=True)
    yf.set_tz_cache_location(ticker_cache)
    # Optionally, you can also set the cookie cache location:
    # yf.set_cookie_cache_location(ticker_cache)
    
    logging.info(f"Fetching data for {ticker} with period={period} and interval={interval}")

    # Create a custom session with a known User-Agent.
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; DataFetcher/1.0)"})

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            ticker_obj = yf.Ticker(ticker, session=session)
            data = ticker_obj.history(period=period, interval=interval)
            if data.empty:
                logging.warning(f"No data returned for {ticker}")
            else:
                file_path = os.path.join(folder, f"{ticker}.csv")
                data.to_csv(file_path)
                logging.info(f"Data for {ticker} written to {file_path} with {len(data)} rows")
            break  # success, exit loop
        except Exception as e:
            logging.error(f"Failed to get ticker '{ticker}' on attempt {attempt}/{max_retries} reason: {e}")
            if attempt < max_retries:
                time.sleep(2)  # wait a bit before retrying
            else:
                logging.error(f"Giving up on ticker '{ticker}' after {max_retries} attempts")

def get_sp500_tickers():
    """
    Get the list of S&P 500 tickers from Wikipedia using pandas.
    """
    logging.info("Fetching S&P 500 tickers from Wikipedia using pandas")
    try:
        tickers_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', flavor='lxml')[0]
        tickers = tickers_df['Symbol'].tolist()
        # Replace dots with hyphens as needed for Yahoo Finance
        tickers = [ticker.replace('.', '-') for ticker in tickers]
        logging.info(f"Found {len(tickers)} S&P 500 tickers")
        return tickers
    except Exception as e:
        logging.error(f"Error fetching S&P 500 tickers: {e}")
        return []

def get_hangseng_tech_tickers():
    """
    Get the list of Hang Seng Tech Index constituents.
    """
    logging.info("Fetching Hang Seng Tech Index constituents")
    try:
        hangseng_tech = [
            "0700.HK", "9988.HK", "3690.HK", "9999.HK", "1810.HK", "0981.HK", "1024.HK", "9618.HK", "2382.HK", "6618.HK",
            "0268.HK", "9888.HK", "1797.HK", "9626.HK", "2015.HK", "0992.HK", "9866.HK", "6690.HK", "0241.HK", "9961.HK",
            "0772.HK", "2382.HK", "9868.HK", "0285.HK", "0522.HK", "1347.HK", "0780.HK", "6060.HK", "0302.HK", "2269.HK"
        ]
        logging.info(f"Found {len(hangseng_tech)} Hang Seng Tech tickers")
        return hangseng_tech
    except Exception as e:
        logging.error(f"Error fetching Hang Seng Tech tickers: {e}")
        return []

def get_mag7_tickers():
    """
    Get the list of Magnificent 7 (MAG7) tech stocks.
    """
    mag7 = ["AAPL", "AMZN", "GOOGL", "META", "MSFT", "NFLX", "TSLA"]
    logging.info(f"Using {len(mag7)} MAG7 tickers")
    return mag7

def get_index_tickers():
    """Return list of major market indexes"""
    return [
        '^GSPC',  # S&P 500
        '^DJI',   # Dow Jones Industrial Average
        '^IXIC',  # Nasdaq Composite
        '^RUT',   # Russell 2000
        '^VIX'    # Volatility Index
    ]

def update_readme():
    """
    Update the README.md file with current information.
    """
    logging.info("Updating README.md")
    try:
        readme_content = f"""# Tech-Stocks-Data

A repository containing historical stock data for major tech indices and companies.

## Data Collections

- **S&P 500**: All companies in the Standard & Poor's 500 Index
- **Hang Seng Tech Index**: Technology companies listed on the Hong Kong Stock Exchange
- **MAG7**: The "Magnificent Seven" tech giants (Apple, Amazon, Google, Meta, Microsoft, Netflix, Tesla)
- **Market Indexes**: Major market indexes including Dow Jones, S&P 500, Nasdaq Composite, Russell 2000, and VIX

## Data Update Frequency

Data is updated daily via GitHub Actions. Each update creates a fresh repository state.

## Last Updated

{datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

## Data Source

All stock data is fetched using the Yahoo Finance API via the yfinance Python package.

## Usage

The data is stored in CSV format and can be used for financial analysis, machine learning models, or visualization projects.
"""
        with open("README.md", "w") as f:
            f.write(readme_content)
        logging.info("README.md updated successfully")
    except Exception as e:
        logging.error(f"Error updating README.md: {e}")

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--group', choices=['sp500', 'hangseng', 'mag7', 'indexes'], required=False)
    parser.add_argument('--update-readme-only', action='store_true', help='Only update the README.md file')
    parser.add_argument('--chunk-index', type=int, help='Index of the chunk to process (for matrix jobs)')
    parser.add_argument('--total-chunks', type=int, help='Total number of chunks (for matrix jobs)')
    args = parser.parse_args()

    # If only updating README, skip data fetching
    if args.update_readme_only:
        logging.info("Only updating README as requested")
        update_readme()
        return

    if not args.group:
        parser.error("--group is required when not using --update-readme-only")

    group_functions = {
        'sp500': (get_sp500_tickers, "SP500"),
        'hangseng': (get_hangseng_tech_tickers, "HangSengTech"),
        'mag7': (get_mag7_tickers, "MAG7"),
        'indexes': (get_index_tickers, "Indexes")
    }
    get_tickers_fn, folder_name = group_functions[args.group]
    os.makedirs(os.path.join("data", folder_name), exist_ok=True)

    # -------------------------------
    # Pre-initialize yfinance cache (shared cache) in the parent process
    # -------------------------------
    try:
        import platformdirs
        from filelock import FileLock
        cache_dir = os.path.join(platformdirs.user_cache_dir(), "py-yfinance")
        db_file = os.path.join(cache_dir, "tkr-tz.db")
        if not os.path.exists(db_file):
            os.makedirs(cache_dir, exist_ok=True)
            lock_path = os.path.join(cache_dir, "yfinance_cache.lock")
            logging.info("Pre-initializing yfinance cache under file lock")
            with FileLock(lock_path, timeout=60):
                import yfinance as yf
                # Force a cache initialization by calling a simple history query
                yf.Ticker("AAPL").history(period="1d", interval="1d")
    except Exception as e:
        logging.warning(f"Cache pre-initialization failed: {e}")

    tickers = get_tickers_fn()

    # If chunk parameters are provided, only process the specified chunk
    if args.chunk_index is not None and args.total_chunks is not None:
        chunk_size = len(tickers) // args.total_chunks + (1 if len(tickers) % args.total_chunks > 0 else 0)
        start_idx = args.chunk_index * chunk_size
        end_idx = min(start_idx + chunk_size, len(tickers))
        tickers = tickers[start_idx:end_idx]
        logging.info(f"Processing chunk {args.chunk_index + 1}/{args.total_chunks} with {len(tickers)} tickers")

    # Use ProcessPoolExecutor to fetch data in parallel.
    from concurrent.futures import ProcessPoolExecutor
    max_workers = min(os.cpu_count() or 4, 8)
    logging.info(f"Starting parallel updates for {args.group} with {len(tickers)} tickers using {max_workers} processes")
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                update_stock_data,
                ticker,
                os.path.join("data", folder_name),
                "max",
                "1d"
            )
            for ticker in tickers
        ]
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error in future: {e}")

    update_readme()

if __name__ == "__main__":
    main()
