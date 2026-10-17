#!/usr/bin/env python3
import os
import pandas as pd
import logging
import random
import time
from datetime import datetime, timezone
from io import StringIO

# Configure logging to display timestamped messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MAX_DATA_AGE_DAYS = 7
SP500_TICKERS_FILE = "sp500_tickers.txt"
SP500_WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
SP500_MEDIAWIKI_API_URL = "https://en.wikipedia.org/w/api.php"
WIKIMEDIA_USER_AGENT = (
    "US-Trading-Data/1.0 "
    "(https://github.com/zjplab/US-Trading-Data; contact via GitHub issues)"
)


def update_stock_data(ticker: str, folder: str, period: str = "max", interval: str = "1d") -> bool:
    """
    Fetch historical data for the given ticker and save it as a CSV file in the specified folder.
    Uses yfinance's default curl_cffi session so cookies and browser TLS
    impersonation are shared across worker threads. Returns True only after a
    non-empty, sufficiently fresh dataset has been written successfully.
    """
    import yfinance as yf

    logging.info(f"Fetching data for {ticker} with period={period} and interval={interval}")

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            ticker_obj = yf.Ticker(ticker)
            data = ticker_obj.history(period=period, interval=interval)
            if data.empty:
                raise ValueError(f"No data returned for {ticker}")

            latest_date = data.index.max().date()
            age_days = (datetime.now(timezone.utc).date() - latest_date).days
            if age_days > MAX_DATA_AGE_DAYS:
                raise ValueError(
                    f"Latest row for {ticker} is {latest_date}, {age_days} days old"
                )

            file_path = os.path.join(folder, f"{ticker}.csv")
            temp_file_path = f"{file_path}.tmp"
            data.to_csv(temp_file_path)
            os.replace(temp_file_path, file_path)
            logging.info(
                f"Data for {ticker} written to {file_path} with {len(data)} rows; "
                f"latest date={latest_date}"
            )
            return True
        except Exception as e:
            logging.error(f"Failed to get ticker '{ticker}' on attempt {attempt}/{max_retries} reason: {e}")

            # A short retry loop makes a Yahoo 429 worse. Let the job fail so a
            # later workflow run can retry from a fresh runner/IP.
            if type(e).__name__ == "YFRateLimitError":
                logging.error(f"Rate limited while fetching {ticker}; not retrying immediately")
                return False

            if attempt < max_retries:
                delay = (2 ** attempt) + random.uniform(0, 1)
                logging.info(f"Retrying {ticker} in {delay:.1f} seconds")
                time.sleep(delay)
            else:
                logging.error(f"Giving up on ticker '{ticker}' after {max_retries} attempts")

    return False

def validate_sp500_tickers(tickers):
    normalized = [str(ticker).strip().replace(".", "-") for ticker in tickers]
    normalized = list(dict.fromkeys(ticker for ticker in normalized if ticker))
    if not 450 <= len(normalized) <= 550:
        raise ValueError(f"Unexpected S&P 500 constituent count: {len(normalized)}")
    return normalized


def load_sp500_tickers(file_path: str = SP500_TICKERS_FILE):
    with open(file_path, encoding="utf-8") as file:
        tickers = validate_sp500_tickers(file.read().splitlines())
    logging.info(f"Loaded {len(tickers)} S&P 500 tickers from {file_path}")
    return tickers


def fetch_sp500_tickers():
    """
    Get the S&P 500 constituents from Wikipedia.

    Wikimedia rejects anonymous Python user agents from some hosted runners,
    so fetch explicitly with an identifiable user agent. Try both the article
    HTML and the MediaWiki API before reporting failure.
    """
    import requests

    headers = {
        "User-Agent": WIKIMEDIA_USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
    }

    def parse_tickers(html: str):
        tables = pd.read_html(StringIO(html), flavor="lxml")
        tickers_df = next((table for table in tables if "Symbol" in table.columns), None)
        if tickers_df is None:
            raise ValueError("Could not find a table containing a Symbol column")

        return validate_sp500_tickers(tickers_df["Symbol"])

    def fetch_article_html():
        response = requests.get(SP500_WIKIPEDIA_URL, headers=headers, timeout=30)
        response.raise_for_status()
        return response.text

    def fetch_api_html():
        response = requests.get(
            SP500_MEDIAWIKI_API_URL,
            headers=headers,
            params={
                "action": "parse",
                "page": "List of S&P 500 companies",
                "prop": "text",
                "format": "json",
                "formatversion": 2,
            },
            timeout=30,
        )
        response.raise_for_status()
        html = response.json().get("parse", {}).get("text")
        if not html:
            raise ValueError("MediaWiki API response did not contain article HTML")
        return html

    sources = (
        ("Wikipedia article", fetch_article_html),
        ("MediaWiki API", fetch_api_html),
    )

    for source_name, fetch in sources:
        logging.info(f"Fetching S&P 500 tickers from {source_name}")
        try:
            html = fetch()
            tickers = parse_tickers(html)
            logging.info(f"Found {len(tickers)} S&P 500 tickers from {source_name}")
            return tickers
        except Exception as e:
            logging.warning(f"Failed to fetch S&P 500 tickers from {source_name}: {e}")

    logging.error("All S&P 500 ticker sources failed")
    return []


def get_sp500_tickers():
    """Load the checked-in manifest, fetching live data only if it is missing."""
    try:
        return load_sp500_tickers()
    except Exception as e:
        logging.warning(f"Could not load {SP500_TICKERS_FILE}: {e}")
        return fetch_sp500_tickers()


def refresh_sp500_tickers(file_path: str = SP500_TICKERS_FILE) -> bool:
    """Refresh the manifest, retaining the last valid copy if Wikipedia fails."""
    tickers = fetch_sp500_tickers()
    if not tickers:
        try:
            load_sp500_tickers(file_path)
            logging.warning("Keeping the existing S&P 500 ticker manifest")
            return True
        except Exception as e:
            logging.error(f"No valid S&P 500 ticker manifest is available: {e}")
            return False

    temp_file_path = f"{file_path}.tmp"
    with open(temp_file_path, "w", encoding="utf-8", newline="\n") as file:
        file.write("\n".join(tickers) + "\n")
    os.replace(temp_file_path, file_path)
    logging.info(f"Refreshed {file_path} with {len(tickers)} tickers")
    return True

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

{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}

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
    parser.add_argument(
        '--refresh-sp500-tickers-only',
        action='store_true',
        help='Refresh the checked-in S&P 500 ticker manifest and exit'
    )
    parser.add_argument('--chunk-index', type=int, help='Index of the chunk to process (for matrix jobs)')
    parser.add_argument('--total-chunks', type=int, help='Total number of chunks (for matrix jobs)')
    args = parser.parse_args()

    # If only updating README, skip data fetching
    if args.update_readme_only:
        logging.info("Only updating README as requested")
        update_readme()
        return

    if args.refresh_sp500_tickers_only:
        if not refresh_sp500_tickers():
            raise SystemExit(1)
        return

    if not args.group:
        parser.error(
            "--group is required unless using --update-readme-only or "
            "--refresh-sp500-tickers-only"
        )

    group_functions = {
        'sp500': (get_sp500_tickers, "SP500"),
        'hangseng': (get_hangseng_tech_tickers, "HangSengTech"),
        'mag7': (get_mag7_tickers, "MAG7"),
        'indexes': (get_index_tickers, "Indexes")
    }
    get_tickers_fn, folder_name = group_functions[args.group]
    os.makedirs(os.path.join("data", folder_name), exist_ok=True)

    # Configure one shared cache and one default curl_cffi-backed yfinance
    # session for all worker threads in this job. Do not pass requests.Session:
    # it bypasses browser TLS impersonation and is commonly rate-limited.
    try:
        import platformdirs
        import yfinance as yf

        cache_dir = os.path.join(platformdirs.user_cache_dir(), "py-yfinance")
        os.makedirs(cache_dir, exist_ok=True)
        yf.set_tz_cache_location(cache_dir)
        yf.config.debug.hide_exceptions = False
        yf.config.network.retries = 2
    except Exception as e:
        logging.error(f"Failed to configure yfinance: {e}")
        raise

    tickers = get_tickers_fn()

    # If chunk parameters are provided, only process the specified chunk
    if args.chunk_index is not None and args.total_chunks is not None:
        chunk_size = len(tickers) // args.total_chunks + (1 if len(tickers) % args.total_chunks > 0 else 0)
        start_idx = args.chunk_index * chunk_size
        end_idx = min(start_idx + chunk_size, len(tickers))
        tickers = tickers[start_idx:end_idx]
        logging.info(f"Processing chunk {args.chunk_index + 1}/{args.total_chunks} with {len(tickers)} tickers")

    if not tickers:
        logging.error(f"No tickers found for group {args.group}")
        raise SystemExit(1)

    # yfinance shares its curl_cffi session and cookies within this process.
    # Keep concurrency deliberately low to avoid Yahoo rate limits.
    from concurrent.futures import ThreadPoolExecutor, as_completed
    max_workers = min(len(tickers), 2)
    logging.info(
        f"Starting updates for {args.group} with {len(tickers)} tickers "
        f"using {max_workers} threads"
    )
    failures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                update_stock_data,
                ticker,
                os.path.join("data", folder_name),
                "max",
                "1d"
            ): ticker
            for ticker in tickers
        }
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                if not future.result():
                    failures.append(ticker)
            except Exception as e:
                logging.error(f"Unhandled error while fetching {ticker}: {e}")
                failures.append(ticker)

    if failures:
        logging.error(
            f"Update failed for {len(failures)}/{len(tickers)} tickers: "
            f"{', '.join(sorted(failures))}"
        )
        raise SystemExit(1)

    logging.info(f"Successfully updated all {len(tickers)} tickers in {args.group}")

if __name__ == "__main__":
    main()
