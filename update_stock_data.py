#!/usr/bin/env python3
import os
import pandas as pd
import logging

import time
from datetime import datetime

# Only import yfinance when needed for stock data operations
# This allows the README update to run without requiring yfinance

# Configure logging to display timestamped messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def update_stock_data(ticker: str, folder: str, period: str = "max", interval: str = "1d"):
    """
    Fetch historical data for the given ticker and save it as a CSV file in the specified folder.

    Parameters:
      ticker (str): The stock ticker symbol.
      folder (str): The folder where the CSV file will be saved.
      period (str): The time period of data to retrieve (default "max" for all available data).
      interval (str): The data interval (default "1d" for daily data).
    """
    logging.info(f"Fetching data for {ticker} with period={period} and interval={interval}")
    try:
        # Import yfinance here when needed
        import yfinance as yf
        
        # Create a dedicated Ticker object for this request
        ticker_obj = yf.Ticker(ticker)
        
        # Use the history method on the Ticker object instead of the global download function
        data = ticker_obj.history(period=period, interval=interval)
        
        # Validate the data belongs to the requested ticker
        if data.empty:
            logging.warning(f"No data returned for {ticker}")
        else:
            # Additional validation could be added here
            file_path = os.path.join(folder, f"{ticker}.csv")
            data.to_csv(file_path)
            logging.info(f"Data for {ticker} written to {file_path} with {len(data)} rows")
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")

def get_sp500_tickers():
    """
    Get the list of S&P 500 tickers from Wikipedia using pandas.
    """
    logging.info("Fetching S&P 500 tickers from Wikipedia using pandas")
    try:
        # Read the S&P 500 table directly from Wikipedia
        tickers_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', flavor='lxml')[0]
        # Extract the ticker symbols and handle replacements
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
        # This is a simplified approach - in a real-world scenario, you might want to scrape from the official source
        # or use a financial data API that provides this information
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
    
    tickers = get_tickers_fn()
    
    # If chunk parameters are provided, only process the specified chunk
    if args.chunk_index is not None and args.total_chunks is not None:
        # Calculate the chunk size and get the appropriate slice of tickers
        chunk_size = len(tickers) // args.total_chunks + (1 if len(tickers) % args.total_chunks > 0 else 0)
        start_idx = args.chunk_index * chunk_size
        end_idx = min(start_idx + chunk_size, len(tickers))
        
        chunk_tickers = tickers[start_idx:end_idx]
        logging.info(f"Processing chunk {args.chunk_index + 1}/{args.total_chunks} with {len(chunk_tickers)} tickers")
        tickers = chunk_tickers
    
    # Use ProcessPoolExecutor instead of ThreadPoolExecutor to avoid shared memory issues
    from concurrent.futures import ProcessPoolExecutor
    
    # Limit the number of processes to avoid overwhelming the system
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
    
if __name__ == "__main__":
    main()
