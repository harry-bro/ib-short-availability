#!/usr/bin/env python3
"""
transform.py - Transform raw IB data and enrich with market data

Two dataset pathways:
1. BASIC: Normalize IB data only (all tickers, all regions)
2. ENRICHED: Normalize + Fetch pricing + Extend to 24h (curated US tickers)

Usage:
    # Basic dataset (normalize only)
    python -m etl.transform --dataset basic --start-date 2025-11-04 --end-date 2025-11-09
    
    # Enriched dataset (normalize + fetch + extend)
    python -m etl.transform --dataset enriched --start-date 2025-11-04 --end-date 2025-11-09
    
    # Both datasets
    python -m etl.transform --dataset both --start-date 2025-11-04 --end-date 2025-11-09
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set, Optional

import pandas as pd
import yfinance as yf
from tqdm import tqdm



# NORMALIZATION


SEPS = [",", "|", "\t", ";"]


def pick_separator(sample_lines: List[str]) -> str:
    """Choose the separator producing the largest number of fields."""
    best_sep, best_score = ",", -1
    for sep in SEPS:
        counts = [len(ln.split(sep)) for ln in sample_lines]
        score = pd.Series(counts).median() if counts else -1
        if score > best_score:
            best_sep, best_score = sep, score
    return best_sep


def read_table(path: Path) -> pd.DataFrame:
    """Read a raw IB file into a DataFrame with detected separator."""
    sample = []
    header_line = None
    
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for ln in f:
            stripped = ln.rstrip("\n")
            
            # Header line starts with # (but not #BOF or #EOF)
            if (stripped.startswith("#") and 
                not stripped.startswith("#BOF") and 
                not stripped.startswith("#EOF") and
                not header_line):
                header_line = stripped.lstrip("#")
            
            # Collect sample of data lines for separator detection
            if not stripped.startswith("#") and stripped.strip():
                sample.append(stripped)
                if len(sample) >= 50:
                    break
    
    # Detect separator
    sep = pick_separator(sample) if sample else ","
    
    # Parse column names from header
    column_names = [col.strip() for col in header_line.split(sep) if col.strip()]
    
    # Read the file
    df = pd.read_csv(
        path, sep=sep, engine="python", comment="#",
        names=column_names, dtype=str, index_col=False
    )
    
    # Clean column names
    df.columns = (
        df.columns.astype(str)
        .str.strip().str.lower()
        .str.replace(r"[^a-z0-9]+", "_", regex=True)
        .str.strip("_")
    )
    
    return df


def parse_availability(s: pd.Series) -> tuple:
    """Return (available_shares Int64, available_unlimited bool) from raw strings."""
    s = s.fillna("").astype(str).str.strip()
    unlimited = s.str.match(r"^>\s*\d+", na=False)
    val = pd.to_numeric(s.str.replace(r"[^\d]", "", regex=True), errors="coerce").astype("Int64")
    return val, unlimited


def normalize_one(data_path: Path, meta_path: Path) -> pd.DataFrame:
    """Normalize a single raw + meta pair."""
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    asof_txt = meta.get("server_mtime_utc") or meta.get("downloaded_at_utc")
    asof_utc = pd.to_datetime(asof_txt, utc=True, errors="coerce")
    region = meta.get("region") or Path(data_path.name.split("_", 1)[1]).stem.lower()
    md5 = meta.get("md5")
    
    # Read raw table
    df = read_table(data_path)
    
    # Check for required columns (availability files only)
    needed_ticker = "sym"
    has_fee = "feerate" in df.columns
    has_avail = "available" in df.columns
    
    if needed_ticker not in df.columns or not (has_fee or has_avail):
        # Unknown schema (e.g., stockmargin), return empty
        return pd.DataFrame(columns=[
            "asof_utc", "ticker", "fee_raw", "rebate_raw", "available_shares",
            "available_unlimited", "region", "source_file", "md5", "row_num"
        ])
    
    # Map known columns
    tcol, fcol, rcol, acol = "sym", "feerate", "rebaterate", "available"
    
    # Extract data
    tick = df[tcol].astype(str).str.strip()
    fee_raw = pd.to_numeric(df[fcol], errors="coerce") if fcol else pd.Series([pd.NA] * len(df))
    rebate_raw = pd.to_numeric(df[rcol], errors="coerce") if rcol else pd.Series([pd.NA] * len(df))
    
    if acol:
        avail, unlimited = parse_availability(df[acol])
    else:
        avail = pd.Series([pd.NA] * len(df), dtype="Int64")
        unlimited = pd.Series([False] * len(df), dtype="boolean")
    
    out = pd.DataFrame({
        "asof_utc": asof_utc,
        "ticker": tick,
        "fee_raw": fee_raw,
        "rebate_raw": rebate_raw,
        "available_shares": avail,
        "available_unlimited": unlimited,
        "region": region,
        "source_file": data_path.name,
        "md5": md5,
        "row_num": range(1, len(df) + 1),
    })
    
    # Don't include empty ticker rows
    out = out[out["ticker"].str.len() > 0]
    return out


def normalize_ib_data(raw_dir: str, out_dir: str) -> int:
    """
    Normalize raw IB snapshots to parquet.
    
    Returns:
        Total number of rows processed
    """
    print("")
    print("NORMALIZE IB DATA")
    print("")
    
    raw_root = Path(raw_dir)
    out_root = Path(out_dir)
    
    if not raw_root.exists():
        print(f"ERROR: Raw directory not found: {raw_root}")
        return 0
    
    days = sorted(p for p in raw_root.iterdir() if p.is_dir())
    if not days:
        print("WARNING: No date directories found in raw data")
        return 0
    
    print(f"Processing {len(days)} days of raw data...")
    total_rows = 0
    
    for day_dir in days:
        parts = []
        
        for data_path in sorted(day_dir.glob("*_*.txt")):
            ts_tag, base = data_path.name.split("_", 1)
            base_stem = Path(base).stem.lower()
            
            # Skip stockmargin or any other non-availability dumps
            if base_stem.startswith("stockmargin"):
                continue
            
            meta_path = day_dir / f"{ts_tag}_{base_stem}.meta.json"
            if not meta_path.exists():
                continue
            
            df = normalize_one(data_path, meta_path)
            if not df.empty:
                parts.append(df)
        
        if not parts:
            continue
        
        day_df = pd.concat(parts, ignore_index=True)
        day_df = day_df.sort_values(["ticker", "asof_utc"]).reset_index(drop=True)
        
        # Write to output
        out_dir_path = out_root / f"dt={day_dir.name}"
        out_dir_path.mkdir(parents=True, exist_ok=True)
        out_path = out_dir_path / "part-000.parquet"
        day_df.to_parquet(out_path, index=False)
        
        total_rows += len(day_df)
        print(f"  {day_dir.name}: {len(day_df):,} rows -> {out_path}")
    
    print(f"\nNormalization complete! Total rows: {total_rows:,}")
    return total_rows



# MARKET DATA ENRICHMENT 


# Curated list of US tickers
CURATED_TICKERS = [

    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA',
    'V', 'JNJ', 'WMT', 'JPM', 'PG', 'MA', 'UNH', 'HD',
    'NFLX', 'DIS', 'CSCO', 'ADBE', 'CRM', 'ORCL', 'INTC', 'AMD',
    'BAC', 'WFC', 'GS', 'MS', 'C', 'AXP', 'SCHW',
    'PFE', 'ABBV', 'MRK', 'TMO', 'LLY', 'ABT',
    'KO', 'PEP', 'MCD', 'SBUX', 'TGT',
    'XOM', 'CVX', 'BA', 'CAT', 'HON', 'GE',
    'GME', 'AMC', 'BBBY'
]

CURATED_TICKERS2 = ['YDES', 'NEGG', 'ZOOZ', 'HSDT', 'ELBM', 'VFS', 'MULN', 'BYND', 
                    'RILY', 'SAVA', 'CGC', 'MSTR', 'SIRI', 'OTLY', 'NKLA', 'IBRX', 
                    'HUT', 'FISKER', 'PCT', 'BBAI', 'HTZ', 'AIRS', 'LCID', 'GRPN', 
                    'AI', 'SYM', 'HIMS', 'ABR', 'KSS', 'CAPR', 'AVXL', 'ENVX', 'SPHR', 
                    'MPW', 'NVTS', 'ATMU', 'DJT', 'GME', 'AMC', 'NVAX', 'PLUG', 'LMND', 
                    'VKTX', 'SOUN', 'OPEN', 'SBET', 'MARA', 'RIOT', 'UPST', 'SOFI', 'PROK', 
                    'ARQT', 'MRNA', 'RXRX', 'PLCE', 'RUN', 'EOSE', 'FCEL', 'NIO']


def fetch_ohlcv_data(tickers: Set[str], start_date: str, end_date: str, interval: str = '5m') -> pd.DataFrame:
    """Fetch OHLCV data for tickers."""
    print(f"\nFetching OHLCV data for {len(tickers)} tickers...")
    print(f"  Interval: {interval}")
    print(f"  Date range: {start_date} to {end_date} (inclusive)")
    
    # yfinance end_date is exclusive, so add 1 day to make CLI end_date inclusive
    end_date_inclusive = (pd.to_datetime(end_date) + timedelta(days=1)).strftime('%Y-%m-%d')
    
    all_data = []
    
    for ticker in tqdm(list(tickers), desc="  Fetching data"):
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(start=start_date, end=end_date_inclusive, interval=interval)
            
            if hist.empty:
                continue
            
            # Reset index to make date/datetime a column
            hist = hist.reset_index()
            hist['ticker'] = ticker
            
            # Standardize column names
            if 'Datetime' in hist.columns:
                hist = hist.rename(columns={'Datetime': 'timestamp'})
            elif 'Date' in hist.columns:
                hist = hist.rename(columns={'Date': 'timestamp'})
            
            hist = hist.rename(columns={
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume',
            })
            
            # Keep only needed columns
            hist = hist[['timestamp', 'ticker', 'open', 'high', 'low', 'close', 'volume']]
            all_data.append(hist)
            
            time.sleep(0.2)  # Rate limiting
            
        except Exception as e:
            print(f"    Error fetching {ticker}: {e}")
            continue
    
    if not all_data:
        print("  No data fetched!")
        return pd.DataFrame()
    
    # Combine all data
    df = pd.concat(all_data, ignore_index=True)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['date'] = df['timestamp'].dt.date
    
    print(f"  Fetched {len(df):,} rows for {df['ticker'].nunique()} tickers")
    return df


def fetch_ticker_metadata(tickers: Set[str]) -> pd.DataFrame:
    """Fetch metadata for tickers."""
    print(f"\nFetching metadata for {len(tickers)} tickers...")
    
    metadata = []
    for ticker in tqdm(list(tickers), desc="  Fetching metadata"):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            metadata.append({
                'ticker': ticker,
                'market_cap': info.get('marketCap'),
                'sector': info.get('sector'),
                'industry': info.get('industry'),
                'exchange': info.get('exchange'),
                'currency': info.get('currency'),
            })
            
            time.sleep(0.15)
            
        except Exception as e:
            print(f"    Error fetching metadata for {ticker}: {e}")
            continue
    
    df = pd.DataFrame(metadata)
    print(f"  Fetched metadata for {len(df)} tickers")
    return df


def fetch_market_data(output_dir: str, start_date: str, end_date: str) -> int:
    """
    Fetch market data for curated tickers.
    
    Returns:
        Number of tickers successfully fetched
    """
    print("\n" + "=" * 70)
    print("FETCH MARKET DATA")
    print("=" * 70)
    
    output_path = Path(output_dir)
    tickers = set(CURATED_TICKERS2)
    
    print(f"Using curated ticker list: {len(tickers)} tickers")
    
    # Fetch OHLCV data
    ohlcv_df = fetch_ohlcv_data(tickers, start_date, end_date, interval='5m')
    
    if ohlcv_df.empty:
        print("ERROR: No OHLCV data fetched")
        return 0
    
    # Fetch metadata
    tickers_with_data = set(ohlcv_df['ticker'].unique())
    metadata_df = fetch_ticker_metadata(tickers_with_data)
    
    # Save OHLCV data partitioned by date
    for date in ohlcv_df['date'].unique():
        date_df = ohlcv_df[ohlcv_df['date'] == date]
        out_dir = output_path / 'ohlcv' / f"dt={date}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "ohlcv.parquet"
        date_df.to_parquet(out_file, index=False)
        print(f"  Saved {len(date_df):,} OHLCV rows for {date}")
    
    # Save metadata
    if not metadata_df.empty:
        metadata_file = output_path / 'metadata' / "ticker_metadata.parquet"
        metadata_file.parent.mkdir(parents=True, exist_ok=True)
        metadata_df.to_parquet(metadata_file, index=False)
        print(f"  Saved metadata for {len(metadata_df)} tickers")
    
    # Save ticker list
    ticker_list_file = output_path / "enriched_tickers.txt"
    tickers_list = sorted(ohlcv_df['ticker'].unique())
    ticker_list_file.write_text('\n'.join(tickers_list))
    print(f"  Saved ticker list to {ticker_list_file}")
    
    print(f"\nMarket data fetch complete! {len(tickers_with_data)} tickers")
    return len(tickers_with_data)



# PRICING EXTENSION 


def extend_to_24h(df: pd.DataFrame, prev_day_closes: Optional[Dict[str, float]] = None) -> pd.DataFrame:
    """Extend market hours data (6.5 hours) to full 24-hour coverage."""
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])
    
    extended_dfs = []
    
    for ticker, group in df.groupby('ticker', sort=False):
        group = group.sort_values('timestamp').reset_index(drop=True)
        trade_date = group['date'].iloc[0]
        tz = group['timestamp'].dt.tz
        
        first_market_ts = group['timestamp'].min()
        last_market_ts = group['timestamp'].max()
        
        # Day boundaries
        if tz is not None:
            day_start = pd.Timestamp(trade_date).tz_localize(tz).replace(hour=0, minute=0, second=0)
            day_end = pd.Timestamp(trade_date).tz_localize(tz).replace(hour=23, minute=55, second=0)
        else:
            day_start = pd.Timestamp(trade_date).replace(hour=0, minute=0, second=0)
            day_end = pd.Timestamp(trade_date).replace(hour=23, minute=55, second=0)
        
        last_close = group['close'].iloc[-1]
        
        # Pre-market (only if we have previous day's close)
        pre_df = pd.DataFrame()
        if prev_day_closes is not None and ticker in prev_day_closes:
            pre_market_close = prev_day_closes[ticker]
            pre_market_times = pd.date_range(
                start=day_start,
                end=first_market_ts - timedelta(minutes=5),
                freq='5min',
                tz=tz
            )
            
            pre_market_rows = []
            for ts in pre_market_times:
                pre_market_rows.append({
                    'timestamp': ts,
                    'ticker': ticker,
                    'open': pre_market_close,
                    'high': pre_market_close,
                    'low': pre_market_close,
                    'close': pre_market_close,
                    'volume': 0,
                    'date': trade_date
                })
            
            if pre_market_rows:
                pre_df = pd.DataFrame(pre_market_rows)
        
        # Post-market 
        post_market_times = pd.date_range(
            start=last_market_ts + timedelta(minutes=5),
            end=day_end,
            freq='5min',
            tz=tz
        )
        
        post_market_rows = []
        for ts in post_market_times:
            post_market_rows.append({
                'timestamp': ts,
                'ticker': ticker,
                'open': last_close,
                'high': last_close,
                'low': last_close,
                'close': last_close,
                'volume': 0,
                'date': trade_date
            })
        
        post_df = pd.DataFrame(post_market_rows) if post_market_rows else pd.DataFrame()
        
        # Combine
        extended_group = pd.concat([pre_df, group, post_df], ignore_index=True)
        extended_dfs.append(extended_group)
    
    result = pd.concat(extended_dfs, ignore_index=True)
    result = result[df.columns]
    
    return result


def get_closing_prices(df: pd.DataFrame) -> Dict[str, float]:
    """Extract closing prices for each ticker from last bar of the day."""
    closing_prices = {}
    for ticker, group in df.groupby('ticker'):
        group = group.sort_values('timestamp')
        closing_prices[ticker] = group['close'].iloc[-1]
    return closing_prices


def extend_pricing_to_24h(input_dir: str, output_dir: str) -> int:
    """
    Extend pricing data from market hours to 24h period.
    
    Returns:
        Total number of rows added
    """
    print("")
    print("EXTEND PRICING TO 24H")
    print("")
    
    in_root = Path(input_dir) / "ohlcv"
    out_root = Path(output_dir) / "ohlcv_extended"
    out_root.mkdir(parents=True, exist_ok=True)
    
    if not in_root.exists():
        print(f"ERROR: Input directory not found: {in_root}")
        return 0
    
    days = sorted(p for p in in_root.iterdir() if p.is_dir())
    if not days:
        print("WARNING: No date directories found")
        return 0
    
    print(f"Processing {len(days)} days...")
    
    total_input_rows = 0
    total_output_rows = 0
    prev_day_closes = None
    
    for day_idx, day_dir in enumerate(days):
        day_name = day_dir.name
        out_day_dir = out_root / day_name
        out_day_dir.mkdir(parents=True, exist_ok=True)
        
        for parquet_path in sorted(day_dir.glob("*.parquet")):
            df = pd.read_parquet(parquet_path)
            input_rows = len(df)
            total_input_rows += input_rows
            
            # Extend to 24 hours
            df_extended = extend_to_24h(df, prev_day_closes)
            output_rows = len(df_extended)
            total_output_rows += output_rows
            
            # Extract closing prices for next day
            current_day_closes = get_closing_prices(df)
            
            added_rows = output_rows - input_rows
            status = "[First day: no pre-market]" if day_idx == 0 else ""
            print(f"  {day_name}: {input_rows:,} -> {output_rows:,} (+{added_rows:,}) {status}")
            
            # Write output
            out_path = out_day_dir / parquet_path.name
            df_extended.to_parquet(out_path, index=False)
        
        prev_day_closes = current_day_closes
    
    rows_added = total_output_rows - total_input_rows
    print(f"\nExtension complete! Added {rows_added:,} rows ({total_input_rows:,} -> {total_output_rows:,})")
    return rows_added



# PIPELINE 


def transform_pipeline(dataset: str, start_date: str, end_date: str, config: dict):
    """
    Main transform pipeline orchestrator.
    
    Args:
        dataset: 'basic', 'enriched', or 'both'
        start_date: Start date for market data (YYYY-MM-DD)
        end_date: End date for market data (YYYY-MM-DD)
        config: Configuration dictionary with paths
    """
    print("")
    print("TRANSFORM PIPELINE")
    print("")
    print(f"Dataset mode: {dataset.upper()}")
    print(f"Date range: {start_date} to {end_date}")
    
    results = {}
    
    # BASIC or BOTH: Normalize IB data
    if dataset in ['basic', 'both']:
        rows = normalize_ib_data(
            raw_dir=config['raw_dir'],
            out_dir=config['normalized_dir']
        )
        results['normalized_rows'] = rows
    
    # ENRICHED or BOTH: Full enrichment pipeline
    if dataset in ['enriched', 'both']:
        
        # Normalize (if not already done)
        if dataset == 'enriched':
            rows = normalize_ib_data(
                raw_dir=config['raw_dir'],
                out_dir=config['normalized_dir']
            )
            results['normalized_rows'] = rows
        
        # Fetch market data
        tickers = fetch_market_data(
            output_dir=config['market_data_dir'],
            start_date=start_date,
            end_date=end_date
        )
        results['enriched_tickers'] = tickers
        
        # Extend pricing to 24h
        rows_added = extend_pricing_to_24h(
            input_dir=config['market_data_dir'],
            output_dir=config['market_data_dir']
        )
        results['pricing_rows_added'] = rows_added
    
    # Print summary
    print("")
    print("TRANSFORM COMPLETE")
    print("")
    if 'normalized_rows' in results:
        print(f"  Normalized IB data: {results['normalized_rows']:,} rows")
    if 'enriched_tickers' in results:
        print(f"  Market data tickers: {results['enriched_tickers']}")
        print(f"  Pricing rows added: {results['pricing_rows_added']:,}")
    print("")



# CLI


def parse_args():
    parser = argparse.ArgumentParser(
        description="Transform raw IB data and enrich with market data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic dataset (normalize only)
  python -m etl.transform --dataset basic --start-date 2025-11-04 --end-date 2025-11-09
  
  # Enriched dataset (normalize + fetch + extend)
  python -m etl.transform --dataset enriched --start-date 2025-11-04 --end-date 2025-11-09
  
  # Both datasets
  python -m etl.transform --dataset both --start-date 2025-11-04 --end-date 2025-11-09
        """
    )
    
    parser.add_argument('--dataset', required=True,
                       choices=['basic', 'enriched', 'both'],
                       help='Dataset type to build')
    parser.add_argument('--start-date', required=True,
                       help='Start date for market data (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True,
                       help='End date for market data (YYYY-MM-DD)')
    
    # Directory configuration
    parser.add_argument('--raw-dir', default='data/raw/ib',
                       help='Input directory with raw IB data')
    parser.add_argument('--normalized-dir', default='data/normalized/ib_availability',
                       help='Output directory for normalized IB data')
    parser.add_argument('--market-data-dir', default='data/normalized/market_data',
                       help='Output directory for market data')
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    config = {
        'raw_dir': args.raw_dir,
        'normalized_dir': args.normalized_dir,
        'market_data_dir': args.market_data_dir,
    }
    
    try:
        transform_pipeline(
            dataset=args.dataset,
            start_date=args.start_date,
            end_date=args.end_date,
            config=config
        )
        return 0
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        return 130
    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())