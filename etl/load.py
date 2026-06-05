#!/usr/bin/env python3
"""
load.py - Build final analysis datasets

Two dataset types:
1. BASIC: IB availability data with computed features (all tickers, all regions)
2. ENRICHED: IB + pricing data with extended features (50 curated US tickers)

Usage:
    # Basic dataset only
    python -m etl.load --dataset basic --start-date 2025-11-04 --end-date 2025-11-09
    
    # Enriched dataset only
    python -m etl.load --dataset enriched --start-date 2025-11-04 --end-date 2025-11-09
    
    # Both datasets
    python -m etl.load --dataset both --start-date 2025-11-04 --end-date 2025-11-09
"""

import argparse
import sys
from datetime import time
from pathlib import Path
from typing import Dict, List, Tuple
import json
import pandas as pd
from tqdm import tqdm



# HELPER FUNCTIONS


def get_date_range(start_date: str, end_date: str) -> List[str]:
    """Generate list of dates between start and end (inclusive)."""
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    return pd.date_range(start, end, freq="D").strftime("%Y-%m-%d").tolist()


def normalize_timestamp(ts):
    """Convert any timestamp to timezone-naive datetime."""
    if pd.isna(ts):
        return ts
    
    if isinstance(ts, pd.Timestamp):
        if ts.tz is not None:
            return ts.tz_convert('UTC').tz_localize(None)
        return ts
    
    dt = pd.to_datetime(ts)
    if dt.tz is not None:
        return dt.tz_convert('UTC').tz_localize(None)
    return dt


def load_availability_data(base_path: Path, dates: List[str], usa_only: bool = False) -> pd.DataFrame:
    """Load IB availability data for specified dates."""
    region_filter = "USA region only" if usa_only else "all regions"
    print(f"\nLoading availability data ({region_filter}) for {len(dates)} dates...")
    
    dfs = []
    for date in dates:
        file_path = base_path / "ib_availability" / f"dt={date}" / "part-000.parquet"
        
        if not file_path.exists():
            print(f"  WARNING: File not found: {file_path}")
            continue
        
        df = pd.read_parquet(file_path)
        
        if usa_only:
            df = df[df["region"].str.lower() == 'usa']
        
        print(f"  {date}: Loaded {len(df)} records for {df['ticker'].nunique()} tickers")
        dfs.append(df)
    
    if not dfs:
        raise ValueError("No availability data loaded!")
    
    availability = pd.concat(dfs, ignore_index=True)
    
    availability["asof_utc"] = availability["asof_utc"].apply(normalize_timestamp)
    
    print(f"  Total: {len(availability):,} records for {availability['ticker'].nunique()} tickers")
    return availability


def save_dataset(df: pd.DataFrame, output_dir: Path, dataset_name: str):
    """Save dataset partitioned by date."""
    print(f"\nSaving {dataset_name} dataset to {output_dir}...")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    df = df.sort_values(["ticker", "asof_utc"]).reset_index(drop=True)
    
    # Add date column for partitioning
    df["dt"] = df["asof_utc"].dt.strftime("%Y-%m-%d")
    
    # Save partitioned by date
    for date in sorted(df["dt"].unique()):
        
        date_df = df[df["dt"] == date].drop(columns=["dt"])
        
        partition_dir = output_dir / f"dt={date}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        output_path = partition_dir / f"{dataset_name}.parquet"
        date_df.to_parquet(output_path, index=False)
        
        print(f"  {date}: Saved {len(date_df):,} records to {output_path}")
    
    print(f"\n{dataset_name.capitalize()} dataset build complete!")



# BUILD BASIC DATASET 


def build_basic_dataset(availability: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Build basic dataset with availability data and computed features.
    No pricing data or metadata used.
    
    Returns:
        tuple: (processed dataframe, stats dict)
    """
    print("\nBuilding basic dataset...")
    
    initial_count = len(availability)
    
    # Compute spread
    availability["spread_raw"] = availability["fee_raw"] - availability["rebate_raw"]
    
    # Compute basis points
    availability["fee_bps"] = availability["fee_raw"] * 100
    availability["rebate_bps"] = availability["rebate_raw"] * 100
    availability["spread_bps"] = availability["spread_raw"] * 100
    
    # Time features
    availability["hour_utc"] = availability["asof_utc"].dt.hour
    availability["day_of_week"] = availability["asof_utc"].dt.dayofweek
    
    # Select columns for basic dataset
    columns_to_keep = [
        "ticker",
        "asof_utc",
        "region",
        "fee_raw",
        "rebate_raw",
        "spread_raw",
        "fee_bps",
        "rebate_bps",
        "spread_bps",
        "available_shares",
        "available_unlimited",
        "hour_utc",
        "day_of_week",
    ]
    
    basic = availability[columns_to_keep].copy()
    

    before_drop = len(basic)
    basic = basic.dropna()
    after_drop = len(basic)
    dropped_count = before_drop - after_drop
    
    if dropped_count > 0:
        print(f"  Dropped {dropped_count:,} rows with null values ({dropped_count/before_drop*100:.2f}%)")
    else:
        print("  No rows with null values found")
    
    stats = {
        "initial_count": initial_count,
        "after_processing": after_drop,
        "dropped_nulls": dropped_count,
        "final_tickers": basic["ticker"].nunique(),
    }
    
    return basic, stats


# ENRICHED DATASET 

def load_metadata(base_path: Path) -> pd.DataFrame:
    """Load ticker metadata."""
    metadata_path = base_path / "market_data" / "metadata" / "ticker_metadata.parquet"
    
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")
    
    metadata = pd.read_parquet(metadata_path)
    print(f"  Loaded {len(metadata)} tickers")
    return metadata


def load_pricing_data(base_path: Path, dates: List[str]) -> pd.DataFrame:
    """Load market pricing data for specified dates."""
    
    dfs = []
    for date in dates:
        file_path = base_path / "market_data" / "ohlcv_extended" / f"dt={date}" / "ohlcv.parquet"
        
        if not file_path.exists():
            print(f"  WARNING: File not found: {file_path}")
            continue
        
        df = pd.read_parquet(file_path)
        print(f"  {date}: Loaded {len(df):,} records for {df['ticker'].nunique()} tickers")
        dfs.append(df)
    
    if not dfs:
        raise ValueError("No pricing data loaded!")
    
    pricing = pd.concat(dfs, ignore_index=True)
    
    # Normalize timestamps
    pricing["timestamp"] = pricing["timestamp"].apply(normalize_timestamp)

    actual_dates = sorted(pricing['timestamp'].dt.date.unique()) # to avoid usage of excess availability data
    
    print(f"  Total: {len(pricing):,} records")
    return pricing, actual_dates


def compute_pricing_features(row) -> Dict:
    """Compute derived features from a single pricing row."""
    features = {}
    
    # Average price
    features["avg_price"] = (row["open"] + row["close"]) / 2
    
    # Intra-period return
    if row["open"] != 0:
        features["intra_period_return"] = (row["close"] - row["open"]) / row["open"]
    else:
        features["intra_period_return"] = 0.0
    
    # Intra-period volatility (price range as fraction of open)
    if row["open"] != 0:
        features["intraperiod_volatility"] = (row["high"] - row["low"]) / row["open"]
        features["price_range_pct"] = (row["high"] - row["low"]) / row["open"] * 100
    else:
        features["intraperiod_volatility"] = 0.0
        features["price_range_pct"] = 0.0
    
    # Dollar volume
    features["dollar_volume"] = row["volume"] * features["avg_price"]
    
    # OHLC values
    features["open"] = row["open"]
    features["high"] = row["high"]
    features["low"] = row["low"]
    features["close"] = row["close"]
    features["volume"] = row["volume"]
    
    return features


def is_market_hours(dt) -> bool:
    """Check if timestamp is within US market hours (14:30-21:00 UTC)."""
    if pd.isna(dt):
        return False
    market_open = time(14, 30)
    market_close = time(21, 0)
    dt_time = dt.time()
    return market_open <= dt_time < market_close


def build_enriched_dataset(
    availability: pd.DataFrame,
    pricing: pd.DataFrame,
    metadata: pd.DataFrame
) -> pd.DataFrame:
    """
    Build enriched dataset by merging availability with pricing data.
    Reuses build_basic_dataset() for availability feature computation.
    """
    print("\nBuilding enriched dataset...")
    
    # This computes all the spread/bps/time features and drops nulls
    basic_processed, basic_stats = build_basic_dataset(availability)
    


    pricing_sorted = pricing.sort_values(["ticker", "timestamp"]).reset_index(drop=True)
    pricing_by_ticker = {ticker: group for ticker, group in pricing_sorted.groupby("ticker")}
    metadata_dict = metadata.set_index("ticker").to_dict("index")
    

    
    enriched_records = []
    missing_count = 0
    
    # Iterate through processed availability records and merge with pricing
    for _, row in tqdm(basic_processed.iterrows(), total=len(basic_processed), desc="  Merging"):
        ticker = row["ticker"]
        avail_time = row["asof_utc"]
        
        # Get pricing data for this ticker
        if ticker not in pricing_by_ticker:
            missing_count += 1
            continue
        
        ticker_pricing = pricing_by_ticker[ticker]
        
        # Find the most recent pricing timestamp <= availability timestamp
        valid_prices = ticker_pricing[ticker_pricing["timestamp"] <= avail_time]
        
        if len(valid_prices) == 0:
            missing_count += 1
            continue
        
        price_row = valid_prices.iloc[-1]
        
        # Compute pricing features
        pricing_features = compute_pricing_features(price_row)
        
        # Build enriched record starting with all processed availability data
        enriched = {
            # Core availability data (already processed by build_basic_dataset)
            "ticker": row["ticker"],
            "asof_utc": row["asof_utc"],
            "region": row["region"],
            "fee_raw": row["fee_raw"],
            "rebate_raw": row["rebate_raw"],
            "spread_raw": row["spread_raw"],
            "fee_bps": row["fee_bps"],
            "rebate_bps": row["rebate_bps"],
            "spread_bps": row["spread_bps"],
            "available_shares": row["available_shares"],
            "available_unlimited": row["available_unlimited"],
            "hour_utc": row["hour_utc"],
            "day_of_week": row["day_of_week"],
            
            # Enriched-specific time feature
            "is_market_hours": is_market_hours(avail_time),
        }
        
        # Add pricing features
        enriched.update(pricing_features)
        
        # Add metadata
        if ticker in metadata_dict:
            meta = metadata_dict[ticker]
            enriched["sector"] = meta.get("sector")
            enriched["industry"] = meta.get("industry")
            enriched["market_cap"] = meta.get("market_cap")
            enriched["exchange"] = meta.get("exchange")
            enriched["currency"] = meta.get("currency")
        
        enriched_records.append(enriched)
    
    if missing_count > 0:
        print(f"\n  WARNING: {missing_count:,} availability records had no matching pricing data")
    
    # Convert to df
    enriched_df = pd.DataFrame(enriched_records)
    enriched_df = enriched_df.sort_values(["ticker", "region", "asof_utc"])
    
    print(f"  Successfully merged {len(enriched_df):,} records")
    
    return enriched_df, basic_stats


# PIPELINE ORCHESTRATION


def load_pipeline(dataset: str, start_date: str, end_date: str, config: Dict):
    """
    Main load pipeline orchestrator.
    
    Args:
        dataset: 'basic', 'enriched', or 'both'
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        config: Configuration dictionary with paths
    """
    print("")
    print("LOAD PIPELINE")
    print("")
    print(f"Dataset mode: {dataset.upper()}")
    print(f"Date range: {start_date} to {end_date}")
    
    # Generate date range
    dates = get_date_range(start_date, end_date)
    
    base_path = Path(config['normalized_dir'])
    results = {}
    
    # BASIC or BOTH: Build basic dataset
    if dataset in ['basic', 'both']:
        print("")
        print("BASIC DATASET")
        print("")
        
        # Load availability data (all regions)
        availability = load_availability_data(base_path, dates, usa_only=False)
        
        # Build basic dataset
        basic, stats = build_basic_dataset(availability)

        
        # Print summary
        print("\nBasic Dataset Summary:")
        print(f"  Initial records: {stats['initial_count']:,}")
        print(f"  Rows dropped (nulls): {stats['dropped_nulls']:,}")
        print(f"  Final records: {stats['after_processing']:,}")
        print(f"  Unique tickers: {stats['final_tickers']:,}")
        
        # Save (and also stats)
        save_dataset(basic, Path(config['basic_output_dir']), "basic")
        stats_path = Path(config['basic_output_dir']) / "drop_stats"
        with open(stats_path, "w") as f:
            json.dump(stats, f, indent=2)
        
        results['basic_rows'] = stats['after_processing']
        results['basic_tickers'] = stats['final_tickers']
    
    # ENRICHED or BOTH: Build enriched dataset
    if dataset in ['enriched', 'both']:
        print("")
        print("ENRICHED DATASET")
        print("")
        
        # Load metadata
        metadata = load_metadata(base_path)
        valid_tickers = set(metadata["ticker"])

        # Load pricing data
        pricing, actual_dates = load_pricing_data(base_path, dates)

        dates = get_date_range(min(actual_dates), max(actual_dates))
        
        # Load availability data (USA only, filtered to curated tickers)
        availability = load_availability_data(base_path, dates, usa_only=True)
        availability = availability[availability["ticker"].isin(valid_tickers)]
        print(f"  Filtered to {len(availability):,} records for {availability['ticker'].nunique()} curated tickers")
        
        
        # Build enriched dataset
        enriched, stats = build_enriched_dataset(availability, pricing, metadata)
        
        # Print summary
        print("\nEnriched Dataset Summary:")
        print(f"  Total records: {len(enriched):,}")
        print(f"  Date range: {enriched['asof_utc'].min()} to {enriched['asof_utc'].max()}")
        print(f"  Unique tickers: {enriched['ticker'].nunique()}")
        print(f"  Mean spread (bps): {enriched['spread_bps'].mean():.2f}")
        print(f"  Mean avg price: ${enriched['avg_price'].mean():.2f}")
        print(f"  Market hours records: {enriched['is_market_hours'].sum():,} ({enriched['is_market_hours'].mean()*100:.1f}%)")
        
        # Save
        save_dataset(enriched, Path(config['enriched_output_dir']), "enriched")
        stats_path = Path(config['enriched_output_dir']) / "drop_stats"
        with open(stats_path, "w") as f:
            json.dump(stats, f, indent=2)

        results['enriched_rows'] = len(enriched)
        results['enriched_tickers'] = enriched['ticker'].nunique()

    # Print final summary
    print("")
    print("LOAD COMPLETE")
    print("")
    if 'basic_rows' in results:
        print(f"  Basic dataset: {results['basic_rows']:,} rows, {results['basic_tickers']} tickers")
    if 'enriched_rows' in results:
        print(f"  Enriched dataset: {results['enriched_rows']:,} rows, {results['enriched_tickers']} tickers")
    print("")



# CLI


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build final analysis datasets from transformed data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic dataset only (all tickers, all regions)
  python -m etl.load --dataset basic --start-date 2025-11-04 --end-date 2025-11-09
  
  # Enriched dataset only (curated US tickers with pricing)
  python -m etl.load --dataset enriched --start-date 2025-11-04 --end-date 2025-11-09
  
  # Both datasets
  python -m etl.load --dataset both --start-date 2025-11-04 --end-date 2025-11-09
        """
    )
    
    parser.add_argument('--dataset', required=True,
                       choices=['basic', 'enriched', 'both'],
                       help='Dataset type to build')
    parser.add_argument('--start-date', required=True,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True,
                       help='End date (YYYY-MM-DD)')
    
    # Directory configuration
    parser.add_argument('--normalized-dir', default='data/normalized',
                       help='Input directory with normalized data')
    parser.add_argument('--basic-output-dir', default='data/basic',
                       help='Output directory for basic dataset')
    parser.add_argument('--enriched-output-dir', default='data/enriched',
                       help='Output directory for enriched dataset')
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    config = {
        'normalized_dir': args.normalized_dir,
        'basic_output_dir': args.basic_output_dir,
        'enriched_output_dir': args.enriched_output_dir,
    }
    
    try:
        load_pipeline(
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