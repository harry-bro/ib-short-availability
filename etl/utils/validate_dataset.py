#!/usr/bin/env python3
"""
validate_dataset.py - Quality validation for analysis datasets

Validates data quality metrics for basic or enriched datasets.

Usage:
    # All dates in dataset
    python etl/utils/validate_dataset.py --dataset basic
    python etl/utils/validate_dataset.py --dataset enriched
    
    # Specific date range
    python validate_dataset.py --dataset basic --start-date 2025-11-04 --end-date 2025-11-09
    python validate_dataset.py --dataset enriched --start-date 2025-11-04 --end-date 2025-11-09
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import numpy as np


def load_dataset(dataset_type: str, base_dir: str,start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """Load dataset for specified date range, or all dates if not specified."""
    
    if dataset_type == 'basic':
        directory = Path(base_dir) / 'basic'
    else:
        directory = Path(base_dir) / 'enriched'
    
    dfs = []
    
    if start_date is None or end_date is None:
        for date_dir in sorted(directory.glob('dt=*')):
            if date_dir.is_dir():
                file_path = date_dir / f'{dataset_type}.parquet'
                if file_path.exists():
                    dfs.append(pd.read_parquet(file_path))
    else:
        # Use specified date range
        dates = pd.date_range(start_date, end_date, freq='D').strftime('%Y-%m-%d').tolist()
        for date in dates:
            file_path = directory / f'dt={date}' / f'{dataset_type}.parquet'
            if file_path.exists():
                dfs.append(pd.read_parquet(file_path))
    
    if not dfs:
        raise ValueError(f"No data found for {dataset_type} dataset")
    
    df = pd.concat(dfs, ignore_index=True)
    df['asof_utc'] = pd.to_datetime(df['asof_utc'])
    return df


def check_nulls(df: pd.DataFrame) -> Dict:
    """Check for null values."""
    null_counts = {}

    if 'fee_raw' in df.columns:
        null_counts['fee_raw'] = int(df['fee_raw'].isnull().sum())
    if 'rebate_raw' in df.columns:
        null_counts['rebate_raw'] = int(df['rebate_raw'].isnull().sum())
    if 'open' in df.columns:
        null_counts['open'] = int(df['open'].isnull().sum())
    if 'close' in df.columns:
        null_counts['close'] = int(df['close'].isnull().sum())
    
    null_counts = {k: v for k, v in null_counts.items() if v > 0}
    total_nulls = sum(null_counts.values())
    
    return {
        'total_nulls': int(total_nulls),
        'null_by_column': null_counts
    }


def check_coverage(df: pd.DataFrame) -> Dict:
    """Check temporal coverage per ticker."""
    coverage_stats = []
    
    for ticker, group in df.groupby(['ticker', 'region']):
        ticker_name, region = ticker
        group = group.sort_values('asof_utc')
        
        first_ts = group['asof_utc'].min()
        last_ts = group['asof_utc'].max()
        hours_span = (last_ts - first_ts).total_seconds() / 3600
        
        n_points = len(group)
        density = n_points / hours_span if hours_span > 0 else 0
        
        coverage_stats.append({
            'ticker': ticker_name,
            'region': region,
            'hours_span': hours_span,
            'n_points': n_points,
            'density_per_hour': density
        })
    first_ts = df['asof_utc'].min()
    last_ts = df['asof_utc'].max()

    coverage_df = pd.DataFrame(coverage_stats)
    
    return {
        'mean_hours_span': float(coverage_df['hours_span'].mean()),
        'min_hours_span': float(coverage_df['hours_span'].min()),
        'max_hours_span': float(coverage_df['hours_span'].max()),
        'mean_density': float(coverage_df['density_per_hour'].mean()),
        'min_density': float(coverage_df['density_per_hour'].min()),
        'max_density': float(coverage_df['density_per_hour'].max()),
        'tickers_below_23h': int((coverage_df['hours_span'] < 23).sum()),
        'tickers_density_below_3_8': int((coverage_df['density_per_hour'] < 3.8).sum()),
        'total_runtime': ((last_ts - first_ts).total_seconds() / 3600)
    }


def check_temporal_gaps(df: pd.DataFrame) -> Dict:
    """Check for gaps > 20 minutes in time series."""
    gap_counts = []
    
    for ticker, group in df.groupby(['ticker', 'region']):
        ticker_name, region = ticker
        group = group.sort_values('asof_utc')
        
        if len(group) < 2:
            continue
        
        time_diffs = group['asof_utc'].diff()
        gaps_over_20min = (time_diffs > pd.Timedelta(minutes=20)).sum()
        
        if gaps_over_20min > 0:
            gap_counts.append({
                'ticker': ticker_name,
                'region': region,
                'gaps_over_20min': gaps_over_20min
            })
    
    total_gaps = sum(g['gaps_over_20min'] for g in gap_counts)
    
    return {
        'total_gaps_over_20min': total_gaps,
        'tickers_with_gaps': len(gap_counts),
        'gap_details': gap_counts if gap_counts else []
    }


def check_duplicates(df: pd.DataFrame) -> Dict:
    """Check for duplicate (ticker, timestamp, region) tuples."""
    duplicates = df[df.duplicated(subset=['ticker', 'asof_utc', 'region'], keep=False)]
    
    if len(duplicates) > 0:
        duplicates = duplicates.sort_values(['ticker', 'region', 'asof_utc'])
        dup_list = duplicates[['ticker', 'asof_utc', 'region']].to_dict('records')
    else:
        dup_list = []
    
    return {
        'n_duplicate_rows': len(duplicates),
        'duplicates': dup_list
    }


def check_value_ranges(df: pd.DataFrame, dataset_type: str) -> Dict:
    """Check value ranges for pricing data."""
    if dataset_type != 'enriched':
        return {}
    
    issues = []
    
    # Check for non-positive prices
    price_cols = ['open', 'high', 'low', 'close']
    for col in price_cols:
        if col in df.columns:
            non_positive = (df[col] <= 0).sum()
            if non_positive > 0:
                issues.append(f"{col}: {non_positive} non-positive values")
    
    # Check for negative volume
    if 'volume' in df.columns:
        neg_volume = (df['volume'] < 0).sum()
        if neg_volume > 0:
            issues.append(f"volume: {neg_volume} negative values")
    
    return {
        'value_range_issues': issues
    }


def check_enrichment_rate(df: pd.DataFrame, dataset_type: str) -> Dict:
    """Check enrichment success rate for enriched dataset."""
    if dataset_type != 'enriched':
        return {}
    
    pricing_cols = ['open', 'high', 'low', 'close', 'volume']
    metadata_cols = ['sector', 'industry', 'market_cap']
    
    has_pricing = df[pricing_cols].notna().all(axis=1).sum()
    has_metadata = df[metadata_cols].notna().all(axis=1).sum()
    
    total_rows = len(df)
    
    return {
        'pricing_match_rate': float(has_pricing / total_rows) if total_rows > 0 else 0,
        'metadata_match_rate': float(has_metadata / total_rows) if total_rows > 0 else 0
    }


def print_report(results: Dict, dataset_type: str):
    """Print validation report."""
    print("")
    print(f"DATASET QUALITY REPORT: {dataset_type.upper()}")
    print("")
    
    # Null values
    nulls = results['nulls']
    print(f"Null values: {nulls['total_nulls']:,}")
    if nulls['null_by_column']:
        for col, count in nulls['null_by_column'].items():
            print(f"  {col}: {count:,}")
    
    # Coverage
    print("")
    cov = results['coverage']
    print(f"Temporal coverage (Total collection time: {cov['total_runtime']:.2f}h)")
    print(f"  Hours span: {cov['mean_hours_span']:.2f}h (min={cov['min_hours_span']:.2f}, max={cov['max_hours_span']:.2f})")
    print(f"  Timestamp density: {cov['mean_density']:.2f} pts/h (min={cov['min_density']:.2f}, max={cov['max_density']:.2f})")
    print(f"  Tickers < 23h coverage: {cov['tickers_below_23h']}")
    print(f"  Tickers < 3.8 pts/h: {cov['tickers_density_below_3_8']}")
    
    # Temporal gaps
    print("")
    gaps = results['gaps']
    print(f"Temporal gaps (>20min): {gaps['total_gaps_over_20min']} across {gaps['tickers_with_gaps']} tickers")
    if gaps['gap_details']:
        print("  Top 10 tickers with gaps:")
        sorted_gaps = sorted(gaps['gap_details'], key=lambda x: x['gaps_over_20min'], reverse=True)[:10]
        for item in sorted_gaps:
            print(f"    {item['ticker']} ({item['region']}): {item['gaps_over_20min']} gaps")
    
    # Duplicates
    print("")
    dups = results['duplicates']
    print(f"Duplicates: {dups['n_duplicate_rows']} rows")
    if dups['duplicates']:
        print("  Duplicate records:")
        for dup in dups['duplicates'][:20]:  # Show first 20
            print(f"    {dup['ticker']} @ {dup['asof_utc']} ({dup['region']})")
        if len(dups['duplicates']) > 20:
            print(f"    ... and {len(dups['duplicates']) - 20} more")
    
    # Value ranges (enriched only)
    if 'value_ranges' in results and results['value_ranges']:
        print("")
        vr = results['value_ranges']
        if vr['value_range_issues']:
            print("Value range issues:")
            for issue in vr['value_range_issues']:
                print(f"  {issue}")
        else:
            print("Value range issues: None")
    
    # Enrichment rate (enriched only)
    if 'enrichment' in results and results['enrichment']:
        print("")
        enr = results['enrichment']
        print(f"Enrichment rates:")
        print(f"  Pricing match: {enr['pricing_match_rate']*100:.1f}%")
        print(f"  Metadata match: {enr['metadata_match_rate']*100:.1f}%")
    
    print("")


def validate_dataset(dataset_type: str, base_dir: str, start_date: str, end_date: str):
    """Main validation pipeline."""
    if start_date and end_date:
        print(f"Loading {dataset_type} dataset ({start_date} to {end_date})...")
    else:
        print(f"Loading {dataset_type} dataset (all dates)...")
    df = load_dataset(dataset_type, base_dir, start_date, end_date)
    print(f"Loaded {len(df):,} rows for {df['ticker'].nunique()} tickers")
    
    results = {}
    
    print("Running validation checks...")
    results['nulls'] = check_nulls(df)
    results['coverage'] = check_coverage(df)
    results['gaps'] = check_temporal_gaps(df)
    results['duplicates'] = check_duplicates(df)
    results['value_ranges'] = check_value_ranges(df, dataset_type)
    results['enrichment'] = check_enrichment_rate(df, dataset_type)
    
    
    print_report(results, dataset_type)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Validate dataset quality",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--dataset', required=True,
                       choices=['basic', 'enriched'],
                       help='Dataset type to validate')
    parser.add_argument('--start-date',
                       help='Start date (YYYY-MM-DD), optional')
    parser.add_argument('--end-date',
                       help='End date (YYYY-MM-DD), optional')
    parser.add_argument('--base-dir', default='data',
                        help='Base directory of datasets, typically data'
    )
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    try:
        validate_dataset(
            dataset_type=args.dataset,
            base_dir=args.base_dir,
            start_date=args.start_date,
            end_date=args.end_date
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