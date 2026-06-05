#!/usr/bin/env python3
"""
Extract transfer latency and file size data from IB metadata files.

Usage:
    python extract_metadata.py
    python extract_metadata.py --start-date 2025-11-04 --end-date 2025-11-07
    python extract_metadata.py --output metadata_stats.csv
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract transfer latency and file size from IB metadata"
    )
    parser.add_argument(
        "--raw-dir",
        default="data/raw/ib",
        help="Directory containing raw IB data (default: data/raw/ib)"
    )
    parser.add_argument(
        "--start-date",
        help="Start date YYYY-MM-DD (optional, includes all if not specified)"
    )
    parser.add_argument(
        "--end-date",
        help="End date YYYY-MM-DD (optional, includes all if not specified)"
    )
    parser.add_argument(
        "--output",
        help="Output CSV file (optional, prints to stdout if not specified)"
    )
    return parser.parse_args()


def extract_metadata(raw_dir: Path, start_date: str = None, end_date: str = None):
    """
    Extract transfer_latency_sec and size_bytes from all metadata JSON files.
    
    Returns:
        List of dicts with metadata
    """
    records = []
    
    # Get all date directories
    date_dirs = sorted([d for d in raw_dir.iterdir() if d.is_dir()])
    
    # Filter by date range if specified
    if start_date or end_date:
        start = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
        end = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None
        
        filtered_dirs = []
        for d in date_dirs:
            try:
                dir_date = datetime.strptime(d.name, "%Y-%m-%d").date()
                if start and dir_date < start:
                    continue
                if end and dir_date > end:
                    continue
                filtered_dirs.append(d)
            except ValueError:
                # Skip directories that don't match date format
                continue
        
        date_dirs = filtered_dirs
    
    print(f"Processing {len(date_dirs)} date directories...", file=sys.stderr)
    
    # Process each date directory
    for date_dir in date_dirs:
        
        # Find all .meta.json (non-stockmargin) files
        for json_file in date_dir.glob("*.meta.json"):
            if "stockmargin" not in json_file.name:
                try:
                    with open(json_file, 'r') as f:
                        meta = json.load(f)
                    
                    # Extract relevant fields
                    record = {
                        'date': date_dir.name,
                        'filename': meta.get('filename'),
                        'region': meta.get('region'),
                        'size_bytes': meta.get('size_bytes'),
                        'transfer_latency_sec': meta.get('transfer_latency_sec'),
                        'server_mtime_utc': meta.get('server_mtime_utc'),
                        'downloaded_at_utc': meta.get('downloaded_at_utc'),
                    }
                    
                    records.append(record)
                
                except Exception as e:
                    print(f"    Error reading {json_file.name}: {e}", file=sys.stderr)
                    continue
        
    print(f"\nExtracted {len(records)} records", file=sys.stderr)
    return records


def main():
    args = parse_args()
    
    raw_dir = Path(args.raw_dir)
    
    if not raw_dir.exists():
        print(f"ERROR: Directory not found: {raw_dir}", file=sys.stderr)
        return 1
    
    # Extract data
    records = extract_metadata(raw_dir, args.start_date, args.end_date)
    
    if not records:
        print("No records found!", file=sys.stderr)
        return 1
    
    # Convert to DataFrame for easy output
    df = pd.DataFrame(records)
    
    # Print summary statistics
    print("\n SUMMARY STATISTICS ", file=sys.stderr)
    print(f"Total files: {len(df)}", file=sys.stderr)
    print(f"\nFile sizes (bytes):", file=sys.stderr)
    print(f"  Mean: {df['size_bytes'].mean():,.0f}", file=sys.stderr)
    print(f"  Median: {df['size_bytes'].median():,.0f}", file=sys.stderr)
    print(f"  Min: {df['size_bytes'].min():,.0f}", file=sys.stderr)
    print(f"  Max: {df['size_bytes'].max():,.0f}", file=sys.stderr)
    print(f"\nTransfer latency (seconds):", file=sys.stderr)
    print(f"  Mean: {df['transfer_latency_sec'].mean():.3f}", file=sys.stderr)
    print(f"  Median: {df['transfer_latency_sec'].median():.3f}", file=sys.stderr)
    print(f"  Min: {df['transfer_latency_sec'].min():.3f}", file=sys.stderr)
    print(f"  Max: {df['transfer_latency_sec'].max():.3f}", file=sys.stderr)
    
    # Output data
    if args.output:
        df.to_csv(args.output, index=False)
        print(f"\nData written to {args.output}", file=sys.stderr)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())