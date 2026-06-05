#!/usr/bin/env python3
"""
extract.py - Extract raw IB securities lending data from FTP

Three extraction steps:
1. Monitor FTP for changes (from monitoring.py)
2. Parse monitoring logs to events CSV (from log_to_csv.py)  
3. Download changed files from FTP (from ingest_ib.py)

Usage:
    # Continuous monitoring mode (72h collection)
    python etl/extract.py --mode monitor --duration 72 --check-interval 60
    
    # One-time ingestion from existing events
    python etl/extract.py --mode ingest --events data/events/events.csv
    
    # Parse logs to CSV only
    python etl/extract.py --mode parse --logs data/logs/monitoring.raw.log
"""

import argparse
import csv
import json
import time
import re
import sys
from datetime import datetime, timezone
from ftplib import FTP
from pathlib import Path
from typing import Optional, List

# Import the monitor class from utils
from etl.utils.ftp_monitor import FTPMonitor


# EVENT PARSING


HEADER_RE = re.compile(r'^\[(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s+(?P<msg>.+)$')
CHANGE_RE = re.compile(r'^\s*(?P<kind>NEW|UPDATED|DELETED):\s*(?P<base>.+?)\s*$')


def parse_monitoring_logs(log_paths: List[Path], include_heartbeat: bool = False) -> List[tuple]:
    """
    Parse monitoring logs into structured events.
    
    Args:
        log_paths: List of log file paths to parse
        include_heartbeat: Include "No changes" rows in output
        
    Returns:
        List of (timestamp, message) tuples
    """
    rows = []
    current_ts = None
    
    for path in log_paths:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            for raw in f:
                line = raw.rstrip('\n')
                
                # Try to match header line
                m = HEADER_RE.match(line)
                if m:
                    current_ts = m.group('ts')
                    msg = m.group('msg').strip()
                    if msg.startswith('No changes'):
                        if include_heartbeat:
                            rows.append((current_ts, 'No changes'))
                    elif msg.startswith('Changes detected'):
                        rows.append((current_ts, 'Changes detected'))
                    continue
                
                # Try to match change line
                m = CHANGE_RE.match(line)
                if m and current_ts:
                    rows.append((current_ts, f"{m.group('kind')}: {m.group('base')}"))
    
    return rows


def write_events_csv(rows: List[tuple], output_path: Path):
    """Write parsed events to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as w:
        writer = csv.writer(w)
        writer.writerow(['timestamp', 'message'])
        writer.writerows(rows)



# FTP INGESTION 


def iso_utc(dt: Optional[datetime]) -> Optional[str]:
    """Format UTC datetime as 'YYYY-MM-DDTHH:MM:SSZ' or return None."""
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z") if dt else None


def read_events(paths: List[Path]) -> List[str]:
    """
    Read events CSV files and return base filenames to fetch.
    Accepts rows whose message starts with 'NEW:' or 'UPDATED:'.
    """
    out = []
    for p in paths:
        with p.open(newline='', encoding='utf-8', errors='ignore') as f:
            r = csv.DictReader(f)
            for row in r:
                msg = (row.get('message') or '').strip()
                if msg.startswith('NEW:') or msg.startswith('UPDATED:'):
                    base = msg.split(':', 1)[1].strip()
                    if base:
                        out.append(base)
    return out


def ftp_read_md5(ftp: FTP, base: str) -> Optional[str]:
    """Fetch '<base>.md5' and return first token as lowercase hex, or None."""
    lines = []
    try:
        ftp.retrlines(f"RETR {base}.md5", lines.append)
    except Exception:
        return None
    if not lines:
        return None
    token = " ".join(lines).strip().split()[0].lower()
    return token if len(token) >= 32 else None


def ftp_mdtm_utc(ftp: FTP, base: str) -> Optional[datetime]:
    """Get server mtime in UTC."""
    try:
        resp = ftp.sendcmd(f"MDTM {base}")
        parts = resp.split()
        if len(parts) == 2 and parts[0] == '213':
            ts = datetime.strptime(parts[1], "%Y%m%d%H%M%S")
            return ts.replace(tzinfo=timezone.utc)
    except Exception:
        return None
    return None


def atomic_download(ftp: FTP, base: str, final_path: Path) -> int:
    """Download to tmp path then rename to final_path. Returns byte size."""
    tmp = final_path.parent / ("._tmp_" + final_path.name)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    with tmp.open('wb') as w:
        ftp.retrbinary(f"RETR {base}", w.write)
    tmp.replace(final_path)  # atomic on same filesystem
    return final_path.stat().st_size


def save_json(path: Path, obj: dict) -> None:
    """Write JSON, creating parent dirs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding='utf-8')


def ingest_one(ftp: FTP, base: str, config: dict, md5_index: dict) -> None:
    """Fetch one base file and write a sidecar meta JSON."""
    # Debounce before each fetch
    if config['debounce_sec'] > 0:
        time.sleep(config['debounce_sec'])
    
    # Expected md5 from server, skip if already seen
    exp_md5 = ftp_read_md5(ftp, base)
    if exp_md5 and exp_md5 in md5_index:
        print(f"[SKIP md5] {base} already ingested at {md5_index[exp_md5]}")
        return
    
    # Get server mtime
    server_mtime = ftp_mdtm_utc(ftp, base)
    
    # Name under date partition
    stamp = server_mtime or datetime.now(timezone.utc)
    day = stamp.date().isoformat()
    ts_tag = stamp.strftime("%Y%m%dT%H%M%SZ")
    
    out_dir = Path(config['raw_dir']) / day
    data_path = out_dir / f"{ts_tag}_{Path(base).name}"
    meta_path = out_dir / f"{ts_tag}_{Path(base).stem}.meta.json"
    
    # Download file
    t0 = datetime.now(timezone.utc)
    size_bytes = atomic_download(ftp, base, data_path)
    t1 = datetime.now(timezone.utc)
    
    # Track by md5 to avoid duplicates
    if exp_md5:
        md5_index[exp_md5] = data_path.name
    

    meta = {
        "filename": Path(base).name,
        "region": Path(base).stem.lower(),
        "size_bytes": size_bytes,
        "md5": exp_md5,
        "server_mtime_utc": iso_utc(server_mtime),
        "downloaded_at_utc": iso_utc(t1),
        "source": {
            "host": config['ftp_host'],
            "user": config['ftp_user'],
            "dir": config['ftp_dir']
        },
        "final_path": str(data_path),
        "transfer_latency_sec": (t1 - t0).total_seconds()
    }
    
    save_json(meta_path, meta)
    print(f"[OK] {base} -> {data_path.name} size={size_bytes} md5={exp_md5[:8] if exp_md5 else 'none'}")


def ingest_from_events(event_files: List[Path], config: dict):
    """Download files listed in events CSV with auto-reconnect logic."""
    idx_path = Path(config['index_file'])
    md5_index = json.loads(idx_path.read_text()) if idx_path.exists() else {}
    
    base_files = read_events(event_files)
    if not base_files:
        return
    
    print(f"Found {len(base_files)} files to ingest")

    def connect_ftp():
        """Helper to create a fresh, logged-in FTP connection."""
        f = FTP(timeout=config['timeout'])
        f.connect(config['ftp_host'], 21, timeout=config['timeout'])
        f.login(user=config['ftp_user'])
        f.cwd(config['ftp_dir'])
        return f

    ftp = connect_ftp()
    
    try:
        for base in base_files:
            # Retry logic for each individual file
            for attempt in range(3): 
                try:
                    ingest_one(ftp, base, config, md5_index)
                    break 
                except (OSError, ConnectionError, EOFError) as e:
                    # catches errors
                    print(f"[RECONNECT] Connection lost on {base}. Attempting recovery... ({e})")
                    time.sleep(5) 
                    try:
                        ftp.close() # Clean up old socket
                    except:
                        pass
                    ftp = connect_ftp() # Re-establish the connection
            else:
                print(f"[FATAL] Failed to download {base} after 3 attempts. Skipping.")
    
    finally:
        try:
            ftp.quit()
        except:
            pass
        # Save index even if we crashed, to preserve progress
        save_json(idx_path, md5_index)
        print(f"Ingestion batch finished. MD5 index updated.")



# MONITORING MODE 


def run_monitoring_loop(config: dict):
    """
    Run continuous monitoring loop.
    
    Monitors FTP, writes logs, parses to CSV, and ingests files.
    Similar to what run_72h.sh does but in Python.
    """
    # Setup paths
    log_file = Path(config['log_file'])
    events_file = Path(config['events_file'])
    
    # Ensure directories exist
    log_file.parent.mkdir(parents=True, exist_ok=True)
    events_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Initialize events CSV if it doesn't exist
    if not events_file.exists():
        with open(events_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'message'])
    
    # Create monitor
    monitor = FTPMonitor(
        host=config['ftp_host'],
        user=config['ftp_user'],
        directory=config['ftp_dir'],
        check_interval=config['check_interval'],
        state_file=config['state_file'],
        timeout=config['timeout']
    )
    
    print(f"Starting monitoring loop")
    print(f"FTP: {config['ftp_host']}")
    print(f"Check interval: {config['check_interval']} seconds")
    print(f"Duration: {config['duration_hours']} hours")
    print(f"Log file: {log_file}")
    print(f"Events file: {events_file}")
    
    start_time = time.time()
    duration_seconds = config['duration_hours'] * 3600
    
    with open(log_file, 'a', buffering=1) as log_f: 
        iteration = 0
        while True:
            iteration += 1
            elapsed = time.time() - start_time
            
            # Check if we've exceeded duration
            if elapsed >= duration_seconds:
                print(f"\nMonitoring complete after {config['duration_hours']} hours")
                break
            
            try:
                iter_start = time.time()
                formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(iter_start))
                
                # Check for changes
                changes = monitor.check_changes()
                
                # Log to file
                if changes:
                    log_msg = f"[{formatted_time}] Changes detected:\n"
                    log_f.write(log_msg)
                    print(log_msg, end='')
                    
                    for change_type, filename, md5_hash in changes:
                        base_file = filename.replace('.md5', '')
                        if change_type == 'new':
                            change_msg = f"  NEW: {base_file}\n"
                        elif change_type == 'updated':
                            change_msg = f"  UPDATED: {base_file}\n"
                        elif change_type == 'deleted':
                            change_msg = f"  DELETED: {base_file}\n"
                        
                        log_f.write(change_msg)
                        print(change_msg, end='')
                else:
                    log_msg = f"[{formatted_time}] No changes\n"
                    log_f.write(log_msg)
                    print(log_msg, end='')
                
                # Parse logs to CSV (every iteration)
                rows = parse_monitoring_logs([log_file], include_heartbeat=True)
                write_events_csv(rows, events_file)
                
                # Ingest if there are actual changes (not just heartbeat)
                if changes and any(ct in ['new', 'updated'] for ct, _, _ in changes):
                    print(f"[Iteration {iteration}] Running ingestion...")
                    ingest_from_events([events_file], config)
                
                # Sleep to maintain check interval
                elapsed_iter = time.time() - iter_start
                sleep_time = max(0, config['check_interval'] - elapsed_iter)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
            except Exception as e:
                error_msg = f"\n[{formatted_time}] Error: {e}\n"
                log_f.write(error_msg)
                print(error_msg, end='')
                time.sleep(config['check_interval'])



# ARGUMENTS AND MAIN


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract IB securities lending data from FTP",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 72-hour monitoring (standard data collection)
  python etl/extract.py --mode monitor --duration 72
  
  # One-time ingestion from existing events
  python etl/extract.py --mode ingest --events data/events/events.csv
  
  # Parse logs to CSV only
  python etl/extract.py --mode parse --logs data/logs/monitoring.raw.log --out data/events/events.csv
        """
    )
    
    parser.add_argument('--mode', required=True, 
                       choices=['monitor', 'ingest', 'parse'],
                       help='Extraction mode: monitor (continuous), ingest (one-time), or parse (logs only)')
    
    # Monitoring mode arguments
    parser.add_argument('--duration', type=float, default=72,
                       help='Monitoring duration in hours (default: 72)')
    parser.add_argument('--check-interval', type=int, default=60,
                       help='Seconds between FTP checks (default: 60)')
    
    # File paths
    parser.add_argument('--log-file', default='data/logs/monitoring.raw.log',
                       help='Log file path for monitoring mode')
    parser.add_argument('--events', nargs='+', 
                       help='Event CSV file(s) for ingestion or parsing')
    parser.add_argument('--logs', nargs='+',
                       help='Log file(s) to parse (for parse mode)')
    parser.add_argument('--out', help='Output CSV path (for parse mode)')
    parser.add_argument('--include-heartbeat', action='store_true',
                       help='Include "No changes" rows in parsed CSV')
    
    # FTP configuration
    parser.add_argument('--ftp-host', default='ftp2.interactivebrokers.com')
    parser.add_argument('--ftp-user', default='shortstock')
    parser.add_argument('--ftp-dir', default='/')
    parser.add_argument('--timeout', type=int, default=60)
    
    # Storage paths
    parser.add_argument('--raw-dir', default='data/raw/ib',
                       help='Output directory for raw files')
    parser.add_argument('--events-file', default='data/events/events.csv',
                       help='Events CSV file path')
    parser.add_argument('--state-file', default='data/state/ftp_monitor_state.json',
                       help='FTP monitor state file')
    parser.add_argument('--index-file', default='data/state/ingested_md5_index.json',
                       help='MD5 index file for deduplication')
    parser.add_argument('--debounce-sec', type=int, default=30,
                       help='Sleep before each file fetch (seconds)')
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Build config dict
    config = {
        'ftp_host': args.ftp_host,
        'ftp_user': args.ftp_user,
        'ftp_dir': args.ftp_dir,
        'timeout': args.timeout,
        'raw_dir': args.raw_dir,
        'check_interval': args.check_interval,
        'duration_hours': args.duration,
        'log_file': args.log_file,
        'events_file': args.events_file,
        'state_file': args.state_file,
        'index_file': args.index_file,
        'debounce_sec': args.debounce_sec,
    }
    
    try:
        if args.mode == 'monitor':
            # Continuous monitoring mode 
            run_monitoring_loop(config)
        
        elif args.mode == 'ingest':
            # One-time ingestion from events
            if not args.events:
                print("ERROR: --events required for ingest mode")
                return 1
            event_files = [Path(p) for p in args.events]
            ingest_from_events(event_files, config)
        
        elif args.mode == 'parse':
            # Parse logs to CSV only
            if not args.logs or not args.out:
                print("ERROR: --logs and --out required for parse mode")
                return 1
            log_files = [Path(p) for p in args.logs]
            rows = parse_monitoring_logs(log_files, args.include_heartbeat)
            write_events_csv(rows, Path(args.out))
            print(f"Parsed {len(rows)} events to {args.out}")
        
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