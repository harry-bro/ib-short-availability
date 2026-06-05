# IB Securities Lending ETL

The end-to-end driver is `orchestrate.sh`, which runs:
- `python -m etl.extract --mode monitor ...`
- `python -m etl.transform --dataset both ...`
- `python -m etl.load --dataset both ...`


## Setup


###  Install dependencies
```bash
pip install -r requirements.txt
```

## Run the full pipeline (recommended)

Make the script executable (Linux/macOS):
```bash
chmod +x orchestrate.sh
```

Run:
```bash
./orchestrate.sh <DURATION_HOURS> <START_DATE> <END_DATE>
```

Example:
```bash
./orchestrate.sh 24 2025-11-10 2025-11-12
```

Arguments:
- `DURATION_HOURS`: how long to monitor + ingest from FTP (default: 24)
- `START_DATE`, `END_DATE`: date range (YYYY-MM-DD) used for transform/load steps  
  

Outputs:
- `data/basic/` (partitioned parquet under `dt=YYYY-MM-DD/`)
- `data/enriched/` (partitioned parquet under `dt=YYYY-MM-DD/`)

## Run individual stages (debugging etc..)

### Extract (monitor / ingest / parse)

Monitor the FTP and ingest new/updated files:
```bash
python -m etl.extract --mode monitor --duration 72 --check-interval 60
```

One-time ingest from existing events CSVs:
```bash
python -m etl.extract --mode ingest --events data/events/events.csv
```

Parse monitoring logs into an events CSV:
```bash
python -m etl.extract --mode parse \
  --logs data/logs/monitoring.raw.log \
  --out data/events/events.csv \
  --include-heartbeat
```

Key paths / options (all overridable via flags):
- Raw files: `data/raw/ib/`
- Monitoring log: `data/logs/monitoring.raw.log`
- Events CSV: `data/events/events.csv`
- State: `data/state/ftp_monitor_state.json`
- MD5 dedupe index: `data/state/ingested_md5_index.json`

### Transform (normalize IB + optional market-data enrichment)

Build either `basic`, `enriched`, or `both`:
```bash
python -m etl.transform --dataset both --start-date 2025-11-04 --end-date 2025-11-09
```

Notes:
- Normalized IB availability parquet is written under:
  - `data/normalized/ib_availability/dt=YYYY-MM-DD/part-000.parquet`
- Market data (OHLCV + metadata) is written under:
  - `data/normalized/market_data/`

### Load (final datasets)

Build either `basic`, `enriched`, or `both`:
```bash
python -m etl.load --dataset both --start-date 2025-11-04 --end-date 2025-11-09
```

By default it reads from `data/normalized/` and writes:
- `data/basic/`
- `data/enriched/`

## Helper utilities

### Validate datasets (basic or enriched)

Validates nulls, coverage, temporal gaps, duplicates, and (for enriched) value ranges + enrichment rate.

All available dates:
```bash
python validate_dataset.py --dataset basic
python validate_dataset.py --dataset enriched
```

Specific date range:
```bash
python validate_dataset.py --dataset basic --start-date 2025-11-04 --end-date 2025-11-09
python validate_dataset.py --dataset enriched --start-date 2025-11-04 --end-date 2025-11-09
```

If datasets live somewhere else, point `--base-dir` at the folder that contains `basic/` and `enriched/`:
```bash
python validate_dataset.py --dataset basic --base-dir data
```
# Analysis

See analysis_notebook.ipynb for discussion, exploratory data analysis, and feature proposal.