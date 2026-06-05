#!/usr/bin/env bash
# orchestrate.sh - Complete ETL pipeline orchestration
# example usage: ./orchestrate.sh 24 2025-11-10 2025-11-12

set -euo pipefail
cd "$(dirname "$0")"

if [ -f .venv/bin/activate ]; then
    source .venv/bin/activate  # Linux/Mac
elif [ -f .venv/Scripts/activate ]; then
    source .venv/Scripts/activate  # Windows
else
    echo "Warning: Virtual environment not found"
fi

# Configuration
DURATION="${1:-24}"  # hours
START_DATE="${2:-2025-11-04}"
END_DATE="${3:-2025-11-09}"

echo "IB SECURITIES LENDING ETL PIPELINE"
echo "Duration: ${DURATION}h"
echo "Date range: ${START_DATE} to ${END_DATE}"
echo ""

# EXTRACT: Monitor and ingest 
echo "PHASE 1: EXTRACT (${DURATION}h monitoring)"
until python -m etl.extract --mode monitor --duration "$DURATION"; do
    echo "Extract crashed with exit code $?. Respawning in 60 seconds..." >&2
    sleep 60
done

# TRANSFORM: Normalize and enrich
echo ""
echo "PHASE 2: TRANSFORM"
python -m etl.transform --dataset both \
    --start-date "$START_DATE" \
    --end-date "$END_DATE"

# LOAD: Build final datasets
echo ""
echo "PHASE 3: LOAD"
python -m etl.load --dataset both \
    --start-date "$START_DATE" \
    --end-date "$END_DATE"

echo ""
echo "PIPELINE COMPLETE!"
echo ""
echo "Outputs:"
echo "  - Basic dataset: data/basic/"
echo "  - Enriched dataset: data/enriched/"