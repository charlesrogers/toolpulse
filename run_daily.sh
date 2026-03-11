#!/bin/bash
# ToolPulse daily scraper — run via cron or launchd
# Scrapes go.harborfreight.com for current deals and saves to SQLite

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/scrape_${TIMESTAMP}.log"

echo "[$TIMESTAMP] Starting ToolPulse daily scrape..." | tee "$LOG_FILE"

# Run the deals scraper with DB storage
python3 "$SCRIPT_DIR/scrapers/go_hf_scraper.py" --db 2>&1 | tee -a "$LOG_FILE"

# Print DB stats
python3 "$SCRIPT_DIR/db.py" 2>&1 | tee -a "$LOG_FILE"

echo "[$(date +%Y%m%d_%H%M%S)] Scrape complete." | tee -a "$LOG_FILE"

# Cleanup old logs (keep last 30 days)
find "$LOG_DIR" -name "scrape_*.log" -mtime +30 -delete 2>/dev/null || true
