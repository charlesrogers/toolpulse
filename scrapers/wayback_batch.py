#!/usr/bin/env python3
"""
ToolPulse: Wayback Machine batch backfill runner.

Designed to run in GitHub Actions. Processes a batch of products per invocation,
tracking progress across runs via a JSON state file.

Priority order:
  1. Products with active deals (most valuable — we can compare deal vs regular price)
  2. Products from the live sitemap (current catalog)
  3. All remaining products from CDX discovery (historical/discontinued)

Usage:
    python3 wayback_batch.py --db --batch-size 30
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import requests

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
sys.path.insert(0, BASE_DIR)
sys.path.insert(0, os.path.join(BASE_DIR, "scrapers"))

from wayback_backfill import backfill_product, find_snapshots

PROGRESS_FILE = os.path.join(DATA_DIR, "backfill_progress.json")
CDX_API = "https://web.archive.org/cdx/search/cdx"
HEADERS = {"User-Agent": "ToolPulse/1.0 (historical price research)"}


_url_cache = None  # {sku: url} — loaded once from JSON files


def _load_url_cache() -> dict[str, str]:
    """Load all known SKU→URL mappings from local JSON files."""
    global _url_cache
    if _url_cache is not None:
        return _url_cache

    _url_cache = {}

    # all_product_urls.json: {sku: {url, source, active}} or {sku: url}
    all_urls_file = os.path.join(DATA_DIR, "all_product_urls.json")
    if os.path.exists(all_urls_file):
        with open(all_urls_file) as f:
            all_urls = json.load(f)
        for sku, val in all_urls.items():
            url = val.get("url") if isinstance(val, dict) else val
            if url:
                _url_cache[sku] = url

    # product_urls.json: flat list of URLs
    urls_file = os.path.join(DATA_DIR, "product_urls.json")
    if os.path.exists(urls_file):
        with open(urls_file) as f:
            urls = json.load(f)
        for url in urls:
            m = re.search(r"-(\d{5,})\.html", url)
            if m and m.group(1) not in _url_cache:
                _url_cache[m.group(1)] = url

    print(f"  URL cache: {len(_url_cache)} SKUs loaded")
    return _url_cache


def resolve_url_for_sku(sku: str) -> str | None:
    """Resolve a SKU to its harborfreight.com product URL.

    Uses cached local files first, falls back to CDX API.
    """
    cache = _load_url_cache()
    if sku in cache:
        return cache[sku]

    # Fall back to CDX API
    try:
        params = {
            "url": f"www.harborfreight.com/*-{sku}.html",
            "matchType": "prefix",
            "output": "text",
            "fl": "original",
            "collapse": "urlkey",
            "filter": "statuscode:200",
            "limit": "1",
        }
        resp = requests.get(CDX_API, params=params, headers=HEADERS, timeout=30)
        if resp.ok and resp.text.strip():
            url = resp.text.strip().split("\n")[0].strip()
            cache[sku] = url  # Cache for future lookups in this run
            return url
    except Exception as e:
        print(f"    CDX lookup failed for {sku}: {e}")

    return None


def load_progress() -> dict:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"completed_skus": [], "current_index": 0, "total_processed": 0}


def save_progress(progress: dict):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def build_priority_queue() -> list[dict]:
    """Build an ordered list of products to backfill, prioritized by value.

    Returns list of {sku, url, priority, reason} dicts.
    """
    queue = []
    seen_skus = set()

    # Priority 0: Items from email deal extraction (highest value)
    email_items_file = os.path.join(DATA_DIR, "email_deal_items.json")
    if os.path.exists(email_items_file):
        with open(email_items_file) as f:
            email_items = json.load(f)
        cache = _load_url_cache()
        resolved = 0
        print(f"  Email deal items: {len(email_items)}")
        for sku in email_items:
            if sku not in seen_skus:
                url = cache.get(sku)
                if url:
                    resolved += 1
                queue.append({
                    "sku": sku,
                    "url": url,  # May be None — resolved at processing time via CDX
                    "priority": 0,
                    "reason": "email_deal",
                })
                seen_skus.add(sku)
        print(f"    Pre-resolved URLs: {resolved}/{len(email_items)}")

    # Priority 1: Products with active deals (from go.hf scraper)
    db_path = os.path.join(DATA_DIR, "toolpulse.db")
    if os.path.exists(db_path):
        try:
            from db import ToolPulseDB
            db = ToolPulseDB()

            rows = db.conn.execute(
                """SELECT DISTINCT d.item_number, p.hf_url, p.product_name
                   FROM deals d
                   JOIN products p ON d.item_number = p.item_number"""
            ).fetchall()

            for row in rows:
                sku = row["item_number"]
                if sku not in seen_skus:
                    queue.append({
                        "sku": sku,
                        "url": row["hf_url"],
                        "priority": 1,
                        "reason": "has_deals",
                    })
                    seen_skus.add(sku)

            # Priority 2: All products with URLs (from sitemap/CDX)
            rows = db.conn.execute(
                "SELECT item_number, hf_url FROM products WHERE hf_url IS NOT NULL"
            ).fetchall()
            for row in rows:
                sku = row["item_number"]
                if sku not in seen_skus:
                    queue.append({
                        "sku": sku,
                        "url": row["hf_url"],
                        "priority": 2,
                        "reason": "catalog",
                    })
                    seen_skus.add(sku)

            db.close()
        except Exception as e:
            print(f"  Warning: could not read database: {e}")

    # Also check product_urls.json (from discover_products.py)
    urls_file = os.path.join(DATA_DIR, "product_urls.json")
    if os.path.exists(urls_file):
        with open(urls_file) as f:
            urls = json.load(f)
        for url in urls:
            sku_match = re.search(r"-(\d{5,})\.html", url)
            if sku_match:
                sku = sku_match.group(1)
                if sku not in seen_skus:
                    queue.append({
                        "sku": sku,
                        "url": url,
                        "priority": 3,
                        "reason": "cdx_discovery",
                    })
                    seen_skus.add(sku)

    # Sort by priority
    queue.sort(key=lambda x: x["priority"])
    return queue


def main():
    save_db = "--db" in sys.argv
    batch_size = 30

    if "--batch-size" in sys.argv:
        idx = sys.argv.index("--batch-size")
        if idx + 1 < len(sys.argv):
            batch_size = int(sys.argv[idx + 1])

    print(f"ToolPulse Wayback Batch Backfill")
    print(f"  Batch size: {batch_size}")
    print()

    # Load progress
    progress = load_progress()
    completed_skus = set(progress.get("completed_skus", []))
    total_processed = progress.get("total_processed", 0)

    print(f"  Previously completed: {len(completed_skus)} SKUs")

    # Build priority queue
    queue = build_priority_queue()
    print(f"  Total in queue: {len(queue)}")

    # Filter out already-completed SKUs
    pending = [item for item in queue if item["sku"] not in completed_skus]
    print(f"  Remaining: {len(pending)}")

    if not pending:
        print("\nAll products have been backfilled!")
        return

    # Process this batch
    batch = pending[:batch_size]
    priority_counts = {}
    for item in batch:
        r = item["reason"]
        priority_counts[r] = priority_counts.get(r, 0) + 1

    print(f"\nProcessing batch of {len(batch)}:")
    for reason, count in priority_counts.items():
        print(f"  {reason}: {count}")

    db = None
    if save_db:
        try:
            from db import ToolPulseDB
            db = ToolPulseDB()
        except ImportError:
            print("  Warning: db.py not available")

    batch_prices = 0
    skipped_no_url = 0
    for i, item in enumerate(batch):
        sku = item["sku"]
        url = item["url"]

        # Resolve URL if missing (email deal items only have SKU)
        if not url:
            print(f"  [{i+1}/{len(batch)}] SKU {sku} — resolving URL...")
            url = resolve_url_for_sku(sku)
            if not url:
                print(f"    ✗ No URL found for SKU {sku}, skipping")
                completed_skus.add(sku)
                skipped_no_url += 1
                continue
            print(f"    → {url}")

        try:
            prices = backfill_product(url, max_snapshots=30)

            if prices and db:
                count = db.import_wayback_prices(sku, prices)
                batch_prices += count
                print(f"  → DB: {count} new snapshots")

            completed_skus.add(sku)
            total_processed += 1

        except Exception as e:
            print(f"  ✗ Error on SKU {sku}: {e}")
            completed_skus.add(sku)  # Skip on error, don't retry forever

        if (i + 1) % 10 == 0:
            print(f"\n  Batch progress: {i + 1}/{len(batch)}")
            # Save intermediate progress
            progress["completed_skus"] = list(completed_skus)
            progress["total_processed"] = total_processed
            progress["last_run"] = datetime.now(timezone.utc).isoformat()
            save_progress(progress)

        time.sleep(0.5)

    # Final progress save
    progress["completed_skus"] = list(completed_skus)
    progress["total_processed"] = total_processed
    progress["last_run"] = datetime.now(timezone.utc).isoformat()
    save_progress(progress)

    if db:
        stats = db.get_stats()
        db.close()
        print(f"\nDatabase: {stats['products']} products, {stats['price_snapshots']} snapshots")

    remaining = len(pending) - len(batch)
    runs_needed = (remaining + batch_size - 1) // batch_size if remaining > 0 else 0

    print(f"\n{'='*60}")
    print(f"Batch complete:")
    print(f"  Processed this run: {len(batch)}")
    if skipped_no_url:
        print(f"  Skipped (no URL found): {skipped_no_url}")
    print(f"  New price snapshots: {batch_prices}")
    print(f"  Total completed: {len(completed_skus)}")
    print(f"  Remaining: {remaining}")
    print(f"  Estimated runs left: {runs_needed}")
    if runs_needed > 0:
        days = runs_needed / 4  # 4 runs per day
        print(f"  Estimated days at 4x/day: {days:.0f}")


if __name__ == "__main__":
    main()
