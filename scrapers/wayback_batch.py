#!/usr/bin/env python3
"""
ToolPulse: Wayback Machine batch backfill runner.

Designed to run in GitHub Actions. Processes a batch of products per invocation,
tracking progress across runs via a JSON state file.

Supports parallel slicing: multiple jobs can run simultaneously, each processing
a different slice of the pending queue (--slice N --total-slices M).

Priority order:
  1. Products with active deals (most valuable — we can compare deal vs regular price)
  2. Products from the live sitemap (current catalog)
  3. All remaining products from CDX discovery (historical/discontinued)

Usage:
    python3 wayback_batch.py --db --batch-size 15
    python3 wayback_batch.py --db --batch-size 15 --slice 0 --total-slices 3
"""

import glob as glob_module
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

CDX_API = "https://web.archive.org/cdx/search/cdx"
HEADERS = {"User-Agent": "ToolPulse/1.0 (historical price research)"}

CDX_CACHE_FILE = os.path.join(DATA_DIR, "cdx_snapshot_cache.json")

_url_cache = None  # {sku: url} — loaded once from JSON files


def _load_url_cache() -> dict[str, str]:
    """Load all known SKU->URL mappings from local JSON files."""
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


# ── Slice-aware progress tracking ───────────────────────────────────────────

def progress_file_for_slice(slice_idx: int | None) -> str:
    """Return the progress file path for a given slice."""
    if slice_idx is not None:
        return os.path.join(DATA_DIR, f"backfill_progress_{slice_idx}.json")
    return os.path.join(DATA_DIR, "backfill_progress.json")


def load_all_completed_skus() -> set[str]:
    """Load completed SKUs from ALL slice progress files + legacy file.

    This ensures no slice redoes work that another slice already completed.
    """
    completed = set()

    # Legacy progress file (from before slicing)
    legacy_file = os.path.join(DATA_DIR, "backfill_progress.json")
    if os.path.exists(legacy_file):
        try:
            with open(legacy_file) as f:
                data = json.load(f)
            skus = data.get("completed_skus", [])
            completed.update(skus)
            print(f"  Loaded legacy progress: {len(skus)} SKUs")
        except Exception as e:
            print(f"  Warning: could not read legacy progress: {e}")

    # All slice progress files
    pattern = os.path.join(DATA_DIR, "backfill_progress_*.json")
    for path in sorted(glob_module.glob(pattern)):
        try:
            with open(path) as f:
                data = json.load(f)
            skus = data.get("completed_skus", [])
            completed.update(skus)
            basename = os.path.basename(path)
            print(f"  Loaded {basename}: {len(skus)} SKUs")
        except Exception as e:
            print(f"  Warning: could not read {path}: {e}")

    print(f"  Total completed across all slices: {len(completed)} SKUs")
    return completed


def load_progress(slice_idx: int | None) -> dict:
    """Load progress for a specific slice."""
    path = progress_file_for_slice(slice_idx)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"completed_skus": [], "current_index": 0, "total_processed": 0}


def save_progress(progress: dict, slice_idx: int | None):
    """Save progress for a specific slice."""
    path = progress_file_for_slice(slice_idx)
    with open(path, "w") as f:
        json.dump(progress, f, indent=2)
    print(f"  Progress saved to {os.path.basename(path)}: {len(progress.get('completed_skus', []))} SKUs")


# ── CDX bulk pre-fetch ──────────────────────────────────────────────────────

def load_cdx_cache() -> dict:
    """Load CDX snapshot cache from disk."""
    if os.path.exists(CDX_CACHE_FILE):
        try:
            with open(CDX_CACHE_FILE) as f:
                cache = json.load(f)
            print(f"  CDX cache loaded: {len(cache)} SKUs cached")
            return cache
        except Exception as e:
            print(f"  Warning: could not load CDX cache: {e}")
    return {}


def save_cdx_cache(cache: dict):
    """Save CDX snapshot cache to disk."""
    with open(CDX_CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)
    print(f"  CDX cache saved: {len(cache)} SKUs")


def prefetch_cdx_snapshots(skus: list[str], cdx_cache: dict) -> dict:
    """Pre-fetch CDX snapshot availability for a batch of SKUs.

    Queries CDX with a broad prefix to get snapshot counts for multiple SKUs
    in fewer API calls. Results are merged into cdx_cache.

    Returns the updated cache.
    """
    uncached = [sku for sku in skus if sku not in cdx_cache]
    if not uncached:
        print(f"  CDX pre-fetch: all {len(skus)} SKUs already cached")
        return cdx_cache

    print(f"  CDX pre-fetch: {len(uncached)} uncached SKUs (of {len(skus)} total)")

    # Group SKUs by their first 3 digits to batch CDX queries
    prefix_groups: dict[str, list[str]] = {}
    for sku in uncached:
        prefix = sku[:3] if len(sku) >= 3 else sku
        prefix_groups.setdefault(prefix, []).append(sku)

    print(f"  CDX pre-fetch: {len(prefix_groups)} prefix groups to query")

    fetched = 0
    for i, (prefix, group_skus) in enumerate(sorted(prefix_groups.items())):
        if i > 0 and i % 10 == 0:
            print(f"    CDX pre-fetch progress: {i}/{len(prefix_groups)} prefixes, {fetched} SKUs found")

        try:
            params = {
                "url": f"www.harborfreight.com/*-{prefix}*.html",
                "matchType": "prefix",
                "output": "text",
                "fl": "original,timestamp",
                "collapse": "urlkey",
                "filter": "statuscode:200",
                "limit": "500",
            }
            resp = requests.get(CDX_API, params=params, headers=HEADERS, timeout=30)
            if resp.ok and resp.text.strip():
                for line in resp.text.strip().split("\n"):
                    parts = line.split()
                    if len(parts) >= 2:
                        url = parts[0]
                        m = re.search(r"-(\d{5,})\.html", url)
                        if m:
                            found_sku = m.group(1)
                            if found_sku in uncached or found_sku not in cdx_cache:
                                cdx_cache[found_sku] = {
                                    "url": url,
                                    "has_snapshots": True,
                                    "checked_at": datetime.now(timezone.utc).isoformat(),
                                }
                                fetched += 1
            time.sleep(0.3)  # Rate limit CDX queries
        except Exception as e:
            print(f"    CDX pre-fetch error for prefix {prefix}: {e}")

    # Mark SKUs with no results as checked (so we don't re-query)
    for sku in uncached:
        if sku not in cdx_cache:
            cdx_cache[sku] = {
                "url": None,
                "has_snapshots": False,
                "checked_at": datetime.now(timezone.utc).isoformat(),
            }

    print(f"  CDX pre-fetch complete: {fetched} SKUs found with snapshots")
    save_cdx_cache(cdx_cache)
    return cdx_cache


# ── Priority queue ──────────────────────────────────────────────────────────

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
    batch_size = 15

    if "--batch-size" in sys.argv:
        idx = sys.argv.index("--batch-size")
        if idx + 1 < len(sys.argv):
            batch_size = int(sys.argv[idx + 1])

    # Slice parameters for parallel execution
    slice_idx = None
    total_slices = 1

    if "--slice" in sys.argv:
        idx = sys.argv.index("--slice")
        if idx + 1 < len(sys.argv):
            slice_idx = int(sys.argv[idx + 1])

    if "--total-slices" in sys.argv:
        idx = sys.argv.index("--total-slices")
        if idx + 1 < len(sys.argv):
            total_slices = int(sys.argv[idx + 1])

    print(f"ToolPulse Wayback Batch Backfill")
    print(f"  Batch size: {batch_size}")
    if slice_idx is not None:
        print(f"  Slice: {slice_idx} of {total_slices}")
    print()

    # Load completed SKUs from ALL slices (so we don't redo work)
    print("Loading completed SKUs from all slices...")
    completed_skus = load_all_completed_skus()

    # Load this slice's own progress for its metadata
    progress = load_progress(slice_idx)
    total_processed = progress.get("total_processed", 0)

    # Build priority queue
    print("\nBuilding priority queue...")
    queue = build_priority_queue()
    print(f"  Total in queue: {len(queue)}")

    # Filter out already-completed SKUs
    pending = [item for item in queue if item["sku"] not in completed_skus]
    print(f"  Remaining (all slices): {len(pending)}")

    if not pending:
        print("\nAll products have been backfilled!")
        return

    # Apply slicing: each slice only processes items where index % total_slices == slice_idx
    if slice_idx is not None and total_slices > 1:
        sliced_pending = [
            item for i, item in enumerate(pending)
            if i % total_slices == slice_idx
        ]
        print(f"  This slice's share: {len(sliced_pending)} items (slice {slice_idx}/{total_slices})")
        pending = sliced_pending

    if not pending:
        print(f"\nNo items for slice {slice_idx}!")
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

    # CDX bulk pre-fetch for this batch
    print("\nPre-fetching CDX snapshot data...")
    cdx_cache = load_cdx_cache()
    batch_skus = [item["sku"] for item in batch]
    cdx_cache = prefetch_cdx_snapshots(batch_skus, cdx_cache)

    db = None
    if save_db:
        try:
            from db import ToolPulseDB
            db = ToolPulseDB()
        except ImportError:
            print("  Warning: db.py not available")

    batch_prices = 0
    skipped_no_url = 0
    # Track this slice's newly completed SKUs separately
    slice_completed = set(progress.get("completed_skus", []))

    for i, item in enumerate(batch):
        sku = item["sku"]
        url = item["url"]

        # Resolve URL if missing (email deal items only have SKU)
        if not url:
            # Check CDX cache first
            if sku in cdx_cache and cdx_cache[sku].get("url"):
                url = cdx_cache[sku]["url"]
                print(f"  [{i+1}/{len(batch)}] SKU {sku} — URL from CDX cache")
            else:
                print(f"  [{i+1}/{len(batch)}] SKU {sku} — resolving URL...")
                url = resolve_url_for_sku(sku)
            if not url:
                print(f"    No URL found for SKU {sku}, skipping")
                completed_skus.add(sku)
                slice_completed.add(sku)
                skipped_no_url += 1
                continue
            print(f"    -> {url}")

        try:
            prices = backfill_product(url, max_snapshots=30)

            if prices and db:
                count = db.import_wayback_prices(sku, prices)
                batch_prices += count
                print(f"  -> DB: {count} new snapshots")

            completed_skus.add(sku)
            slice_completed.add(sku)
            total_processed += 1

        except Exception as e:
            print(f"  Error on SKU {sku}: {e}")
            completed_skus.add(sku)  # Skip on error, don't retry forever
            slice_completed.add(sku)

        if (i + 1) % 5 == 0:
            print(f"\n  Batch progress: {i + 1}/{len(batch)}")
            # Save intermediate progress
            progress["completed_skus"] = list(slice_completed)
            progress["total_processed"] = total_processed
            progress["last_run"] = datetime.now(timezone.utc).isoformat()
            save_progress(progress, slice_idx)

        time.sleep(0.5)

    # Final progress save
    progress["completed_skus"] = list(slice_completed)
    progress["total_processed"] = total_processed
    progress["last_run"] = datetime.now(timezone.utc).isoformat()
    save_progress(progress, slice_idx)

    if db:
        stats = db.get_stats()
        db.close()
        print(f"\nDatabase: {stats['products']} products, {stats['price_snapshots']} snapshots")

    remaining = len(pending) - len(batch)
    runs_needed = (remaining + batch_size - 1) // batch_size if remaining > 0 else 0

    print(f"\n{'='*60}")
    print(f"Batch complete (slice {slice_idx if slice_idx is not None else 'N/A'}):")
    print(f"  Processed this run: {len(batch)}")
    if skipped_no_url:
        print(f"  Skipped (no URL found): {skipped_no_url}")
    print(f"  New price snapshots: {batch_prices}")
    print(f"  Total completed (this slice): {len(slice_completed)}")
    print(f"  Total completed (all slices): {len(completed_skus)}")
    print(f"  Remaining (this slice): {remaining}")
    print(f"  Estimated runs left (this slice): {runs_needed}")
    if runs_needed > 0:
        hours = runs_needed  # 1 run per hour
        print(f"  Estimated hours at hourly runs: {hours}")


if __name__ == "__main__":
    main()
