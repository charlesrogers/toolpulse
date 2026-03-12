#!/usr/bin/env python3
"""
ToolPulse: Local batch backfill runner.

Runs the full product Wayback backfill locally with parallel workers.
Residential IP = no rate limiting. No time limits. 10+ parallel products.

Usage:
    python3 scrapers/local_batch_run.py                    # Backfill all pending products
    python3 scrapers/local_batch_run.py --workers 10       # Custom worker count
    python3 scrapers/local_batch_run.py --limit 100        # Only process first 100
    python3 scrapers/local_batch_run.py --go-hf            # Backfill go.hf URLs instead
    python3 scrapers/local_batch_run.py --go-hf --workers 8
    python3 scrapers/local_batch_run.py --live              # Scrape missing prices from HF
    python3 scrapers/local_batch_run.py --live --all        # Scrape ALL products (refresh)
    python3 scrapers/local_batch_run.py --live --workers 4  # Custom concurrency
"""

import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
sys.path.insert(0, BASE_DIR)
sys.path.insert(0, os.path.join(BASE_DIR, "scrapers"))


def run_product_backfill(workers: int = 10, limit: int = 0):
    """Backfill harborfreight.com product pages via Wayback Machine."""
    from wayback_batch import build_priority_queue, load_all_completed_skus, load_progress, save_progress
    from wayback_backfill import backfill_product
    from db import ToolPulseDB

    print("=" * 60)
    print("ToolPulse Local Product Backfill")
    print(f"  Workers: {workers}")
    print(f"  Limit: {limit or 'all'}")
    print("=" * 60)

    # Load completed SKUs
    print("\nLoading progress...")
    completed_skus = load_all_completed_skus()
    progress = load_progress(None)
    slice_completed = set(progress.get("completed_skus", []))

    # Build priority queue
    print("\nBuilding priority queue...")
    queue = build_priority_queue()
    print(f"  Total in queue: {len(queue)}")

    pending = [item for item in queue if item["sku"] not in completed_skus]
    print(f"  Pending: {len(pending)}")

    if not pending:
        print("\nAll products already backfilled!")
        return

    if limit:
        pending = pending[:limit]
        print(f"  Limited to: {len(pending)}")

    # Filter out items with no URL (resolve later would slow things down)
    with_url = [item for item in pending if item.get("url")]
    no_url = len(pending) - len(with_url)
    if no_url:
        print(f"  Skipping {no_url} items with no URL")
    pending = with_url

    db = ToolPulseDB()
    start_time = time.time()
    total_prices = 0
    processed = 0
    errors = 0

    # Inner parallelism: split budget between outer workers and inner snapshot threads
    # Total concurrency ≈ workers × inner_workers — keep under ~8 to avoid archive.org throttling
    inner_workers = max(1, 8 // workers)

    def process_one(item):
        """Process a single product — called by workers."""
        sku = item["sku"]
        url = item["url"]
        try:
            prices = backfill_product(url, max_snapshots=12, max_workers=inner_workers)
            return sku, prices, None
        except Exception as e:
            return sku, [], str(e)

    print(f"\nStarting backfill with {workers} workers on {len(pending)} products...")
    print()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_item = {}
        for item in pending:
            future = executor.submit(process_one, item)
            future_to_item[future] = item

        for future in as_completed(future_to_item):
            item = future_to_item[future]
            sku = item["sku"]
            try:
                sku, prices, error = future.result()
            except Exception as e:
                prices = []
                error = str(e)

            processed += 1

            if error:
                errors += 1
                print(f"  [{processed}/{len(pending)}] SKU {sku}: ERROR - {error}")
            elif prices:
                count = db.import_wayback_prices(sku, prices)
                total_prices += count
                print(f"  [{processed}/{len(pending)}] SKU {sku}: {count} new snapshots")
            else:
                print(f"  [{processed}/{len(pending)}] SKU {sku}: no data")

            completed_skus.add(sku)
            slice_completed.add(sku)

            # Save progress every 50 products
            if processed % 50 == 0:
                elapsed = time.time() - start_time
                rate = processed / elapsed * 3600 if elapsed > 0 else 0
                progress["completed_skus"] = list(slice_completed)
                progress["total_processed"] = len(slice_completed)
                progress["last_run"] = datetime.now(timezone.utc).isoformat()
                save_progress(progress, None)
                print(f"\n  --- Progress: {processed}/{len(pending)} | "
                      f"{total_prices} prices | {rate:.0f}/hr | "
                      f"{elapsed/60:.1f}min elapsed ---\n")

    # Final save
    progress["completed_skus"] = list(slice_completed)
    progress["total_processed"] = len(slice_completed)
    progress["last_run"] = datetime.now(timezone.utc).isoformat()
    save_progress(progress, None)

    elapsed = time.time() - start_time
    stats = db.get_stats()
    db.close()

    print(f"\n{'=' * 60}")
    print(f"Local backfill complete!")
    print(f"  Products processed: {processed}")
    print(f"  New price snapshots: {total_prices}")
    print(f"  Errors: {errors}")
    print(f"  Time: {elapsed/60:.1f} minutes")
    print(f"  Rate: {processed/elapsed*3600:.0f} products/hour")
    print(f"  DB totals: {stats['products']} products, {stats['price_snapshots']} snapshots")


def run_go_hf_backfill(workers: int = 8, limit: int = 0):
    """Backfill go.harborfreight.com deal pages via Wayback Machine."""
    from wayback_go_hf import (
        backfill_go_hf_url, load_all_completed_urls, load_progress, save_progress
    )
    from db import ToolPulseDB

    print("=" * 60)
    print("ToolPulse Local go.hf Backfill")
    print(f"  Workers: {workers}")
    print(f"  Limit: {limit or 'all'}")
    print("=" * 60)

    # Load URL list
    url_cache_file = os.path.join(DATA_DIR, "go_hf_wayback_urls.json")
    if not os.path.exists(url_cache_file):
        print(f"\nERROR: {url_cache_file} not found.")
        print("Run the CDX discovery first or ensure the file exists.")
        return

    with open(url_cache_file) as f:
        all_url_entries = json.load(f)
    print(f"\n  Total URLs in list: {len(all_url_entries)}")

    # Skip email URLs
    pre_filter = len(all_url_entries)
    all_url_entries = [e for e in all_url_entries if e.get("type") != "email"]
    skipped_emails = pre_filter - len(all_url_entries)
    if skipped_emails:
        print(f"  Skipped {skipped_emails} email URLs")

    # Load completed
    print("\nLoading progress...")
    completed_urls = load_all_completed_urls()
    progress = load_progress(None)
    slice_completed = set(progress.get("completed_urls", []))
    total_deals_found = progress.get("total_deals_found", 0)

    pending = [e for e in all_url_entries if e["url"] not in completed_urls]
    print(f"  Pending: {len(pending)}")

    if not pending:
        print("\nAll URLs already backfilled!")
        return

    if limit:
        pending = pending[:limit]
        print(f"  Limited to: {len(pending)}")

    db = ToolPulseDB()
    start_time = time.time()
    processed = 0
    batch_deals = 0
    batch_inserted = 0
    errors = 0

    inner_workers = max(1, 8 // workers)

    def process_one(entry):
        """Process a single go.hf URL — called by workers."""
        url = entry["url"]
        try:
            deals = backfill_go_hf_url(url, max_workers=inner_workers)
            return url, deals, None
        except Exception as e:
            return url, [], str(e)

    print(f"\nStarting go.hf backfill with {workers} workers on {len(pending)} URLs...")
    print()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_entry = {}
        for entry in pending:
            future = executor.submit(process_one, entry)
            future_to_entry[future] = entry

        for future in as_completed(future_to_entry):
            entry = future_to_entry[future]
            url = entry["url"]
            try:
                url, deals, error = future.result()
            except Exception as e:
                deals = []
                error = str(e)

            processed += 1

            if error:
                errors += 1
                print(f"  [{processed}/{len(pending)}] ERROR - {error[:80]}")
            elif deals:
                batch_deals += len(deals)
                total_deals_found += len(deals)
                ins, upd = db.upsert_deals(deals)
                batch_inserted += ins
                print(f"  [{processed}/{len(pending)}] {url}: {len(deals)} deals ({ins} new)")
            else:
                print(f"  [{processed}/{len(pending)}] {url}: no deals")

            completed_urls.add(url)
            slice_completed.add(url)

            # Save progress every 50 URLs
            if processed % 50 == 0:
                elapsed = time.time() - start_time
                rate = processed / elapsed * 3600 if elapsed > 0 else 0
                progress["completed_urls"] = list(slice_completed)
                progress["total_deals_found"] = total_deals_found
                progress["last_run"] = datetime.now(timezone.utc).isoformat()
                save_progress(progress, None)
                print(f"\n  --- Progress: {processed}/{len(pending)} | "
                      f"{batch_deals} deals | {rate:.0f}/hr | "
                      f"{elapsed/60:.1f}min elapsed ---\n")

    # Final save
    progress["completed_urls"] = list(slice_completed)
    progress["total_deals_found"] = total_deals_found
    progress["last_run"] = datetime.now(timezone.utc).isoformat()
    save_progress(progress, None)

    elapsed = time.time() - start_time
    stats = db.get_stats()
    db.close()

    print(f"\n{'=' * 60}")
    print(f"Local go.hf backfill complete!")
    print(f"  URLs processed: {processed}")
    print(f"  Deals found: {batch_deals}")
    print(f"  New DB inserts: {batch_inserted}")
    print(f"  Errors: {errors}")
    print(f"  Time: {elapsed/60:.1f} minutes")
    print(f"  Rate: {processed/elapsed*3600:.0f} URLs/hour")
    print(f"  DB totals: {stats['products']} products, {stats['price_snapshots']} snapshots, {stats['deals']} deals")


def main():
    workers = 10
    limit = 0
    go_hf = "--go-hf" in sys.argv
    live = "--live" in sys.argv
    scrape_all = "--all" in sys.argv

    if "--workers" in sys.argv:
        idx = sys.argv.index("--workers")
        if idx + 1 < len(sys.argv):
            workers = int(sys.argv[idx + 1])

    if "--limit" in sys.argv:
        idx = sys.argv.index("--limit")
        if idx + 1 < len(sys.argv):
            limit = int(sys.argv[idx + 1])

    if live:
        from live_hf_scraper import run_live_scrape
        run_live_scrape(limit=limit, scrape_all=scrape_all)
    elif go_hf:
        run_go_hf_backfill(workers=workers, limit=limit)
    else:
        run_product_backfill(workers=workers, limit=limit)


if __name__ == "__main__":
    main()
