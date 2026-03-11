#!/usr/bin/env python3
"""
ToolPulse: Wayback Machine backfill for go.harborfreight.com deal/coupon pages.

Uses the Internet Archive CDX API to find archived go.harborfreight.com pages
(grid listings, individual coupons, email promos), then extracts historical
deal/coupon data using the same ALT_PATTERN regex as go_hf_scraper.py.

Supports parallel slicing: multiple jobs can run simultaneously, each processing
a different slice of the pending queue (--slice N --total-slices M).

Usage:
    python3 wayback_go_hf.py                          # Dry run, JSON only
    python3 wayback_go_hf.py --db                     # Also save deals to SQLite
    python3 wayback_go_hf.py --db --batch-size 15 --slice 0 --total-slices 2
    python3 wayback_go_hf.py --url "https://..."      # Single URL mode
"""

import glob as glob_module
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

sys.path.insert(0, BASE_DIR)

HEADERS = {
    "User-Agent": "ToolPulse/1.0 (historical price research)",
}

CDX_API = "https://web.archive.org/cdx/search/cdx"
WAYBACK_BASE = "https://web.archive.org/web"

MAX_RETRIES = 2
RETRY_BACKOFF = 3  # seconds, multiplied by attempt number

# Optional: route archive.org requests through a Cloudflare Worker proxy
WAYBACK_PROXY_URL = os.environ.get("WAYBACK_PROXY_URL", "")

# Same regex as go_hf_scraper.py — matches deal alt text on go.harborfreight.com
ALT_PATTERN = re.compile(
    r"Buy the (.+?)\s*\(Item\s*([\d/]+)\)\s*for \$([0-9,.]+)"
    r"(?:,?\s*valid through\s*(\d{1,2}/\d{1,2}/\d{4}))?",
    re.IGNORECASE,
)


# ── HTTP with retry ──────────────────────────────────────────────────────────

def _maybe_proxy_url(url: str) -> str:
    """Route archive.org requests through CF Worker proxy if configured."""
    if WAYBACK_PROXY_URL and url.startswith("https://web.archive.org/"):
        from urllib.parse import quote
        return f"{WAYBACK_PROXY_URL}?url={quote(url, safe='')}"
    return url


def fetch_with_retry(url: str, timeout: int = 15) -> requests.Response | None:
    """Fetch URL with fast retry on connection errors."""
    actual_url = _maybe_proxy_url(url)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(actual_url, headers=HEADERS, timeout=timeout)
            if resp.status_code == 429:  # Rate limited
                wait = RETRY_BACKOFF * attempt
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            return resp
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * attempt
                print(f"    Connection error, retry {attempt}/{MAX_RETRIES} in {wait}s...")
                time.sleep(wait)
            else:
                print(f"    Skipping: {type(e).__name__}")
                return None
    return None


# ── Slice-aware progress tracking ────────────────────────────────────────────

def progress_file_for_slice(slice_idx: int | None) -> str:
    """Return the progress file path for a given slice."""
    if slice_idx is not None:
        return os.path.join(DATA_DIR, f"go_hf_backfill_progress_{slice_idx}.json")
    return os.path.join(DATA_DIR, "go_hf_backfill_progress.json")


def load_all_completed_urls() -> set[str]:
    """Load completed URLs from ALL slice progress files + legacy file.

    This ensures no slice redoes work that another slice already completed.
    """
    completed = set()

    # Legacy progress file (from before slicing)
    legacy_file = os.path.join(DATA_DIR, "go_hf_backfill_progress.json")
    if os.path.exists(legacy_file):
        try:
            with open(legacy_file) as f:
                data = json.load(f)
            urls = data.get("completed_urls", [])
            completed.update(urls)
            print(f"  Loaded legacy progress: {len(urls)} URLs")
        except Exception as e:
            print(f"  Warning: could not read legacy progress: {e}")

    # All slice progress files
    pattern = os.path.join(DATA_DIR, "go_hf_backfill_progress_*.json")
    for path in sorted(glob_module.glob(pattern)):
        try:
            with open(path) as f:
                data = json.load(f)
            urls = data.get("completed_urls", [])
            completed.update(urls)
            basename = os.path.basename(path)
            print(f"  Loaded {basename}: {len(urls)} URLs")
        except Exception as e:
            print(f"  Warning: could not read {path}: {e}")

    print(f"  Total completed across all slices: {len(completed)} URLs")
    return completed


def load_progress(slice_idx: int | None) -> dict:
    """Load backfill progress for a specific slice."""
    path = progress_file_for_slice(slice_idx)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {
        "completed_urls": [],
        "total_deals_found": 0,
        "total_snapshots_processed": 0,
        "last_run": None,
    }


def save_progress(progress: dict, slice_idx: int | None):
    """Save backfill progress for a specific slice."""
    path = progress_file_for_slice(slice_idx)
    with open(path, "w") as f:
        json.dump(progress, f, indent=2)
    print(f"  Progress saved to {os.path.basename(path)}: {len(progress['completed_urls'])} URLs completed")


# ── CDX API: Discover archived go.harborfreight.com pages ────────────────────

def discover_go_hf_urls() -> list[dict]:
    """Query CDX API to find all archived go.harborfreight.com deal pages.

    Searches three URL patterns:
      1. Grid pages: go.harborfreight.com/cpi/digital/* (60+ deals per page)
      2. Individual coupons: go.harborfreight.com/coupons/*
      3. Email promos: go.harborfreight.com/email*

    Returns list of {url, type, snapshot_count} dicts, deduplicated by URL.
    """
    prefixes = [
        ("go.harborfreight.com/cpi/digital/*", "grid"),
        ("go.harborfreight.com/coupons/*", "coupon"),
        ("go.harborfreight.com/email*", "email"),
    ]

    all_urls = {}  # url -> {type, snapshots}

    for prefix, page_type in prefixes:
        print(f"\n  Querying CDX for {page_type} pages: {prefix}")

        params = {
            "url": prefix,
            "matchType": "prefix",
            "output": "json",
            "fl": "original,timestamp,statuscode",
            "filter": "statuscode:200",
            "collapse": "urlkey",  # One entry per unique URL
            "limit": 5000,
        }

        query_url = f"{CDX_API}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
        resp = fetch_with_retry(query_url, timeout=60)

        if not resp or resp.status_code != 200:
            print(f"    CDX query failed (status={resp.status_code if resp else 'None'})")
            continue

        try:
            data = resp.json()
        except ValueError:
            print(f"    CDX returned non-JSON response")
            continue

        if len(data) <= 1:
            print(f"    No results")
            continue

        headers_row = data[0]
        rows = [dict(zip(headers_row, row)) for row in data[1:]]

        new_count = 0
        for row in rows:
            url = row["original"]
            # Normalize URL: strip trailing slash, ensure https
            url = url.rstrip("/")
            if url.startswith("http://"):
                url = "https://" + url[7:]

            if url not in all_urls:
                all_urls[url] = {"url": url, "type": page_type, "snapshot_count": 1}
                new_count += 1
            else:
                all_urls[url]["snapshot_count"] += 1

        print(f"    Found {new_count} unique URLs ({len(rows)} CDX rows)")
        time.sleep(1)  # Be nice to CDX API between queries

    result = sorted(all_urls.values(), key=lambda x: (
        0 if x["type"] == "grid" else 1 if x["type"] == "email" else 2
    ))

    type_counts = {}
    for item in result:
        t = item["type"]
        type_counts[t] = type_counts.get(t, 0) + 1

    print(f"\n  Total unique URLs discovered: {len(result)}")
    for t, c in sorted(type_counts.items()):
        print(f"    {t}: {c}")

    return result


# ── CDX API: Find snapshots for a specific URL ──────────────────────────────

def find_snapshots_for_url(url: str, limit: int = 50) -> list[dict]:
    """Query CDX API for archived snapshots of a specific go.hf URL.

    Collapses to one snapshot per month (timestamp:6) to get a good spread
    without fetching hundreds of near-duplicate pages.
    """
    params = {
        "url": url,
        "output": "json",
        "fl": "timestamp,original,statuscode",
        "filter": "statuscode:200",
        "limit": limit,
        "collapse": "timestamp:6",  # One per month
    }

    query_url = f"{CDX_API}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
    resp = fetch_with_retry(query_url)

    if not resp or resp.status_code != 200:
        return []

    try:
        data = resp.json()
    except ValueError:
        return []

    if len(data) <= 1:
        return []

    headers_row = data[0]
    return [dict(zip(headers_row, row)) for row in data[1:]]


# ── Parse deals from an archived go.hf page ─────────────────────────────────

def extract_deals_from_snapshot(timestamp: str, original_url: str) -> list[dict]:
    """Fetch an archived go.harborfreight.com page and extract deal data.

    Uses the id_ flag for raw HTML (no Wayback toolbar injection).
    Parses deals from img alt text using the ALT_PATTERN regex.
    """
    wayback_url = f"{WAYBACK_BASE}/{timestamp}id_/{original_url}"
    date_str = f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]}"

    resp = fetch_with_retry(wayback_url)
    if not resp or resp.status_code != 200:
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    deals = []

    # Method 1: Parse img alt text (primary method — works on grid + coupon pages)
    for img in soup.find_all("img", alt=ALT_PATTERN):
        alt = img.get("alt", "")
        m = ALT_PATTERN.search(alt)
        if not m:
            continue

        product_name = m.group(1).strip()
        item_numbers_raw = m.group(2).strip()
        price = float(m.group(3).replace(",", ""))
        valid_through = m.group(4).strip() if m.group(4) else None

        # Handle multiple item numbers: "63496/63499"
        item_numbers = [n.strip() for n in item_numbers_raw.split("/")]
        primary_item = item_numbers[0]
        alt_items = item_numbers[1:] if len(item_numbers) > 1 else []

        # Extract coupon/promo info from parent <a> link
        coupon_url = None
        promo_id = None
        parent_a = img.find_parent("a")
        if parent_a and parent_a.get("href"):
            coupon_url = parent_a["href"]
            url_match = re.search(r"/(\d{6,})-(\d+)/?$", coupon_url)
            if url_match:
                promo_id = url_match.group(1)

        deals.append({
            "product_name": product_name,
            "item_number": primary_item,
            "alt_item_numbers": alt_items,
            "price": price,
            "valid_through": valid_through,
            "coupon_code": None,
            "promo_id": promo_id,
            "coupon_url": coupon_url,
            "source": "wayback_go_hf",
            "source_url": original_url,
            "wayback_url": wayback_url,
            "snapshot_date": date_str,
            "snapshot_timestamp": timestamp,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })

    # Method 2: Parse body text for deals not captured by img alt
    # (some pages have deal info in entry-content text instead of images)
    entry = soup.find("div", class_="entry-content")
    if entry:
        text = entry.get_text(" ", strip=True)
        # Find all matches in the text that weren't already found via img alt
        existing_items = {d["item_number"] for d in deals}
        for m in ALT_PATTERN.finditer(text):
            primary_item = m.group(2).strip().split("/")[0].strip()
            if primary_item not in existing_items:
                item_numbers_raw = m.group(2).strip()
                item_numbers = [n.strip() for n in item_numbers_raw.split("/")]
                alt_items = item_numbers[1:] if len(item_numbers) > 1 else []

                deals.append({
                    "product_name": m.group(1).strip(),
                    "item_number": primary_item,
                    "alt_item_numbers": alt_items,
                    "price": float(m.group(3).replace(",", "")),
                    "valid_through": m.group(4).strip() if m.group(4) else None,
                    "coupon_code": None,
                    "promo_id": None,
                    "coupon_url": None,
                    "source": "wayback_go_hf",
                    "source_url": original_url,
                    "wayback_url": wayback_url,
                    "snapshot_date": date_str,
                    "snapshot_timestamp": timestamp,
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                })
                existing_items.add(primary_item)

        # Also try to extract coupon codes from text
        code_match = re.search(r"coupon code\s*(\d{6,})", text, re.IGNORECASE)
        if code_match and deals:
            code = code_match.group(1)
            for deal in deals:
                if not deal.get("coupon_code"):
                    deal["coupon_code"] = code

    return deals


# ── Process a single go.hf URL across its snapshots ─────────────────────────

def backfill_go_hf_url(url: str, max_snapshots: int = 20) -> list[dict]:
    """Backfill deal history for a single go.harborfreight.com URL.

    Fetches a sample of snapshots (one per month) and extracts deals from each.
    Returns all unique deals found across all snapshots.
    """
    print(f"\n{'~'*60}")
    print(f"URL: {url}")

    snapshots = find_snapshots_for_url(url, limit=max_snapshots)
    print(f"  Found {len(snapshots)} monthly snapshots")

    if not snapshots:
        return []

    all_deals = []
    seen_deals = set()  # Deduplicate by (item_number, price, snapshot_date)

    for i, snap in enumerate(snapshots):
        ts = snap["timestamp"]
        date_str = f"{ts[:4]}-{ts[4:6]}-{ts[6:8]}"

        deals = extract_deals_from_snapshot(ts, snap["original"])

        new_count = 0
        for deal in deals:
            key = (deal["item_number"], deal["price"], deal["snapshot_date"])
            if key not in seen_deals:
                seen_deals.add(key)
                all_deals.append(deal)
                new_count += 1

        print(f"  [{i+1}/{len(snapshots)}] {date_str}: {len(deals)} deals extracted ({new_count} new)")

        time.sleep(2)  # Be nice to archive.org

    print(f"  Total unique deals from this URL: {len(all_deals)}")
    return all_deals


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    save_db = "--db" in sys.argv

    batch_size = 30
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

    print("ToolPulse: Wayback Machine go.harborfreight.com Backfill")
    print(f"  Save to DB: {save_db}")
    print(f"  Batch size: {batch_size}")
    if slice_idx is not None:
        print(f"  Slice: {slice_idx} of {total_slices}")
    print()

    # ── Single URL mode ──────────────────────────────────────────────────────
    if "--url" in sys.argv:
        idx = sys.argv.index("--url")
        if idx + 1 < len(sys.argv):
            url = sys.argv[idx + 1]
            deals = backfill_go_hf_url(url)

            if deals:
                outfile = os.path.join(
                    DATA_DIR,
                    f"wayback_go_hf_single_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json",
                )
                with open(outfile, "w") as f:
                    json.dump(deals, f, indent=2)
                print(f"\nSaved {len(deals)} deals to {outfile}")

                if save_db:
                    from db import ToolPulseDB
                    db = ToolPulseDB()
                    inserted, updated = db.upsert_deals(deals)
                    print(f"Database: {inserted} new deals inserted, {updated} updated")
                    db.close()
            else:
                print("\nNo deals found.")
            return

    # ── Batch mode ───────────────────────────────────────────────────────────

    # Load completed URLs from ALL slices (so we don't redo work)
    print("Loading completed URLs from all slices...")
    completed_urls = load_all_completed_urls()

    # Load this slice's own progress for its metadata
    progress = load_progress(slice_idx)
    total_deals_found = progress.get("total_deals_found", 0)
    total_snapshots = progress.get("total_snapshots_processed", 0)

    print(f"  Previously found (this slice): {total_deals_found} deals")

    # Step 1: Load pre-discovered URLs or discover via CDX
    url_cache_file = os.path.join(DATA_DIR, "go_hf_wayback_urls.json")
    if os.path.exists(url_cache_file):
        print("\nStep 1: Loading pre-discovered go.hf URLs...")
        with open(url_cache_file) as f:
            all_url_entries = json.load(f)
        # Ensure entries have the right format
        for entry in all_url_entries:
            if "snapshot_count" not in entry:
                entry["snapshot_count"] = 1
        type_summary = {}
        for e in all_url_entries:
            t = e.get("type", "unknown")
            type_summary[t] = type_summary.get(t, 0) + 1
        print(f"  Loaded {len(all_url_entries)} URLs from cache")
        for t, c in sorted(type_summary.items()):
            print(f"    {t}: {c}")
    else:
        print("\nStep 1: Discovering archived go.harborfreight.com URLs via CDX...")
        all_url_entries = discover_go_hf_urls()

    if not all_url_entries:
        print("No archived URLs found. Nothing to do.")
        print("  Tip: Run locally first to generate go_hf_wayback_urls.json, then upload as artifact")
        return

    # Filter out completed URLs
    pending = [entry for entry in all_url_entries if entry["url"] not in completed_urls]
    print(f"\n  Pending URLs (all slices): {len(pending)} (of {len(all_url_entries)} total)")

    if not pending:
        print("\nAll URLs have been backfilled!")
        return

    # Apply slicing: each slice only processes items where index % total_slices == slice_idx
    if slice_idx is not None and total_slices > 1:
        sliced_pending = [
            entry for i, entry in enumerate(pending)
            if i % total_slices == slice_idx
        ]
        print(f"  This slice's share: {len(sliced_pending)} URLs (slice {slice_idx}/{total_slices})")
        pending = sliced_pending

    if not pending:
        print(f"\nNo URLs for slice {slice_idx}!")
        return

    # Take this batch
    batch = pending[:batch_size]

    type_counts = {}
    for entry in batch:
        t = entry["type"]
        type_counts[t] = type_counts.get(t, 0) + 1

    print(f"\nStep 2: Processing batch of {len(batch)} URLs:")
    for t, c in sorted(type_counts.items()):
        print(f"  {t}: {c}")

    # Open DB if needed
    db = None
    if save_db:
        try:
            from db import ToolPulseDB
            db = ToolPulseDB()
            print("  Database connected")
        except ImportError:
            print("  Warning: db.py not available, skipping DB saves")

    # Step 3: Process each URL
    batch_deals_total = 0
    batch_inserted = 0
    batch_updated = 0
    # Track this slice's newly completed URLs separately
    slice_completed = set(progress.get("completed_urls", []))

    start_time = time.time()
    MAX_RUN_SECONDS = 25 * 60  # Exit cleanly before 30-min job timeout

    for i, entry in enumerate(batch):
        elapsed = time.time() - start_time
        if elapsed > MAX_RUN_SECONDS:
            print(f"\n  Time limit reached ({elapsed/60:.1f}min), stopping to save progress")
            break

        url = entry["url"]
        page_type = entry["type"]

        print(f"\n  [{i+1}/{len(batch)}] ({page_type}) {url}")

        try:
            deals = backfill_go_hf_url(url)

            if deals:
                batch_deals_total += len(deals)
                total_deals_found += len(deals)

                # Save to DB
                if db:
                    ins, upd = db.upsert_deals(deals)
                    batch_inserted += ins
                    batch_updated += upd
                    print(f"  -> DB: {ins} inserted, {upd} updated")

            completed_urls.add(url)
            slice_completed.add(url)
            total_snapshots += 1

        except Exception as e:
            print(f"  ERROR processing {url}: {e}")
            completed_urls.add(url)  # Don't retry failed URLs forever
            slice_completed.add(url)

        # Save progress every 5 URLs
        if (i + 1) % 5 == 0:
            progress["completed_urls"] = list(slice_completed)
            progress["total_deals_found"] = total_deals_found
            progress["total_snapshots_processed"] = total_snapshots
            progress["last_run"] = datetime.now(timezone.utc).isoformat()
            save_progress(progress, slice_idx)
            print(f"\n  --- Batch progress: {i+1}/{len(batch)} URLs ---")

        time.sleep(1)  # Pause between URLs

    # Final progress save
    progress["completed_urls"] = list(slice_completed)
    progress["total_deals_found"] = total_deals_found
    progress["total_snapshots_processed"] = total_snapshots
    progress["last_run"] = datetime.now(timezone.utc).isoformat()
    save_progress(progress, slice_idx)

    # Save deals JSON for this run
    outfile = os.path.join(
        DATA_DIR,
        f"wayback_go_hf_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json",
    )
    # Collect summary info
    summary = {
        "run_date": datetime.now(timezone.utc).isoformat(),
        "batch_size": len(batch),
        "slice": slice_idx,
        "total_slices": total_slices,
        "deals_found": batch_deals_total,
        "db_inserted": batch_inserted,
        "db_updated": batch_updated,
    }
    with open(outfile, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nRun summary saved to {outfile}")

    # Close DB
    if db:
        stats = db.get_stats()
        print(f"\nDatabase totals: {stats['products']} products, "
              f"{stats['price_snapshots']} snapshots, {stats['deals']} deals")
        db.close()

    # Final summary
    remaining = len(pending) - len(batch)
    runs_needed = (remaining + batch_size - 1) // batch_size if remaining > 0 else 0

    print(f"\n{'='*60}")
    print(f"Batch complete (slice {slice_idx if slice_idx is not None else 'N/A'}):")
    print(f"  URLs processed this run: {len(batch)}")
    print(f"  Deals found this run:    {batch_deals_total}")
    if save_db:
        print(f"  DB inserts:              {batch_inserted}")
        print(f"  DB updates:              {batch_updated}")
    print(f"  Total completed (this slice): {len(slice_completed)}")
    print(f"  Total completed (all slices): {len(completed_urls)}")
    print(f"  Remaining (this slice):  {remaining}")
    print(f"  Estimated runs left:     {runs_needed}")


if __name__ == "__main__":
    main()
