#!/usr/bin/env python3
"""
ToolPulse: Live Harbor Freight price scraper via GraphQL API.

Uses Playwright to establish a browser session (PerimeterX bypass), then
calls the HF GraphQL FetchPrices API from within the browser context to
get prices in bulk (100 SKUs per request).

Usage:
    python3 scrapers/live_hf_scraper.py                    # Scrape products missing prices
    python3 scrapers/live_hf_scraper.py --all               # Scrape ALL products (refresh)
    python3 scrapers/live_hf_scraper.py --limit 500         # Only first N products
    python3 scrapers/live_hf_scraper.py --batch-size 50     # SKUs per API call (default 100)
"""

import json
import os
import re
import sys
import time
from datetime import date

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, BASE_DIR)
sys.path.insert(0, os.path.join(BASE_DIR, "scrapers"))

BROWSER_DATA_DIR = os.path.expanduser("~/.cache/hf-scraper-browser")
BATCH_SIZE = 100  # SKUs per GraphQL request


def get_products_to_scrape(db, scrape_all: bool = False) -> list[dict]:
    """Get products that need live scraping, prioritizing modern URLs."""
    if scrape_all:
        rows = db.conn.execute(
            "SELECT item_number, hf_url FROM products WHERE hf_url IS NOT NULL"
        ).fetchall()
    else:
        rows = db.conn.execute(
            """SELECT p.item_number, p.hf_url FROM products p
               LEFT JOIN price_snapshots ps ON p.item_number = ps.item_number
               WHERE p.hf_url IS NOT NULL
               GROUP BY p.item_number
               HAVING COUNT(ps.id) = 0"""
        ).fetchall()

    return [{"item_number": r[0], "hf_url": r[1]} for r in rows]


def fetch_prices_batch(page, skus: list[str]) -> list[dict]:
    """Call FetchPrices GraphQL API from browser context for a batch of SKUs."""
    result = page.evaluate('''async (skus) => {
        try {
            const resp = await fetch("https://api.harborfreight.com/graphql", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                credentials: "include",
                body: JSON.stringify({
                    operationName: "FetchPrices",
                    variables: { skus: skus },
                    query: `query FetchPrices($skus: [String!]!) {
                        fetchPrices(skus: $skus) {
                            sku
                            price_range {
                                minimum_price {
                                    final_price { value currency }
                                    regular_price { value currency }
                                }
                            }
                        }
                    }`
                })
            });
            if (!resp.ok) {
                const text = await resp.text();
                return { error: `HTTP ${resp.status}`, body: text.substring(0, 200) };
            }
            return await resp.json();
        } catch(e) {
            return { error: e.message };
        }
    }''', skus)

    if isinstance(result, dict) and "error" in result:
        return result  # Return error for caller to handle

    if isinstance(result, dict) and "data" in result and "fetchPrices" in result["data"]:
        return result["data"]["fetchPrices"]

    return {"error": "Unexpected response", "body": str(result)[:200]}


def run_live_scrape(batch_size: int = BATCH_SIZE, limit: int = 0, scrape_all: bool = False):
    """Scrape current prices from harborfreight.com via GraphQL API."""
    from playwright.sync_api import sync_playwright
    from db import ToolPulseDB

    db = ToolPulseDB()

    print("=" * 60)
    print("ToolPulse Live HF Price Scraper (GraphQL API)")
    print(f"  Batch size: {batch_size} SKUs per request")
    print(f"  Mode: {'all products' if scrape_all else 'missing prices only'}")
    print(f"  Limit: {limit or 'all'}")
    print("=" * 60)

    products = get_products_to_scrape(db, scrape_all)
    print(f"\n  Products to scrape: {len(products)}")

    if not products:
        print("\nNo products need scraping!")
        db.close()
        return

    if limit:
        products = products[:limit]
        print(f"  Limited to: {len(products)}")

    # Build SKU list and lookup map
    sku_list = [p["item_number"] for p in products]
    url_map = {p["item_number"]: p.get("hf_url") for p in products}
    num_batches = (len(sku_list) + batch_size - 1) // batch_size
    print(f"  Batches: {num_batches} (of {batch_size} SKUs each)")

    os.makedirs(BROWSER_DATA_DIR, exist_ok=True)

    print(f"\nLaunching browser...")

    start_time = time.time()
    total_prices = 0
    total_skus = 0
    errors = 0
    today = date.today().isoformat()

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            BROWSER_DATA_DIR,
            headless=False,
            viewport={"width": 1920, "height": 1080},
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = context.new_page()

        # Warm up: visit homepage to establish PerimeterX session
        print("  Warming up browser (visiting HF homepage)...")
        page.goto("https://www.harborfreight.com/", wait_until="domcontentloaded", timeout=30000)

        # Wait for captcha if needed
        for i in range(18):
            time.sleep(5)
            content = page.content()
            if "px-captcha" not in content.lower():
                print(f"  Session established after {(i+1)*5}s")
                break
            if i == 0:
                print("  Captcha detected — please solve it in the browser window...")
            print(f"    Waiting... ({(i+1)*5}s)")
        else:
            print("  ERROR: Could not get past captcha after 90s. Aborting.")
            context.close()
            db.close()
            return

        print(f"\n  Starting batch price fetch...\n")

        for batch_num in range(num_batches):
            batch_start = batch_num * batch_size
            batch_end = min(batch_start + batch_size, len(sku_list))
            batch_skus = sku_list[batch_start:batch_end]

            # Small delay between batches
            if batch_num > 0:
                time.sleep(0.5)

            result = fetch_prices_batch(page, batch_skus)

            if isinstance(result, dict) and "error" in result:
                errors += 1
                err = result["error"]
                print(f"  Batch {batch_num+1}/{num_batches}: ERROR - {err}")

                # If PerimeterX blocked, try to recover
                if "403" in str(err) or "px" in str(result.get("body", "")).lower():
                    print("    PerimeterX block detected — refreshing session...")
                    page.goto("https://www.harborfreight.com/", wait_until="domcontentloaded", timeout=30000)
                    time.sleep(10)
                    content = page.content()
                    if "px-captcha" in content.lower():
                        print("    Captcha appeared — please solve it...")
                        for i in range(12):
                            time.sleep(10)
                            if "px-captcha" not in page.content().lower():
                                print("    Captcha solved! Retrying batch...")
                                break
                        else:
                            print("    Could not solve captcha. Stopping.")
                            break

                    # Retry this batch
                    time.sleep(2)
                    result = fetch_prices_batch(page, batch_skus)
                    if isinstance(result, dict) and "error" in result:
                        print(f"    Retry failed: {result['error']}")
                        continue
                else:
                    continue

            # Process successful results
            batch_prices = 0
            for item in result:
                sku = item.get("sku")
                if not sku:
                    continue
                pr = item.get("price_range", {}).get("minimum_price", {})
                final_price = pr.get("final_price", {}).get("value")
                regular_price = pr.get("regular_price", {}).get("value")

                if final_price is None:
                    continue

                added = db.add_price_snapshot(
                    item_number=sku,
                    date=today,
                    price=final_price,
                    source="live_scrape",
                    source_url=url_map.get(sku),
                    in_stock=True,  # If API returns it, it's in stock
                    raw_data={
                        "final_price": final_price,
                        "regular_price": regular_price,
                        "source": "graphql_api",
                        "date": today,
                    },
                )
                if added:
                    batch_prices += 1
                    total_prices += 1

            total_skus += len(batch_skus)

            elapsed = time.time() - start_time
            print(f"  [{batch_num+1}/{num_batches}] Batch of {len(batch_skus)} SKUs: "
                  f"{len(result)} active, {batch_prices} new prices saved "
                  f"({elapsed:.0f}s elapsed)")

        # Clean up
        page.close()
        context.close()

    elapsed = time.time() - start_time
    stats = db.get_stats()
    db.close()

    print(f"\n{'=' * 60}")
    print(f"Live scrape complete!")
    print(f"  SKUs queried: {total_skus}")
    print(f"  New price snapshots: {total_prices}")
    print(f"  API errors: {errors}")
    if elapsed > 0:
        print(f"  Time: {elapsed:.0f}s ({elapsed/60:.1f} minutes)")
    print(f"  DB totals: {stats['products']} products, {stats['price_snapshots']} snapshots")


def main():
    batch_size = BATCH_SIZE
    limit = 0
    scrape_all = "--all" in sys.argv

    if "--batch-size" in sys.argv:
        idx = sys.argv.index("--batch-size")
        if idx + 1 < len(sys.argv):
            batch_size = int(sys.argv[idx + 1])

    if "--limit" in sys.argv:
        idx = sys.argv.index("--limit")
        if idx + 1 < len(sys.argv):
            limit = int(sys.argv[idx + 1])

    run_live_scrape(batch_size=batch_size, limit=limit, scrape_all=scrape_all)


if __name__ == "__main__":
    main()
