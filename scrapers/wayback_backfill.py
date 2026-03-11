#!/usr/bin/env python3
"""
ToolPulse: Wayback Machine historical price backfill.

Uses the Internet Archive CDX API to find archived Harbor Freight product pages,
then extracts historical prices from og:price:amount meta tags and JSON-LD data.

Usage:
    python3 wayback_backfill.py                      # Backfill seed products
    python3 wayback_backfill.py --db                  # Also save to SQLite
    python3 wayback_backfill.py --url "https://..."   # Single product
    python3 wayback_backfill.py --discover-from-sitemap  # Find URLs from archived sitemap
    python3 wayback_backfill.py --discover-from-deals    # Use URLs from deal scraper output
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

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
# Set WAYBACK_PROXY_URL to e.g. "https://toolpulse-wayback.charlesrogers.workers.dev/wayback-proxy"
WAYBACK_PROXY_URL = os.environ.get("WAYBACK_PROXY_URL", "")


# ── HTTP with retry ──────────────────────────────────────────────────────────

def _maybe_proxy_url(url: str) -> str:
    """If WAYBACK_PROXY_URL is set and url is a web.archive.org URL, route through the proxy."""
    if WAYBACK_PROXY_URL and url.startswith("https://web.archive.org/"):
        from urllib.parse import quote
        return f"{WAYBACK_PROXY_URL}?url={quote(url, safe='')}"
    return url


def fetch_with_retry(url: str, timeout: int = 15) -> requests.Response | None:
    """Fetch URL with fast retry on connection errors.

    When WAYBACK_PROXY_URL is set, archive.org requests are routed through
    the Cloudflare Worker proxy for better IP reputation / rate-limit avoidance.
    """
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
                print(f"    ✗ Skipping: {type(e).__name__}")
                return None
    return None


# ── CDX API: Find archived snapshots ────────────────────────────────────────

def find_snapshots(product_url: str, limit: int = 100) -> list[dict]:
    """Query the Wayback Machine CDX API for snapshots of a product URL.

    Focuses on 2024+ data (recent prices matter most).
    Collapses to one per month for efficiency.
    """
    params = {
        "url": product_url,
        "output": "json",
        "fl": "timestamp,original,statuscode",
        "filter": "statuscode:200",
        "from": "20240101",  # Focus on recent data (2024+)
        "limit": limit,
        "collapse": "timestamp:6",  # One per month
    }

    resp = fetch_with_retry(f"{CDX_API}?{'&'.join(f'{k}={v}' for k, v in params.items())}")
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


# ── Extract price from archived page ────────────────────────────────────────

def extract_price_from_snapshot(timestamp: str, original_url: str) -> dict | None:
    """Fetch an archived HF product page and extract price data."""
    wayback_url = f"{WAYBACK_BASE}/{timestamp}id_/{original_url}"

    resp = fetch_with_retry(wayback_url)
    if not resp or resp.status_code != 200:
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    result = {
        "timestamp": timestamp,
        "date": f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]}",
        "source": "wayback",
        "wayback_url": wayback_url,
    }

    # Method 1: og:price:amount meta tag
    og_price = soup.find("meta", property="og:price:amount")
    if og_price and og_price.get("content"):
        try:
            result["price"] = float(og_price["content"])
        except ValueError:
            pass

    # Method 2: JSON-LD schema.org Product
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string)
            products = []
            if isinstance(ld, list):
                products = [item for item in ld if item.get("@type") == "Product"]
            elif isinstance(ld, dict):
                if ld.get("@type") == "Product":
                    products = [ld]
                elif "@graph" in ld:
                    products = [item for item in ld["@graph"] if item.get("@type") == "Product"]

            for product in products:
                if not result.get("product_name"):
                    result["product_name"] = product.get("name")
                if not result.get("sku"):
                    result["sku"] = product.get("sku")
                if not result.get("brand"):
                    brand = product.get("brand")
                    result["brand"] = brand.get("name") if isinstance(brand, dict) else brand

                offers = product.get("offers", {})
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}
                if offers.get("price") and not result.get("price"):
                    try:
                        result["price"] = float(offers["price"])
                    except (ValueError, TypeError):
                        pass
                if offers.get("availability"):
                    result["in_stock"] = "InStock" in offers["availability"]

                rating = product.get("aggregateRating", {})
                if rating:
                    result["rating"] = rating.get("ratingValue")
                    result["review_count"] = rating.get("reviewCount")

        except (json.JSONDecodeError, TypeError, KeyError):
            continue

    # Fallback: product name from h1
    if not result.get("product_name"):
        h1 = soup.find("h1", class_="product-name") or soup.find("h1")
        if h1:
            result["product_name"] = h1.get_text(strip=True)

    # Extract SKU from URL
    if not result.get("sku"):
        sku_match = re.search(r"-(\d{5,})\.html", original_url)
        if sku_match:
            result["sku"] = sku_match.group(1)

    return result if result.get("price") is not None else None


# ── Backfill a single product ────────────────────────────────────────────────

def backfill_product(product_url: str, max_snapshots: int = 50) -> list[dict]:
    """Backfill price history for a single product URL via Wayback Machine."""
    sku_match = re.search(r"-(\d{5,})\.html", product_url)
    sku = sku_match.group(1) if sku_match else "unknown"

    print(f"\n{'─'*60}")
    print(f"SKU {sku}: {product_url}")

    snapshots = find_snapshots(product_url, limit=max_snapshots)
    print(f"  Found {len(snapshots)} archived snapshots")

    if not snapshots:
        return []

    prices = []
    last_price = None
    consecutive_failures = 0

    for i, snap in enumerate(snapshots):
        ts = snap["timestamp"]
        date_str = f"{ts[:4]}-{ts[4:6]}-{ts[6:8]}"

        price_data = extract_price_from_snapshot(ts, snap["original"])
        if price_data:
            consecutive_failures = 0
            current_price = price_data.get("price")
            change = ""
            if last_price is not None and current_price != last_price:
                diff = current_price - last_price
                change = f" ({'↑' if diff > 0 else '↓'} ${abs(diff):.2f})"
            print(f"  {date_str}: ${current_price:.2f}{change}")
            last_price = current_price
            prices.append(price_data)
        else:
            consecutive_failures += 1
            print(f"  {date_str}: (no price found)")
            # If we've failed 5 in a row, this URL probably never had structured data
            if consecutive_failures >= 5 and not prices:
                print(f"  Skipping remaining — no structured price data found")
                break

        if (i + 1) % 10 == 0:
            print(f"  Progress: {i + 1}/{len(snapshots)} snapshots, {len(prices)} prices found")

        time.sleep(1)  # Be nice to archive.org

    return prices


# ── URL Discovery ────────────────────────────────────────────────────────────

def discover_urls_from_sitemap(max_products: int = 1000) -> list[str]:
    """Find product URLs from an archived HF sitemap."""
    print("Searching for archived HF sitemap...")

    sitemap_urls = [
        "https://www.harborfreight.com/sitemap.xml",
        "https://www.harborfreight.com/pub/sitemap/sitemap.xml",
    ]

    for sitemap_url in sitemap_urls:
        snapshots = find_snapshots(sitemap_url, limit=5)
        if snapshots:
            latest = snapshots[-1]
            print(f"  Found archived sitemap from {latest['timestamp'][:8]}")
            wayback_url = f"{WAYBACK_BASE}/{latest['timestamp']}id_/{latest['original']}"
            resp = fetch_with_retry(wayback_url, timeout=60)
            if resp and resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "lxml-xml")
                product_urls = []
                for loc in soup.find_all("loc"):
                    url = loc.get_text(strip=True)
                    if re.search(r"-\d{5,}\.html$", url):
                        product_urls.append(url)
                        if len(product_urls) >= max_products:
                            break
                print(f"  Extracted {len(product_urls)} product URLs")
                return product_urls

    print("  No archived sitemap found")
    return []


def discover_urls_from_deals() -> list[str]:
    """Build HF product URLs from item numbers found in deal scraper output.

    Maps item numbers to harborfreight.com URLs by checking Wayback CDX
    for URL patterns containing the item number.
    """
    print("Building product URLs from deal scraper data...")

    # Find the most recent deals JSON
    deal_files = sorted(Path(DATA_DIR).glob("deals_*.json"), reverse=True)
    if not deal_files:
        print("  No deal files found. Run go_hf_scraper.py first.")
        return []

    with open(deal_files[0]) as f:
        deals = json.load(f)

    item_numbers = list(set(d["item_number"] for d in deals if d.get("item_number")))
    print(f"  Found {len(item_numbers)} unique item numbers from {deal_files[0].name}")

    # For each item number, query CDX to find the actual HF URL
    urls = []
    for i, item in enumerate(item_numbers):
        cdx_url = (
            f"{CDX_API}?url=harborfreight.com/*-{item}.html"
            f"&output=json&fl=original&limit=1&filter=statuscode:200"
        )
        resp = fetch_with_retry(cdx_url)
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                if len(data) > 1:
                    original_url = data[1][0]
                    if original_url not in urls:
                        urls.append(original_url)
                        print(f"  {item} → {original_url.split('/')[-1]}")
            except (ValueError, IndexError):
                pass

        if (i + 1) % 25 == 0:
            print(f"  URL discovery progress: {i + 1}/{len(item_numbers)}")
        time.sleep(0.5)

    print(f"  Resolved {len(urls)}/{len(item_numbers)} item numbers to URLs")
    return urls


# ── Seed Products ────────────────────────────────────────────────────────────

SEED_PRODUCTS = [
    "https://www.harborfreight.com/4-in-x-36-in-belt-and-6-in-disc-sander-58339.html",
    "https://www.harborfreight.com/foldable-aluminum-sports-chair-blue-56719.html",
    "https://www.harborfreight.com/professional-scraper-57099.html",
]


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    save_db = "--db" in sys.argv

    # Single URL mode
    if "--url" in sys.argv:
        idx = sys.argv.index("--url")
        if idx + 1 < len(sys.argv):
            url = sys.argv[idx + 1]
            prices = backfill_product(url)
            if prices:
                sku = prices[0].get("sku", "unknown")
                outfile = os.path.join(DATA_DIR, f"wayback_{sku}.json")
                with open(outfile, "w") as f:
                    json.dump(prices, f, indent=2)
                print(f"\nSaved {len(prices)} price points to {outfile}")

                if save_db:
                    from db import ToolPulseDB
                    db = ToolPulseDB()
                    count = db.import_wayback_prices(sku, prices)
                    print(f"Database: {count} new price snapshots imported")
                    db.close()
            return

    # URL discovery
    if "--discover-from-sitemap" in sys.argv:
        product_urls = discover_urls_from_sitemap(max_products=500)
    elif "--discover-from-deals" in sys.argv:
        product_urls = discover_urls_from_deals()
    else:
        product_urls = SEED_PRODUCTS

    if not product_urls:
        print("No product URLs to process.")
        print("Options: --url URL, --discover-from-sitemap, --discover-from-deals")
        return

    # Save discovered URLs for reuse
    if "--discover-from-sitemap" in sys.argv or "--discover-from-deals" in sys.argv:
        urls_file = os.path.join(DATA_DIR, "product_urls.json")
        with open(urls_file, "w") as f:
            json.dump(product_urls, f, indent=2)
        print(f"Saved {len(product_urls)} URLs to {urls_file}")

    # Backfill all products
    print(f"\nBackfilling {len(product_urls)} products from Wayback Machine...")
    all_prices = {}

    db = None
    if save_db:
        from db import ToolPulseDB
        db = ToolPulseDB()

    for i, url in enumerate(product_urls):
        try:
            prices = backfill_product(url)
            sku_match = re.search(r"-(\d{5,})\.html", url)
            sku = sku_match.group(1) if sku_match else f"unknown_{i}"
            if prices:
                all_prices[sku] = prices
                if db:
                    count = db.import_wayback_prices(sku, prices)
                    print(f"  → DB: {count} new snapshots for SKU {sku}")
        except Exception as e:
            print(f"  ✗ Error processing {url}: {e}")

        if (i + 1) % 50 == 0:
            print(f"\n  Overall progress: {i + 1}/{len(product_urls)} products")

        time.sleep(0.5)

    if db:
        stats = db.get_stats()
        print(f"\nDatabase totals: {stats['products']} products, {stats['price_snapshots']} snapshots")
        db.close()

    # Save JSON
    outfile = os.path.join(DATA_DIR, f"wayback_backfill_{datetime.now(timezone.utc).strftime('%Y%m%d')}.json")
    with open(outfile, "w") as f:
        json.dump(all_prices, f, indent=2)

    total_points = sum(len(v) for v in all_prices.values())
    print(f"\n{'='*60}")
    print(f"Backfill complete:")
    print(f"  Products with data: {len(all_prices)}/{len(product_urls)}")
    print(f"  Total price points: {total_points}")
    print(f"  JSON saved to: {outfile}")

    for sku, prices in sorted(all_prices.items()):
        price_vals = [p["price"] for p in prices if p.get("price")]
        if price_vals:
            name = prices[0].get("product_name", "Unknown")[:40]
            dates = [p["date"] for p in prices]
            print(f"  SKU {sku} ({name}): ${min(price_vals):.2f}-${max(price_vals):.2f} ({len(prices)} pts, {dates[0]} to {dates[-1]})")


if __name__ == "__main__":
    main()
