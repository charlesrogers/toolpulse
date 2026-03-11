#!/usr/bin/env python3
"""
ToolPulse: go.harborfreight.com coupon/deal scraper.

Scrapes the Harbor Freight deals blog for coupon and instant savings data.
No bot protection to bypass — it's a WordPress site on a CDN.

Data sources:
  - Monthly instant savings grid pages (bulk deal listings)
  - Individual coupon detail pages (single coupon with code)

Usage:
    python3 go_hf_scraper.py              # Fast: grid pages only
    python3 go_hf_scraper.py --detail     # Also fetch individual coupon pages
    python3 go_hf_scraper.py --db         # Save to SQLite database
    python3 go_hf_scraper.py --detail --db
"""

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

# Add parent dir so we can import db module
sys.path.insert(0, BASE_DIR)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

GO_HF_BASE = "https://go.harborfreight.com"


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Alt text parser (used by both grid and individual pages) ─────────────────

# Matches: "Buy the PRODUCT NAME (Item XXXXX) for $XX.XX, valid through M/D/YYYY."
# Also handles multiple item numbers: (Item 63496/63499)
ALT_PATTERN = re.compile(
    r"Buy the (.+?)\s*\(Item\s*([\d/]+)\)\s*for \$([0-9,.]+)"
    r"(?:,?\s*valid through\s*(\d{1,2}/\d{1,2}/\d{4}))?",
    re.IGNORECASE,
)


def parse_deal_from_alt(alt_text: str, coupon_url: str = None, source_url: str = None) -> dict | None:
    """Extract deal data from an img alt text string."""
    m = ALT_PATTERN.search(alt_text)
    if not m:
        return None

    product_name = m.group(1).strip()
    item_numbers_raw = m.group(2).strip()
    price = float(m.group(3).replace(",", ""))
    valid_through = m.group(4).strip() if m.group(4) else None

    # Handle multiple item numbers: "63496/63499"
    item_numbers = [n.strip() for n in item_numbers_raw.split("/")]
    primary_item = item_numbers[0]
    alt_items = item_numbers[1:] if len(item_numbers) > 1 else []

    # Extract coupon code from URL pattern: /184469-58324/
    # The first number (6+ digits) is the coupon/promo ID
    coupon_code = None
    promo_id = None
    if coupon_url:
        url_match = re.search(r"/(\d{6,})-(\d+)/?$", coupon_url)
        if url_match:
            promo_id = url_match.group(1)
            # The promo_id IS the coupon code for these instant savings

    return {
        "product_name": product_name,
        "item_number": primary_item,
        "alt_item_numbers": alt_items,
        "price": price,
        "valid_through": valid_through,
        "coupon_code": coupon_code,
        "promo_id": promo_id,
        "coupon_url": coupon_url,
        "source": "go_hf",
        "source_url": source_url,
        "scraped_at": now_utc(),
    }


# ── Grid Page Parser (fast — gets 40-150+ deals per page) ───────────────────

def parse_grid_page(url: str) -> list[dict]:
    """Parse an instant savings grid page for all deal items.

    Each deal is an <li> with:
      <a href="/coupons/YYYY/MM/PROMOID-ITEM/">
        <img alt="Buy the PRODUCT (Item XXXXX) for $XX.XX, valid through M/D/YYYY.">
        <strong>PRODUCT for $XX.XX</strong>
      </a>
    """
    print(f"  Fetching grid: {url}")
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    deals = []
    for img in soup.find_all("img", alt=ALT_PATTERN):
        alt = img.get("alt", "")
        parent_a = img.find_parent("a")
        coupon_url = parent_a["href"] if parent_a and parent_a.get("href") else None

        deal = parse_deal_from_alt(alt, coupon_url=coupon_url, source_url=url)
        if deal:
            deals.append(deal)

    print(f"    → {len(deals)} deals")
    return deals


# ── Individual Coupon Page Parser ────────────────────────────────────────────

def parse_coupon_page(url: str) -> dict | None:
    """Parse a single coupon detail page for coupon code and details.

    Entry content has text like:
      "Buy the PRODUCT (Item XXXXX) for $XX with coupon code XXXXXXXX, valid through DATE."
    """
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    # First try alt text on the featured image (same pattern as grid)
    img = soup.find("img", alt=ALT_PATTERN)
    deal = None
    if img:
        deal = parse_deal_from_alt(img.get("alt", ""), coupon_url=url, source_url=url)

    # Extract coupon code from entry text
    entry = soup.find("div", class_="entry-content")
    if entry:
        text = entry.get_text(" ", strip=True)

        # Look for explicit coupon code
        code_match = re.search(r"coupon code\s*(\d{6,})", text, re.IGNORECASE)
        if code_match and deal:
            deal["coupon_code"] = code_match.group(1)

        # If we didn't get a deal from alt text, try the entry text
        if not deal:
            m = ALT_PATTERN.search(text)
            if m:
                deal = parse_deal_from_alt(text, coupon_url=url, source_url=url)

        # Check ITC
        if deal:
            deal["is_itc"] = bool(re.search(r"inside track|ITC|member", text, re.IGNORECASE))

    if deal:
        deal["source"] = "coupon_page"

    return deal


# ── Discovery ────────────────────────────────────────────────────────────────

def discover_grid_pages() -> list[str]:
    """Find current instant savings grid page URLs from go.harborfreight.com."""
    print("Discovering instant savings grid pages...")

    grid_pages = []
    coupon_pages = []

    # Check the main coupons hub
    hub_urls = [
        f"{GO_HF_BASE}/monthly-instant-savings-catalog-coupon-book/",
        f"{GO_HF_BASE}/",
    ]

    for hub_url in hub_urls:
        try:
            resp = requests.get(hub_url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

            for a in soup.find_all("a", href=True):
                href = a["href"]
                # Grid pages: /cpi/digital/YYYY/MM/instant-savings-...
                if "/cpi/digital/" in href and "instant-savings" in href:
                    if href not in grid_pages:
                        grid_pages.append(href)
                # Individual coupon pages: /coupons/YYYY/MM/XXXXXX-YYYYY/
                elif re.match(r"https?://go\.harborfreight\.com/coupons/\d{4}/\d{2}/\d+-\d+/?", href):
                    if href not in coupon_pages:
                        coupon_pages.append(href)
        except Exception as e:
            print(f"  Warning: could not fetch {hub_url}: {e}")
        time.sleep(1)

    print(f"  Found {len(grid_pages)} grid pages, {len(coupon_pages)} individual coupon links")
    return grid_pages, coupon_pages


# ── Main ─────────────────────────────────────────────────────────────────────

def scrape_all(fetch_detail_pages: bool = False, save_to_db: bool = False):
    """Main scrape routine.

    1. Discover grid listing pages + individual coupon page links
    2. Parse grid pages (fast — all data in alt text, 40-150 deals per page)
    3. Optionally fetch individual coupon pages for explicit coupon codes
    4. Save to JSON and optionally to SQLite
    """
    all_deals = []
    seen_items = set()  # Deduplicate by item number

    # Step 1: Discover pages
    grid_pages, coupon_page_urls = discover_grid_pages()

    # Step 2: Parse grid pages (fast, bulk)
    if grid_pages:
        print(f"\nParsing {len(grid_pages)} grid pages...")
        for url in grid_pages:
            try:
                deals = parse_grid_page(url)
                for deal in deals:
                    item = deal["item_number"]
                    if item not in seen_items:
                        all_deals.append(deal)
                        seen_items.add(item)
            except Exception as e:
                print(f"    ✗ Error: {e}")
            time.sleep(1)
    else:
        print("\nNo grid pages found — falling back to individual coupon pages only")
        fetch_detail_pages = True

    # Step 3: Parse individual coupon pages
    if fetch_detail_pages and coupon_page_urls:
        # Also discover coupon pages linked from grid pages
        for url in grid_pages:
            try:
                resp = requests.get(url, headers=HEADERS, timeout=30)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "lxml")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if re.match(r"https?://go\.harborfreight\.com/coupons/\d{4}/\d{2}/\d+-\d+/?", href):
                        if href not in coupon_page_urls:
                            coupon_page_urls.append(href)
            except Exception:
                pass

        print(f"\nFetching {len(coupon_page_urls)} individual coupon pages...")
        for i, curl in enumerate(coupon_page_urls):
            try:
                detail = parse_coupon_page(curl)
                if detail:
                    item = detail.get("item_number")
                    if item and item in seen_items:
                        # Merge coupon code into existing deal
                        existing = next((d for d in all_deals if d["item_number"] == item), None)
                        if existing:
                            if detail.get("coupon_code"):
                                existing["coupon_code"] = detail["coupon_code"]
                            if detail.get("is_itc"):
                                existing["is_itc"] = detail["is_itc"]
                    elif item:
                        all_deals.append(detail)
                        seen_items.add(item)
            except Exception as e:
                print(f"    ✗ Error on {curl}: {e}")

            if (i + 1) % 25 == 0:
                print(f"  Progress: {i + 1}/{len(coupon_page_urls)} coupon pages")
            time.sleep(1)

    # Step 4: Save to JSON
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    outfile = os.path.join(DATA_DIR, f"deals_{timestamp}.json")
    with open(outfile, "w") as f:
        json.dump(all_deals, f, indent=2)

    # Step 5: Save to database
    if save_to_db:
        try:
            from db import ToolPulseDB
            db = ToolPulseDB()
            inserted, updated = db.upsert_deals(all_deals)
            print(f"\nDatabase: {inserted} new deals inserted, {updated} existing deals updated")
            db.close()
        except ImportError:
            print("\n⚠ db.py not found — skipping database save")

    # Summary
    print(f"\n{'='*60}")
    print(f"Scraped {len(all_deals)} unique deals")
    print(f"JSON saved to: {outfile}")

    prices = [d["price"] for d in all_deals]
    if prices:
        print(f"Price range: ${min(prices):.2f} — ${max(prices):.2f}")

    with_codes = sum(1 for d in all_deals if d.get("coupon_code") or d.get("promo_id"))
    print(f"With promo/coupon IDs: {with_codes}/{len(all_deals)}")

    # Brand summary
    brands = {}
    for d in all_deals:
        name = d["product_name"]
        brand_match = re.match(r"^([A-Z][A-Z\s&]+?)(?:\s[A-Z][a-z]|\s\d)", name)
        brand = brand_match.group(1).strip() if brand_match else "Other"
        brands[brand] = brands.get(brand, 0) + 1

    print(f"\nTop brands:")
    for brand, count in sorted(brands.items(), key=lambda x: -x[1])[:10]:
        print(f"  {brand}: {count}")

    return all_deals


if __name__ == "__main__":
    detail = "--detail" in sys.argv
    save_db = "--db" in sys.argv

    if detail:
        print("Mode: grid pages + individual coupon pages (slower, gets coupon codes)")
    else:
        print("Mode: grid pages only (fast)")
        print("  Use --detail to also fetch individual coupon pages")
    if save_db:
        print("  Saving to SQLite database")
    print()

    deals = scrape_all(fetch_detail_pages=detail, save_to_db=save_db)
