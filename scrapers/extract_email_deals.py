#!/usr/bin/env python3
"""
ToolPulse: Extract deals from Harbor Freight emails.

Pipeline:
  1. Read all saved .eml files
  2. Resolve clicks.harborfreight.com tracking links → real URLs
  3. Collect go.harborfreight.com coupon/email page URLs
  4. Scrape those pages for deal data (product name, SKU, price, expiry)
  5. Save to database

Usage:
    python3 extract_email_deals.py           # Full pipeline
    python3 extract_email_deals.py --db      # Also save to SQLite
    python3 extract_email_deals.py --recent 30  # Only last 30 emails
"""

import email
import email.policy
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
EMAIL_DIR = os.path.join(BASE_DIR, "emails")
os.makedirs(DATA_DIR, exist_ok=True)

sys.path.insert(0, BASE_DIR)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

# Reuse the go_hf alt text parser
ALT_PATTERN = re.compile(
    r"Buy the (.+?)\s*\(Item\s*([\d/]+)\)\s*for \$([0-9,.]+)"
    r"(?:,?\s*valid through\s*(\d{1,2}/\d{1,2}/\d{4}))?",
    re.IGNORECASE,
)


# ── Step 1: Resolve tracking links from emails ──────────────────────────────

def resolve_email_links(eml_path: str) -> dict:
    """Resolve all tracking links from a single .eml file.

    Returns {go_hf_urls: [...], product_urls: [...], email_date: ..., subject: ...}
    """
    with open(eml_path, "rb") as f:
        msg = email.message_from_bytes(f.read(), policy=email.policy.default)

    subject = msg.get("Subject", "")
    date_str = msg.get("Date", "")

    html = ""
    for part in msg.walk():
        if part.get_content_type() == "text/html":
            html = part.get_content()
            break

    if not html:
        return {"go_hf_urls": [], "product_urls": [], "subject": subject, "date": date_str}

    # Get all unique tracking links
    tracking_links = re.findall(
        r'href="(https?://clicks\.harborfreight\.com/[^"]+)"', html
    )
    tracking_links = list(dict.fromkeys(tracking_links))

    go_hf_urls = set()
    product_urls = set()

    for link in tracking_links:
        try:
            resp = requests.head(link, allow_redirects=True, timeout=8)
            final = resp.url.split("?")[0]

            if "go.harborfreight.com/coupons/" in final:
                go_hf_urls.add(final)
            elif "go.harborfreight.com/email" in final:
                go_hf_urls.add(final)
            elif re.search(r"harborfreight\.com/.*-\d{5,}\.html", final):
                product_urls.add(final)
        except Exception:
            pass

    return {
        "go_hf_urls": list(go_hf_urls),
        "product_urls": list(product_urls),
        "subject": subject,
        "date": date_str,
        "tracking_links_total": len(tracking_links),
    }


# ── Step 2: Scrape go.harborfreight.com coupon pages ────────────────────────

def scrape_coupon_page(url: str) -> list[dict]:
    """Scrape a go.harborfreight.com page for deals.

    Works on both:
    - Individual coupon pages: /coupons/YYYY/MM/PROMOID-ITEM/
    - Email listing pages: /emails/YYYY/MM/coupon-deals-now-thru-.../
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception:
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    deals = []

    # Find all deals via img alt text pattern
    for img in soup.find_all("img", alt=ALT_PATTERN):
        alt = img.get("alt", "")
        m = ALT_PATTERN.search(alt)
        if not m:
            continue

        product_name = m.group(1).strip()
        item_numbers_raw = m.group(2).strip()
        price = float(m.group(3).replace(",", ""))
        valid_through = m.group(4).strip() if m.group(4) else None

        item_numbers = [n.strip() for n in item_numbers_raw.split("/")]

        # Get coupon URL from parent link
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
            "item_number": item_numbers[0],
            "alt_item_numbers": item_numbers[1:] if len(item_numbers) > 1 else [],
            "price": price,
            "valid_through": valid_through,
            "promo_id": promo_id,
            "coupon_url": coupon_url,
            "source": "email_extracted",
            "source_url": url,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })

    # Also try entry-content text (individual coupon pages)
    if not deals:
        entry = soup.find("div", class_="entry-content")
        if entry:
            text = entry.get_text(" ", strip=True)
            m = ALT_PATTERN.search(text)
            if m:
                product_name = m.group(1).strip()
                item_numbers = m.group(2).strip().split("/")
                price = float(m.group(3).replace(",", ""))
                valid_through = m.group(4).strip() if m.group(4) else None

                coupon_code = None
                code_match = re.search(r"coupon code\s*(\d{6,})", text, re.IGNORECASE)
                if code_match:
                    coupon_code = code_match.group(1)

                deals.append({
                    "product_name": product_name,
                    "item_number": item_numbers[0],
                    "alt_item_numbers": item_numbers[1:] if len(item_numbers) > 1 else [],
                    "price": price,
                    "valid_through": valid_through,
                    "coupon_code": coupon_code,
                    "source": "email_extracted",
                    "source_url": url,
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                })

    return deals


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    save_db = "--db" in sys.argv
    recent_n = None
    if "--recent" in sys.argv:
        idx = sys.argv.index("--recent")
        if idx + 1 < len(sys.argv):
            recent_n = int(sys.argv[idx + 1])

    if not os.path.exists(EMAIL_DIR):
        print("No emails directory. Run email_fetcher.py --save-raw first.")
        return

    eml_files = sorted([f for f in os.listdir(EMAIL_DIR) if f.endswith(".eml")])
    if recent_n:
        eml_files = eml_files[-recent_n:]

    print(f"Processing {len(eml_files)} emails...")

    # ── Phase 1: Resolve all tracking links ──
    print(f"\n{'─'*60}")
    print("Phase 1: Resolving tracking links from emails...")

    all_go_hf_urls = set()
    all_product_urls = set()
    emails_with_links = 0

    # Check for cached results
    cache_file = os.path.join(DATA_DIR, "email_resolved_links.json")
    if os.path.exists(cache_file) and not recent_n:
        print("  Using cached link resolution...")
        with open(cache_file) as f:
            cached = json.load(f)
        all_go_hf_urls = set(cached.get("go_hf_urls", []))
        all_product_urls = set(cached.get("product_urls", []))
        print(f"  Cached: {len(all_go_hf_urls)} coupon URLs, {len(all_product_urls)} product URLs")
    else:
        for i, fname in enumerate(eml_files):
            fpath = os.path.join(EMAIL_DIR, fname)
            result = resolve_email_links(fpath)

            if result["go_hf_urls"] or result["product_urls"]:
                emails_with_links += 1

            all_go_hf_urls.update(result["go_hf_urls"])
            all_product_urls.update(result["product_urls"])

            if (i + 1) % 10 == 0:
                print(f"  Progress: {i + 1}/{len(eml_files)} emails | "
                      f"{len(all_go_hf_urls)} coupon URLs, {len(all_product_urls)} product URLs")

        # Cache results
        with open(cache_file, "w") as f:
            json.dump({
                "go_hf_urls": sorted(all_go_hf_urls),
                "product_urls": sorted(all_product_urls),
                "emails_processed": len(eml_files),
            }, f, indent=2)

    print(f"\n  Emails with deal links: {emails_with_links}")
    print(f"  Unique coupon page URLs: {len(all_go_hf_urls)}")
    print(f"  Unique product URLs: {len(all_product_urls)}")

    # ── Phase 2: Scrape coupon pages for deal data ──
    print(f"\n{'─'*60}")
    print(f"Phase 2: Scraping {len(all_go_hf_urls)} coupon pages...")

    all_deals = []
    seen_items = set()

    for i, url in enumerate(sorted(all_go_hf_urls)):
        deals = scrape_coupon_page(url)
        for deal in deals:
            item = deal["item_number"]
            key = (item, deal.get("promo_id"), deal["price"])
            if key not in seen_items:
                all_deals.append(deal)
                seen_items.add(key)

        if (i + 1) % 25 == 0:
            print(f"  Progress: {i + 1}/{len(all_go_hf_urls)} pages | {len(all_deals)} deals")
        time.sleep(0.5)

    # ── Phase 3: Save results ──
    print(f"\n{'─'*60}")
    print(f"Phase 3: Saving results...")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    outfile = os.path.join(DATA_DIR, f"email_deals_{timestamp}.json")
    with open(outfile, "w") as f:
        json.dump(all_deals, f, indent=2)

    # Save unique item numbers for Wayback backfill prioritization
    item_numbers = list(set(d["item_number"] for d in all_deals))
    items_file = os.path.join(DATA_DIR, "email_deal_items.json")
    with open(items_file, "w") as f:
        json.dump(item_numbers, f, indent=2)

    # Save product URLs (from email links + coupon pages)
    email_product_urls = list(all_product_urls)
    # Also add product URLs we can construct from item numbers in deals
    # (we'll need to resolve these via CDX for Wayback backfill)
    urls_file = os.path.join(DATA_DIR, "email_product_urls.json")
    with open(urls_file, "w") as f:
        json.dump(email_product_urls, f, indent=2)

    if save_db:
        try:
            from db import ToolPulseDB
            db = ToolPulseDB()
            inserted, updated = db.upsert_deals(all_deals)
            print(f"  Database: {inserted} new deals, {updated} updated")
            stats = db.get_stats()
            print(f"  Total: {stats['products']} products, {stats['deals']} deals")
            db.close()
        except ImportError:
            print("  ⚠ db.py not found")

    # ── Summary ──
    print(f"\n{'='*60}")
    print(f"Email Deal Extraction Complete")
    print(f"  Emails processed: {len(eml_files)}")
    print(f"  Coupon pages scraped: {len(all_go_hf_urls)}")
    print(f"  Unique deals found: {len(all_deals)}")
    print(f"  Unique item numbers: {len(item_numbers)}")
    print(f"  Saved to: {outfile}")
    print(f"  Item numbers for backfill: {items_file}")

    if all_deals:
        prices = [d["price"] for d in all_deals]
        print(f"  Price range: ${min(prices):.2f} — ${max(prices):.2f}")

        # Expiry date breakdown
        expiry_dates = {}
        for d in all_deals:
            exp = d.get("valid_through") or "unknown"
            expiry_dates[exp] = expiry_dates.get(exp, 0) + 1
        print(f"\n  Deal batches by expiry:")
        for exp, count in sorted(expiry_dates.items()):
            print(f"    {exp}: {count} deals")


if __name__ == "__main__":
    main()
