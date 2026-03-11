#!/usr/bin/env python3
"""
ToolPulse: Product URL discovery.

Discovers all Harbor Freight product URLs from multiple sources:
  1. Live HF sitemap (no bot protection on XML files)
  2. Wayback CDX API (historical/discontinued products)
  3. Email link resolution (tracking links → real URLs)

Usage:
    python3 discover_products.py                # Download live sitemap + CDX lookup
    python3 discover_products.py --db           # Also save products to SQLite
    python3 discover_products.py --sitemap-only # Just download live sitemap
    python3 discover_products.py --cdx-only     # Just query Wayback CDX
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

sys.path.insert(0, BASE_DIR)

HEADERS = {
    "User-Agent": "ToolPulse/1.0 (product catalog research)",
}

CDX_API = "https://web.archive.org/cdx/search/cdx"


def extract_sku(url: str) -> str | None:
    """Extract SKU (item number) from an HF product URL."""
    m = re.search(r"-(\d{5,})\.html", url)
    return m.group(1) if m else None


# ── Live Sitemap ─────────────────────────────────────────────────────────────

def download_live_sitemap() -> dict[str, str]:
    """Download HF's current sitemap and extract all product URLs.

    Returns dict of {sku: canonical_url}.
    Note: PerimeterX may block this from datacenter IPs (GitHub Actions, etc.).
    Falls back gracefully if blocked.
    """
    print("Downloading live HF sitemap...")

    # Step 1: Get sitemap index
    index_url = "https://www.harborfreight.com/sitemap.xml"
    try:
        resp = requests.get(index_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.HTTPError as e:
        print(f"  ✗ Sitemap blocked ({e}). PerimeterX likely blocking this IP.")
        print("  Falling back to CDX discovery only.")
        return {}

    soup = BeautifulSoup(resp.text, "lxml-xml")
    sitemap_urls = [loc.text for loc in soup.find_all("loc")]
    print(f"  Found {len(sitemap_urls)} sub-sitemaps")

    # Step 2: Download each sub-sitemap and extract product URLs
    products = {}
    for smap_url in sitemap_urls:
        print(f"  Fetching {smap_url}...")
        try:
            resp = requests.get(smap_url, headers=HEADERS, timeout=60)
            resp.raise_for_status()
            smap_soup = BeautifulSoup(resp.text, "lxml-xml")

            count = 0
            for loc in smap_soup.find_all("loc"):
                url = loc.text.strip()
                sku = extract_sku(url)
                if sku and sku not in products:
                    products[sku] = url
                    count += 1

            print(f"    → {count} new product URLs")
        except Exception as e:
            print(f"    ✗ Error: {e}")
        time.sleep(1)

    print(f"  Total: {len(products)} unique SKUs from live sitemap")
    return products


# ── Wayback CDX Discovery ────────────────────────────────────────────────────

def discover_from_cdx(max_results: int = 100000) -> dict[str, str]:
    """Query Wayback CDX API for all historically archived HF product pages.

    This captures discontinued products not in the current sitemap.
    """
    print("\nQuerying Wayback CDX for all archived HF product pages...")

    params = {
        "url": "www.harborfreight.com/",
        "matchType": "prefix",
        "output": "text",
        "fl": "original",
        "collapse": "urlkey",
        "filter": "statuscode:200",
        "limit": str(max_results),
    }

    resp = requests.get(CDX_API, params=params, headers=HEADERS, timeout=120)
    resp.raise_for_status()

    lines = resp.text.strip().split("\n")
    print(f"  CDX returned {len(lines)} unique URLs")

    products = {}
    for line in lines:
        url = line.strip()
        sku = extract_sku(url)
        if sku and sku not in products:
            products[sku] = url

    print(f"  Total: {len(products)} unique SKUs from Wayback CDX")
    return products


# ── Email Link Resolution ────────────────────────────────────────────────────

def discover_from_emails() -> dict[str, str]:
    """Extract product URLs from saved email tracking link resolutions."""
    import email as emailmod
    import email.policy

    email_dir = os.path.join(BASE_DIR, "emails")
    if not os.path.exists(email_dir):
        print("\nNo emails directory found. Run email_fetcher.py first.")
        return {}

    files = sorted(os.listdir(email_dir))
    if not files:
        return {}

    print(f"\nResolving tracking links from {len(files)} emails...")
    # Only process recent emails (tracking links expire)
    recent_files = files[-30:]

    products = {}
    go_hf_links = set()

    for i, fname in enumerate(recent_files):
        fpath = os.path.join(email_dir, fname)
        if not fname.endswith(".eml"):
            continue

        with open(fpath, "rb") as f:
            msg = emailmod.message_from_bytes(f.read(), policy=emailmod.policy.default)

        html = ""
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                html = part.get_content()
                break

        tracking_links = re.findall(
            r'href="(https?://clicks\.harborfreight\.com/[^"]+)"', html
        )
        tracking_links = list(dict.fromkeys(tracking_links))

        resolved = 0
        for link in tracking_links[:20]:
            try:
                resp = requests.head(link, allow_redirects=True, timeout=8)
                final = resp.url.split("?")[0]

                sku = extract_sku(final)
                if sku and sku not in products:
                    products[sku] = final
                    resolved += 1

                if "go.harborfreight.com" in final:
                    go_hf_links.add(final)
            except Exception:
                pass

        if (i + 1) % 10 == 0:
            print(f"  Progress: {i + 1}/{len(recent_files)} emails, {len(products)} products found")

    print(f"  Products from emails: {len(products)}")
    print(f"  go.hf.com links found: {len(go_hf_links)}")

    # Save go.hf.com links for the deals scraper
    if go_hf_links:
        links_file = os.path.join(DATA_DIR, "email_go_hf_links.json")
        with open(links_file, "w") as f:
            json.dump(sorted(go_hf_links), f, indent=2)

    return products


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    save_db = "--db" in sys.argv
    sitemap_only = "--sitemap-only" in sys.argv
    cdx_only = "--cdx-only" in sys.argv

    all_products = {}  # {sku: url}

    # Source 1: Live sitemap
    if not cdx_only:
        sitemap_products = download_live_sitemap()
        for sku, url in sitemap_products.items():
            all_products[sku] = {"url": url, "source": "sitemap", "active": True}

    # Source 2: Wayback CDX
    if not sitemap_only:
        cdx_products = discover_from_cdx()
        for sku, url in cdx_products.items():
            if sku not in all_products:
                all_products[sku] = {"url": url, "source": "wayback_cdx", "active": False}

    # Source 3: Emails (if available)
    if not sitemap_only and not cdx_only:
        email_products = discover_from_emails()
        for sku, url in email_products.items():
            if sku not in all_products:
                all_products[sku] = {"url": url, "source": "email", "active": True}

    # Save results
    outfile = os.path.join(DATA_DIR, "all_product_urls.json")
    with open(outfile, "w") as f:
        json.dump(all_products, f, indent=2)

    # Also save as flat URL list for Wayback backfill
    urls_file = os.path.join(DATA_DIR, "product_urls.json")
    urls = [p["url"] for p in all_products.values()]
    with open(urls_file, "w") as f:
        json.dump(urls, f, indent=2)

    # Save to database
    if save_db:
        try:
            from db import ToolPulseDB
            db = ToolPulseDB()
            new_count = 0
            for sku, info in all_products.items():
                is_new = db.upsert_product(
                    item_number=sku,
                    hf_url=info["url"],
                )
                if is_new:
                    new_count += 1
            print(f"\nDatabase: {new_count} new products added")
            stats = db.get_stats()
            print(f"  Total products: {stats['products']}")
            db.close()
        except ImportError:
            print("\n⚠ db.py not found — skipping database save")

    # Summary
    active = sum(1 for p in all_products.values() if p.get("active"))
    inactive = len(all_products) - active

    sources = {}
    for p in all_products.values():
        s = p.get("source", "unknown")
        sources[s] = sources.get(s, 0) + 1

    print(f"\n{'='*60}")
    print(f"Product Discovery Complete")
    print(f"  Total unique SKUs: {len(all_products)}")
    print(f"  Active (in sitemap): {active}")
    print(f"  Discontinued (Wayback only): {inactive}")
    print(f"\n  By source:")
    for s, c in sorted(sources.items(), key=lambda x: -x[1]):
        print(f"    {s}: {c}")
    print(f"\n  Saved to: {outfile}")
    print(f"  URL list: {urls_file}")


if __name__ == "__main__":
    main()
