#!/usr/bin/env python3
"""
ToolPulse: Gmail IMAP email fetcher.

Connects to Gmail via IMAP and downloads all emails with a specific label.
Parses Harbor Freight promotional emails for deal data.

Setup:
    1. In Gmail, create a label (e.g., "HF-Deals") and set up a filter:
       From: harborfreight.com → Apply label: HF-Deals
    2. Enable IMAP in Gmail settings
    3. Create an App Password: https://myaccount.google.com/apppasswords
       (requires 2FA enabled on your Google account)
    4. Create .env file in hf_tracker root:
       GMAIL_ADDRESS=you@gmail.com
       GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx
       GMAIL_LABEL=HF-Deals

Usage:
    python3 email_fetcher.py                    # Fetch & parse all emails
    python3 email_fetcher.py --db               # Also save to SQLite
    python3 email_fetcher.py --save-raw         # Save raw .eml files
    python3 email_fetcher.py --since 2025-01-01 # Only fetch since date
"""

import email
import email.policy
import imaplib
import json
import os
import re
import sys
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
EMAIL_DIR = os.path.join(BASE_DIR, "emails")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(EMAIL_DIR, exist_ok=True)

sys.path.insert(0, BASE_DIR)

IMAP_SERVER = "imap.gmail.com"
IMAP_PORT = 993


# ── Config ───────────────────────────────────────────────────────────────────

def load_config() -> dict:
    """Load Gmail credentials from .env file."""
    env_path = os.path.join(BASE_DIR, ".env")
    config = {}

    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    config[key.strip()] = val.strip()

    # Also check environment variables
    config["GMAIL_ADDRESS"] = os.environ.get("GMAIL_ADDRESS", config.get("GMAIL_ADDRESS", ""))
    config["GMAIL_APP_PASSWORD"] = os.environ.get("GMAIL_APP_PASSWORD", config.get("GMAIL_APP_PASSWORD", ""))
    config["GMAIL_LABEL"] = os.environ.get("GMAIL_LABEL", config.get("GMAIL_LABEL", "HF-Deals"))

    return config


# ── IMAP Connection ──────────────────────────────────────────────────────────

def connect_gmail(address: str, app_password: str) -> imaplib.IMAP4_SSL:
    """Connect to Gmail IMAP and authenticate."""
    print(f"Connecting to Gmail as {address}...")
    imap = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
    imap.login(address, app_password)
    print("  Authenticated successfully")
    return imap


def fetch_emails_by_label(imap: imaplib.IMAP4_SSL, label: str,
                           since_date: str = None) -> list[email.message.EmailMessage]:
    """Fetch all emails from a Gmail label/folder.

    Gmail labels are accessible as IMAP folders.
    Label names with spaces need quoting.
    """
    # Gmail uses special folder name format for labels
    # A label "HF-Deals" becomes the IMAP folder "HF-Deals"
    # Nested labels use "/" separator
    folder = f'"{label}"'

    status, _ = imap.select(folder, readonly=True)
    if status != "OK":
        # Try with Gmail label prefix
        folder = f'"[Gmail]/{label}"'
        status, _ = imap.select(folder, readonly=True)
        if status != "OK":
            print(f"  ✗ Could not open folder '{label}'")
            print("  Available folders:")
            _, folders = imap.list()
            for f in folders[:20]:
                print(f"    {f.decode()}")
            return []

    # Build search criteria
    search_criteria = "ALL"
    if since_date:
        # IMAP date format: DD-Mon-YYYY
        dt = datetime.strptime(since_date, "%Y-%m-%d")
        imap_date = dt.strftime("%d-%b-%Y")
        search_criteria = f'(SINCE {imap_date})'

    status, msg_ids = imap.search(None, search_criteria)
    if status != "OK":
        print("  ✗ Search failed")
        return []

    ids = msg_ids[0].split()
    print(f"  Found {len(ids)} emails in '{label}'")

    messages = []
    for i, msg_id in enumerate(ids):
        status, data = imap.fetch(msg_id, "(RFC822)")
        if status == "OK":
            raw = data[0][1]
            msg = email.message_from_bytes(raw, policy=email.policy.default)
            messages.append(msg)

        if (i + 1) % 25 == 0:
            print(f"  Progress: {i + 1}/{len(ids)} emails fetched")

    print(f"  Fetched {len(messages)} emails")
    return messages


# ── Email Parsing ────────────────────────────────────────────────────────────

def extract_html_body(msg: email.message.EmailMessage) -> str:
    """Get the HTML body from an email message."""
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            if content_type == "text/html":
                return part.get_content()
    elif msg.get_content_type() == "text/html":
        return msg.get_content()
    return ""


def extract_text_body(msg: email.message.EmailMessage) -> str:
    """Get the plain text body from an email message."""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                return part.get_content()
    elif msg.get_content_type() == "text/plain":
        return msg.get_content()
    return ""


def parse_hf_email(msg: email.message.EmailMessage) -> dict:
    """Parse a Harbor Freight email for deal data.

    HF emails are heavily image-based, but we can extract:
    - Subject line (often has the deal headline)
    - "View in browser" link (points to go.harborfreight.com)
    - Any text content with item numbers, prices, coupon codes
    - Links to product pages and coupon pages
    """
    subject = msg.get("Subject", "")
    from_addr = msg.get("From", "")
    date_str = msg.get("Date", "")
    msg_id = msg.get("Message-ID", "")

    try:
        date = parsedate_to_datetime(date_str)
    except Exception:
        date = None

    html = extract_html_body(msg)
    text = extract_text_body(msg)

    result = {
        "subject": subject,
        "from": from_addr,
        "date": date.isoformat() if date else date_str,
        "message_id": msg_id,
        "deals": [],
        "links": [],
    }

    # Extract all links from HTML
    if html:
        # Find go.harborfreight.com links (these are the deal/coupon pages)
        go_hf_links = re.findall(
            r'href="(https?://go\.harborfreight\.com/[^"]+)"', html
        )
        result["links"] = list(set(go_hf_links))

        # Find harborfreight.com product links
        product_links = re.findall(
            r'href="(https?://(?:www\.)?harborfreight\.com/[^"]*-\d{5,}\.html[^"]*)"', html
        )
        result["product_links"] = list(set(product_links))

        # Try to find "View in Browser" link
        view_browser = re.findall(
            r'href="(https?://[^"]*(?:view|browser|web)[^"]*)"', html, re.IGNORECASE
        )
        if view_browser:
            result["view_in_browser"] = view_browser[0]

    # Extract item numbers, prices, and coupon codes from all text
    combined_text = f"{subject} {text} "
    # Strip HTML tags for text extraction from HTML body
    if html:
        clean_html = re.sub(r"<[^>]+>", " ", html)
        combined_text += clean_html

    # Find item numbers
    item_numbers = re.findall(r"(?:Item|SKU|#)\s*(\d{5,6})", combined_text, re.IGNORECASE)
    result["item_numbers"] = list(set(item_numbers))

    # Find prices
    prices = re.findall(r"\$(\d+(?:,\d{3})*\.?\d{0,2})", combined_text)
    result["prices_found"] = list(set(prices))

    # Find coupon codes (typically 8-digit numbers near "code" or "coupon")
    codes = re.findall(r"(?:code|coupon)\s*[:#]?\s*(\d{7,9})", combined_text, re.IGNORECASE)
    result["coupon_codes"] = list(set(codes))

    # Find "valid through" / expiration dates
    expiry = re.findall(
        r"(?:valid through|expires?|exp\.?)\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        combined_text, re.IGNORECASE
    )
    result["expiry_dates"] = list(set(expiry))

    # Try to parse deal items from alt text in images (same pattern as go_hf)
    alt_matches = re.findall(
        r"Buy the (.+?)\s*\(Item\s*([\d/]+)\)\s*for \$([0-9,.]+)"
        r"(?:,?\s*valid through\s*(\d{1,2}/\d{1,2}/\d{4}))?",
        combined_text, re.IGNORECASE
    )
    for match in alt_matches:
        product_name, item_nums, price, valid_through = match
        result["deals"].append({
            "product_name": product_name.strip(),
            "item_number": item_nums.split("/")[0],
            "price": float(price.replace(",", "")),
            "valid_through": valid_through or None,
            "source": "email",
        })

    # Classify email type based on subject
    subject_lower = subject.lower()
    if "inside track" in subject_lower or "itc" in subject_lower:
        result["email_type"] = "itc"
    elif "instant savings" in subject_lower or "items on sale" in subject_lower:
        result["email_type"] = "instant_savings"
    elif "% off" in subject_lower:
        result["email_type"] = "percent_off"
    elif "coupon" in subject_lower:
        result["email_type"] = "coupon"
    elif "new" in subject_lower and ("item" in subject_lower or "product" in subject_lower):
        result["email_type"] = "new_product"
    else:
        result["email_type"] = "other"

    return result


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    save_db = "--db" in sys.argv
    save_raw = "--save-raw" in sys.argv
    since_date = None
    if "--since" in sys.argv:
        idx = sys.argv.index("--since")
        if idx + 1 < len(sys.argv):
            since_date = sys.argv[idx + 1]

    config = load_config()
    if not config["GMAIL_ADDRESS"] or not config["GMAIL_APP_PASSWORD"]:
        print("Missing Gmail credentials!")
        print()
        print("Create a .env file in the hf_tracker root directory:")
        print("  GMAIL_ADDRESS=your.email@gmail.com")
        print("  GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx")
        print("  GMAIL_LABEL=HF-Deals")
        print()
        print("To get an App Password:")
        print("  1. Enable 2FA on your Google account")
        print("  2. Go to https://myaccount.google.com/apppasswords")
        print("  3. Create a new app password for 'Mail'")
        return

    label = config["GMAIL_LABEL"]
    print(f"Fetching Harbor Freight emails from label: {label}")
    if since_date:
        print(f"  Since: {since_date}")

    imap = connect_gmail(config["GMAIL_ADDRESS"], config["GMAIL_APP_PASSWORD"])

    try:
        messages = fetch_emails_by_label(imap, label, since_date=since_date)

        if not messages:
            print("No emails found.")
            return

        # Parse all emails
        all_parsed = []
        total_deals = 0
        total_links = 0

        for i, msg in enumerate(messages):
            parsed = parse_hf_email(msg)
            all_parsed.append(parsed)
            total_deals += len(parsed["deals"])
            total_links += len(parsed.get("links", []))

            # Save raw .eml if requested
            if save_raw:
                date_prefix = parsed["date"][:10] if parsed["date"] else "unknown"
                safe_subject = re.sub(r'[^\w\s-]', '', parsed["subject"])[:50].strip()
                eml_path = os.path.join(EMAIL_DIR, f"{date_prefix}_{safe_subject}.eml")
                with open(eml_path, "wb") as f:
                    f.write(msg.as_bytes())

            if (i + 1) % 25 == 0:
                print(f"  Parsed: {i + 1}/{len(messages)} emails")

        # Save parsed data
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        outfile = os.path.join(DATA_DIR, f"emails_{timestamp}.json")
        with open(outfile, "w") as f:
            json.dump(all_parsed, f, indent=2, default=str)

        # Collect all go.harborfreight.com links for follow-up scraping
        all_go_hf_links = set()
        for p in all_parsed:
            for link in p.get("links", []):
                if "go.harborfreight.com/coupons/" in link:
                    all_go_hf_links.add(link)

        if all_go_hf_links:
            links_file = os.path.join(DATA_DIR, f"email_coupon_links_{timestamp}.json")
            with open(links_file, "w") as f:
                json.dump(sorted(all_go_hf_links), f, indent=2)
            print(f"  Coupon links saved to: {links_file}")

        # Save to DB
        if save_db:
            try:
                from db import ToolPulseDB
                db = ToolPulseDB()
                inserted = 0
                for p in all_parsed:
                    for deal in p["deals"]:
                        deal["source"] = "email"
                        deal["source_url"] = p.get("view_in_browser")
                    ins, upd = db.upsert_deals(p["deals"])
                    inserted += ins
                print(f"\n  Database: {inserted} new deals from emails")
                db.close()
            except ImportError:
                print("  ⚠ db.py not found — skipping database save")

        # Summary
        print(f"\n{'='*60}")
        print(f"Processed {len(all_parsed)} emails")
        print(f"  Inline deals parsed: {total_deals}")
        print(f"  go.hf.com links found: {len(all_go_hf_links)}")
        print(f"  Unique item numbers: {len(set(n for p in all_parsed for n in p['item_numbers']))}")
        print(f"  Saved to: {outfile}")
        if save_raw:
            print(f"  Raw .eml files: {EMAIL_DIR}/")

        # Email type breakdown
        types = {}
        for p in all_parsed:
            t = p.get("email_type", "other")
            types[t] = types.get(t, 0) + 1
        print(f"\n  Email types:")
        for t, c in sorted(types.items(), key=lambda x: -x[1]):
            print(f"    {t}: {c}")

    finally:
        imap.logout()
        print("\nDisconnected from Gmail")


if __name__ == "__main__":
    main()
