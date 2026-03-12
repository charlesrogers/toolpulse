#!/usr/bin/env python3
"""
ToolPulse: Generate a Current Sales page.

Shows active deals (valid_through >= today) with:
- Discount off regular price
- Comparison to best deal in last year and all-time
- Buy signal (BUY NOW / GOOD DEAL / WAIT)
- Grouped by sale event
- Expiration countdown

Usage:
    python3 generate_current_sales.py
"""

import json
import os
import re
import sqlite3
from datetime import datetime, date, timedelta, timezone

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "data", "toolpulse.db")
OUT_PATH = os.path.join(BASE_DIR, "current-sales.html")


def parse_date(date_str):
    """Parse M/D/YYYY or YYYY-MM-DD date string to a Python date object."""
    if not date_str:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    return None


def parse_event_name(source_url):
    """Extract a human-readable event name from a source URL slug."""
    if not source_url:
        return "Unknown Event"
    slug = source_url.rstrip("/").split("/")[-1]
    slug = re.sub(r'[-_](extended-)?thru[-_]\d{1,2}[-_]\d{1,2}$', '', slug)
    slug = re.sub(r'[-_]valid[-_]through[-_]\d{1,2}[-_]\d{1,2}$', '', slug)
    slug = re.sub(r'[-_]now[-_]thru[-_]\d{1,2}[-_]\d{1,2}$', '', slug)
    slug = re.sub(r'[-_]thru[-_]\d{1,2}[-_]\d{1,2}[-_]\d{2,4}$', '', slug)
    slug = re.sub(r'[-_]ends[-_]\d{1,2}[-_]\d{1,2}$', '', slug)
    slug = re.sub(r'[-_]\d{8,}$', '', slug)
    name = slug.replace("-", " ").replace("_", " ").strip().title()
    name = name.replace("Itc", "ITC")
    name = name.replace("Inside Track Club Member Deals", "Inside Track Club")
    name = name.replace("Instant Savings Items On Sale", "Instant Savings")
    name = name.replace("Black Friday Sale Extended", "Black Friday Sale")
    name = name.replace("Parking Lot Sale Extended", "Parking Lot Sale")
    if len(name) > 40:
        name = name[:37] + "..."
    return name or "Unknown Event"


def _julian(date_str):
    """Convert date string to ordinal for distance comparison."""
    if not date_str:
        raise ValueError("empty date")
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str[:10], fmt).toordinal()
        except ValueError:
            continue
    raise ValueError(f"unparseable date: {date_str}")


def load_active_deals():
    today = date.today()
    one_year_ago = today - timedelta(days=365)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Load all deals with product info
    print("  Loading all deals...")
    all_deals = conn.execute("""
        SELECT
            d.id, d.item_number, d.deal_price, d.coupon_code, d.promo_id,
            d.is_itc, d.valid_from, d.valid_through, d.source, d.source_url,
            d.coupon_url,
            p.product_name, p.brand, p.hf_url, p.category_path
        FROM deals d
        JOIN products p ON d.item_number = p.item_number
    """).fetchall()
    print(f"  Total deals in DB: {len(all_deals):,}")

    # Filter to active deals (valid_through >= today)
    active_deals = []
    for r in all_deals:
        vt = parse_date(r["valid_through"])
        if vt and vt >= today:
            active_deals.append(r)
    print(f"  Active deals (valid_through >= {today}): {len(active_deals):,}")

    # Build historical best prices per item_number
    print("  Computing historical best deal prices...")
    hist_rows = conn.execute("""
        SELECT item_number, deal_price, valid_from, valid_through
        FROM deals WHERE deal_price IS NOT NULL
    """).fetchall()

    # Best deal price in last year & all-time per item
    best_1y = {}  # item_number -> lowest deal_price in last 365 days
    best_ever = {}  # item_number -> lowest deal_price ever
    deal_count_per_item = {}  # item_number -> total deal count

    for hr in hist_rows:
        sku = hr["item_number"]
        price = hr["deal_price"]

        deal_count_per_item[sku] = deal_count_per_item.get(sku, 0) + 1

        # All-time best
        if sku not in best_ever or price < best_ever[sku]:
            best_ever[sku] = price

        # Best in last year — use valid_from or valid_through date
        deal_date = parse_date(hr["valid_from"]) or parse_date(hr["valid_through"])
        if deal_date and deal_date >= one_year_ago:
            if sku not in best_1y or price < best_1y[sku]:
                best_1y[sku] = price

    print(f"  Historical best prices: {len(best_ever):,} products (all-time), {len(best_1y):,} (last year)")

    # Load price snapshots for nearest regular price
    print("  Loading price snapshots...")
    price_rows = conn.execute("""
        SELECT item_number, regular_price, snapshot_date
        FROM price_snapshots
        WHERE regular_price IS NOT NULL
        ORDER BY item_number, snapshot_date
    """).fetchall()

    price_history = {}
    for pr in price_rows:
        sku = pr["item_number"]
        if sku not in price_history:
            price_history[sku] = []
        price_history[sku].append((pr["snapshot_date"] or "", pr["regular_price"]))
    print(f"  Price history for {len(price_history):,} products")

    # Build enriched deal records
    deals = []
    categories_set = set()
    brands_set = set()
    events_map = {}  # source_url -> event info

    for idx, r in enumerate(active_deals):
        if idx % 100 == 0:
            print(f"  Processing active deal {idx+1:,}/{len(active_deals):,}...")

        sku = r["item_number"]
        deal_price = r["deal_price"]
        deal_date_str = r["valid_from"] or r["valid_through"] or ""
        vt = parse_date(r["valid_through"])

        # Find nearest regular price
        nearest_price = None
        if sku in price_history:
            snapshots = price_history[sku]
            if deal_date_str:
                best_dist = float('inf')
                for snap_date, snap_price in snapshots:
                    try:
                        dist = abs(_julian(snap_date) - _julian(deal_date_str))
                    except (ValueError, TypeError):
                        continue
                    if dist < best_dist:
                        best_dist = dist
                        nearest_price = snap_price
            else:
                nearest_price = snapshots[-1][1] if snapshots else None

        # Compute metrics
        discount_pct = None
        if nearest_price and nearest_price > 0 and deal_price is not None:
            pct = round((1 - deal_price / nearest_price) * 100, 1)
            if 0 < pct <= 100:
                discount_pct = pct

        vs_best_1y = None
        lowest_1y = best_1y.get(sku)
        if lowest_1y and lowest_1y > 0 and deal_price is not None:
            vs_best_1y = round((deal_price / lowest_1y - 1) * 100, 1)

        vs_best_ever = None
        lowest_ever = best_ever.get(sku)
        if lowest_ever and lowest_ever > 0 and deal_price is not None:
            vs_best_ever = round((deal_price / lowest_ever - 1) * 100, 1)

        # Days until expiration
        days_left = (vt - today).days if vt else None

        # Category
        cat_path = r["category_path"] or ""
        top_cat = cat_path.split(" > ")[0].strip() if cat_path else ""
        if top_cat:
            categories_set.add(top_cat)

        brand = r["brand"] or ""
        if brand:
            brands_set.add(brand)

        # Buy signal
        signal = "wait"  # default
        if discount_pct is not None and vs_best_ever is not None:
            if discount_pct >= 20 and vs_best_ever <= 5:
                signal = "buy"
            elif discount_pct >= 10 or (vs_best_1y is not None and vs_best_1y <= 10):
                signal = "good"
        elif discount_pct is not None:
            # No historical comparison available
            if discount_pct >= 20:
                signal = "good"

        # Track events
        src_url = r["source_url"] or ""
        if src_url and src_url not in events_map:
            events_map[src_url] = {
                "url": src_url,
                "name": parse_event_name(src_url),
                "deal_count": 0,
                "product_count": 0,
                "end": r["valid_through"] or "",
                "items": set(),
            }
        if src_url:
            events_map[src_url]["deal_count"] += 1
            events_map[src_url]["items"].add(sku)

        deal = {
            "sku": sku,
            "name": r["product_name"] or "",
            "brand": brand,
            "category": top_cat,
            "hf_url": r["hf_url"] or "",
            "price": deal_price,
            "reg_price": nearest_price,
            "discount": discount_pct,
            "vs_1y": vs_best_1y,
            "best_1y": lowest_1y,
            "vs_ever": vs_best_ever,
            "best_ever": lowest_ever,
            "signal": signal,
            "thru": r["valid_through"] or "",
            "days_left": days_left,
            "code": r["coupon_code"],
            "coupon_url": r["coupon_url"],
            "source_url": src_url,
            "itc": bool(r["is_itc"]),
            "deal_count": deal_count_per_item.get(sku, 1),
        }
        deals.append(deal)

    # Finalize events
    events = []
    for ev in events_map.values():
        ev["product_count"] = len(ev["items"])
        del ev["items"]
        events.append(ev)
    events.sort(key=lambda e: e["deal_count"], reverse=True)

    # Stats
    discounts = [d["discount"] for d in deals if d["discount"] is not None]
    expiring_soon = sum(1 for d in deals if d["days_left"] is not None and d["days_left"] <= 7)
    unique_products = len(set(d["sku"] for d in deals))

    stats = {
        "total": len(deals),
        "products": unique_products,
        "avg_discount": round(sum(discounts) / len(discounts), 1) if discounts else 0,
        "expiring_soon": expiring_soon,
        "buy_count": sum(1 for d in deals if d["signal"] == "buy"),
        "good_count": sum(1 for d in deals if d["signal"] == "good"),
    }

    conn.close()
    return deals, stats, events, sorted(categories_set), sorted(brands_set)


def generate_html(deals, stats, events, categories, brands):
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    cat_options = ""
    for cat in categories:
        cat_escaped = cat.replace('"', '&quot;').replace("'", "\\'")
        cnt = sum(1 for d in deals if d.get("category") == cat)
        cat_options += f'\n    <option value="{cat_escaped}">{cat} ({cnt:,})</option>'

    brand_options = ""
    for br in brands:
        br_escaped = br.replace('"', '&quot;').replace("'", "\\'")
        cnt = sum(1 for d in deals if d.get("brand") == br)
        if cnt > 0:
            brand_options += f'\n    <option value="{br_escaped}">{br} ({cnt:,})</option>'

    events_json = json.dumps(events, separators=(',', ':'))

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ToolPulse — Current Sales</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f1117; color: #e0e0e0; }}

.header {{ background: linear-gradient(135deg, #1a1d29, #2a2d3a); padding: 24px 32px; border-bottom: 1px solid #333; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px; }}
.header h1 {{ font-size: 24px; color: #fff; }}
.header .subtitle {{ color: #888; font-size: 14px; margin-top: 4px; }}
.header .nav {{ display: flex; gap: 12px; }}
.header .nav a {{ color: #6c9fff; text-decoration: none; padding: 6px 14px; border: 1px solid #333; border-radius: 6px; font-size: 13px; }}
.header .nav a:hover {{ background: #1e2030; border-color: #6c9fff; }}

.stats-bar {{ display: flex; gap: 24px; padding: 16px 32px; background: #161822; border-bottom: 1px solid #282a36; flex-wrap: wrap; }}
.stat {{ text-align: center; }}
.stat .num {{ font-size: 28px; font-weight: 700; color: #7ddf64; }}
.stat .num.blue {{ color: #6c9fff; }}
.stat .num.orange {{ color: #f0c040; }}
.stat .num.pink {{ color: #ff6b9d; }}
.stat .label {{ font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 0.5px; }}

.events-section {{ padding: 20px 32px; background: #13151f; border-bottom: 1px solid #282a36; }}
.events-section h2 {{ font-size: 16px; color: #fff; margin-bottom: 12px; }}
.events-row {{ display: flex; gap: 12px; overflow-x: auto; padding-bottom: 8px; scroll-behavior: smooth; }}
.events-row::-webkit-scrollbar {{ height: 6px; }}
.events-row::-webkit-scrollbar-track {{ background: #1e2030; border-radius: 3px; }}
.events-row::-webkit-scrollbar-thumb {{ background: #444; border-radius: 3px; }}
.event-card {{ min-width: 220px; max-width: 260px; background: #1e2030; border: 1px solid #333; border-radius: 10px; padding: 14px 16px; cursor: pointer; transition: all 0.2s; flex-shrink: 0; }}
.event-card:hover {{ border-color: #6c9fff; background: #242640; }}
.event-card.active {{ border-color: #7ddf64; background: #1a2a1a; }}
.event-card .event-name {{ font-size: 14px; font-weight: 600; color: #fff; margin-bottom: 6px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
.event-card .event-dates {{ font-size: 11px; color: #888; margin-bottom: 8px; }}
.event-card .event-stats {{ display: flex; gap: 12px; }}
.event-card .event-stat {{ text-align: center; }}
.event-card .event-stat .ev-num {{ font-size: 18px; font-weight: 700; color: #6c9fff; }}
.event-card .event-stat .ev-label {{ font-size: 10px; color: #666; text-transform: uppercase; }}

.controls {{ padding: 16px 32px; background: #161822; border-bottom: 1px solid #282a36; display: flex; gap: 12px; flex-wrap: wrap; align-items: center; }}
.controls input {{ background: #1e2030; border: 1px solid #333; color: #e0e0e0; padding: 8px 14px; border-radius: 6px; font-size: 14px; width: 300px; }}
.controls input:focus {{ outline: none; border-color: #6c9fff; }}
.controls select {{ background: #1e2030; border: 1px solid #333; color: #e0e0e0; padding: 8px 14px; border-radius: 6px; font-size: 14px; }}
.controls .count {{ color: #888; font-size: 13px; margin-left: auto; }}
.controls .clear-event {{ background: #3a1a1a; color: #ff6b6b; border: 1px solid #5a2d2d; padding: 6px 12px; border-radius: 6px; font-size: 13px; cursor: pointer; display: none; }}
.controls .clear-event:hover {{ background: #4a2020; }}

.container {{ padding: 16px 32px; }}

table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
thead th {{ background: #1e2030; padding: 10px 12px; text-align: left; font-weight: 600; color: #aaa; border-bottom: 2px solid #333; cursor: pointer; user-select: none; white-space: nowrap; }}
thead th:hover {{ color: #6c9fff; }}
thead th.sorted-asc::after {{ content: ' \\25B2'; color: #6c9fff; }}
thead th.sorted-desc::after {{ content: ' \\25BC'; color: #6c9fff; }}
tbody tr {{ border-bottom: 1px solid #222; }}
tbody tr:hover {{ background: #1e2030; }}
td {{ padding: 8px 12px; vertical-align: top; }}
td.price {{ font-family: 'SF Mono', monospace; text-align: right; white-space: nowrap; }}
td.num {{ text-align: center; }}
.sku {{ color: #6c9fff; font-weight: 600; font-family: 'SF Mono', monospace; font-size: 11px; }}
.brand {{ color: #888; font-size: 12px; }}
a.product-link {{ color: #6c9fff; text-decoration: none; }}
a.product-link:hover {{ text-decoration: underline; }}
a.coupon-link {{ color: #f0c040; text-decoration: none; font-size: 11px; }}
a.coupon-link:hover {{ text-decoration: underline; }}

/* Signal badges */
.signal {{ padding: 3px 10px; border-radius: 10px; font-size: 11px; font-weight: 700; display: inline-block; white-space: nowrap; }}
.signal.buy {{ background: #1a3a1a; color: #4ade80; border: 1px solid #2d5a1e; }}
.signal.good {{ background: #3a3a1e; color: #f0c040; border: 1px solid #5a5a2e; }}
.signal.wait {{ background: #2a2a30; color: #888; border: 1px solid #3a3a40; }}

/* Discount & vs-best badges */
.discount-badge {{ padding: 3px 10px; border-radius: 10px; font-size: 12px; font-weight: 700; display: inline-block; }}
.discount-badge.great {{ background: #2d5a1e; color: #7ddf64; }}
.discount-badge.good {{ background: #3a3a1e; color: #f0c040; }}
.discount-badge.ok {{ background: #2a2a30; color: #aaa; }}

.vs-badge {{ padding: 2px 8px; border-radius: 8px; font-size: 11px; font-weight: 600; display: inline-block; }}
.vs-badge.best {{ background: #1a3a1a; color: #4ade80; }}
.vs-badge.close {{ background: #3a3a1e; color: #f0c040; }}
.vs-badge.far {{ background: #3a1a1a; color: #ff6b6b; }}

.reg-price {{ color: #666; text-decoration: line-through; font-size: 12px; }}
.deal-price {{ color: #7ddf64; font-weight: 700; font-size: 15px; font-family: 'SF Mono', monospace; }}

.expires {{ font-size: 12px; color: #888; }}
.expires.soon {{ color: #ff6b6b; font-weight: 600; }}
.expires.ok {{ color: #f0c040; }}

.freq {{ font-size: 11px; color: #666; }}
.itc-tag {{ display: inline-block; padding: 1px 5px; border-radius: 4px; font-size: 10px; font-weight: 600; background: #3a1a3a; color: #d07ddf; border: 1px solid #5a2d5a; margin-left: 4px; }}
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>ToolPulse — Current Sales</h1>
    <div class="subtitle">What's on sale right now &amp; how good is it? &mdash; Generated {generated}</div>
  </div>
  <div class="nav">
    <a href="index.html">Products &amp; Prices</a>
    <a href="deals.html">Deal History</a>
  </div>
</div>

<div class="stats-bar">
  <div class="stat"><div class="num">{stats['total']:,}</div><div class="label">Active Deals</div></div>
  <div class="stat"><div class="num blue">{stats['products']:,}</div><div class="label">Products</div></div>
  <div class="stat"><div class="num orange">{stats['avg_discount']}%</div><div class="label">Avg Discount</div></div>
  <div class="stat"><div class="num">{stats['buy_count']:,}</div><div class="label">Buy Now</div></div>
  <div class="stat"><div class="num orange">{stats['good_count']:,}</div><div class="label">Good Deals</div></div>
  <div class="stat"><div class="num pink">{stats['expiring_soon']:,}</div><div class="label">Expiring &lt;7 Days</div></div>
</div>

<div class="events-section">
  <h2>Active Sale Events</h2>
  <div class="events-row" id="eventsRow"></div>
</div>

<div class="controls">
  <input type="text" id="search" placeholder="Search by SKU, product name, or brand..." autofocus>
  <select id="signalFilter">
    <option value="all">All Signals</option>
    <option value="buy">Buy Now</option>
    <option value="good">Good Deal</option>
    <option value="wait">Wait</option>
  </select>
  <select id="categoryFilter">
    <option value="all">All Categories</option>{cat_options}
  </select>
  <select id="brandFilter">
    <option value="all">All Brands</option>{brand_options}
  </select>
  <select id="sortSelect">
    <option value="signal">Sort: Signal (Best First)</option>
    <option value="discount">Sort: Discount %</option>
    <option value="vs_1y">Sort: vs Best Deal (1Y)</option>
    <option value="price">Sort: Price (Low to High)</option>
    <option value="days_left">Sort: Expiring Soon</option>
    <option value="name">Sort: Name</option>
  </select>
  <button class="clear-event" id="clearEvent">Clear Event Filter</button>
  <div class="count" id="count"></div>
</div>

<div class="container">
  <table>
    <thead>
      <tr>
        <th data-col="signal">Signal</th>
        <th data-col="name">Product</th>
        <th data-col="price">Deal Price</th>
        <th data-col="discount">Discount</th>
        <th data-col="vs_1y">vs Best (1Y)</th>
        <th data-col="vs_ever">vs Best Ever</th>
        <th data-col="days_left">Expires</th>
        <th data-col="deal_count">Frequency</th>
        <th>Links</th>
      </tr>
    </thead>
    <tbody id="tbody"></tbody>
  </table>
</div>

<script>
const DEALS = {json.dumps(deals, separators=(',', ':'))};
const EVENTS = {events_json};

let sortCol = 'signal';
let sortDir = 1;
let signalFilter = 'all';
let categoryFilter = 'all';
let brandFilter = 'all';
let eventFilter = null;
let searchTerm = '';

function fmt(v) {{ return v != null ? '$' + v.toFixed(2) : '\\u2014'; }}

const signalOrder = {{'buy': 0, 'good': 1, 'wait': 2}};

// ── Events ──────────────────────────────────────────
function renderEvents() {{
  const row = document.getElementById('eventsRow');
  row.innerHTML = EVENTS.map((ev, i) => {{
    const isActive = eventFilter === ev.url;
    const cls = isActive ? 'event-card active' : 'event-card';
    const endDate = ev.end || 'No date';
    return `<div class="${{cls}}" onclick="toggleEvent(${{i}})">
      <div class="event-name" title="${{ev.name}}">${{ev.name}}</div>
      <div class="event-dates">Ends: ${{endDate}}</div>
      <div class="event-stats">
        <div class="event-stat"><div class="ev-num">${{ev.deal_count}}</div><div class="ev-label">Deals</div></div>
        <div class="event-stat"><div class="ev-num">${{ev.product_count}}</div><div class="ev-label">Products</div></div>
      </div>
    </div>`;
  }}).join('');
}}

function toggleEvent(idx) {{
  const ev = EVENTS[idx];
  eventFilter = eventFilter === ev.url ? null : ev.url;
  document.getElementById('clearEvent').style.display = eventFilter ? 'inline-block' : 'none';
  renderEvents();
  renderTable();
}}

document.getElementById('clearEvent').addEventListener('click', () => {{
  eventFilter = null;
  document.getElementById('clearEvent').style.display = 'none';
  renderEvents();
  renderTable();
}});

function getFiltered() {{
  let items = DEALS;

  if (signalFilter !== 'all') {{
    items = items.filter(d => d.signal === signalFilter);
  }}
  if (categoryFilter !== 'all') {{
    items = items.filter(d => d.category === categoryFilter);
  }}
  if (brandFilter !== 'all') {{
    items = items.filter(d => d.brand === brandFilter);
  }}
  if (eventFilter) {{
    items = items.filter(d => d.source_url === eventFilter);
  }}
  if (searchTerm) {{
    const q = searchTerm.toLowerCase();
    items = items.filter(d =>
      d.sku.includes(q) ||
      (d.name && d.name.toLowerCase().includes(q)) ||
      (d.brand && d.brand.toLowerCase().includes(q))
    );
  }}
  return items;
}}

function renderTable() {{
  let items = getFiltered();

  items.sort((a, b) => {{
    if (sortCol === 'signal') {{
      const ao = signalOrder[a.signal] ?? 3, bo = signalOrder[b.signal] ?? 3;
      if (ao !== bo) return sortDir * (ao - bo);
      // Secondary: discount descending
      return (b.discount || 0) - (a.discount || 0);
    }}
    let av = a[sortCol], bv = b[sortCol];
    if (av == null) av = sortDir > 0 ? Infinity : -Infinity;
    if (bv == null) bv = sortDir > 0 ? Infinity : -Infinity;
    if (typeof av === 'string') return sortDir * av.localeCompare(bv);
    return sortDir * (av - bv);
  }});

  const tbody = document.getElementById('tbody');
  const showing = items.slice(0, 1000);
  tbody.innerHTML = showing.map(d => {{
    // Signal badge
    const sigLabels = {{'buy': 'BUY NOW', 'good': 'GOOD DEAL', 'wait': 'WAIT'}};
    const sigBadge = `<span class="signal ${{d.signal}}">${{sigLabels[d.signal] || 'WAIT'}}</span>`;

    // Product name + brand + sku
    const nameLink = d.hf_url
      ? `<a href="${{d.hf_url}}" target="_blank" class="product-link">${{d.name || 'SKU ' + d.sku}}</a>`
      : (d.name || '<em>Unknown</em>');
    const brandHtml = d.brand ? `<div class="brand">${{d.brand}}</div>` : '';
    const skuHtml = `<span class="sku">${{d.sku}}</span>`;
    const itcHtml = d.itc ? '<span class="itc-tag">ITC</span>' : '';

    // Price column
    const regHtml = d.reg_price != null ? `<div class="reg-price">${{fmt(d.reg_price)}}</div>` : '';
    const priceHtml = `<span class="deal-price">${{fmt(d.price)}}</span>${{regHtml}}`;

    // Discount badge
    let discBadge = '\\u2014';
    if (d.discount != null) {{
      const cls = d.discount >= 25 ? 'great' : d.discount >= 10 ? 'good' : 'ok';
      discBadge = `<span class="discount-badge ${{cls}}">${{d.discount}}% off</span>`;
    }}

    // vs Best 1Y
    let vs1yHtml = '\\u2014';
    if (d.vs_1y != null) {{
      if (d.vs_1y <= 0) {{
        vs1yHtml = `<span class="vs-badge best">Best price!</span>`;
      }} else if (d.vs_1y <= 10) {{
        vs1yHtml = `<span class="vs-badge close">+${{d.vs_1y}}%</span>`;
      }} else {{
        vs1yHtml = `<span class="vs-badge far">+${{d.vs_1y}}%</span>`;
      }}
      if (d.best_1y != null) vs1yHtml += `<div style="font-size:10px;color:#666">Best: ${{fmt(d.best_1y)}}</div>`;
    }}

    // vs Best Ever
    let vsEverHtml = '\\u2014';
    if (d.vs_ever != null) {{
      if (d.vs_ever <= 0) {{
        vsEverHtml = `<span class="vs-badge best">All-time best!</span>`;
      }} else if (d.vs_ever <= 10) {{
        vsEverHtml = `<span class="vs-badge close">+${{d.vs_ever}}%</span>`;
      }} else {{
        vsEverHtml = `<span class="vs-badge far">+${{d.vs_ever}}%</span>`;
      }}
      if (d.best_ever != null) vsEverHtml += `<div style="font-size:10px;color:#666">Best: ${{fmt(d.best_ever)}}</div>`;
    }}

    // Expiration
    let expiresHtml = d.thru || '\\u2014';
    if (d.days_left != null) {{
      const cls = d.days_left <= 3 ? 'expires soon' : d.days_left <= 7 ? 'expires ok' : 'expires';
      expiresHtml = `<div class="${{cls}}">${{d.thru}}</div><div style="font-size:11px;color:#888">${{d.days_left}} day${{d.days_left !== 1 ? 's' : ''}} left</div>`;
    }}

    // Frequency
    const freqHtml = `<span class="freq">${{d.deal_count}} deal${{d.deal_count !== 1 ? 's' : ''}} total</span>`;

    // Links
    const links = [];
    if (d.coupon_url) links.push(`<a href="${{d.coupon_url}}" target="_blank" class="coupon-link">Coupon</a>`);
    if (d.hf_url) links.push(`<a href="${{d.hf_url}}" target="_blank" class="product-link" style="font-size:11px">Product</a>`);

    return `<tr>
      <td class="num">${{sigBadge}}</td>
      <td>${{nameLink}}${{itcHtml}}${{brandHtml}}<div>${{skuHtml}}</div></td>
      <td class="price">${{priceHtml}}</td>
      <td class="num">${{discBadge}}</td>
      <td class="num">${{vs1yHtml}}</td>
      <td class="num">${{vsEverHtml}}</td>
      <td>${{expiresHtml}}</td>
      <td class="num">${{freqHtml}}</td>
      <td>${{links.join(' ')}}</td>
    </tr>`;
  }}).join('');

  document.getElementById('count').textContent =
    items.length + ' deal' + (items.length !== 1 ? 's' : '') +
    (items.length > 1000 ? ' (showing first 1,000)' : '');
}}

// Column sorting
document.querySelectorAll('thead th').forEach(th => {{
  th.addEventListener('click', () => {{
    const col = th.dataset.col;
    if (!col) return;
    if (sortCol === col) sortDir *= -1;
    else {{
      sortCol = col;
      sortDir = (col === 'name') ? 1 : (col === 'days_left' || col === 'price') ? 1 : -1;
    }}
    document.querySelectorAll('thead th').forEach(t => t.classList.remove('sorted-asc', 'sorted-desc'));
    th.classList.add(sortDir > 0 ? 'sorted-asc' : 'sorted-desc');
    document.getElementById('sortSelect').value = col;
    renderTable();
  }});
}});

// Sort dropdown
document.getElementById('sortSelect').addEventListener('change', e => {{
  sortCol = e.target.value;
  sortDir = (sortCol === 'name') ? 1 : (sortCol === 'days_left' || sortCol === 'price') ? 1 : -1;
  if (sortCol === 'signal') sortDir = 1;
  document.querySelectorAll('thead th').forEach(t => t.classList.remove('sorted-asc', 'sorted-desc'));
  renderTable();
}});

document.getElementById('search').addEventListener('input', e => {{
  searchTerm = e.target.value;
  renderTable();
}});
document.getElementById('signalFilter').addEventListener('change', e => {{
  signalFilter = e.target.value;
  renderTable();
}});
document.getElementById('categoryFilter').addEventListener('change', e => {{
  categoryFilter = e.target.value;
  renderTable();
}});
document.getElementById('brandFilter').addEventListener('change', e => {{
  brandFilter = e.target.value;
  renderTable();
}});

// Initial render
renderEvents();
renderTable();
</script>
</body>
</html>"""

    return html


def main():
    if not os.path.exists(DB_PATH):
        print(f"Database not found: {DB_PATH}")
        return

    print("Loading active deals from database...")
    deals, stats, events, categories, brands = load_active_deals()
    print(f"\n  Active deals: {stats['total']:,}")
    print(f"  Unique products: {stats['products']:,}")
    print(f"  Avg discount: {stats['avg_discount']}%")
    print(f"  Buy Now signals: {stats['buy_count']:,}")
    print(f"  Good Deal signals: {stats['good_count']:,}")
    print(f"  Expiring <7 days: {stats['expiring_soon']:,}")
    print(f"  Sale events: {len(events):,}")
    print(f"  Categories: {len(categories):,}")
    print(f"  Brands: {len(brands):,}")

    print("\nGenerating current sales HTML...")
    html = generate_html(deals, stats, events, categories, brands)

    with open(OUT_PATH, "w") as f:
        f.write(html)

    size_kb = os.path.getsize(OUT_PATH) / 1024
    print(f"  Saved: {OUT_PATH} ({size_kb:.0f} KB)")
    print(f"\n  Open with: open current-sales.html")


if __name__ == "__main__":
    main()
