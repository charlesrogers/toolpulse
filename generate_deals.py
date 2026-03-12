#!/usr/bin/env python3
"""
ToolPulse: Generate a self-contained Deals Explorer page.

Shows every deal with discount percentages, interactive histogram with
hoverable/clickable dots, sale events leaderboard, category/brand filters,
and a full sortable deals table.

Usage:
    python3 generate_deals.py
"""

import json
import os
import re
import sqlite3
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "data", "toolpulse.db")
OUT_PATH = os.path.join(BASE_DIR, "deals.html")


def parse_event_name(source_url):
    """Extract a human-readable event name from a source URL slug."""
    if not source_url:
        return "Unknown Event"

    # Extract the slug from the URL (last path segment)
    slug = source_url.rstrip("/").split("/")[-1]

    # Remove common date suffixes: thru-4-9, valid-through-11-22, now-thru-10-13, etc.
    slug = re.sub(r'[-_](extended-)?thru[-_]\d{1,2}[-_]\d{1,2}$', '', slug)
    slug = re.sub(r'[-_]valid[-_]through[-_]\d{1,2}[-_]\d{1,2}$', '', slug)
    slug = re.sub(r'[-_]now[-_]thru[-_]\d{1,2}[-_]\d{1,2}$', '', slug)
    slug = re.sub(r'[-_]thru[-_]\d{1,2}[-_]\d{1,2}[-_]\d{2,4}$', '', slug)
    slug = re.sub(r'[-_]ends[-_]\d{1,2}[-_]\d{1,2}$', '', slug)
    # Remove promo codes at the end
    slug = re.sub(r'[-_]\d{8,}$', '', slug)

    # Convert slug to title case
    name = slug.replace("-", " ").replace("_", " ").strip()
    name = name.title()

    # Clean up common patterns
    name = name.replace("Itc", "ITC")
    name = name.replace("Inside Track Club Member Deals", "Inside Track Club")
    name = name.replace("Instant Savings Items On Sale", "Instant Savings")
    name = name.replace("Black Friday Sale Extended", "Black Friday Sale")
    name = name.replace("Parking Lot Sale Extended", "Parking Lot Sale")

    # Truncate if too long
    if len(name) > 40:
        name = name[:37] + "..."

    return name or "Unknown Event"


def load_deals():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Load all price snapshots grouped by item_number for nearest-price lookup
    print("  Loading price snapshots for nearest-price matching...")
    price_rows = conn.execute("""
        SELECT item_number, regular_price, snapshot_date
        FROM price_snapshots
        WHERE regular_price IS NOT NULL
        ORDER BY item_number, snapshot_date
    """).fetchall()

    # Build lookup: item_number -> list of (date_str, price)
    price_history = {}
    for pr in price_rows:
        sku = pr["item_number"]
        if sku not in price_history:
            price_history[sku] = []
        price_history[sku].append((pr["snapshot_date"] or "", pr["regular_price"]))
    print(f"  Price history loaded for {len(price_history):,} products ({len(price_rows):,} snapshots)")

    # Get all deals with product info
    rows = conn.execute("""
        SELECT
            d.id,
            d.item_number,
            d.deal_price,
            d.coupon_code,
            d.promo_id,
            d.is_itc,
            d.valid_from,
            d.valid_through,
            d.source,
            d.source_url,
            d.coupon_url,
            p.product_name,
            p.brand,
            p.hf_url,
            p.category_path
        FROM deals d
        JOIN products p ON d.item_number = p.item_number
        ORDER BY d.valid_from DESC, d.deal_price ASC
    """).fetchall()

    # Sale events leaderboard
    print("  Loading sale events...")
    event_rows = conn.execute("""
        SELECT source_url, COUNT(*) as deal_count,
               COUNT(DISTINCT item_number) as product_count,
               MIN(valid_from) as start_date, MAX(valid_through) as end_date,
               AVG(deal_price) as avg_deal_price,
               GROUP_CONCAT(DISTINCT source) as sources
        FROM deals WHERE source_url IS NOT NULL AND source_url != ''
        GROUP BY source_url ORDER BY MAX(valid_from) DESC
    """).fetchall()

    events = []
    for er in event_rows:
        events.append({
            "url": er["source_url"],
            "name": parse_event_name(er["source_url"]),
            "deal_count": er["deal_count"],
            "product_count": er["product_count"],
            "start": er["start_date"] or "",
            "end": er["end_date"] or "",
            "avg_price": round(er["avg_deal_price"], 2) if er["avg_deal_price"] else None,
            "sources": er["sources"] or "",
        })
    print(f"  Found {len(events):,} sale events")

    # Deal counts per product
    deal_counts = {}
    for r in conn.execute(
        "SELECT item_number, COUNT(*) as cnt FROM deals GROUP BY item_number"
    ).fetchall():
        deal_counts[r["item_number"]] = r["cnt"]

    # Stats
    stats = {
        "total_deals": len(rows),
        "unique_products": len(deal_counts),
        "with_discount": 0,
        "avg_discount": 0,
        "max_discount": 0,
        "total_sources": {},
    }

    deals = []
    discount_sum = 0
    discount_count = 0
    categories_set = set()
    brands_set = set()

    for idx, r in enumerate(rows):
        if idx % 500 == 0:
            print(f"  Processing deal {idx+1:,}/{len(rows):,}...")

        sku = r["item_number"]
        deal_date = r["valid_from"] or r["valid_through"] or ""

        # Find nearest-in-time regular price
        nearest_price = None
        if sku in price_history:
            snapshots = price_history[sku]
            if deal_date:
                # Find snapshot closest in time to deal date
                best_dist = float('inf')
                for snap_date, snap_price in snapshots:
                    try:
                        dist = abs(_julian(snap_date) - _julian(deal_date))
                    except (ValueError, TypeError):
                        continue
                    if dist < best_dist:
                        best_dist = dist
                        nearest_price = snap_price
            else:
                # No deal date — use latest snapshot
                nearest_price = snapshots[-1][1] if snapshots else None

        # Extract top-level category
        cat_path = r["category_path"] or ""
        top_cat = cat_path.split(" > ")[0].strip() if cat_path else ""
        if top_cat:
            categories_set.add(top_cat)

        brand = r["brand"] or ""
        if brand:
            brands_set.add(brand)

        deal = {
            "id": r["id"],
            "sku": sku,
            "price": r["deal_price"],
            "code": r["coupon_code"],
            "promo": r["promo_id"],
            "itc": bool(r["is_itc"]),
            "from": r["valid_from"],
            "thru": r["valid_through"],
            "source": r["source"],
            "source_url": r["source_url"],
            "coupon_url": r["coupon_url"],
            "name": r["product_name"] or "",
            "brand": brand,
            "hf_url": r["hf_url"] or "",
            "avg_price": nearest_price,
            "deal_count": deal_counts.get(sku, 1),
            "category": top_cat,
        }

        # Calculate discount percentage against nearest regular price
        if nearest_price and nearest_price > 0 and r["deal_price"] is not None:
            pct = round((1 - r["deal_price"] / nearest_price) * 100, 1)
            if 0 < pct <= 100:
                deal["discount"] = pct
                discount_sum += pct
                discount_count += 1
                stats["with_discount"] += 1
                if pct > stats["max_discount"]:
                    stats["max_discount"] = pct

        # Source counts
        src = r["source"] or "unknown"
        stats["total_sources"][src] = stats["total_sources"].get(src, 0) + 1

        deals.append(deal)

    if discount_count > 0:
        stats["avg_discount"] = round(discount_sum / discount_count, 1)

    conn.close()
    return deals, stats, events, sorted(categories_set), sorted(brands_set)


def _julian(date_str):
    """Convert date string to a numeric day value for distance comparison."""
    if not date_str:
        raise ValueError("empty date")
    # Handle various date formats
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(date_str[:10], fmt)
            return dt.toordinal()
        except ValueError:
            continue
    raise ValueError(f"unparseable date: {date_str}")


def generate_html(deals, stats, events, categories, brands):
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Build category and brand option HTML
    cat_options = ""
    for cat in categories:
        cat_escaped = cat.replace('"', '&quot;').replace("'", "\\'")
        deal_count = sum(1 for d in deals if d.get("category") == cat)
        cat_options += f'\n    <option value="{cat_escaped}">{cat} ({deal_count:,})</option>'

    brand_options = ""
    for br in brands:
        br_escaped = br.replace('"', '&quot;').replace("'", "\\'")
        deal_count = sum(1 for d in deals if d.get("brand") == br)
        if deal_count > 0:
            brand_options += f'\n    <option value="{br_escaped}">{br} ({deal_count:,})</option>'

    # Build events JSON for JS
    events_json = json.dumps(events[:50], separators=(',', ':'))  # Top 50 most recent

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ToolPulse — Deal Explorer</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
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

/* Events Leaderboard */
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
.event-card.recent {{ border-color: #2d5a1e; }}

.chart-section {{ padding: 24px 32px; }}
.chart-section h2 {{ font-size: 18px; color: #fff; margin-bottom: 4px; }}
.chart-section .desc {{ color: #888; font-size: 13px; margin-bottom: 16px; }}
.chart-wrapper {{ position: relative; height: 400px; background: #161822; border-radius: 12px; padding: 16px; border: 1px solid #282a36; }}
.chart-wrapper canvas {{ cursor: pointer; }}

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
.sku {{ color: #6c9fff; font-weight: 600; font-family: 'SF Mono', monospace; }}
.brand {{ color: #888; font-size: 12px; }}
.deal-badge {{ background: #2d5a1e; color: #7ddf64; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }}
.discount-badge {{ padding: 3px 10px; border-radius: 10px; font-size: 12px; font-weight: 700; display: inline-block; }}
.discount-badge.great {{ background: #2d5a1e; color: #7ddf64; }}
.discount-badge.good {{ background: #3a3a1e; color: #f0c040; }}
.discount-badge.ok {{ background: #2a2a30; color: #aaa; }}
a.product-link {{ color: #6c9fff; text-decoration: none; }}
a.product-link:hover {{ text-decoration: underline; }}
a.coupon-link {{ color: #f0c040; text-decoration: none; font-size: 11px; }}
a.coupon-link:hover {{ text-decoration: underline; }}
.source-tag {{ display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 10px; font-weight: 600; background: #1e2030; color: #888; border: 1px solid #333; }}
.itc-tag {{ background: #3a1a3a; color: #d07ddf; border-color: #5a2d5a; }}
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>ToolPulse — Deal Explorer</h1>
    <div class="subtitle">Generated {generated}</div>
  </div>
  <div class="nav">
    <a href="index.html">Products &amp; Prices</a>
  </div>
</div>

<div class="stats-bar">
  <div class="stat"><div class="num">{stats['total_deals']:,}</div><div class="label">Total Deals</div></div>
  <div class="stat"><div class="num blue">{stats['unique_products']:,}</div><div class="label">Products with Deals</div></div>
  <div class="stat"><div class="num orange">{stats['avg_discount']}%</div><div class="label">Avg Discount</div></div>
  <div class="stat"><div class="num pink">{stats['max_discount']}%</div><div class="label">Max Discount</div></div>
  <div class="stat"><div class="num blue">{stats['with_discount']:,}</div><div class="label">With Discount Calc</div></div>
</div>

<div class="events-section">
  <h2>Sale Events</h2>
  <div class="events-row" id="eventsRow"></div>
</div>

<div class="chart-section">
  <h2>Discount Distribution</h2>
  <div class="desc">Each dot is a deal. Hover to see price and product. Click to visit Harbor Freight.</div>
  <div class="chart-wrapper">
    <canvas id="discountChart"></canvas>
  </div>
</div>

<div class="controls">
  <input type="text" id="search" placeholder="Search by SKU, product name, or brand..." autofocus>
  <select id="filter">
    <option value="all">All Deals</option>
    <option value="great">50%+ Off</option>
    <option value="good">25-50% Off</option>
    <option value="any-discount">Any Discount</option>
    <option value="itc">Inside Track Only</option>
    <option value="repeat">Repeat Deals (3+)</option>
  </select>
  <select id="sourceFilter">
    <option value="all">All Sources</option>"""

    for src in sorted(stats['total_sources'].keys()):
        cnt = stats['total_sources'][src]
        html += f"""
    <option value="{src}">{src} ({cnt:,})</option>"""

    html += f"""
  </select>
  <select id="categoryFilter">
    <option value="all">All Categories</option>{cat_options}
  </select>
  <select id="brandFilter">
    <option value="all">All Brands</option>{brand_options}
  </select>
  <button class="clear-event" id="clearEvent">Clear Event Filter</button>
  <div class="count" id="count"></div>
</div>

<div class="container">
  <table>
    <thead>
      <tr>
        <th data-col="discount">Discount</th>
        <th data-col="sku">SKU</th>
        <th data-col="name">Product</th>
        <th data-col="brand">Brand</th>
        <th data-col="price">Deal Price</th>
        <th data-col="avg_price">Reg. Price</th>
        <th data-col="from">Date</th>
        <th data-col="deal_count"># Deals</th>
        <th data-col="source">Source</th>
        <th>Links</th>
      </tr>
    </thead>
    <tbody id="tbody"></tbody>
  </table>
</div>

<script>
const DEALS = {json.dumps(deals, separators=(',', ':'))};
const EVENTS = {events_json};

let sortCol = 'discount';
let sortDir = -1;
let currentFilter = 'all';
let sourceFilter = 'all';
let categoryFilter = 'all';
let brandFilter = 'all';
let eventFilter = null; // source_url to filter by
let searchTerm = '';

function fmt(v) {{ return v != null ? '$' + v.toFixed(2) : '\\u2014'; }}

// ── Events Leaderboard ──────────────────────────────────────────
function renderEvents() {{
  const row = document.getElementById('eventsRow');
  row.innerHTML = EVENTS.map((ev, i) => {{
    const isRecent = ev.end && ev.end >= '2025-01-01';
    const isActive = eventFilter === ev.url;
    const classes = ['event-card'];
    if (isRecent) classes.push('recent');
    if (isActive) classes.push('active');
    const dateRange = ev.start && ev.end
      ? ev.start.slice(0, 10) + ' — ' + ev.end.slice(0, 10)
      : (ev.start || ev.end || 'No dates');
    return `<div class="${{classes.join(' ')}}" data-idx="${{i}}" onclick="toggleEvent(${{i}})">
      <div class="event-name" title="${{ev.name}}">${{ev.name}}</div>
      <div class="event-dates">${{dateRange}}</div>
      <div class="event-stats">
        <div class="event-stat"><div class="ev-num">${{ev.deal_count}}</div><div class="ev-label">Deals</div></div>
        <div class="event-stat"><div class="ev-num">${{ev.product_count}}</div><div class="ev-label">Products</div></div>
      </div>
    </div>`;
  }}).join('');
}}

function toggleEvent(idx) {{
  const ev = EVENTS[idx];
  if (eventFilter === ev.url) {{
    eventFilter = null;
  }} else {{
    eventFilter = ev.url;
  }}
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

  if (currentFilter === 'great') items = items.filter(d => d.discount >= 50);
  else if (currentFilter === 'good') items = items.filter(d => d.discount >= 25 && d.discount < 50);
  else if (currentFilter === 'any-discount') items = items.filter(d => d.discount != null);
  else if (currentFilter === 'itc') items = items.filter(d => d.itc);
  else if (currentFilter === 'repeat') items = items.filter(d => d.deal_count >= 3);

  if (sourceFilter !== 'all') {{
    items = items.filter(d => d.source === sourceFilter);
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
    let av = a[sortCol], bv = b[sortCol];
    if (av == null) av = sortDir > 0 ? Infinity : -Infinity;
    if (bv == null) bv = sortDir > 0 ? Infinity : -Infinity;
    if (typeof av === 'string') return sortDir * av.localeCompare(bv);
    return sortDir * (av - bv);
  }});

  const tbody = document.getElementById('tbody');
  const showing = items.slice(0, 1000);
  tbody.innerHTML = showing.map(d => {{
    let discBadge = '';
    if (d.discount != null) {{
      const cls = d.discount >= 50 ? 'great' : d.discount >= 25 ? 'good' : 'ok';
      discBadge = `<span class="discount-badge ${{cls}}">${{d.discount}}%</span>`;
    }}
    const nameLink = d.hf_url
      ? `<a href="${{d.hf_url}}" target="_blank" class="product-link">${{d.name || 'SKU ' + d.sku}}</a>`
      : (d.name || '<em>Unknown</em>');
    const links = [];
    if (d.hf_url) links.push(`<a href="${{d.hf_url}}" target="_blank" class="product-link">Product</a>`);
    if (d.coupon_url) links.push(`<a href="${{d.coupon_url}}" target="_blank" class="coupon-link">Coupon</a>`);
    if (d.source_url) links.push(`<a href="${{d.source_url}}" target="_blank" class="coupon-link">Source</a>`);
    const srcClass = d.itc ? 'source-tag itc-tag' : 'source-tag';
    const srcLabel = d.itc ? 'ITC' : (d.source || '');
    return `<tr>
      <td class="num">${{discBadge}}</td>
      <td class="sku">${{d.sku}}</td>
      <td>${{nameLink}}</td>
      <td class="brand">${{d.brand}}</td>
      <td class="price">${{fmt(d.price)}}</td>
      <td class="price">${{d.avg_price != null ? fmt(d.avg_price) : '\\u2014'}}</td>
      <td>${{d.from || d.thru || ''}}</td>
      <td class="num"><span class="deal-badge">${{d.deal_count}}</span></td>
      <td><span class="${{srcClass}}">${{srcLabel}}</span></td>
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
    else {{ sortCol = col; sortDir = col === 'name' || col === 'brand' || col === 'sku' || col === 'from' || col === 'source' ? 1 : -1; }}
    document.querySelectorAll('thead th').forEach(t => t.classList.remove('sorted-asc', 'sorted-desc'));
    th.classList.add(sortDir > 0 ? 'sorted-asc' : 'sorted-desc');
    renderTable();
  }});
}});

document.getElementById('search').addEventListener('input', e => {{
  searchTerm = e.target.value;
  renderTable();
}});
document.getElementById('filter').addEventListener('change', e => {{
  currentFilter = e.target.value;
  renderTable();
}});
document.getElementById('sourceFilter').addEventListener('change', e => {{
  sourceFilter = e.target.value;
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

// ── Discount Scatter Chart ──────────────────────────────────────────
function buildChart() {{
  const withDiscount = DEALS.filter(d => d.discount != null && d.discount > 0 && d.discount <= 100);

  // Build histogram buckets (0-5, 5-10, ..., 70-75)
  const bucketSize = 5;
  const buckets = {{}};
  for (let i = 0; i <= 75; i += bucketSize) buckets[i] = 0;
  withDiscount.forEach(d => {{
    const b = Math.floor(d.discount / bucketSize) * bucketSize;
    const key = Math.min(b, 75);
    buckets[key] = (buckets[key] || 0) + 1;
  }});

  const bucketLabels = Object.keys(buckets).map(Number).sort((a,b) => a - b);
  const barData = bucketLabels.map(b => buckets[b]);

  // Build scatter points — one per deal, x = discount %, y = jittered within bucket height
  const bucketGroups = {{}};
  withDiscount.forEach(d => {{
    const b = Math.min(Math.floor(d.discount / bucketSize) * bucketSize, 75);
    if (!bucketGroups[b]) bucketGroups[b] = [];
    bucketGroups[b].push(d);
  }});

  const scatterData = [];
  Object.entries(bucketGroups).forEach(([bucket, deals]) => {{
    const maxH = buckets[Number(bucket)];
    deals.forEach((d, i) => {{
      const yBase = (i + 0.5) / deals.length * maxH;
      scatterData.push({{
        x: d.discount,
        y: yBase,
        deal: d,
      }});
    }});
  }});

  const ctx = document.getElementById('discountChart').getContext('2d');

  const chart = new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels: bucketLabels.map(b => b + '-' + (b + bucketSize) + '%'),
      datasets: [
        {{
          type: 'bar',
          label: 'Deals per bucket',
          data: barData,
          backgroundColor: barData.map((_, i) => {{
            const pct = bucketLabels[i];
            if (pct >= 50) return 'rgba(125,223,100,0.25)';
            if (pct >= 25) return 'rgba(240,192,64,0.2)';
            return 'rgba(108,159,255,0.15)';
          }}),
          borderColor: barData.map((_, i) => {{
            const pct = bucketLabels[i];
            if (pct >= 50) return 'rgba(125,223,100,0.5)';
            if (pct >= 25) return 'rgba(240,192,64,0.4)';
            return 'rgba(108,159,255,0.3)';
          }}),
          borderWidth: 1,
          barPercentage: 1.0,
          categoryPercentage: 1.0,
          order: 2,
        }},
        {{
          type: 'scatter',
          label: 'Individual deals',
          data: scatterData,
          backgroundColor: scatterData.map(pt => {{
            if (pt.deal.discount >= 50) return '#7ddf64';
            if (pt.deal.discount >= 25) return '#f0c040';
            return '#6c9fff';
          }}),
          borderColor: 'transparent',
          pointRadius: 3.5,
          pointHoverRadius: 7,
          pointHoverBackgroundColor: '#fff',
          order: 1,
        }},
      ],
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      interaction: {{
        mode: 'point',
        intersect: true,
      }},
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          filter: function(tooltipItem) {{
            return tooltipItem.datasetIndex === 1;
          }},
          callbacks: {{
            title: function(items) {{
              if (!items.length) return '';
              const pt = items[0].raw;
              if (!pt || !pt.deal) return '';
              return pt.deal.name || 'SKU ' + pt.deal.sku;
            }},
            label: function(ctx) {{
              const pt = ctx.raw;
              if (!pt || !pt.deal) return '';
              const d = pt.deal;
              const lines = [];
              lines.push('Deal: ' + fmt(d.price) + ' (' + d.discount + '% off)');
              if (d.avg_price) lines.push('Regular: ~' + fmt(d.avg_price));
              if (d.from) lines.push('Date: ' + d.from);
              if (d.brand) lines.push('Brand: ' + d.brand);
              lines.push('Click to visit product page');
              return lines;
            }},
          }},
        }},
      }},
      scales: {{
        x: {{
          type: 'category',
          ticks: {{ color: '#888' }},
          grid: {{ color: '#282a36' }},
          title: {{ display: true, text: 'Discount %', color: '#aaa' }},
        }},
        y: {{
          ticks: {{ color: '#888' }},
          grid: {{ color: '#282a36' }},
          title: {{ display: true, text: '# of Deals', color: '#aaa' }},
        }},
      }},
      onClick: function(event, elements) {{
        if (elements.length > 0 && elements[0].datasetIndex === 1) {{
          const pt = scatterData[elements[0].index];
          if (pt && pt.deal && pt.deal.hf_url) {{
            window.open(pt.deal.hf_url, '_blank');
          }}
        }}
      }},
    }},
  }});

  // Map scatter x to category index with jitter
  scatterData.forEach(pt => {{
    const bucketIdx = Math.min(Math.floor(pt.deal.discount / bucketSize) * bucketSize, 75);
    pt.x = bucketLabels.indexOf(bucketIdx);
    pt.x += (Math.random() - 0.5) * 0.8;
  }});
  chart.update();
}}

// Initial render
renderEvents();
renderTable();
buildChart();
</script>
</body>
</html>"""

    return html


def main():
    if not os.path.exists(DB_PATH):
        print(f"Database not found: {DB_PATH}")
        return

    print("Loading deals from database...")
    deals, stats, events, categories, brands = load_deals()
    print(f"  Total deals: {stats['total_deals']:,}")
    print(f"  Unique products: {stats['unique_products']:,}")
    print(f"  With discount calc: {stats['with_discount']:,}")
    print(f"  Avg discount: {stats['avg_discount']}%")
    print(f"  Max discount: {stats['max_discount']}%")
    print(f"  Sale events: {len(events):,}")
    print(f"  Categories: {len(categories):,}")
    print(f"  Brands: {len(brands):,}")
    for src, cnt in sorted(stats['total_sources'].items(), key=lambda x: -x[1]):
        print(f"    {src}: {cnt:,}")

    print("Generating deals HTML...")
    html = generate_html(deals, stats, events, categories, brands)

    with open(OUT_PATH, "w") as f:
        f.write(html)

    size_kb = os.path.getsize(OUT_PATH) / 1024
    print(f"  Saved: {OUT_PATH} ({size_kb:.0f} KB)")
    print(f"\n  Open with: open deals.html")


if __name__ == "__main__":
    main()
