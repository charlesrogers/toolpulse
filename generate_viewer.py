#!/usr/bin/env python3
"""
ToolPulse: Generate a self-contained HTML viewer for all SKU and price data.

Reads from toolpulse.db and produces viewer.html with:
  - Searchable/sortable product table
  - Price history charts (inline Chart.js)
  - Deal history per product
  - Summary stats

Usage:
    python3 generate_viewer.py
"""

import json
import os
import re
import sqlite3
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "data", "toolpulse.db")
OUT_PATH = os.path.join(BASE_DIR, "viewer.html")


def load_data():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Products with aggregated data
    products = conn.execute("""
        SELECT
            p.item_number,
            p.product_name,
            p.brand,
            p.hf_url,
            p.is_active,
            p.first_seen,
            p.last_seen,
            COUNT(DISTINCT ps.id) AS snapshot_count,
            COUNT(DISTINCT d.id) AS deal_count,
            MIN(ps.regular_price) AS min_price,
            MAX(ps.regular_price) AS max_price,
            MIN(d.deal_price) AS best_deal,
            MAX(ps.snapshot_date) AS latest_snapshot_date
        FROM products p
        LEFT JOIN price_snapshots ps ON p.item_number = ps.item_number
        LEFT JOIN deals d ON p.item_number = d.item_number
        GROUP BY p.item_number
        ORDER BY
            (COUNT(DISTINCT ps.id) + COUNT(DISTINCT d.id)) DESC,
            p.product_name
    """).fetchall()

    # Price snapshots
    snapshots = conn.execute("""
        SELECT item_number, snapshot_date, regular_price, sale_price, source
        FROM price_snapshots
        ORDER BY item_number, snapshot_date
    """).fetchall()

    # Deals
    deals = conn.execute("""
        SELECT item_number, deal_price, coupon_code, promo_id, valid_from,
               valid_through, source, coupon_url
        FROM deals
        ORDER BY item_number, valid_through DESC
    """).fetchall()

    stats = {
        "products": conn.execute("SELECT COUNT(*) FROM products").fetchone()[0],
        "with_prices": conn.execute(
            "SELECT COUNT(DISTINCT item_number) FROM price_snapshots"
        ).fetchone()[0],
        "with_deals": conn.execute(
            "SELECT COUNT(DISTINCT item_number) FROM deals"
        ).fetchone()[0],
        "snapshots": conn.execute("SELECT COUNT(*) FROM price_snapshots").fetchone()[0],
        "deals": conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0],
    }

    conn.close()

    # Group snapshots and deals by SKU
    snap_by_sku = {}
    for s in snapshots:
        sku = s["item_number"]
        snap_by_sku.setdefault(sku, []).append({
            "date": s["snapshot_date"],
            "price": s["regular_price"],
            "sale": s["sale_price"],
            "source": s["source"],
        })

    deal_by_sku = {}
    for d in deals:
        sku = d["item_number"]
        deal_by_sku.setdefault(sku, []).append({
            "price": d["deal_price"],
            "code": d["coupon_code"],
            "promo": d["promo_id"],
            "from": d["valid_from"],
            "thru": d["valid_through"],
            "source": d["source"],
            "url": d["coupon_url"],
        })

    product_list = []
    for p in products:
        sku = p["item_number"]
        product_list.append({
            "sku": sku,
            "name": p["product_name"] or "",
            "brand": p["brand"] or "",
            "url": p["hf_url"] or "",
            "active": bool(p["is_active"]),
            "snaps": p["snapshot_count"],
            "deals": p["deal_count"],
            "min": p["min_price"],
            "max": p["max_price"],
            "best_deal": p["best_deal"],
            "snapshots": snap_by_sku.get(sku, []),
            "deal_list": deal_by_sku.get(sku, []),
        })

    # ── Fair Price / Buy Signal ────────────────────────────────────────────
    today = date.today().isoformat()
    for p in product_list:
        prices = [s["price"] for s in p["snapshots"] if s["price"] is not None]
        if len(prices) < 2:
            continue
        # Current price = most recent snapshot
        current = prices[-1]
        hist_avg = sum(prices) / len(prices)
        # Percentile: what % of historical prices are >= current (higher = better buy)
        pctl = round(sum(1 for pr in prices if pr >= current) / len(prices) * 100)
        # Signal
        best_deal = p["best_deal"]
        if pctl >= 80 or (best_deal is not None and current <= best_deal):
            sig = "green"
        elif pctl >= 50:
            sig = "yellow"
        else:
            sig = "red"
        p["cur"] = current
        p["pctl"] = pctl
        p["sig"] = sig
        p["avg"] = round(hist_avg, 2)

    # ── Sale Event Calendar ───────────────────────────────────────────────
    def normalize_date(d):
        """Parse MM/DD/YYYY or YYYY-MM-DD into date object."""
        if not d:
            return None
        try:
            if "/" in d:
                parts = d.split("/")
                if len(parts) == 3:
                    return date(int(parts[2]), int(parts[0]), int(parts[1]))
            elif "-" in d and len(d) >= 10:
                return date.fromisoformat(d[:10])
        except (ValueError, IndexError):
            pass
        return None

    # Group deals by month to detect sale events
    month_deals = defaultdict(set)  # month -> set of item_numbers
    month_prices = defaultdict(list)  # month -> list of deal_prices
    for p in product_list:
        for d in p["deal_list"]:
            dt = normalize_date(d.get("from") or d.get("thru"))
            if dt and dt.year >= 2020:
                month_key = f"{dt.year}-{dt.month:02d}"
                month_deals[month_key].add(p["sku"])
                if d["price"]:
                    month_prices[month_key].append(d["price"])

    # Events = months with 50+ products on deal
    events = []
    month_labels = {1: "New Year", 2: "Presidents' Day", 3: "Spring", 4: "Spring",
                    5: "Memorial Day", 6: "Summer", 7: "4th of July", 8: "Back to School",
                    9: "Fall", 10: "Fall", 11: "Black Friday", 12: "Holiday"}
    for month_key in sorted(month_deals.keys()):
        count = len(month_deals[month_key])
        if count >= 50:
            y, m = month_key.split("-")
            events.append({
                "month": month_key,
                "products": count,
                "label": f"{month_labels.get(int(m), '')} Sale",
            })

    # Predict next event from historical month patterns
    event_months = defaultdict(list)  # month_num -> list of product counts
    for ev in events:
        m = int(ev["month"].split("-")[1])
        event_months[m].append(ev["products"])

    today_dt = date.today()
    next_event = None
    for offset in range(1, 13):
        check = today_dt.replace(day=1) + timedelta(days=32 * offset)
        m = check.month
        if m in event_months:
            avg_products = sum(event_months[m]) // len(event_months[m])
            target = date(check.year, m, 1)
            days_away = (target - today_dt).days
            if days_away > 0:
                next_event = {
                    "month": f"{check.year}-{m:02d}",
                    "label": month_labels.get(m, "Sale"),
                    "avg_products": avg_products,
                    "days_away": days_away,
                    "occurrences": len(event_months[m]),
                }
                break

    stats["events"] = events
    stats["next_event"] = next_event

    # ── Deal Prediction ───────────────────────────────────────────────────
    for p in product_list:
        if len(p["deal_list"]) < 3:
            continue
        deal_dates = []
        for d in p["deal_list"]:
            dt = normalize_date(d.get("from") or d.get("thru"))
            if dt:
                deal_dates.append(dt)
        deal_dates = sorted(set(deal_dates))
        if len(deal_dates) < 3:
            continue
        # Calculate intervals
        intervals = [(deal_dates[i+1] - deal_dates[i]).days for i in range(len(deal_dates)-1)]
        intervals = [iv for iv in intervals if iv > 7]  # Filter out same-event duplicates
        if not intervals:
            continue
        avg_cycle = sum(intervals) // len(intervals)
        days_since = (today_dt - deal_dates[-1]).days
        days_until = avg_cycle - days_since
        p["deal_freq"] = {
            "avg_cycle": avg_cycle,
            "last_deal": deal_dates[-1].isoformat(),
            "days_since": days_since,
            "days_until": days_until,
            "overdue": days_until < 0,
        }

    return product_list, stats


def generate_html(products, stats):
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ToolPulse — Harbor Freight Price Tracker</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f1117; color: #e0e0e0; }}

.header {{ background: linear-gradient(135deg, #1a1d29, #2a2d3a); padding: 24px 32px; border-bottom: 1px solid #333; }}
.header h1 {{ font-size: 24px; color: #fff; }}
.header .subtitle {{ color: #888; font-size: 14px; margin-top: 4px; }}

.stats-bar {{ display: flex; gap: 24px; padding: 16px 32px; background: #161822; border-bottom: 1px solid #282a36; flex-wrap: wrap; }}
.stat {{ text-align: center; }}
.stat .num {{ font-size: 28px; font-weight: 700; color: #6c9fff; }}
.stat .label {{ font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 0.5px; }}

.controls {{ padding: 16px 32px; background: #161822; border-bottom: 1px solid #282a36; display: flex; gap: 12px; flex-wrap: wrap; align-items: center; }}
.controls input {{ background: #1e2030; border: 1px solid #333; color: #e0e0e0; padding: 8px 14px; border-radius: 6px; font-size: 14px; width: 300px; }}
.controls input:focus {{ outline: none; border-color: #6c9fff; }}
.controls select {{ background: #1e2030; border: 1px solid #333; color: #e0e0e0; padding: 8px 14px; border-radius: 6px; font-size: 14px; }}
.controls .count {{ color: #888; font-size: 13px; margin-left: auto; }}

.container {{ padding: 16px 32px; }}

table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
thead th {{ background: #1e2030; padding: 10px 12px; text-align: left; font-weight: 600; color: #aaa; border-bottom: 2px solid #333;
  cursor: pointer; user-select: none; white-space: nowrap; }}
thead th:hover {{ color: #6c9fff; }}
thead th.sorted-asc::after {{ content: ' ▲'; color: #6c9fff; }}
thead th.sorted-desc::after {{ content: ' ▼'; color: #6c9fff; }}
tbody tr {{ border-bottom: 1px solid #222; cursor: pointer; }}
tbody tr:hover {{ background: #1e2030; }}
tbody tr.has-data {{ }}
tbody tr.no-data td {{ color: #555; }}
td {{ padding: 8px 12px; vertical-align: top; }}
td.price {{ font-family: 'SF Mono', monospace; text-align: right; white-space: nowrap; }}
td.num {{ text-align: center; }}
.sku {{ color: #6c9fff; font-weight: 600; font-family: 'SF Mono', monospace; }}
.brand {{ color: #888; font-size: 12px; }}
.deal-badge {{ background: #2d5a1e; color: #7ddf64; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }}
.price-range {{ font-size: 12px; color: #888; }}

/* Detail panel */
.detail-overlay {{ display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.6); z-index: 100; }}
.detail-overlay.open {{ display: flex; justify-content: center; align-items: flex-start; padding-top: 60px; }}
.detail-panel {{ background: #1a1d29; border-radius: 12px; width: 90%; max-width: 900px; max-height: 80vh; overflow-y: auto; padding: 24px; box-shadow: 0 20px 60px rgba(0,0,0,0.5); }}
.detail-panel h2 {{ font-size: 20px; color: #fff; margin-bottom: 4px; }}
.detail-panel .meta {{ color: #888; font-size: 13px; margin-bottom: 16px; }}
.detail-panel .meta a {{ color: #6c9fff; text-decoration: none; }}
.detail-panel .meta a:hover {{ text-decoration: underline; }}
.detail-panel .close-btn {{ float: right; background: none; border: none; color: #888; font-size: 24px; cursor: pointer; }}
.detail-panel .close-btn:hover {{ color: #fff; }}

.chart-container {{ height: 250px; margin-bottom: 24px; }}

.deal-table {{ width: 100%; font-size: 13px; margin-top: 12px; }}
.deal-table th {{ background: #222; padding: 6px 10px; text-align: left; color: #aaa; }}
.deal-table td {{ padding: 6px 10px; border-bottom: 1px solid #282a36; }}
.deal-table a {{ color: #6c9fff; text-decoration: none; }}

.section-title {{ font-size: 14px; font-weight: 600; color: #aaa; margin: 16px 0 8px; text-transform: uppercase; letter-spacing: 0.5px; }}

.no-data-msg {{ color: #555; font-style: italic; padding: 12px 0; }}

/* Event banner */
.event-banner {{ display: flex; align-items: center; gap: 12px; padding: 12px 32px; background: linear-gradient(135deg, #1a2a1a, #1a1d29); border-bottom: 1px solid #2d5a1e; font-size: 14px; flex-wrap: wrap; }}
.event-banner strong {{ color: #7ddf64; }}
.event-icon {{ font-size: 20px; }}
.event-detail {{ color: #888; font-size: 12px; }}

/* Signal dots */
.signal {{ display: inline-block; width: 10px; height: 10px; border-radius: 50%; }}
.signal.green {{ background: #7ddf64; box-shadow: 0 0 6px rgba(125,223,100,0.4); }}
.signal.yellow {{ background: #f0c040; box-shadow: 0 0 6px rgba(240,192,64,0.4); }}
.signal.red {{ background: #ff6b6b; box-shadow: 0 0 6px rgba(255,107,107,0.4); }}
td.signal-cell {{ text-align: center; }}

/* Fair price banner in detail */
.fair-price {{ padding: 14px 18px; border-radius: 8px; margin-bottom: 16px; font-size: 14px; }}
.fair-price.green {{ background: rgba(45,90,30,0.4); border: 1px solid #2d5a1e; }}
.fair-price.yellow {{ background: rgba(60,50,15,0.4); border: 1px solid #5a4a1e; }}
.fair-price.red {{ background: rgba(60,20,20,0.4); border: 1px solid #5a1e1e; }}
.fair-price .big {{ font-size: 18px; font-weight: 700; }}
.fair-price .green {{ color: #7ddf64; }}
.fair-price .yellow {{ color: #f0c040; }}
.fair-price .red {{ color: #ff6b6b; }}

/* Price bar */
.price-bar {{ position: relative; height: 8px; background: #282a36; border-radius: 4px; margin: 10px 0; }}
.price-bar .fill {{ position: absolute; height: 100%; background: linear-gradient(90deg, #7ddf64, #f0c040, #ff6b6b); border-radius: 4px; left: 0; }}
.price-bar .marker {{ position: absolute; top: -4px; width: 3px; height: 16px; background: #fff; border-radius: 2px; }}
.price-bar-labels {{ display: flex; justify-content: space-between; font-size: 11px; color: #888; }}

/* Deal prediction */
.deal-pred {{ padding: 12px 16px; background: #1e2030; border-radius: 8px; margin-bottom: 16px; font-size: 13px; }}
.deal-pred .overdue {{ color: #ff6b6b; font-weight: 600; }}
.deal-pred .upcoming {{ color: #7ddf64; }}

/* Event timeline */
.event-timeline {{ display: flex; gap: 6px; flex-wrap: wrap; padding: 8px 0; }}
.event-block {{ padding: 6px 12px; border-radius: 6px; font-size: 11px; background: #1e2030; border: 1px solid #333; }}
.event-block .count {{ font-weight: 700; color: #6c9fff; }}
</style>
</head>
<body>

<div class="header" style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px">
  <div>
    <h1>ToolPulse — Harbor Freight Price Tracker</h1>
    <div class="subtitle">Generated {generated}</div>
  </div>
  <div style="display:flex;gap:12px">
    <a href="current-sales.html" style="color:#6c9fff;text-decoration:none;padding:6px 14px;border:1px solid #333;border-radius:6px;font-size:13px">Current Sales</a>
    <a href="deals.html" style="color:#6c9fff;text-decoration:none;padding:6px 14px;border:1px solid #333;border-radius:6px;font-size:13px">Deal History</a>
  </div>
</div>

<div class="stats-bar">
  <div class="stat"><div class="num">{stats['products']:,}</div><div class="label">Products</div></div>
  <div class="stat"><div class="num">{stats['with_prices']}</div><div class="label">With Price History</div></div>
  <div class="stat"><div class="num">{stats['with_deals']}</div><div class="label">With Deals</div></div>
  <div class="stat"><div class="num">{stats['snapshots']:,}</div><div class="label">Price Snapshots</div></div>
  <div class="stat"><div class="num">{stats['deals']:,}</div><div class="label">Deals Tracked</div></div>
</div>"""

    # Next event banner
    ne = stats.get("next_event")
    if ne:
        html += f"""
<div class="event-banner">
  <span class="event-icon">📅</span>
  <span>Next predicted sale: <strong>{ne['label']} Sale — {ne['month']}</strong></span>
  <span class="event-detail">~{ne['days_away']} days away · Based on {ne['occurrences']} past event{'s' if ne['occurrences'] > 1 else ''} averaging {ne['avg_products']} products on deal</span>
</div>"""

    html += f"""

<div class="controls">
  <input type="text" id="search" placeholder="Search by SKU, name, or brand..." autofocus>
  <select id="filter">
    <option value="all">All Products</option>
    <option value="has-data" selected>With Price/Deal Data</option>
    <option value="has-prices">With Price History</option>
    <option value="has-deals">With Deals</option>
    <option value="buy-now">🟢 Buy Now (Great Price)</option>
    <option value="overdue">⏰ Overdue for Deal</option>
  </select>
  <div class="count" id="count"></div>
</div>

<div class="container">
  <table>
    <thead>
      <tr>
        <th data-col="sig" title="Buy signal">Signal</th>
        <th data-col="sku">SKU</th>
        <th data-col="name">Product</th>
        <th data-col="brand">Brand</th>
        <th data-col="cur">Current</th>
        <th data-col="min">Range</th>
        <th data-col="best_deal">Best Deal</th>
        <th data-col="deals">Deals</th>
        <th data-col="deal_next">Next Deal</th>
      </tr>
    </thead>
    <tbody id="tbody"></tbody>
  </table>
</div>

<div class="detail-overlay" id="overlay">
  <div class="detail-panel" id="detail"></div>
</div>

<script>
const DATA = {json.dumps(products, separators=(',', ':'))};

let sortCol = 'snaps';
let sortDir = -1;
let currentFilter = 'has-data';
let searchTerm = '';

function formatPrice(v) {{
  return v != null ? '$' + v.toFixed(2) : '—';
}}

function renderTable() {{
  let items = DATA.filter(p => {{
    if (currentFilter === 'has-data') return p.snaps > 0 || p.deals > 0;
    if (currentFilter === 'has-prices') return p.snaps > 0;
    if (currentFilter === 'has-deals') return p.deals > 0;
    if (currentFilter === 'buy-now') return p.sig === 'green';
    if (currentFilter === 'overdue') return p.deal_freq && p.deal_freq.overdue;
    return true;
  }});

  if (searchTerm) {{
    const q = searchTerm.toLowerCase();
    items = items.filter(p =>
      p.sku.includes(q) ||
      (p.name && p.name.toLowerCase().includes(q)) ||
      (p.brand && p.brand.toLowerCase().includes(q))
    );
  }}

  items.sort((a, b) => {{
    let av, bv;
    if (sortCol === 'sig') {{
      const sigOrder = {{green: 0, yellow: 1, red: 2}};
      av = a.sig ? sigOrder[a.sig] : 9;
      bv = b.sig ? sigOrder[b.sig] : 9;
    }} else if (sortCol === 'deal_next') {{
      av = a.deal_freq ? a.deal_freq.days_until : null;
      bv = b.deal_freq ? b.deal_freq.days_until : null;
    }} else {{
      av = a[sortCol]; bv = b[sortCol];
    }}
    if (av == null) av = sortDir > 0 ? Infinity : -Infinity;
    if (bv == null) bv = sortDir > 0 ? Infinity : -Infinity;
    if (typeof av === 'string') return sortDir * av.localeCompare(bv);
    return sortDir * (av - bv);
  }});

  const tbody = document.getElementById('tbody');
  tbody.innerHTML = items.slice(0, 500).map(p => {{
    const hasData = p.snaps > 0 || p.deals > 0;
    const priceRange = p.min != null ? (p.min === p.max ? formatPrice(p.min) : formatPrice(p.min) + '–' + formatPrice(p.max)) : '—';
    const sigDot = p.sig ? `<span class="signal ${{p.sig}}"></span>` : '';
    const curPrice = p.cur != null ? formatPrice(p.cur) : '—';
    let nextDeal = '';
    if (p.deal_freq) {{
      if (p.deal_freq.overdue) nextDeal = `<span style="color:#ff6b6b;font-weight:600">OVERDUE</span>`;
      else nextDeal = `<span style="color:#7ddf64">~${{p.deal_freq.days_until}}d</span>`;
    }}
    return `<tr class="${{hasData ? 'has-data' : 'no-data'}}" onclick="showDetail('${{p.sku}}')">
      <td class="signal-cell">${{sigDot}}</td>
      <td class="sku">${{p.sku}}</td>
      <td>${{p.name || '<em>Unknown</em>'}}</td>
      <td class="brand">${{p.brand}}</td>
      <td class="price">${{curPrice}}</td>
      <td class="price">${{priceRange}}</td>
      <td class="price">${{p.best_deal != null ? formatPrice(p.best_deal) : ''}}</td>
      <td class="num">${{p.deals ? '<span class="deal-badge">' + p.deals + '</span>' : ''}}</td>
      <td class="num">${{nextDeal}}</td>
    </tr>`;
  }}).join('');

  document.getElementById('count').textContent =
    items.length + ' product' + (items.length !== 1 ? 's' : '') +
    (items.length > 500 ? ' (showing first 500)' : '');
}}

// Column sort
document.querySelectorAll('thead th').forEach(th => {{
  th.addEventListener('click', () => {{
    const col = th.dataset.col;
    if (sortCol === col) sortDir *= -1;
    else {{ sortCol = col; sortDir = col === 'name' || col === 'brand' || col === 'sku' ? 1 : -1; }}
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

// Detail panel
let chartInstance = null;

function buildTimeline(p) {{
  const timeline = [];
  // Add price snapshots
  p.snapshots.forEach(s => {{
    timeline.push({{ date: s.date, price: s.price, type: 'regular', source: s.source, sale: s.sale }});
  }});
  // Add deals
  p.deal_list.forEach(d => {{
    const dealDate = d.from || d.thru || '';
    if (dealDate) {{
      timeline.push({{ date: dealDate, price: d.price, type: 'deal', source: d.source, promo: d.promo, code: d.code, thru: d.thru, url: d.url }});
    }}
  }});
  // Sort by date
  timeline.sort((a, b) => a.date.localeCompare(b.date));
  // Annotate changes
  let lastRegular = null;
  timeline.forEach(t => {{
    if (t.type === 'regular') {{
      if (lastRegular === null) {{
        t.change = 'first'; t.label = 'First recorded price';
      }} else if (t.price > lastRegular) {{
        t.change = 'up'; t.delta = t.price - lastRegular; t.label = 'Price increase +' + formatPrice(t.delta);
      }} else if (t.price < lastRegular) {{
        t.change = 'down'; t.delta = lastRegular - t.price; t.label = 'Price decrease -' + formatPrice(t.delta);
      }} else {{
        t.change = 'same'; t.label = 'No change';
      }}
      lastRegular = t.price;
    }} else {{
      t.change = 'deal';
      t.label = 'Deal' + (t.code ? ' (code: ' + t.code + ')' : '') + (t.promo ? ' promo #' + t.promo : '');
      if (lastRegular != null && t.price < lastRegular) {{
        const savings = lastRegular - t.price;
        t.label += ' — save ' + formatPrice(savings) + ' off $' + lastRegular.toFixed(2);
      }}
    }}
  }});
  return timeline;
}}

function showDetail(sku) {{
  const p = DATA.find(x => x.sku === sku);
  if (!p) return;

  const panel = document.getElementById('detail');
  const hfLink = p.url ? `<a href="${{p.url}}" target="_blank">${{p.url}}</a>` : 'No URL';

  // Build unified timeline: merge price snapshots + deals, sorted by date
  const timeline = buildTimeline(p);
  const hasTimeline = timeline.length > 0;

  let timelineHtml = '';
  if (hasTimeline) {{
    const rows = timeline.map(t => {{
      const priceColor = t.type === 'deal' ? 'color:#7ddf64;font-weight:600' : '';
      const rowBg = t.change === 'up' ? 'background:rgba(255,107,107,0.08)' : t.change === 'down' ? 'background:rgba(125,223,100,0.08)' : t.type === 'deal' ? 'background:rgba(240,192,64,0.08)' : '';
      const changeIcon = t.change === 'up' ? '<span style="color:#ff6b6b">&#9650;</span>'
        : t.change === 'down' ? '<span style="color:#7ddf64">&#9660;</span>'
        : t.type === 'deal' ? '<span style="color:#f0c040">&#9733;</span>'
        : t.change === 'first' ? '&#127991;' : '—';
      const typeLabel = t.type === 'deal' ? '<span class="deal-badge">Deal</span>' : t.source === 'wayback' ? 'Wayback' : (t.source || '');
      const linkHtml = t.url ? ' <a href="' + t.url + '" target="_blank" style="color:#6c9fff">[coupon]</a>' : '';
      return `<tr style="${{rowBg}}"><td>${{t.date}}</td><td style="${{priceColor}}">${{formatPrice(t.price)}}</td><td>${{typeLabel}}</td><td>${{changeIcon}}</td><td>${{t.label}}${{linkHtml}}</td></tr>`;
    }}).join('');
    timelineHtml = `<div class="section-title">Price Timeline (${{timeline.length}} events)</div>
      <div class="chart-container"><canvas id="priceChart"></canvas></div>
      <table class="deal-table">
        <thead><tr><th>Date</th><th>Price</th><th>Type</th><th></th><th>Details</th></tr></thead>
        <tbody>${{rows}}</tbody>
      </table>`;
  }} else {{
    timelineHtml = '<div class="section-title">Price Timeline</div><div class="no-data-msg">No price data yet — pending Wayback backfill</div>';
  }}

  // Fair Price analysis
  let fairPriceHtml = '';
  if (p.sig && p.cur != null) {{
    const sigLabels = {{green: 'GREAT PRICE — Buy Now', yellow: 'FAIR PRICE', red: 'WAIT FOR SALE'}};
    const sigDescs = {{
      green: `This <strong>${{formatPrice(p.cur)}}</strong> is cheaper than <strong>${{p.pctl}}%</strong> of prices we've tracked`,
      yellow: `This <strong>${{formatPrice(p.cur)}}</strong> is in the middle of the historical range`,
      red: `This <strong>${{formatPrice(p.cur)}}</strong> is higher than usual — consider waiting for a sale`
    }};
    let barHtml = '';
    if (p.min != null && p.max != null && p.max > p.min) {{
      const pos = Math.min(100, Math.max(0, ((p.cur - p.min) / (p.max - p.min)) * 100));
      barHtml = `<div class="price-bar"><div class="fill" style="width:100%"></div><div class="marker" style="left:${{pos}}%"></div></div>
        <div class="price-bar-labels"><span>Low ${{formatPrice(p.min)}}</span><span>Avg ${{formatPrice(p.avg)}}</span><span>High ${{formatPrice(p.max)}}</span></div>`;
    }}
    let dealCompare = '';
    if (p.best_deal != null && p.best_deal < p.cur) {{
      const diff = p.cur - p.best_deal;
      dealCompare = `<div style="margin-top:8px;color:#888;font-size:12px">Best deal ever: ${{formatPrice(p.best_deal)}} (${{formatPrice(diff)}} below current)</div>`;
    }}
    fairPriceHtml = `<div class="fair-price ${{p.sig}}">
      <div class="big"><span class="${{p.sig}}">${{sigLabels[p.sig]}}</span></div>
      <div style="margin-top:4px">${{sigDescs[p.sig]}}</div>
      ${{barHtml}}${{dealCompare}}
    </div>`;
  }}

  // Deal prediction
  let dealPredHtml = '';
  if (p.deal_freq) {{
    const df = p.deal_freq;
    const status = df.overdue
      ? `<span class="overdue">OVERDUE by ${{Math.abs(df.days_until)}} days</span>`
      : `<span class="upcoming">Next deal likely in ~${{df.days_until}} days</span>`;
    dealPredHtml = `<div class="deal-pred">
      <strong>Deal Frequency:</strong> Goes on sale roughly every <strong>${{df.avg_cycle}} days</strong><br>
      Last deal: ${{df.last_deal}} (${{df.days_since}} days ago) · ${{status}}
    </div>`;
  }}

  // Savings calc
  let savingsHtml = '';
  if (p.best_deal != null && p.max != null && p.max > p.best_deal) {{
    const saved = p.max - p.best_deal;
    const pct = ((saved / p.max) * 100).toFixed(0);
    savingsHtml = `<div style="background:#2d5a1e;padding:12px 16px;border-radius:8px;margin-bottom:16px;">
      Best deal: <strong style="color:#7ddf64">${{formatPrice(p.best_deal)}}</strong> vs
      regular ${{formatPrice(p.max)}} — save ${{formatPrice(saved)}} (${{pct}}% off)
    </div>`;
  }}

  panel.innerHTML = `
    <button class="close-btn" onclick="closeDetail()">&times;</button>
    <h2>${{p.name || 'SKU ' + p.sku}}</h2>
    <div class="meta">
      SKU: <strong>${{p.sku}}</strong>
      ${{p.brand ? ' · ' + p.brand : ''}}
      · ${{hfLink}}
    </div>
    ${{fairPriceHtml}}
    ${{dealPredHtml}}
    ${{savingsHtml}}
    ${{timelineHtml}}
  `;

  document.getElementById('overlay').classList.add('open');

  // Render chart from unified timeline
  if (hasTimeline && timeline.length > 1) {{
    if (chartInstance) chartInstance.destroy();
    const ctx = document.getElementById('priceChart').getContext('2d');

    const regularPts = timeline.filter(t => t.type === 'regular');
    const dealPts = timeline.filter(t => t.type === 'deal');
    // Use all dates as labels for x-axis
    const allDates = timeline.map(t => t.date);

    chartInstance = new Chart(ctx, {{
      type: 'line',
      data: {{
        labels: allDates,
        datasets: [
          {{
            label: 'Regular Price',
            data: timeline.map(t => t.type === 'regular' ? t.price : null),
            borderColor: '#6c9fff',
            backgroundColor: 'rgba(108,159,255,0.1)',
            fill: true,
            tension: 0.3,
            pointRadius: t => {{
              const idx = timeline.indexOf(t);
              return idx >= 0 && timeline[idx].change !== 'same' ? 5 : 3;
            }},
            pointBackgroundColor: timeline.map(t => t.change === 'up' ? '#ff6b6b' : t.change === 'down' ? '#7ddf64' : '#6c9fff'),
            spanGaps: true,
          }},
          ...(dealPts.length ? [{{
            label: 'Deal Price',
            data: timeline.map(t => t.type === 'deal' ? t.price : null),
            borderColor: '#7ddf64',
            backgroundColor: '#f0c040',
            pointRadius: 7,
            pointStyle: 'triangle',
            showLine: false,
          }}] : []),
        ],
      }},
      options: {{
        responsive: true,
        maintainAspectRatio: false,
        plugins: {{
          legend: {{ labels: {{ color: '#aaa' }} }},
          tooltip: {{
            callbacks: {{
              label: function(ctx) {{
                const t = timeline[ctx.dataIndex];
                return t ? (t.type === 'deal' ? 'Deal: ' : 'Price: ') + '$' + t.price.toFixed(2) + (t.label ? ' — ' + t.label : '') : '';
              }}
            }}
          }},
          annotation: {{
            annotations: {{
              ...(p.avg != null ? {{avgLine: {{
                type: 'line', yMin: p.avg, yMax: p.avg,
                borderColor: 'rgba(108,159,255,0.4)', borderDash: [6,3], borderWidth: 1,
                label: {{ display: true, content: 'Avg $' + p.avg.toFixed(2), position: 'start', color: '#888', font: {{size: 10}}, backgroundColor: 'transparent' }}
              }}}} : {{}}),
              ...(p.best_deal != null ? {{dealLine: {{
                type: 'line', yMin: p.best_deal, yMax: p.best_deal,
                borderColor: 'rgba(125,223,100,0.4)', borderDash: [6,3], borderWidth: 1,
                label: {{ display: true, content: 'Best Deal $' + p.best_deal.toFixed(2), position: 'end', color: '#7ddf64', font: {{size: 10}}, backgroundColor: 'transparent' }}
              }}}} : {{}}),
            }}
          }},
        }},
        scales: {{
          x: {{ ticks: {{ color: '#888', maxRotation: 45 }}, grid: {{ color: '#282a36' }} }},
          y: {{
            ticks: {{
              color: '#888',
              callback: v => '$' + v.toFixed(2),
            }},
            grid: {{ color: '#282a36' }},
          }},
        }},
      }},
    }});
  }}
}}

function closeDetail() {{
  document.getElementById('overlay').classList.remove('open');
  if (chartInstance) {{ chartInstance.destroy(); chartInstance = null; }}
}}

document.getElementById('overlay').addEventListener('click', e => {{
  if (e.target === e.currentTarget) closeDetail();
}});
document.addEventListener('keydown', e => {{
  if (e.key === 'Escape') closeDetail();
}});

// Initial render
renderTable();
</script>
</body>
</html>"""

    return html


def main():
    if not os.path.exists(DB_PATH):
        print(f"Database not found: {DB_PATH}")
        return

    print("Loading data from database...")
    products, stats = load_data()
    print(f"  Products: {stats['products']:,}")
    print(f"  With prices: {stats['with_prices']}")
    print(f"  With deals: {stats['with_deals']}")
    print(f"  Snapshots: {stats['snapshots']:,}")
    print(f"  Deals: {stats['deals']:,}")

    print("Generating HTML viewer...")
    html = generate_html(products, stats)

    with open(OUT_PATH, "w") as f:
        f.write(html)

    size_kb = os.path.getsize(OUT_PATH) / 1024
    print(f"  Saved: {OUT_PATH} ({size_kb:.0f} KB)")
    print(f"\n  Open with: open viewer.html")


if __name__ == "__main__":
    main()
