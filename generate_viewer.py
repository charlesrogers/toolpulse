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
import sqlite3
from datetime import datetime, timezone

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
</style>
</head>
<body>

<div class="header">
  <h1>ToolPulse — Harbor Freight Price Tracker</h1>
  <div class="subtitle">Generated {generated}</div>
</div>

<div class="stats-bar">
  <div class="stat"><div class="num">{stats['products']:,}</div><div class="label">Products</div></div>
  <div class="stat"><div class="num">{stats['with_prices']}</div><div class="label">With Price History</div></div>
  <div class="stat"><div class="num">{stats['with_deals']}</div><div class="label">With Deals</div></div>
  <div class="stat"><div class="num">{stats['snapshots']:,}</div><div class="label">Price Snapshots</div></div>
  <div class="stat"><div class="num">{stats['deals']:,}</div><div class="label">Deals Tracked</div></div>
</div>

<div class="controls">
  <input type="text" id="search" placeholder="Search by SKU, name, or brand..." autofocus>
  <select id="filter">
    <option value="all">All Products</option>
    <option value="has-data" selected>With Price/Deal Data</option>
    <option value="has-prices">With Price History</option>
    <option value="has-deals">With Deals</option>
  </select>
  <div class="count" id="count"></div>
</div>

<div class="container">
  <table>
    <thead>
      <tr>
        <th data-col="sku">SKU</th>
        <th data-col="name">Product</th>
        <th data-col="brand">Brand</th>
        <th data-col="snaps">Snapshots</th>
        <th data-col="deals">Deals</th>
        <th data-col="min">Price Range</th>
        <th data-col="best_deal">Best Deal</th>
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
    let av = a[sortCol], bv = b[sortCol];
    if (av == null) av = sortDir > 0 ? Infinity : -Infinity;
    if (bv == null) bv = sortDir > 0 ? Infinity : -Infinity;
    if (typeof av === 'string') return sortDir * av.localeCompare(bv);
    return sortDir * (av - bv);
  }});

  const tbody = document.getElementById('tbody');
  tbody.innerHTML = items.slice(0, 500).map(p => {{
    const hasData = p.snaps > 0 || p.deals > 0;
    const priceRange = p.min != null ? (p.min === p.max ? formatPrice(p.min) : formatPrice(p.min) + ' – ' + formatPrice(p.max)) : '—';
    return `<tr class="${{hasData ? 'has-data' : 'no-data'}}" onclick="showDetail('${{p.sku}}')">
      <td class="sku">${{p.sku}}</td>
      <td>${{p.name || '<em>Unknown</em>'}}</td>
      <td class="brand">${{p.brand}}</td>
      <td class="num">${{p.snaps || ''}}</td>
      <td class="num">${{p.deals ? '<span class="deal-badge">' + p.deals + '</span>' : ''}}</td>
      <td class="price">${{priceRange}}</td>
      <td class="price">${{p.best_deal != null ? formatPrice(p.best_deal) : ''}}</td>
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
