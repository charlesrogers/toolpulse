/**
 * ToolPulse Wayback Worker
 *
 * Cloudflare Worker that runs on a cron schedule to backfill historical
 * Harbor Freight prices from the Wayback Machine.
 *
 * Architecture:
 *   - KV stores: product URL queue, progress state, extracted price data
 *   - Each cron invocation processes a batch of products (≤10 per run)
 *   - Results accumulate in KV; download via HTTP endpoint
 *   - Also backfills go.harborfreight.com deal/coupon pages
 *
 * KV key prefixes:
 *   prices:<sku>          — product page price history
 *   go_hf:<url_hash>      — go.hf deal page extracted deals
 *   go_hf_queue           — list of go.hf URLs to process
 *   go_hf_progress        — { index, last_run }
 *
 * Setup:
 *   1. wrangler kv namespace create TOOLPULSE_KV
 *   2. Paste the ID into wrangler.toml
 *   3. Seed product URLs: curl -X POST https://toolpulse-wayback.<you>.workers.dev/seed
 *   4. Deploy: wrangler deploy
 */

const CDX_API = "https://web.archive.org/cdx/search/cdx";
const WAYBACK_BASE = "https://web.archive.org/web";
const BATCH_SIZE = 2; // Products per cron invocation (Workers have 30s wall clock)
const GO_HF_BATCH_SIZE = 2; // go.hf pages per cron invocation (pages are larger)
const MAX_SNAPSHOTS_PER_PRODUCT = 8; // Cap to stay within time limits
const MAX_SNAPSHOTS_PER_GO_HF = 5; // Fewer snapshots for larger go.hf pages
const SNAPSHOT_DELAY_MS = 500; // Delay between Wayback fetches
const FETCH_HEADERS = {
  "User-Agent": "ToolPulse/1.0 (historical price research; Cloudflare Worker)",
  Accept: "application/json, text/html, */*",
};

// go.harborfreight.com deal regex — matches img alt text and body text
// e.g. "Buy the 20V Cordless Drill (Item 63496/63499) for $29.99, valid through 12/31/2023"
const ALT_PATTERN =
  /Buy the (.+?)\s*\(Item\s*([\d/]+)\)\s*for \$([0-9,.]+)(?:,?\s*valid through\s*(\d{1,2}\/\d{1,2}\/\d{4}))?/gi;

// ── CDX API ─────────────────────────────────────────────────────────────────

async function findSnapshots(productUrl, limit = 30, collapseDigits = 8) {
  const params = new URLSearchParams({
    url: productUrl,
    output: "json",
    fl: "timestamp,original,statuscode",
    filter: "statuscode:200",
    limit: String(limit),
    collapse: `timestamp:${collapseDigits}`, // 8 = one per day, 6 = one per month
  });

  const resp = await fetch(`${CDX_API}?${params}`, { headers: FETCH_HEADERS });
  if (!resp.ok) return [];

  const data = await resp.json();
  if (data.length <= 1) return [];

  const headers = data[0];
  return data.slice(1).map((row) => {
    const obj = {};
    headers.forEach((h, i) => (obj[h] = row[i]));
    return obj;
  });
}

// ── Price extraction from archived product page ─────────────────────────────

async function extractPrice(timestamp, originalUrl) {
  const waybackUrl = `${WAYBACK_BASE}/${timestamp}id_/${originalUrl}`;

  let resp;
  try {
    resp = await fetch(waybackUrl, { headers: FETCH_HEADERS });
    if (!resp.ok) return null;
  } catch {
    return null;
  }

  const html = await resp.text();
  const result = {
    timestamp,
    date: `${timestamp.slice(0, 4)}-${timestamp.slice(4, 6)}-${timestamp.slice(6, 8)}`,
    source: "wayback",
  };

  // Method 1: og:price:amount meta tag
  const ogMatch = html.match(
    /meta\s+property="og:price:amount"\s+content="([^"]+)"/
  );
  if (ogMatch) {
    result.price = parseFloat(ogMatch[1]);
  }

  // Method 2: JSON-LD
  const ldMatches = html.matchAll(
    /<script\s+type="application\/ld\+json">([\s\S]*?)<\/script>/g
  );
  for (const ldMatch of ldMatches) {
    try {
      let ld = JSON.parse(ldMatch[1]);
      const products = [];
      if (Array.isArray(ld)) {
        products.push(...ld.filter((x) => x["@type"] === "Product"));
      } else if (ld["@type"] === "Product") {
        products.push(ld);
      } else if (ld["@graph"]) {
        products.push(...ld["@graph"].filter((x) => x["@type"] === "Product"));
      }

      for (const product of products) {
        if (!result.product_name) result.product_name = product.name;
        if (!result.sku) result.sku = product.sku;
        if (product.brand) {
          result.brand =
            typeof product.brand === "object"
              ? product.brand.name
              : product.brand;
        }

        let offers = product.offers || {};
        if (Array.isArray(offers)) offers = offers[0] || {};
        if (offers.price && !result.price) {
          result.price = parseFloat(offers.price);
        }
        if (offers.availability) {
          result.in_stock = offers.availability.includes("InStock");
        }
      }
    } catch {
      continue;
    }
  }

  // SKU from URL
  if (!result.sku) {
    const skuMatch = originalUrl.match(/-(\d{5,})\.html/);
    if (skuMatch) result.sku = skuMatch[1];
  }

  return result.price != null ? result : null;
}

// ── Extract deals from an archived go.harborfreight.com page ────────────────

async function extractDealsFromSnapshot(timestamp, originalUrl) {
  const waybackUrl = `${WAYBACK_BASE}/${timestamp}id_/${originalUrl}`;
  const dateStr = `${timestamp.slice(0, 4)}-${timestamp.slice(4, 6)}-${timestamp.slice(6, 8)}`;

  let resp;
  try {
    resp = await fetch(waybackUrl, { headers: FETCH_HEADERS });
    if (!resp.ok) return [];
  } catch {
    return [];
  }

  const html = await resp.text();
  const deals = [];
  const seenItems = new Set();

  // Method 1: Parse img alt text (primary — works on grid + coupon pages)
  // Look for <img ... alt="Buy the ... (Item ...) for $...">
  const imgAltRegex = /<img[^>]+alt="([^"]*Buy the [^"]+)"[^>]*>/gi;
  let imgMatch;
  while ((imgMatch = imgAltRegex.exec(html)) !== null) {
    const altText = imgMatch[1];
    // Reset ALT_PATTERN lastIndex since it's global
    ALT_PATTERN.lastIndex = 0;
    const m = ALT_PATTERN.exec(altText);
    if (!m) continue;

    const productName = m[1].trim();
    const itemNumbersRaw = m[2].trim();
    const price = parseFloat(m[3].replace(",", ""));
    const validThrough = m[4] ? m[4].trim() : null;

    const itemNumbers = itemNumbersRaw.split("/").map((n) => n.trim());
    const primaryItem = itemNumbers[0];
    const altItems = itemNumbers.slice(1);

    // Extract coupon URL from parent <a> if present
    let couponUrl = null;
    let promoId = null;
    // Look backwards in the HTML for the nearest <a> wrapping this <img>
    const beforeImg = html.slice(Math.max(0, imgMatch.index - 500), imgMatch.index);
    const aMatch = beforeImg.match(/<a[^>]+href="([^"]+)"[^>]*>\s*$/i);
    if (aMatch) {
      couponUrl = aMatch[1];
      const urlMatch = couponUrl.match(/\/(\d{6,})-(\d+)\/?$/);
      if (urlMatch) promoId = urlMatch[1];
    }

    const key = `${primaryItem}:${price}:${dateStr}`;
    if (!seenItems.has(key)) {
      seenItems.add(key);
      deals.push({
        product_name: productName,
        item_number: primaryItem,
        alt_item_numbers: altItems,
        price,
        valid_through: validThrough,
        promo_id: promoId,
        coupon_url: couponUrl,
        source: "wayback_go_hf",
        source_url: originalUrl,
        snapshot_date: dateStr,
        snapshot_timestamp: timestamp,
      });
    }
  }

  // Method 2: Parse body text for deals not captured by img alt
  // (some pages have deal info in entry-content text instead of images)
  ALT_PATTERN.lastIndex = 0;
  let textMatch;
  while ((textMatch = ALT_PATTERN.exec(html)) !== null) {
    const primaryItem = textMatch[2].trim().split("/")[0].trim();
    const price = parseFloat(textMatch[3].replace(",", ""));
    const dateKey = `${primaryItem}:${price}:${dateStr}`;

    if (!seenItems.has(dateKey)) {
      seenItems.add(dateKey);
      const itemNumbers = textMatch[2].trim().split("/").map((n) => n.trim());
      deals.push({
        product_name: textMatch[1].trim(),
        item_number: primaryItem,
        alt_item_numbers: itemNumbers.slice(1),
        price,
        valid_through: textMatch[4] ? textMatch[4].trim() : null,
        promo_id: null,
        coupon_url: null,
        source: "wayback_go_hf",
        source_url: originalUrl,
        snapshot_date: dateStr,
        snapshot_timestamp: timestamp,
      });
    }
  }

  return deals;
}

// ── Process one product page ────────────────────────────────────────────────

async function backfillProduct(productUrl) {
  const skuMatch = productUrl.match(/-(\d{5,})\.html/);
  const sku = skuMatch ? skuMatch[1] : "unknown";

  console.log(`  Processing SKU ${sku}...`);
  const snapshots = await findSnapshots(productUrl, MAX_SNAPSHOTS_PER_PRODUCT);
  console.log(`  Found ${snapshots.length} snapshots`);
  if (!snapshots.length) return { sku, prices: [], snapshots: 0 };

  const prices = [];
  for (const snap of snapshots) {
    try {
      const price = await extractPrice(snap.timestamp, snap.original);
      if (price) {
        prices.push(price);
        console.log(`    ${price.date}: $${price.price}`);
      }
    } catch (e) {
      console.log(`    Error on ${snap.timestamp}: ${e.message}`);
    }
    await new Promise((r) => setTimeout(r, SNAPSHOT_DELAY_MS));
  }

  return { sku, prices, snapshots: snapshots.length };
}

// ── Process one go.hf URL across its snapshots ─────────────────────────────

async function backfillGoHfUrl(goHfUrl) {
  console.log(`  Processing go.hf URL: ${goHfUrl}`);

  // Use collapse:6 (one per month) for go.hf pages
  const snapshots = await findSnapshots(goHfUrl, MAX_SNAPSHOTS_PER_GO_HF, 6);
  console.log(`  Found ${snapshots.length} monthly snapshots`);

  if (!snapshots.length) return { url: goHfUrl, deals: [], snapshots: 0 };

  const allDeals = [];
  const seenKeys = new Set();

  for (const snap of snapshots) {
    try {
      const deals = await extractDealsFromSnapshot(snap.timestamp, snap.original);
      let newCount = 0;
      for (const deal of deals) {
        const key = `${deal.item_number}:${deal.price}:${deal.snapshot_date}`;
        if (!seenKeys.has(key)) {
          seenKeys.add(key);
          allDeals.push(deal);
          newCount++;
        }
      }
      const dateStr = `${snap.timestamp.slice(0, 4)}-${snap.timestamp.slice(4, 6)}-${snap.timestamp.slice(6, 8)}`;
      console.log(`    ${dateStr}: ${deals.length} deals extracted (${newCount} new)`);
    } catch (e) {
      console.log(`    Error on ${snap.timestamp}: ${e.message}`);
    }
    await new Promise((r) => setTimeout(r, SNAPSHOT_DELAY_MS));
  }

  console.log(`  Total unique deals from this URL: ${allDeals.length}`);
  return { url: goHfUrl, deals: allDeals, snapshots: snapshots.length };
}

// ── Simple hash for KV key from URL ─────────────────────────────────────────

function urlToKvKey(url) {
  // Create a safe KV key from a URL by stripping protocol and replacing special chars
  return url
    .replace(/^https?:\/\//, "")
    .replace(/[^a-zA-Z0-9._/-]/g, "_")
    .slice(0, 200);
}

// ── Cron handler: product page backfill ─────────────────────────────────────

async function handleProductCron(env) {
  const kv = env.TOOLPULSE_KV;

  const queueRaw = await kv.get("url_queue", "json");
  if (!queueRaw || !queueRaw.length) {
    console.log("No product URLs in queue. POST /seed to add URLs.");
    return false; // Nothing to do
  }

  const progress = JSON.parse((await kv.get("progress")) || '{"index": 0}');
  const startIdx = progress.index;

  if (startIdx >= queueRaw.length) {
    console.log(
      `Product backfill complete! Processed all ${queueRaw.length} products.`
    );
    return false; // Done
  }

  const batch = queueRaw.slice(startIdx, startIdx + BATCH_SIZE);
  console.log(
    `[Product] Processing batch: ${startIdx + 1}-${startIdx + batch.length} of ${queueRaw.length}`
  );

  for (const url of batch) {
    try {
      const result = await backfillProduct(url);
      if (result.prices.length > 0) {
        const existing = (await kv.get(`prices:${result.sku}`, "json")) || [];
        const merged = [...existing, ...result.prices];
        await kv.put(`prices:${result.sku}`, JSON.stringify(merged));
        console.log(
          `  SKU ${result.sku}: ${result.prices.length} prices from ${result.snapshots} snapshots`
        );
      } else {
        console.log(`  SKU ${result.sku}: no prices found`);
      }
    } catch (e) {
      console.log(`  Error processing ${url}: ${e.message}`);
    }
  }

  progress.index = startIdx + batch.length;
  progress.last_run = new Date().toISOString();
  await kv.put("progress", JSON.stringify(progress));
  console.log(`Product progress: ${progress.index}/${queueRaw.length}`);
  return true;
}

// ── Cron handler: go.hf deal page backfill ──────────────────────────────────

async function handleGoHfCron(env) {
  const kv = env.TOOLPULSE_KV;

  const queueRaw = await kv.get("go_hf_queue", "json");
  if (!queueRaw || !queueRaw.length) {
    console.log("No go.hf URLs in queue. POST /seed-go-hf to add URLs.");
    return false;
  }

  const progress = JSON.parse(
    (await kv.get("go_hf_progress")) || '{"index": 0}'
  );
  const startIdx = progress.index;

  if (startIdx >= queueRaw.length) {
    console.log(
      `go.hf backfill complete! Processed all ${queueRaw.length} URLs.`
    );
    return false;
  }

  const batch = queueRaw.slice(startIdx, startIdx + GO_HF_BATCH_SIZE);
  console.log(
    `[go.hf] Processing batch: ${startIdx + 1}-${startIdx + batch.length} of ${queueRaw.length}`
  );

  for (const entry of batch) {
    const url = typeof entry === "string" ? entry : entry.url;
    try {
      const result = await backfillGoHfUrl(url);
      if (result.deals.length > 0) {
        // Store deals under go_hf:<url_key>
        const kvKey = `go_hf:${urlToKvKey(url)}`;
        const existing = (await kv.get(kvKey, "json")) || [];
        const merged = [...existing, ...result.deals];
        await kv.put(kvKey, JSON.stringify(merged));
        console.log(
          `  ${url}: ${result.deals.length} deals from ${result.snapshots} snapshots`
        );
      } else {
        console.log(`  ${url}: no deals found`);
      }
    } catch (e) {
      console.log(`  Error processing ${url}: ${e.message}`);
    }
  }

  progress.index = startIdx + batch.length;
  progress.last_run = new Date().toISOString();
  await kv.put("go_hf_progress", JSON.stringify(progress));
  console.log(`go.hf progress: ${progress.index}/${queueRaw.length}`);
  return true;
}

// ── Combined cron handler: alternates between product and go.hf ─────────────

async function handleCron(env) {
  const kv = env.TOOLPULSE_KV;

  // Read a counter to alternate between product and go.hf backfill
  const cronCount = parseInt((await kv.get("cron_counter")) || "0", 10);
  const isGoHfTurn = cronCount % 2 === 1;

  console.log(`Cron invocation #${cronCount + 1} — ${isGoHfTurn ? "go.hf" : "product"} turn`);

  let didWork;
  if (isGoHfTurn) {
    didWork = await handleGoHfCron(env);
    // If go.hf had nothing to do, try product instead
    if (!didWork) {
      console.log("go.hf queue empty/done, falling back to product backfill");
      didWork = await handleProductCron(env);
    }
  } else {
    didWork = await handleProductCron(env);
    // If product had nothing to do, try go.hf instead
    if (!didWork) {
      console.log("Product queue empty/done, falling back to go.hf backfill");
      didWork = await handleGoHfCron(env);
    }
  }

  // Increment counter
  await kv.put("cron_counter", String(cronCount + 1));
  console.log("Cron run complete.");
}

// ── HTTP handlers ───────────────────────────────────────────────────────────

async function handleRequest(request, env) {
  const url = new URL(request.url);
  const kv = env.TOOLPULSE_KV;

  // POST /seed — seed the product URL queue
  if (url.pathname === "/seed" && request.method === "POST") {
    const body = await request.json().catch(() => null);
    let urls = [];

    if (body && body.urls) {
      urls = body.urls;
    } else if (body && body.item_numbers) {
      for (const item of body.item_numbers) {
        const cdxUrl = `${CDX_API}?url=harborfreight.com/*-${item}.html&output=json&fl=original&limit=1&filter=statuscode:200`;
        try {
          const resp = await fetch(cdxUrl, { headers: FETCH_HEADERS });
          const data = await resp.json();
          if (data.length > 1) {
            urls.push(data[1][0]);
          }
        } catch {
          // skip
        }
        await new Promise((r) => setTimeout(r, 300));
      }
    } else {
      return new Response(
        JSON.stringify({
          error: 'POST body must have "urls" or "item_numbers" array',
        }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }

    await kv.put("url_queue", JSON.stringify(urls));
    await kv.put("progress", JSON.stringify({ index: 0 }));
    return Response.json({ queued: urls.length, urls: urls.slice(0, 10) });
  }

  // POST /seed-go-hf — discover and seed go.harborfreight.com URLs from CDX
  if (url.pathname === "/seed-go-hf" && request.method === "POST") {
    const body = await request.json().catch(() => null);

    // Allow direct URL list or auto-discover from CDX
    if (body && body.urls) {
      const entries = body.urls.map((u) =>
        typeof u === "string" ? { url: u, type: "manual" } : u
      );
      await kv.put("go_hf_queue", JSON.stringify(entries));
      await kv.put("go_hf_progress", JSON.stringify({ index: 0 }));
      return Response.json({ queued: entries.length, sample: entries.slice(0, 5) });
    }

    // Auto-discover from CDX API
    console.log("Discovering go.harborfreight.com URLs from CDX...");
    const prefixes = [
      { pattern: "go.harborfreight.com/cpi/digital/*", type: "grid" },
      { pattern: "go.harborfreight.com/coupons/*", type: "coupon" },
      { pattern: "go.harborfreight.com/email*", type: "email" },
    ];

    const allUrls = {};

    for (const { pattern, type } of prefixes) {
      console.log(`  Querying CDX for ${type} pages: ${pattern}`);

      const params = new URLSearchParams({
        url: pattern,
        matchType: "prefix",
        output: "json",
        fl: "original,timestamp,statuscode",
        filter: "statuscode:200",
        collapse: "urlkey",
        limit: "5000",
      });

      try {
        const resp = await fetch(`${CDX_API}?${params}`, { headers: FETCH_HEADERS });
        if (!resp.ok) {
          console.log(`  CDX query failed: status ${resp.status}`);
          continue;
        }

        const data = await resp.json();
        if (data.length <= 1) {
          console.log(`  No results for ${type}`);
          continue;
        }

        const headersRow = data[0];
        const rows = data.slice(1);
        let newCount = 0;

        for (const row of rows) {
          const entry = {};
          headersRow.forEach((h, i) => (entry[h] = row[i]));
          let entryUrl = entry.original.replace(/\/$/, "");
          if (entryUrl.startsWith("http://")) {
            entryUrl = "https://" + entryUrl.slice(7);
          }

          if (!allUrls[entryUrl]) {
            allUrls[entryUrl] = { url: entryUrl, type };
            newCount++;
          }
        }

        console.log(`  Found ${newCount} unique ${type} URLs (${rows.length} CDX rows)`);
      } catch (e) {
        console.log(`  Error querying CDX for ${type}: ${e.message}`);
      }

      await new Promise((r) => setTimeout(r, 500));
    }

    // Sort: grid first, then email, then coupon
    const typeOrder = { grid: 0, email: 1, coupon: 2, manual: 3 };
    const entries = Object.values(allUrls).sort(
      (a, b) => (typeOrder[a.type] || 99) - (typeOrder[b.type] || 99)
    );

    // Count by type
    const typeCounts = {};
    for (const e of entries) {
      typeCounts[e.type] = (typeCounts[e.type] || 0) + 1;
    }

    await kv.put("go_hf_queue", JSON.stringify(entries));
    await kv.put("go_hf_progress", JSON.stringify({ index: 0 }));

    console.log(`Seeded ${entries.length} go.hf URLs`);
    return Response.json({
      queued: entries.length,
      by_type: typeCounts,
      sample: entries.slice(0, 5),
    });
  }

  // GET /run-go-hf — manually trigger a go.hf batch
  if (url.pathname === "/run-go-hf") {
    await handleGoHfCron(env);
    const progress = JSON.parse(
      (await kv.get("go_hf_progress")) || '{"index": 0}'
    );
    const queue = (await kv.get("go_hf_queue", "json")) || [];
    return Response.json({
      status: "go.hf batch complete",
      progress: {
        processed: progress.index,
        total: queue.length,
        remaining: queue.length - progress.index,
        last_run: progress.last_run,
      },
    });
  }

  // GET /test — diagnostic: test a single Wayback fetch
  if (url.pathname === "/test") {
    const testUrl = "https://www.harborfreight.com/4-in-x-36-in-belt-and-6-in-disc-sander-58339.html";
    const diag = { steps: [] };

    try {
      const cdxParams = new URLSearchParams({
        url: testUrl, output: "json", fl: "timestamp,original,statuscode",
        filter: "statuscode:200", limit: "2", collapse: "timestamp:8",
      });
      const cdxResp = await fetch(`${CDX_API}?${cdxParams}`, { headers: FETCH_HEADERS });
      const cdxText = await cdxResp.text();
      diag.steps.push({ step: "cdx_raw", status: cdxResp.status, body_length: cdxText.length, first_300: cdxText.slice(0, 300) });

      const snaps = await findSnapshots(testUrl, 2);
      diag.steps.push({ step: "cdx_parsed", snapshots: snaps.length, first: snaps[0] || null });

      if (snaps.length > 0) {
        const ts = snaps[0].timestamp;
        const waybackUrl = `${WAYBACK_BASE}/${ts}id_/${snaps[0].original}`;
        const resp = await fetch(waybackUrl, { headers: FETCH_HEADERS });
        const status = resp.status;
        const html = await resp.text();
        diag.steps.push({
          step: "fetch",
          url: waybackUrl,
          status,
          html_length: html.length,
          has_og_price: html.includes('og:price:amount'),
          has_jsonld: html.includes('application/ld+json'),
          first_500_chars: html.slice(0, 500),
        });

        const price = await extractPrice(ts, snaps[0].original);
        diag.steps.push({ step: "extract", result: price });
      }
    } catch (e) {
      diag.steps.push({ step: "error", message: e.message, stack: e.stack });
    }
    return Response.json(diag);
  }

  // GET /status — show backfill progress for both queues
  if (url.pathname === "/status") {
    const queue = (await kv.get("url_queue", "json")) || [];
    const progress = JSON.parse(
      (await kv.get("progress")) || '{"index": 0}'
    );
    const goHfQueue = (await kv.get("go_hf_queue", "json")) || [];
    const goHfProgress = JSON.parse(
      (await kv.get("go_hf_progress")) || '{"index": 0}'
    );
    const cronCount = parseInt((await kv.get("cron_counter")) || "0", 10);

    return Response.json({
      product_backfill: {
        total_products: queue.length,
        processed: progress.index,
        remaining: queue.length - progress.index,
        last_run: progress.last_run,
      },
      go_hf_backfill: {
        total_urls: goHfQueue.length,
        processed: goHfProgress.index,
        remaining: goHfQueue.length - goHfProgress.index,
        last_run: goHfProgress.last_run,
      },
      cron_invocations: cronCount,
      next_turn: cronCount % 2 === 0 ? "product" : "go_hf",
    });
  }

  // GET /prices/:sku — get price history for a SKU
  const priceMatch = url.pathname.match(/^\/prices\/(\d+)$/);
  if (priceMatch) {
    const prices =
      (await kv.get(`prices:${priceMatch[1]}`, "json")) || [];
    return Response.json({ sku: priceMatch[1], prices });
  }

  // GET /prices — list all SKUs with data
  if (url.pathname === "/prices") {
    const list = await kv.list({ prefix: "prices:" });
    const skus = list.keys.map((k) => k.name.replace("prices:", ""));
    return Response.json({ skus, count: skus.length });
  }

  // GET /export — dump all price data as JSON
  if (url.pathname === "/export") {
    const list = await kv.list({ prefix: "prices:" });
    const allData = {};
    for (const key of list.keys) {
      const sku = key.name.replace("prices:", "");
      allData[sku] = await kv.get(key.name, "json");
    }
    return Response.json(allData);
  }

  // GET /deals — export all discovered go.hf deals
  if (url.pathname === "/deals") {
    const list = await kv.list({ prefix: "go_hf:" });
    const allDeals = [];
    const byUrl = {};

    for (const key of list.keys) {
      const urlKey = key.name.replace("go_hf:", "");
      const deals = (await kv.get(key.name, "json")) || [];
      byUrl[urlKey] = deals.length;
      allDeals.push(...deals);
    }

    // Optionally filter by item number
    const itemFilter = url.searchParams.get("item");
    let filtered = allDeals;
    if (itemFilter) {
      filtered = allDeals.filter(
        (d) =>
          d.item_number === itemFilter ||
          (d.alt_item_numbers && d.alt_item_numbers.includes(itemFilter))
      );
    }

    return Response.json({
      total_deals: allDeals.length,
      filtered_deals: filtered.length,
      source_urls: Object.keys(byUrl).length,
      deals_by_url: byUrl,
      deals: filtered,
    });
  }

  // GET /run — manual trigger (same as cron, processes product batch)
  if (url.pathname === "/run") {
    await handleCron(env);
    const progress = JSON.parse(
      (await kv.get("progress")) || '{"index": 0}'
    );
    return Response.json({ status: "batch complete", progress });
  }

  // Default: show help
  return Response.json({
    service: "ToolPulse Wayback Worker",
    endpoints: {
      "POST /seed": "Seed product URL queue (body: {urls: [...]} or {item_numbers: [...]})",
      "POST /seed-go-hf": "Seed go.hf deal page queue (body: {urls: [...]} or empty body for CDX auto-discovery)",
      "GET /status": "Backfill progress for both queues",
      "GET /prices/:sku": "Price history for a SKU",
      "GET /prices": "List all SKUs with data",
      "GET /export": "Export all price data",
      "GET /deals": "Export all go.hf deals (optional ?item=NNNNN filter)",
      "GET /run": "Manually trigger a cron batch (alternates product/go.hf)",
      "GET /run-go-hf": "Manually trigger a go.hf batch",
      "GET /test": "Diagnostic: test a single Wayback fetch",
    },
  });
}

// ── Entry points ────────────────────────────────────────────────────────────

export default {
  async fetch(request, env) {
    return handleRequest(request, env);
  },
  async scheduled(event, env, ctx) {
    ctx.waitUntil(handleCron(env));
  },
};
