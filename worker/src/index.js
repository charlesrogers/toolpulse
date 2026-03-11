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
 *
 * Setup:
 *   1. wrangler kv namespace create TOOLPULSE_KV
 *   2. Paste the ID into wrangler.toml
 *   3. Seed product URLs: curl -X POST https://toolpulse-wayback.<you>.workers.dev/seed
 *   4. Deploy: wrangler deploy
 */

const CDX_API = "https://web.archive.org/cdx/search/cdx";
const WAYBACK_BASE = "https://web.archive.org/web";
const BATCH_SIZE = 5; // Products per cron invocation (keep small for 30s CPU limit)
const SNAPSHOT_DELAY_MS = 1500; // Delay between Wayback fetches

// ── CDX API ─────────────────────────────────────────────────────────────────

async function findSnapshots(productUrl, limit = 30) {
  const params = new URLSearchParams({
    url: productUrl,
    output: "json",
    fl: "timestamp,original,statuscode",
    filter: "statuscode:200",
    limit: String(limit),
    collapse: "timestamp:8", // One per day
  });

  const resp = await fetch(`${CDX_API}?${params}`);
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

// ── Price extraction from archived page ─────────────────────────────────────

async function extractPrice(timestamp, originalUrl) {
  const waybackUrl = `${WAYBACK_BASE}/${timestamp}id_/${originalUrl}`;

  let resp;
  try {
    resp = await fetch(waybackUrl, { cf: { cacheTtl: 3600 } });
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

// ── Process one product ─────────────────────────────────────────────────────

async function backfillProduct(productUrl) {
  const skuMatch = productUrl.match(/-(\d{5,})\.html/);
  const sku = skuMatch ? skuMatch[1] : "unknown";

  const snapshots = await findSnapshots(productUrl);
  if (!snapshots.length) return { sku, prices: [], snapshots: 0 };

  const prices = [];
  for (const snap of snapshots) {
    const price = await extractPrice(snap.timestamp, snap.original);
    if (price) prices.push(price);
    // Small delay to be nice to archive.org
    await new Promise((r) => setTimeout(r, SNAPSHOT_DELAY_MS));
  }

  return { sku, prices, snapshots: snapshots.length };
}

// ── Cron handler ────────────────────────────────────────────────────────────

async function handleCron(env) {
  const kv = env.TOOLPULSE_KV;

  // Get the queue of URLs to process
  const queueRaw = await kv.get("url_queue", "json");
  if (!queueRaw || !queueRaw.length) {
    console.log("No URLs in queue. POST /seed to add URLs.");
    return;
  }

  // Get progress pointer
  const progress = JSON.parse((await kv.get("progress")) || '{"index": 0}');
  const startIdx = progress.index;

  if (startIdx >= queueRaw.length) {
    console.log(
      `Backfill complete! Processed all ${queueRaw.length} products.`
    );
    return;
  }

  const batch = queueRaw.slice(startIdx, startIdx + BATCH_SIZE);
  console.log(
    `Processing batch: ${startIdx + 1}-${startIdx + batch.length} of ${queueRaw.length}`
  );

  for (const url of batch) {
    try {
      const result = await backfillProduct(url);
      if (result.prices.length > 0) {
        // Store prices in KV keyed by SKU
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

  // Update progress
  progress.index = startIdx + batch.length;
  progress.last_run = new Date().toISOString();
  await kv.put("progress", JSON.stringify(progress));
  console.log(`Progress: ${progress.index}/${queueRaw.length}`);
}

// ── HTTP handlers ───────────────────────────────────────────────────────────

async function handleRequest(request, env) {
  const url = new URL(request.url);
  const kv = env.TOOLPULSE_KV;

  // POST /seed — seed the URL queue from deal scraper item numbers
  if (url.pathname === "/seed" && request.method === "POST") {
    const body = await request.json().catch(() => null);
    let urls = [];

    if (body && body.urls) {
      // Direct URL list
      urls = body.urls;
    } else if (body && body.item_numbers) {
      // Resolve item numbers to URLs via CDX
      for (const item of body.item_numbers) {
        const cdxUrl = `${CDX_API}?url=harborfreight.com/*-${item}.html&output=json&fl=original&limit=1&filter=statuscode:200`;
        try {
          const resp = await fetch(cdxUrl);
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

  // GET /status — show backfill progress
  if (url.pathname === "/status") {
    const queue = (await kv.get("url_queue", "json")) || [];
    const progress = JSON.parse(
      (await kv.get("progress")) || '{"index": 0}'
    );
    return Response.json({
      total_products: queue.length,
      processed: progress.index,
      remaining: queue.length - progress.index,
      last_run: progress.last_run,
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

  // GET /run — manual trigger (same as cron)
  if (url.pathname === "/run") {
    await handleCron(env);
    const progress = JSON.parse(
      (await kv.get("progress")) || '{"index": 0}'
    );
    return Response.json({ status: "batch complete", progress });
  }

  return Response.json({
    service: "ToolPulse Wayback Worker",
    endpoints: {
      "POST /seed": "Seed URL queue (body: {urls: [...]} or {item_numbers: [...]})",
      "GET /status": "Backfill progress",
      "GET /prices/:sku": "Price history for a SKU",
      "GET /prices": "List all SKUs with data",
      "GET /export": "Export all price data",
      "GET /run": "Manually trigger a batch",
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
