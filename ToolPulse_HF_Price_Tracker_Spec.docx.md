

**PRODUCT SPEC**

**Harbor Freight Price & Coupon Tracker**

*Codename: ToolPulse*

Version 1.0  |  March 10, 2026

Author: Charles

**Status: DRAFT — Data Accumulation Phase Planning**

*CONFIDENTIAL — Not for distribution*

# **1\. Executive Summary**

This spec defines ToolPulse, a system to independently scrape, store, and analyze Harbor Freight product prices, coupon data, and promotional history. The goal is to build a proprietary dataset from direct HarborFreight.com scraping (not from third-party trackers), accumulate 6–12 months of daily data, and ultimately power consumer-facing deal intelligence that improves on existing solutions.

| ETHICAL STANCE We will NOT scrape Hazard Fraught (hfpricetracker.com). Randy Forte built a great community resource. Scraping his work product would be ethically wrong and legally questionable. Instead, we scrape the same public source he does—HarborFreight.com—and build our own independent dataset. We may also backfill historical data via Wayback Machine snapshots of HarborFreight.com product pages (not of hfpricetracker.com). |
| :---- |

# **2\. Competitive Landscape: What Already Exists**

## **2.1 Hazard Fraught (hfpricetracker.com)**

Built by Randy Forte as a solo passion project. Currently the gold standard for HF price tracking.

**What it tracks:** 8,470+ products, 55,706 price changes, 52,574 coupon events documented.

**Features:** Per-SKU price history charts, coupon history/frequency, “deals” page (items at all-time-low), email alerts when items drop below a user-set threshold, ITC (Inside Track Club) coupon section, Recent Price Changes feed, Under $10/$20 filters, brand browsing.

**Update frequency:** Daily.

**Known limitations:** No product images (recently removed, likely due to HF legal pressure). SKU change problem—when HF retires a SKU and issues a new one for the same product, Hazard Fraught loses continuity. No “coupon-adjusted price” calculation (just shows price \+ separate coupon existence). No category taxonomy. No comparison to prior-year seasonal patterns. No API.

## **2.2 HFQPDB (hfqpdb.com)**

Focuses specifically on coupon codes. Lists active coupons with lot numbers, expiration dates, and coupon codes. More coupon-focused than price-focused. Shows current coupon prices but minimal history. Has been around longer than Hazard Fraught.

## **2.3 Harbor Freight Official Coupon Sources**

* go.harborfreight.com — HF’s official coupon blog. Monthly “instant savings” posted here.

* harborfreight.com/coupons — On-site coupon page. Requires account login for “save” functionality.

* HF App — Pushes coupon notifications. Inside Track Club members get early access.

* Email newsletters — Weekly+ frequency, 100+ items per blast.

# **3\. Data Sources & What We Scrape**

## **3.1 Primary Source: HarborFreight.com**

Harbor Freight publishes a sitemap at harborfreight.com/sitemap.xml. Their robots.txt explicitly allows Googlebot and other major crawlers with no blanket disallow. Product pages follow the URL pattern:

  www.harborfreight.com/{product-slug}-{SKU}.html

The SKU (5+ digit number) is the canonical product identifier.

## **3.2 Data Points Per Product (Daily Snapshot)**

| Field | Source | Notes |
| :---- | :---- | :---- |
| sku | URL pattern | Primary key. Regex: \-(\\d{5,})\\.html$ |
| product\_name | Page title / H1 | Canonical name |
| brand | Product detail section | e.g., BAUER, ICON, US GENERAL |
| current\_price | Price element | Regular shelf price |
| sale\_price | Sale badge if present | Null when not on sale |
| coupon\_price | Coupon section on page | Instant Savings price if active |
| itc\_coupon\_price | ITC badge | Inside Track Club member price |
| coupon\_code | Coupon detail | Numeric code if shown |
| coupon\_expiry | Coupon detail | Expiration date |
| in\_stock | Availability indicator | Boolean |
| category\_breadcrumb | Breadcrumb nav | Full category path |
| lot\_numbers | Product detail | Alternate SKUs / lot numbers |
| snapshot\_timestamp | Our system | UTC timestamp of scrape |

## **3.3 Coupon Aggregation Sources**

* go.harborfreight.com — Scrape the monthly instant savings pages for coupon codes \+ SKU mappings.

* HF email newsletters — Optionally parse with a dedicated inbox (sign up for HF emails, auto-parse with a script).

* harborfreight.com/coupons — On-site coupon listing page.

## **3.4 Historical Backfill: Wayback Machine**

The Internet Archive has snapshots of HarborFreight.com product pages going back years. We can use the CDX API to find all archived snapshots of HF product pages and extract historical prices.

**CDX API query pattern:** http://web.archive.org/cdx/search/cdx?url=harborfreight.com/\*-{SKU}.html\&output=json\&fl=timestamp,original,statuscode

This gives us timestamps and archived URLs we can then fetch to extract historical prices. We should expect spotty coverage—Wayback doesn’t capture every page every day—but it can provide months or years of intermittent price points that would take us years to accumulate organically.

| BACKFILL STRATEGY Phase 0 (pre-launch): Run a Wayback Machine historical extraction for the top 500–1,000 HF SKUs. This seeds our database with months/years of historical context before our daily scraper even starts. This is ethically clean—we’re reading HarborFreight.com’s own pages as archived by a public nonprofit. |
| :---- |

# **4\. System Architecture**

## **4.1 High-Level Components**

| Component | Technology | Purpose |
| :---- | :---- | :---- |
| Scraper | Python (httpx \+ selectolax or Playwright) | Daily product page scraping |
| Scheduler | cron (Phase 1\) → Temporal (Phase 2\) | Orchestrate daily scrape jobs |
| Database | PostgreSQL \+ TimescaleDB | Time-series price storage |
| Object Store | S3 / R2 (Cloudflare) | Raw HTML snapshots for re-parsing |
| Backfill Worker | Python \+ Wayback CDX API | Historical price extraction |
| API Layer | FastAPI | Query interface for frontend/analysis |
| Monitoring | Uptime Kuma \+ PagerDuty | Scrape health, failure alerts |
| Frontend (Phase 3\) | Next.js or Astro | Consumer-facing deal site |

## **4.2 Scraping Strategy**

### **4.2.1 Respectful Crawling**

HarborFreight.com’s robots.txt sets Crawl-Delay: 10 for Bingbot and some others. We’ll respect a conservative crawl delay:

* Minimum 5-second delay between requests (more conservative than required)

* Scrape during off-peak hours (2–6 AM Mountain Time)

* Identify our crawler with an honest User-Agent string (e.g., ToolPulse/1.0 \+contact@toolpulse.com)

* Respect robots.txt disallow directives (we only need product pages anyway)

* Target \~8,500 products × 5s delay \= \~12 hours for a full catalog sweep. This is fine for daily cadence with staggered batches.

### **4.2.2 Product Discovery**

1. Parse harborfreight.com/sitemap.xml to discover all product URLs.

2. Extract SKU from each URL using regex.

3. Maintain a product\_catalog table; diff against sitemap daily to detect new/removed products.

4. For new products, immediately queue a scrape. For removed products, mark as discontinued with timestamp.

### **4.2.3 Data Extraction Approach**

Two-tier strategy depending on HF’s anti-bot posture:

**Tier 1 (preferred): HTTP-only.** Use httpx with rotating residential proxies. Parse HTML with selectolax or lxml. Fastest, cheapest. HarborFreight.com is a Magento-based site and historically hasn’t deployed aggressive anti-bot measures.

**Tier 2 (fallback): Headless browser.** If HF adds Cloudflare/Datadome or heavy JS rendering, fall back to Playwright with stealth plugins. Slower and more expensive but reliable.

Before building either tier, inspect the HF product page for structured data. Many e-commerce sites embed JSON-LD schema.org Product markup in \<script\> tags, which is far more reliable to parse than HTML elements. Check for hidden API endpoints too—the Magento storefront often has GraphQL or REST endpoints that return clean JSON.

# **5\. Database Schema**

## **5.1 Core Tables**

**products**

| Column | Type | Description |
| :---- | :---- | :---- |
| sku | VARCHAR(10) PK | Harbor Freight SKU number |
| product\_name | TEXT | Current canonical name |
| brand | VARCHAR(100) | Brand name |
| category\_path | TEXT | Full breadcrumb path |
| lot\_numbers | TEXT\[\] | Array of alternate lot/SKU numbers |
| first\_seen | TIMESTAMPTZ | When we first scraped this product |
| last\_seen | TIMESTAMPTZ | Most recent successful scrape |
| is\_active | BOOLEAN | Still on HF site? |
| hf\_url | TEXT | Full product URL |

**price\_snapshots  (TimescaleDB hypertable)**

| Column | Type | Description |
| :---- | :---- | :---- |
| snapshot\_time | TIMESTAMPTZ | Time of observation (partition key) |
| sku | VARCHAR(10) FK | References products.sku |
| regular\_price | NUMERIC(8,2) | Shelf price |
| sale\_price | NUMERIC(8,2) | Sale price (null if not on sale) |
| in\_stock | BOOLEAN | Availability at time of scrape |
| source | VARCHAR(20) | "scrape", "wayback", "manual" |

**coupons**

| Column | Type | Description |
| :---- | :---- | :---- |
| coupon\_id | SERIAL PK | Auto-increment ID |
| sku | VARCHAR(10) FK | Product this coupon applies to |
| coupon\_code | VARCHAR(20) | Numeric coupon code |
| coupon\_price | NUMERIC(8,2) | Price with coupon applied |
| is\_itc\_only | BOOLEAN | Requires Inside Track Club? |
| valid\_from | DATE | First date we observed this coupon |
| valid\_until | DATE | Expiration date from coupon |
| source | VARCHAR(50) | "product\_page", "go.hf.com", "email" |
| first\_seen | TIMESTAMPTZ | When we first found this coupon |

**raw\_snapshots  (S3/R2 object store)**

Every scraped HTML page is stored as a compressed object in cloud storage. Key format: {date}/{sku}.html.gz. This allows us to re-parse historical pages if we improve our extraction logic later. Storage cost at \~8,500 pages/day × \~50KB compressed ≈ 425MB/day ≈ \~12GB/month. At R2’s free tier (10GB) or S3 pricing ($0.023/GB), this is essentially free.

## **5.2 Derived/Computed Views**

* **effective\_price:** MIN(regular\_price, sale\_price, best\_coupon\_price) for any given SKU on any given day. This is the key differentiator vs. Hazard Fraught—they show price and coupon separately; we show the actual best price you’d pay.

* **price\_percentile:** Where current effective\_price sits vs. the product’s own historical range. “This is cheaper than 92% of the time we’ve tracked it.”

* **seasonal\_pattern:** After 12+ months of data, compute per-SKU seasonal trends. “This item is historically cheapest in October.”

* **coupon\_frequency:** How often a product gets couponed and the typical discount depth. “This item gets a coupon every \~45 days, averaging 22% off.”

# **6\. Phased Roadmap**

| PHASE 0: Historical Backfill (Weeks 1–2) •  Write Wayback Machine CDX API querier to find all archived HF product page snapshots •  Build HTML parser for HF product pages (handle multiple historical page layouts) •  Extract and load historical prices into database with source="wayback" •  Target: Top 1,000 SKUs, as many historical data points as Wayback has |
| :---- |

| PHASE 1: Data Accumulation (Months 1–6) •  Deploy daily scraper on a $5/mo VPS (Hetzner, DigitalOcean, or Linode) •  Full catalog discovery from sitemap.xml; daily price snapshots for all \~8,500 products •  Coupon scraping from product pages \+ go.harborfreight.com •  Raw HTML archival to S3/R2 •  Basic monitoring: Slack/email alerts on scrape failures •  No public frontend—just accumulating data and building the derived tables |
| :---- |

| PHASE 2: Analysis & API (Months 6–12) •  Compute effective\_price, price\_percentile, coupon\_frequency views •  Build FastAPI query layer: GET /products/{sku}/history, GET /deals, GET /coupons •  SKU linkage resolver: detect when HF retires/replaces a SKU •  Personal alert system: “Notify me when SKU X drops below $Y” •  Begin seasonal pattern analysis (requires 6+ months of data minimum) |
| :---- |

| PHASE 3: Consumer Frontend (Month 12+) •  Public website with product search, price history charts, deal alerts •  Key differentiator: “coupon-adjusted price” as the headline metric (not just sticker price) •  “Buy now or wait?” recommendation engine based on seasonal patterns \+ coupon frequency •  Browser extension (Tampermonkey-compatible) for inline HF.com price history •  Potential monetization: affiliate links, Inside Track Club referrals, ads |
| :---- |

# **7\. How We Improve on Hazard Fraught**

| Gap in Hazard Fraught | ToolPulse Solution |
| :---- | :---- |
| Shows price \+ coupon separately | Compute and display effective\_price (best available price considering all active coupons) |
| No “should I buy now?” signal | Price percentile \+ seasonal pattern \= “This is in the bottom 8% of prices; buy now” |
| SKU discontinuity when HF changes SKU | Lot number cross-referencing \+ product name fuzzy matching to link old→new SKUs |
| No category taxonomy / browsing | Full category tree from breadcrumbs, enabling “show me all drill presses sorted by deal quality” |
| No API | Public REST API from Phase 2—enables community tooling, browser extensions, etc. |
| No coupon frequency analytics | Per-SKU coupon cadence: “This gets couponed every \~6 weeks, next one likely around April 15” |
| No ITC ROI calculator | Given your watchlist, “Inside Track Club would save you $X/year based on ITC-exclusive coupons on your items” |
| Email alerts only | Multi-channel: email, push, SMS, Discord/Slack webhook, RSS feed |
| No tariff impact tracking | Track price changes alongside tariff announcements to show tariff-driven inflation |

# **8\. Infrastructure & Cost Estimate (Phase 1\)**

| Item | Provider | Monthly Cost |
| :---- | :---- | :---- |
| VPS (scraper \+ DB) | Hetzner CX22 (2 vCPU, 4GB RAM, 40GB) | $5–7 |
| Object storage (raw HTML) | Cloudflare R2 (10GB free tier) | $0 |
| Residential proxies (if needed) | Bright Data / Oxylabs pay-as-you-go | $0–20 |
| Domain \+ DNS | Cloudflare | $10/yr |
| Monitoring | Uptime Kuma (self-hosted) | $0 |
| Backups | pg\_dump to R2, daily | $0 |
| TOTAL (Phase 1\) |  | $5–27/mo |

This is deliberately cheap. The entire Phase 1 system should fit on a single small VPS. PostgreSQL \+ TimescaleDB handles the time-series data without needing a separate TSDB. We only scale up when we launch a public frontend in Phase 3\.

# **9\. Legal & Ethical Considerations**

## **9.1 Scraping HarborFreight.com**

* Public pricing data is generally considered factual information, not protectable by copyright.

* We respect robots.txt directives and crawl-delay.

* We identify ourselves with an honest User-Agent.

* We do not circumvent any authentication, paywall, or access control.

* We store facts (prices, names, dates)—not copyrighted creative content.

* Precedent: HiQ v. LinkedIn (9th Circuit) affirmed that scraping publicly available data is not a CFAA violation.

## **9.2 NOT Scraping Hazard Fraught**

Randy Forte’s hfpricetracker.com represents original compilation and curation work. Scraping his processed data would be:

* Ethically wrong—he built a free community tool as a passion project.

* Legally riskier—compiled databases can have sui generis protection, and his site’s ToS likely prohibits scraping.

* Strategically unnecessary—the same raw data is available from HarborFreight.com directly.

## **9.3 Wayback Machine Usage**

The Internet Archive is a public resource. Their CDX API is explicitly provided for programmatic access. We’re reading archived copies of HarborFreight.com’s own pages—pages HF themselves made public. This is clean.

# **10\. Risks & Mitigations**

| Risk | Likelihood | Impact | Mitigation |
| :---- | :---- | :---- | :---- |
| HF deploys anti-bot (Cloudflare, etc.) | Medium | High | Tier 2 fallback (Playwright \+ stealth). Residential proxy rotation. Slower crawl rate. |
| HF changes page structure | High | Medium | Raw HTML archival means we can re-parse. Schema.org JSON-LD as primary extraction target (more stable than HTML classes). |
| HF sends cease-and-desist | Low | High | We’re scraping public data respectfully. Comply with any reasonable request. Have a lawyer review before launch. |
| Wayback coverage is too sparse | Medium | Low | It’s bonus data. Our primary strategy is forward-looking daily scraping. |
| Database growth outpaces VPS | Low | Low | TimescaleDB compression. Move to managed DB when needed. 8,500 rows/day \= \~3M rows/year \= trivial. |
| SKU change problem (same product, new SKU) | High | Medium | Lot number cross-referencing \+ product name matching \+ manual curation for high-value items. |

# **11\. Success Metrics**

## **Phase 1 (Data Accumulation) — 6 months**

* 95%+ daily scrape success rate (products scraped / products in catalog)

* Full catalog coverage: tracking all \~8,500 active HF products

* Zero missed days (uptime target: 99.5%+)

* Historical backfill: 500+ SKUs with 12+ months of Wayback data

## **Phase 2 (Analysis) — 12 months**

* Effective price calculation for 100% of products with active coupons

* Seasonal pattern detection for top 500 SKUs

* API response time \<200ms for single-product queries

## **Phase 3 (Frontend) — 18 months**

* 1,000+ monthly unique visitors within 3 months of launch

* User engagement: \>2 pages/session average

* Community contribution: 10+ user-submitted price corrections/month

# **12\. Immediate Next Steps**

1. **Validate HF page structure:** Manually inspect 10 HF product pages. Check for JSON-LD structured data, identify CSS selectors for price/coupon elements, test if pages render without JS.

2. **Wayback feasibility check:** Query CDX API for 5 popular SKUs. Assess snapshot density and whether archived pages contain parseable price data.

3. **Prototype scraper:** Build a single-product scraper in Python. Test against 100 products. Measure extraction accuracy and rate-limiting behavior.

4. **Set up infrastructure:** Provision VPS, install PostgreSQL \+ TimescaleDB, configure R2 bucket, deploy scraper with cron.

5. **Let it cook:** Daily scraping begins. Monitor for 2 weeks. Fix edge cases. Then walk away and let data accumulate for 6 months.

*END OF SPEC*