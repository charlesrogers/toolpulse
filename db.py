#!/usr/bin/env python3
"""
ToolPulse: SQLite database layer.

Stores products, price snapshots, and coupon/deal observations.
Uses SQLite for Phase 1 — lightweight, zero-config, portable.
Can migrate to PostgreSQL + TimescaleDB later if needed.
"""

import os
import sqlite3
from datetime import datetime, timezone

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "toolpulse.db")


class ToolPulseDB:
    def __init__(self, db_path: str = DB_PATH):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._create_tables()

    def _create_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS products (
                item_number   TEXT PRIMARY KEY,
                product_name  TEXT,
                brand         TEXT,
                category_path TEXT,
                alt_item_numbers TEXT,  -- JSON array of alternate SKUs/lot numbers
                first_seen    TEXT NOT NULL,
                last_seen     TEXT NOT NULL,
                is_active     INTEGER DEFAULT 1,
                hf_url        TEXT
            );

            CREATE TABLE IF NOT EXISTS price_snapshots (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                item_number   TEXT NOT NULL REFERENCES products(item_number),
                snapshot_date TEXT NOT NULL,  -- YYYY-MM-DD
                regular_price REAL,
                sale_price    REAL,
                in_stock      INTEGER,
                source        TEXT NOT NULL,  -- 'wayback', 'go_hf', 'coupon_page', 'scrape'
                source_url    TEXT,
                raw_data      TEXT,  -- JSON blob of full scraped record
                created_at    TEXT NOT NULL,
                UNIQUE(item_number, snapshot_date, source)
            );

            CREATE TABLE IF NOT EXISTS deals (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                item_number   TEXT NOT NULL REFERENCES products(item_number),
                deal_price    REAL NOT NULL,
                coupon_code   TEXT,
                promo_id      TEXT,
                is_itc        INTEGER DEFAULT 0,
                valid_from    TEXT,  -- First date we observed this deal
                valid_through TEXT,  -- Expiration date
                source        TEXT NOT NULL,
                source_url    TEXT,
                coupon_url    TEXT,
                created_at    TEXT NOT NULL,
                UNIQUE(item_number, promo_id, deal_price)
            );

            CREATE INDEX IF NOT EXISTS idx_snapshots_item ON price_snapshots(item_number);
            CREATE INDEX IF NOT EXISTS idx_snapshots_date ON price_snapshots(snapshot_date);
            CREATE INDEX IF NOT EXISTS idx_deals_item ON deals(item_number);
            CREATE INDEX IF NOT EXISTS idx_deals_valid ON deals(valid_through);
        """)
        self.conn.commit()

    # ── Products ─────────────────────────────────────────────────────────────

    def upsert_product(self, item_number: str, product_name: str = None,
                       brand: str = None, alt_items: list = None,
                       hf_url: str = None) -> bool:
        """Insert or update a product. Returns True if new product."""
        now = datetime.now(timezone.utc).isoformat()
        import json as _json
        alt_json = _json.dumps(alt_items) if alt_items else None

        existing = self.conn.execute(
            "SELECT item_number FROM products WHERE item_number = ?",
            (item_number,)
        ).fetchone()

        if existing:
            updates = {"last_seen": now}
            if product_name:
                updates["product_name"] = product_name
            if brand:
                updates["brand"] = brand
            if alt_json:
                updates["alt_item_numbers"] = alt_json
            if hf_url:
                updates["hf_url"] = hf_url

            set_clause = ", ".join(f"{k} = ?" for k in updates)
            self.conn.execute(
                f"UPDATE products SET {set_clause} WHERE item_number = ?",
                (*updates.values(), item_number)
            )
            self.conn.commit()
            return False
        else:
            self.conn.execute(
                """INSERT INTO products (item_number, product_name, brand,
                   alt_item_numbers, first_seen, last_seen, hf_url)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (item_number, product_name, brand, alt_json, now, now, hf_url)
            )
            self.conn.commit()
            return True

    # ── Price Snapshots ──────────────────────────────────────────────────────

    def add_price_snapshot(self, item_number: str, date: str, price: float,
                           source: str, source_url: str = None,
                           in_stock: bool = None, raw_data: dict = None) -> bool:
        """Add a price snapshot. Returns True if inserted (not duplicate)."""
        import json as _json
        now = datetime.now(timezone.utc).isoformat()
        raw_json = _json.dumps(raw_data) if raw_data else None

        try:
            self.conn.execute(
                """INSERT INTO price_snapshots
                   (item_number, snapshot_date, regular_price, in_stock, source,
                    source_url, raw_data, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (item_number, date, price, 1 if in_stock else (0 if in_stock is False else None),
                 source, source_url, raw_json, now)
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False  # Duplicate

    # ── Deals ────────────────────────────────────────────────────────────────

    def upsert_deals(self, deals: list[dict]) -> tuple[int, int]:
        """Bulk insert/update deals from scraper output. Returns (inserted, updated)."""
        inserted = 0
        updated = 0
        now = datetime.now(timezone.utc).isoformat()

        for deal in deals:
            item = deal.get("item_number")
            if not item:
                continue

            # Ensure product exists
            self.upsert_product(
                item_number=item,
                product_name=deal.get("product_name"),
                alt_items=deal.get("alt_item_numbers"),
            )

            promo_id = deal.get("promo_id")
            deal_price = deal.get("price")

            # Use snapshot_date (actual archive date) as valid_from when available
            valid_from = deal.get("snapshot_date") or now[:10]

            try:
                self.conn.execute(
                    """INSERT INTO deals
                       (item_number, deal_price, coupon_code, promo_id, is_itc,
                        valid_from, valid_through, source, source_url, coupon_url, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (item, deal_price, deal.get("coupon_code"), promo_id,
                     1 if deal.get("is_itc") else 0,
                     valid_from, deal.get("valid_through"),
                     deal.get("source", "go_hf"), deal.get("source_url"),
                     deal.get("coupon_url"), now)
                )
                inserted += 1
            except sqlite3.IntegrityError:
                # Update existing
                self.conn.execute(
                    """UPDATE deals SET deal_price = ?, coupon_code = COALESCE(?, coupon_code),
                       valid_through = COALESCE(?, valid_through)
                       WHERE item_number = ? AND promo_id = ? AND deal_price = ?""",
                    (deal_price, deal.get("coupon_code"), deal.get("valid_through"),
                     item, promo_id, deal_price)
                )
                updated += 1

            # Also record as a price snapshot (sale_price) so deals show in price history
            if deal_price and valid_from:
                source = deal.get("source", "go_hf")
                try:
                    self.conn.execute(
                        """INSERT INTO price_snapshots
                           (item_number, snapshot_date, regular_price, sale_price, source,
                            source_url, created_at)
                           VALUES (?, ?, NULL, ?, ?, ?, ?)""",
                        (item, valid_from, deal_price, f"deal_{source}",
                         deal.get("source_url"), now)
                    )
                except sqlite3.IntegrityError:
                    pass  # Already have a snapshot for this item+date+source

        self.conn.commit()
        return inserted, updated

    # ── Wayback price import ─────────────────────────────────────────────────

    def import_wayback_prices(self, sku: str, prices: list[dict]) -> int:
        """Import historical prices from Wayback Machine backfill."""
        count = 0
        for p in prices:
            item_number = p.get("sku", sku)

            # Ensure product exists
            self.upsert_product(
                item_number=item_number,
                product_name=p.get("product_name"),
                brand=p.get("brand"),
            )

            added = self.add_price_snapshot(
                item_number=item_number,
                date=p.get("date"),
                price=p.get("price"),
                source="wayback",
                source_url=p.get("wayback_url"),
                in_stock=p.get("in_stock"),
                raw_data=p,
            )
            if added:
                count += 1

        return count

    # ── Queries ──────────────────────────────────────────────────────────────

    def get_product(self, item_number: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM products WHERE item_number = ?", (item_number,)
        ).fetchone()
        return dict(row) if row else None

    def get_price_history(self, item_number: str) -> list[dict]:
        rows = self.conn.execute(
            """SELECT snapshot_date, regular_price, source
               FROM price_snapshots WHERE item_number = ?
               ORDER BY snapshot_date""",
            (item_number,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_active_deals(self, min_date: str = None) -> list[dict]:
        if not min_date:
            min_date = datetime.now(timezone.utc).strftime("%m/%d/%Y")
        rows = self.conn.execute(
            """SELECT d.*, p.product_name, p.brand
               FROM deals d JOIN products p ON d.item_number = p.item_number
               WHERE d.valid_through >= ? OR d.valid_through IS NULL
               ORDER BY d.deal_price ASC""",
            (min_date,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_stats(self) -> dict:
        products = self.conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        snapshots = self.conn.execute("SELECT COUNT(*) FROM price_snapshots").fetchone()[0]
        deals = self.conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
        return {"products": products, "price_snapshots": snapshots, "deals": deals}

    def close(self):
        self.conn.close()


if __name__ == "__main__":
    db = ToolPulseDB()
    stats = db.get_stats()
    print(f"ToolPulse Database: {DB_PATH}")
    print(f"  Products:        {stats['products']}")
    print(f"  Price snapshots: {stats['price_snapshots']}")
    print(f"  Deals:           {stats['deals']}")
    db.close()
