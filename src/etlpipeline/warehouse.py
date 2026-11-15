"""Load: a SQLite warehouse for the clean orders.

The warehouse is deliberately boring — one fact table, one quarantine
table, one run-log table — so the load is idempotent: running the
pipeline twice over the same orders leaves exactly the same rows.
``INSERT OR REPLACE`` keyed on ``order_id`` is what makes a re-run
safe, and the run log is what makes a re-run visible.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    customer_name TEXT NOT NULL,
    customer_email TEXT NOT NULL,
    customer_country TEXT NOT NULL,
    product_id TEXT NOT NULL,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    currency TEXT NOT NULL,
    fx_rate REAL NOT NULL,
    total_usd REAL NOT NULL,
    order_date TEXT NOT NULL,
    ordered_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quarantine (
    order_id TEXT,
    reason TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    PRIMARY KEY (order_id, reason)
);
CREATE TABLE IF NOT EXISTS runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    extracted INTEGER NOT NULL,
    clean INTEGER NOT NULL,
    quarantined INTEGER NOT NULL,
    duplicates_removed INTEGER NOT NULL,
    retries INTEGER NOT NULL,
    total_usd REAL NOT NULL
);
"""

ORDER_COLUMNS = (
    "order_id", "customer_id", "customer_name", "customer_email",
    "customer_country", "product_id", "product_name", "category",
    "quantity", "unit_price", "currency", "fx_rate", "total_usd",
    "order_date", "ordered_at", "updated_at",
)


class Warehouse:
    """A SQLite warehouse, opened on a file or in memory."""

    def __init__(self, path: str | Path | None = None):
        self.path = str(path) if path is not None else ":memory:"
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Warehouse":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def load_orders(self, rows: list[dict]) -> int:
        """Upsert clean rows. Returns the number of rows loaded."""
        placeholders = ", ".join("?" * len(ORDER_COLUMNS))
        sql = (
            f"INSERT OR REPLACE INTO orders ({', '.join(ORDER_COLUMNS)}) "
            f"VALUES ({placeholders})"
        )
        with self.conn:
            self.conn.executemany(
                sql, [[row[c] for c in ORDER_COLUMNS] for row in rows]
            )
        return len(rows)

    def load_quarantine(self, quarantined: list[dict]) -> int:
        """Store quarantined rows with their reason and raw payload."""
        with self.conn:
            self.conn.executemany(
                "INSERT OR REPLACE INTO quarantine "
                "(order_id, reason, raw_json) VALUES (?, ?, ?)",
                [
                    (q.get("order_id"), q["reason"],
                     json.dumps(q["raw"], sort_keys=True))
                    for q in quarantined
                ],
            )
        return len(quarantined)

    def log_run(self, summary: dict) -> int:
        """Append one row to the run log. Returns the run id."""
        cur = self.conn.execute(
            "INSERT INTO runs (extracted, clean, quarantined,"
            " duplicates_removed, retries, total_usd)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (
                summary["extracted"], summary["clean"],
                summary["quarantined"], summary["duplicates_removed"],
                summary["retries"], summary["total_usd"],
            ),
        )
        self.conn.commit()
        return cur.lastrowid

    def order_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]

    def quarantine_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0]

    def run_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]

    def revenue_by_category(self) -> dict[str, float]:
        return {
            row["category"]: round(row["total"], 2)
            for row in self.conn.execute(
                "SELECT category, SUM(total_usd) AS total FROM orders"
                " GROUP BY category ORDER BY category"
            )
        }

    def revenue_by_country(self) -> dict[str, float]:
        return {
            row["customer_country"]: round(row["total"], 2)
            for row in self.conn.execute(
                "SELECT customer_country, SUM(total_usd) AS total FROM orders"
                " GROUP BY customer_country ORDER BY customer_country"
            )
        }

    def daily_revenue(self) -> dict[str, float]:
        return {
            row["order_date"]: round(row["total"], 2)
            for row in self.conn.execute(
                "SELECT order_date, SUM(total_usd) AS total FROM orders"
                " GROUP BY order_date ORDER BY order_date"
            )
        }

    def top_customers(self, limit: int = 3) -> list[dict]:
        return [
            {
                "customer_id": row["customer_id"],
                "customer_name": row["customer_name"],
                "orders": row["orders"],
                "total_usd": round(row["total"], 2),
            }
            for row in self.conn.execute(
                "SELECT customer_id, customer_name, COUNT(*) AS orders,"
                " SUM(total_usd) AS total FROM orders"
                " GROUP BY customer_id ORDER BY total DESC, customer_id"
                " LIMIT ?",
                (limit,),
            )
        ]

    def quarantine_by_reason(self) -> dict[str, int]:
        return {
            row["reason"]: row["n"]
            for row in self.conn.execute(
                "SELECT reason, COUNT(*) AS n FROM quarantine"
                " GROUP BY reason ORDER BY reason"
            )
        }
