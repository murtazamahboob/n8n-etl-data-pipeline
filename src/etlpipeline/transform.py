"""Transform: clean, validate, dedupe and convert raw orders to USD.

Rules, in order:
1. Dedupe by order_id, keeping the row with the latest updated_at.
2. Validate: unknown customer / product / currency, non-positive
   quantity or unit price, missing order id. Invalid rows are
   quarantined with a reason — never silently dropped.
3. Normalise: customer email lower-cased and stripped; order date
   taken from ordered_at; line total converted to USD at a fixed rate
   and rounded to 2 decimals.
"""
from __future__ import annotations

import copy

# Fixed conversion rates to USD (pinned so runs are deterministic).
FX_RATES = {"USD": 1.0, "EUR": 1.08, "GBP": 1.27}

QUARANTINE_REASONS = (
    "UNKNOWN_CUSTOMER", "UNKNOWN_PRODUCT", "UNKNOWN_CURRENCY",
    "INVALID_QTY", "INVALID_PRICE", "MISSING_ORDER_ID",
)


def dedupe_orders(rows: list[dict]) -> tuple[list[dict], int]:
    """Keep the latest updated_at row per order_id.

    Returns (deduped_rows, duplicates_removed). Input order is kept
    for the surviving rows, sorted by order_id for determinism.
    """
    best: dict[str, dict] = {}
    removed = 0
    for row in rows:
        oid = row.get("order_id")
        if oid is None:
            continue  # handled as MISSING_ORDER_ID by validate below
        if oid in best:
            removed += 1
            if str(row.get("updated_at", "")) > str(best[oid].get("updated_at", "")):
                best[oid] = row
        else:
            best[oid] = row
    return [best[k] for k in sorted(best)], removed


def validate_row(row: dict, customers: dict, products: dict) -> str | None:
    """Return a quarantine reason, or None when the row is valid."""
    if not row.get("order_id"):
        return "MISSING_ORDER_ID"
    if row.get("customer_id") not in customers:
        return "UNKNOWN_CUSTOMER"
    if row.get("product_id") not in products:
        return "UNKNOWN_PRODUCT"
    if row.get("currency") not in FX_RATES:
        return "UNKNOWN_CURRENCY"
    qty = row.get("quantity")
    if not isinstance(qty, (int, float)) or isinstance(qty, bool) or qty <= 0:
        return "INVALID_QTY"
    price = row.get("unit_price")
    if not isinstance(price, (int, float)) or isinstance(price, bool) or price <= 0:
        return "INVALID_PRICE"
    return None


def transform_row(row: dict, customers: dict, products: dict) -> dict:
    """Convert one validated row into a warehouse-ready fact row."""
    customer = customers[row["customer_id"]]
    product = products[row["product_id"]]
    rate = FX_RATES[row["currency"]]
    total_usd = round(float(row["quantity"]) * float(row["unit_price"]) * rate, 2)
    return {
        "order_id": row["order_id"],
        "customer_id": row["customer_id"],
        "customer_name": customer["name"],
        "customer_email": customer["email"].strip().lower(),
        "customer_country": customer["country"],
        "product_id": row["product_id"],
        "product_name": product["name"],
        "category": product["category"],
        "quantity": row["quantity"],
        "unit_price": row["unit_price"],
        "currency": row["currency"],
        "fx_rate": rate,
        "total_usd": total_usd,
        "order_date": str(row["ordered_at"])[:10],
        "ordered_at": row["ordered_at"],
        "updated_at": row["updated_at"],
    }


def transform_orders(rows: list[dict], customers: list[dict],
                     products: list[dict]) -> dict:
    """Run the full transform. Returns clean rows, quarantine and counts."""
    customer_map = {c["customer_id"]: c for c in customers}
    product_map = {p["product_id"]: p for p in products}
    deduped, duplicates_removed = dedupe_orders(rows)
    # Rows with no order_id never reach dedupe; count them as quarantined.
    missing = [r for r in rows if not r.get("order_id")]
    clean: list[dict] = []
    quarantined: list[dict] = [
        {"order_id": None, "reason": "MISSING_ORDER_ID", "raw": copy.deepcopy(r)}
        for r in missing
    ]
    for row in deduped:
        reason = validate_row(row, customer_map, product_map)
        if reason is not None:
            quarantined.append({
                "order_id": row.get("order_id"),
                "reason": reason,
                "raw": copy.deepcopy(row),
            })
        else:
            clean.append(transform_row(row, customer_map, product_map))
    return {
        "clean": clean,
        "quarantined": quarantined,
        "duplicates_removed": duplicates_removed,
        "extracted": len(rows),
    }
