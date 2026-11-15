"""Tests for the transform stage: dedupe, validate, convert."""
from __future__ import annotations

from etlpipeline.source import CUSTOMERS, PRODUCTS, RAW_ORDERS, MockOrdersApi
from etlpipeline.transform import (
    dedupe_orders,
    transform_orders,
    transform_row,
    validate_row,
)

CUSTOMER_MAP = {c["customer_id"]: c for c in CUSTOMERS}
PRODUCT_MAP = {p["product_id"]: p for p in PRODUCTS}


def _rows():
    api = MockOrdersApi()
    rows, _ = api.fetch_all()
    return rows


def test_dedupe_keeps_latest_updated_at():
    deduped, removed = dedupe_orders(_rows())
    assert removed == 1
    kept = [r for r in deduped if r["order_id"] == "ORD-007"][0]
    assert kept["quantity"] == 3
    assert kept["updated_at"] == "2026-11-09T08:00:00"


def test_dedupe_skips_rows_without_order_id():
    rows = [{"customer_id": "CUST-001", "updated_at": "2026-11-01T00:00:00"}]
    deduped, removed = dedupe_orders(rows)
    assert deduped == [] and removed == 0


def test_validate_row_reasons():
    base = {"order_id": "ORD-900", "customer_id": "CUST-001",
            "product_id": "PROD-101", "quantity": 1, "unit_price": 10.0,
            "currency": "USD"}
    assert validate_row(base, CUSTOMER_MAP, PRODUCT_MAP) is None
    assert validate_row({**base, "customer_id": "CUST-999"},
                        CUSTOMER_MAP, PRODUCT_MAP) == "UNKNOWN_CUSTOMER"
    assert validate_row({**base, "product_id": "PROD-999"},
                        CUSTOMER_MAP, PRODUCT_MAP) == "UNKNOWN_PRODUCT"
    assert validate_row({**base, "currency": "JPY"},
                        CUSTOMER_MAP, PRODUCT_MAP) == "UNKNOWN_CURRENCY"
    assert validate_row({**base, "quantity": -1},
                        CUSTOMER_MAP, PRODUCT_MAP) == "INVALID_QTY"
    assert validate_row({**base, "quantity": 0},
                        CUSTOMER_MAP, PRODUCT_MAP) == "INVALID_QTY"
    assert validate_row({**base, "quantity": True},
                        CUSTOMER_MAP, PRODUCT_MAP) == "INVALID_QTY"
    assert validate_row({**base, "unit_price": 0},
                        CUSTOMER_MAP, PRODUCT_MAP) == "INVALID_PRICE"
    assert validate_row({**base, "order_id": ""},
                        CUSTOMER_MAP, PRODUCT_MAP) == "MISSING_ORDER_ID"


def test_transform_row_converts_currency_and_normalises_email():
    row = {"order_id": "ORD-002", "customer_id": "CUST-002",
           "product_id": "PROD-102", "quantity": 1, "unit_price": 89.99,
           "currency": "EUR", "ordered_at": "2026-11-01T11:30:00",
           "updated_at": "2026-11-01T11:30:00"}
    out = transform_row(row, CUSTOMER_MAP, PRODUCT_MAP)
    assert out["total_usd"] == 97.19  # 89.99 * 1.08
    assert out["fx_rate"] == 1.08
    assert out["order_date"] == "2026-11-01"
    assert out["customer_name"] == "Bilal Ahmed"
    assert out["category"] == "Electronics"


def test_email_normalised_from_customer_record():
    row = {"order_id": "ORD-001", "customer_id": "CUST-001",
           "product_id": "PROD-101", "quantity": 2, "unit_price": 25.00,
           "currency": "USD", "ordered_at": "2026-11-01T10:00:00",
           "updated_at": "2026-11-01T10:00:00"}
    out = transform_row(row, CUSTOMER_MAP, PRODUCT_MAP)
    assert out["customer_email"] == "ayesha@example.com"  # was "AYESHA@Example.com "


def test_full_transform_counts():
    result = transform_orders(_rows(), CUSTOMERS, PRODUCTS)
    assert result["extracted"] == 20
    assert len(result["clean"]) == 15
    assert len(result["quarantined"]) == 4
    assert result["duplicates_removed"] == 1


def test_quarantine_reasons_cover_all_four_bad_rows():
    result = transform_orders(_rows(), CUSTOMERS, PRODUCTS)
    reasons = {q["order_id"]: q["reason"] for q in result["quarantined"]}
    assert reasons == {
        "ORD-011": "INVALID_QTY",
        "ORD-013": "UNKNOWN_PRODUCT",
        "ORD-016": "UNKNOWN_CURRENCY",
        "ORD-018": "UNKNOWN_CUSTOMER",
    }


def test_quarantined_rows_keep_raw_payload():
    result = transform_orders(_rows(), CUSTOMERS, PRODUCTS)
    bad = [q for q in result["quarantined"] if q["order_id"] == "ORD-011"][0]
    assert bad["raw"]["quantity"] == -1


def test_missing_order_id_is_quarantined_not_dropped():
    rows = [{"customer_id": "CUST-001", "product_id": "PROD-101",
             "quantity": 1, "unit_price": 10.0, "currency": "USD",
             "ordered_at": "2026-11-01T00:00:00",
             "updated_at": "2026-11-01T00:00:00"}]
    result = transform_orders(rows, CUSTOMERS, PRODUCTS)
    assert len(result["clean"]) == 0
    assert result["quarantined"][0]["reason"] == "MISSING_ORDER_ID"
    assert result["quarantined"][0]["order_id"] is None


def test_total_revenue_is_deterministic():
    result = transform_orders(_rows(), CUSTOMERS, PRODUCTS)
    assert round(sum(r["total_usd"] for r in result["clean"]), 2) == 762.22
