"""Tests for the load stage: the SQLite warehouse."""
from __future__ import annotations

from etlpipeline.source import CUSTOMERS, PRODUCTS, MockOrdersApi
from etlpipeline.transform import transform_orders
from etlpipeline.warehouse import Warehouse


def _result():
    api = MockOrdersApi()
    rows, _ = api.fetch_all()
    return transform_orders(rows, CUSTOMERS, PRODUCTS)


def test_load_is_idempotent():
    result = _result()
    with Warehouse() as wh:
        wh.load_orders(result["clean"])
        wh.load_orders(result["clean"])  # second run over the same rows
        assert wh.order_count() == 15


def test_quarantine_load_and_counts():
    result = _result()
    with Warehouse() as wh:
        wh.load_orders(result["clean"])
        wh.load_quarantine(result["quarantined"])
        assert wh.quarantine_count() == 4
        assert wh.quarantine_by_reason() == {
            "INVALID_QTY": 1,
            "UNKNOWN_CURRENCY": 1,
            "UNKNOWN_CUSTOMER": 1,
            "UNKNOWN_PRODUCT": 1,
        }


def test_revenue_by_category():
    result = _result()
    with Warehouse() as wh:
        wh.load_orders(result["clean"])
        assert wh.revenue_by_category() == {
            "Electronics": 402.17,
            "Home": 277.48,
            "Stationery": 82.57,
        }


def test_revenue_by_country():
    result = _result()
    with Warehouse() as wh:
        wh.load_orders(result["clean"])
        assert wh.revenue_by_country() == {
            "AE": 124.55, "DE": 101.64, "GB": 118.85, "PK": 336.18, "US": 81.0,
        }


def test_daily_revenue_sums_to_total():
    result = _result()
    with Warehouse() as wh:
        wh.load_orders(result["clean"])
        daily = wh.daily_revenue()
        assert round(sum(daily.values()), 2) == 762.22
        assert daily["2026-11-01"] == 147.19  # 50.00 + 97.19


def test_top_customers():
    result = _result()
    with Warehouse() as wh:
        wh.load_orders(result["clean"])
        top = wh.top_customers()
        assert top[0]["customer_id"] == "CUST-002"
        assert top[0]["total_usd"] == 172.19
        assert top[0]["orders"] == 2


def test_run_log_records_each_run():
    result = _result()
    summary = {
        "extracted": 20, "clean": 15, "quarantined": 4,
        "duplicates_removed": 1, "retries": 1, "total_usd": 762.22,
    }
    with Warehouse() as wh:
        assert wh.log_run(summary) == 1
        assert wh.log_run(summary) == 2
        assert wh.run_count() == 2


def test_warehouse_persists_to_file(tmp_path):
    result = _result()
    db = tmp_path / "wh.db"
    with Warehouse(db) as wh:
        wh.load_orders(result["clean"])
    with Warehouse(db) as wh:
        assert wh.order_count() == 15
