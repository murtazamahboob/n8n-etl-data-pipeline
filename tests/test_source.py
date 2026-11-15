"""Tests for the extract stage: the mock paginated orders API."""
from __future__ import annotations

import pytest

from etlpipeline.source import (
    CUSTOMERS,
    PRODUCTS,
    RAW_ORDERS,
    MockOrdersApi,
    TransientError,
)


def test_page_count():
    assert MockOrdersApi().page_count() == 3  # 20 rows, 7 per page


def test_fetch_page_returns_copies():
    api = MockOrdersApi()
    page = api.fetch_page(0)
    assert len(page) == 7
    page[0]["quantity"] = 999
    assert api.fetch_page(0)[0]["quantity"] == 2


def test_transient_failure_on_page_1_then_recovers():
    api = MockOrdersApi()
    api.fetch_page(0)
    with pytest.raises(TransientError):
        api.fetch_page(1)
    assert len(api.fetch_page(1)) == 7  # second attempt succeeds


def test_fetch_all_retries_once_and_returns_all_rows():
    api = MockOrdersApi()
    rows, retries = api.fetch_all()
    assert len(rows) == 20
    assert retries == 1
    assert api.fetch_attempts == 4  # 3 pages + 1 retry


def test_fetch_all_raises_when_retries_exhausted():
    api = MockOrdersApi()
    with pytest.raises(TransientError):
        api.fetch_all(max_attempts=1)


def test_bundled_data_shape():
    assert len(CUSTOMERS) == 6
    assert len(PRODUCTS) == 5
    assert len(RAW_ORDERS) == 20
    assert len({r["order_id"] for r in RAW_ORDERS}) == 19  # ORD-007 twice
