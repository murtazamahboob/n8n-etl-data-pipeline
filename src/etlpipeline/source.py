"""Extract: a deterministic mock paginated REST API for orders.

The "API" is bundled data, so the pipeline runs fully offline and every
run sees exactly the same rows. Page 2 fails once with a transient
error on the first attempt, so the pipeline's retry logic is exercised
on every run instead of only in a unit test.
"""
from __future__ import annotations

import copy

PAGE_SIZE = 7
TRANSIENT_PAGE = 1  # 0-based: the second page fails once, then recovers

CUSTOMERS = [
    {"customer_id": "CUST-001", "name": "Ayesha Khan", "email": "AYESHA@Example.com ", "country": "PK"},
    {"customer_id": "CUST-002", "name": "Bilal Ahmed", "email": "bilal@example.com", "country": "PK"},
    {"customer_id": "CUST-003", "name": "Sara Malik", "email": "sara@example.com", "country": "GB"},
    {"customer_id": "CUST-004", "name": "Omar Farooq", "email": "omar@example.com", "country": "DE"},
    {"customer_id": "CUST-005", "name": "Hina Raza", "email": "hina@example.com", "country": "US"},
    {"customer_id": "CUST-006", "name": "Usman Tariq", "email": "usman@example.com", "country": "AE"},
]

PRODUCTS = [
    {"product_id": "PROD-101", "name": "Wireless Mouse", "category": "Electronics"},
    {"product_id": "PROD-102", "name": "Mechanical Keyboard", "category": "Electronics"},
    {"product_id": "PROD-103", "name": "Notebook Pack", "category": "Stationery"},
    {"product_id": "PROD-104", "name": "Desk Lamp", "category": "Home"},
    {"product_id": "PROD-105", "name": "Water Bottle", "category": "Home"},
]

# 20 raw rows: 19 unique order ids (ORD-007 appears twice) and 4 rows
# that must be quarantined, not silently dropped (see transform.py).
RAW_ORDERS = [
    {"order_id": "ORD-001", "customer_id": "CUST-001", "product_id": "PROD-101", "quantity": 2, "unit_price": 25.00, "currency": "USD", "ordered_at": "2026-11-01T10:00:00", "updated_at": "2026-11-01T10:00:00"},
    {"order_id": "ORD-002", "customer_id": "CUST-002", "product_id": "PROD-102", "quantity": 1, "unit_price": 89.99, "currency": "EUR", "ordered_at": "2026-11-01T11:30:00", "updated_at": "2026-11-01T11:30:00"},
    {"order_id": "ORD-003", "customer_id": "CUST-003", "product_id": "PROD-103", "quantity": 5, "unit_price": 4.50, "currency": "GBP", "ordered_at": "2026-11-02T09:15:00", "updated_at": "2026-11-02T09:15:00"},
    {"order_id": "ORD-004", "customer_id": "CUST-004", "product_id": "PROD-104", "quantity": 1, "unit_price": 32.00, "currency": "EUR", "ordered_at": "2026-11-02T14:00:00", "updated_at": "2026-11-02T14:00:00"},
    {"order_id": "ORD-005", "customer_id": "CUST-005", "product_id": "PROD-105", "quantity": 3, "unit_price": 12.00, "currency": "USD", "ordered_at": "2026-11-03T08:45:00", "updated_at": "2026-11-03T08:45:00"},
    {"order_id": "ORD-006", "customer_id": "CUST-001", "product_id": "PROD-102", "quantity": 1, "unit_price": 89.99, "currency": "USD", "ordered_at": "2026-11-03T16:20:00", "updated_at": "2026-11-03T16:20:00"},
    {"order_id": "ORD-007", "customer_id": "CUST-002", "product_id": "PROD-101", "quantity": 2, "unit_price": 25.00, "currency": "USD", "ordered_at": "2026-11-04T10:00:00", "updated_at": "2026-11-04T10:00:00"},
    {"order_id": "ORD-008", "customer_id": "CUST-003", "product_id": "PROD-104", "quantity": 2, "unit_price": 32.00, "currency": "GBP", "ordered_at": "2026-11-04T12:10:00", "updated_at": "2026-11-04T12:10:00"},
    {"order_id": "ORD-009", "customer_id": "CUST-004", "product_id": "PROD-105", "quantity": 4, "unit_price": 12.00, "currency": "EUR", "ordered_at": "2026-11-05T09:00:00", "updated_at": "2026-11-05T09:00:00"},
    {"order_id": "ORD-010", "customer_id": "CUST-005", "product_id": "PROD-103", "quantity": 10, "unit_price": 4.50, "currency": "USD", "ordered_at": "2026-11-05T15:30:00", "updated_at": "2026-11-05T15:30:00"},
    {"order_id": "ORD-011", "customer_id": "CUST-001", "product_id": "PROD-101", "quantity": -1, "unit_price": 25.00, "currency": "USD", "ordered_at": "2026-11-06T10:00:00", "updated_at": "2026-11-06T10:00:00"},
    {"order_id": "ORD-012", "customer_id": "CUST-006", "product_id": "PROD-102", "quantity": 1, "unit_price": 89.99, "currency": "USD", "ordered_at": "2026-11-06T11:00:00", "updated_at": "2026-11-06T11:00:00"},
    {"order_id": "ORD-013", "customer_id": "CUST-002", "product_id": "PROD-999", "quantity": 1, "unit_price": 10.00, "currency": "USD", "ordered_at": "2026-11-06T12:00:00", "updated_at": "2026-11-06T12:00:00"},
    {"order_id": "ORD-014", "customer_id": "CUST-003", "product_id": "PROD-103", "quantity": 2, "unit_price": 4.50, "currency": "USD", "ordered_at": "2026-11-07T09:00:00", "updated_at": "2026-11-07T09:00:00"},
    {"order_id": "ORD-015", "customer_id": "CUST-004", "product_id": "PROD-105", "quantity": 1, "unit_price": 12.00, "currency": "GBP", "ordered_at": "2026-11-07T10:30:00", "updated_at": "2026-11-07T10:30:00"},
    {"order_id": "ORD-016", "customer_id": "CUST-005", "product_id": "PROD-101", "quantity": 1, "unit_price": 25.00, "currency": "JPY", "ordered_at": "2026-11-07T14:00:00", "updated_at": "2026-11-07T14:00:00"},
    {"order_id": "ORD-017", "customer_id": "CUST-006", "product_id": "PROD-104", "quantity": 1, "unit_price": 32.00, "currency": "EUR", "ordered_at": "2026-11-08T09:00:00", "updated_at": "2026-11-08T09:00:00"},
    {"order_id": "ORD-018", "customer_id": "CUST-999", "product_id": "PROD-101", "quantity": 1, "unit_price": 25.00, "currency": "USD", "ordered_at": "2026-11-08T10:00:00", "updated_at": "2026-11-08T10:00:00"},
    {"order_id": "ORD-019", "customer_id": "CUST-001", "product_id": "PROD-105", "quantity": 2, "unit_price": 12.00, "currency": "USD", "ordered_at": "2026-11-08T11:00:00", "updated_at": "2026-11-08T11:00:00"},
    # Duplicate of ORD-007 with a later updated_at and a corrected quantity.
    {"order_id": "ORD-007", "customer_id": "CUST-002", "product_id": "PROD-101", "quantity": 3, "unit_price": 25.00, "currency": "USD", "ordered_at": "2026-11-04T10:00:00", "updated_at": "2026-11-09T08:00:00"},
]


class TransientError(ConnectionError):
    """A recoverable failure fetching one page of the source API."""


class MockOrdersApi:
    """Paginated mock API over the bundled orders.

    ``fetch_page`` raises :class:`TransientError` the first time page
    index 1 is requested, then succeeds — modelling a flaky upstream.
    """

    def __init__(self, orders=None, page_size: int = PAGE_SIZE):
        self._orders = copy.deepcopy(orders if orders is not None else RAW_ORDERS)
        self.page_size = page_size
        self._failed_pages: set[int] = set()
        self.fetch_attempts = 0

    def page_count(self) -> int:
        return (len(self._orders) + self.page_size - 1) // self.page_size

    def fetch_page(self, page: int) -> list[dict]:
        self.fetch_attempts += 1
        if page == TRANSIENT_PAGE and page not in self._failed_pages:
            self._failed_pages.add(page)
            raise TransientError(f"mock API: page {page} timed out (transient)")
        start = page * self.page_size
        return copy.deepcopy(self._orders[start:start + self.page_size])

    def fetch_all(self, max_attempts: int = 3) -> tuple[list[dict], int]:
        """Fetch every page, retrying transient failures.

        Returns ``(rows, retries)`` where retries counts re-attempts.
        Raises TransientError if a page still fails after max_attempts.
        """
        rows: list[dict] = []
        retries = 0
        for page in range(self.page_count()):
            attempts = 0
            while True:
                attempts += 1
                try:
                    rows.extend(self.fetch_page(page))
                    break
                except TransientError:
                    if attempts >= max_attempts:
                        raise
                    retries += 1
        return rows, retries
