"""Alerts: data-quality rules evaluated on every pipeline run.

Every alert is a plain dict — ``severity``, ``rule``, ``message`` —
so the same list can be printed by the CLI, written into the report,
or posted to Slack by the n8n workflow. Rules fire on the run's own
numbers, never on a guess: a quarantine rate above the threshold, a
bad row of each kind, a duplicate, a flaky upstream, a high-value
order worth a human look.
"""
from __future__ import annotations

DEFAULT_QUARANTINE_THRESHOLD = 0.15
DEFAULT_HIGH_VALUE_USD = 90.0


def evaluate_alerts(summary: dict, *,
                    quarantine_threshold: float = DEFAULT_QUARANTINE_THRESHOLD,
                    high_value_usd: float = DEFAULT_HIGH_VALUE_USD) -> list[dict]:
    """Evaluate every alert rule against one pipeline run's summary."""
    alerts: list[dict] = []
    extracted = summary.get("extracted", 0)
    quarantined = summary.get("quarantined", 0)

    if extracted:
        rate = quarantined / extracted
        if rate > quarantine_threshold:
            alerts.append({
                "severity": "high",
                "rule": "QUARANTINE_RATE",
                "message": (
                    f"Quarantine rate {rate:.0%} ({quarantined}/{extracted})"
                    f" exceeds {quarantine_threshold:.0%} threshold"
                ),
            })

    for reason, count in sorted(summary.get("quarantine_by_reason", {}).items()):
        alerts.append({
            "severity": "medium",
            "rule": f"QUARANTINED_{reason}",
            "message": f"{count} order(s) quarantined: {reason}",
        })

    if summary.get("duplicates_removed"):
        alerts.append({
            "severity": "low",
            "rule": "DUPLICATES_REMOVED",
            "message": (
                f"{summary['duplicates_removed']} duplicate order(s)"
                " removed (kept latest updated_at)"
            ),
        })

    if summary.get("retries"):
        alerts.append({
            "severity": "info",
            "rule": "UPSTREAM_RETRY",
            "message": (
                f"Source API needed {summary['retries']} retry(ies) —"
                " upstream is flaky, pipeline recovered"
            ),
        })

    for row in summary.get("clean_rows", []):
        if row["total_usd"] >= high_value_usd:
            alerts.append({
                "severity": "low",
                "rule": "HIGH_VALUE_ORDER",
                "message": (
                    f"High-value order {row['order_id']}:"
                    f" ${row['total_usd']:.2f} ({row['customer_name']},"
                    f" {row['product_name']})"
                ),
            })

    return alerts
