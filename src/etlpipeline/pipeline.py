"""Pipeline: extract → transform → load → alerts, end to end.

One function, ``run_pipeline``, does the whole run and returns a
plain-dict summary — the CLI prints it, the demo prints it, and the
tests assert on it. The summary is deterministic: same bundled data,
same numbers, every time.
"""
from __future__ import annotations

import json
from pathlib import Path

from .alerts import evaluate_alerts
from .report import render_report
from .source import CUSTOMERS, PRODUCTS, MockOrdersApi
from .transform import transform_orders
from .warehouse import Warehouse
from .workflow import build_workflow, workflow_json


def run_pipeline(db_path: str | Path | None = None,
                 out_dir: str | Path | None = None) -> dict:
    """Run extract → transform → load → alerts. Returns the summary."""
    api = MockOrdersApi()
    rows, retries = api.fetch_all()
    result = transform_orders(rows, CUSTOMERS, PRODUCTS)
    clean = result["clean"]
    quarantined = result["quarantined"]
    total_usd = round(sum(r["total_usd"] for r in clean), 2)

    out = Path(out_dir) if out_dir is not None else None
    if out is not None:
        # Create the output dir before opening the warehouse DB inside it,
        # so a fresh checkout (no outputs/ yet) does not fail to open it.
        out.mkdir(parents=True, exist_ok=True)
    db = str(db_path) if db_path is not None else (
        str(out / "warehouse.db") if out is not None else ":memory:"
    )
    with Warehouse(db) as warehouse:
        warehouse.load_orders(clean)
        warehouse.load_quarantine(quarantined)
        summary = {
            "extracted": result["extracted"],
            "retries": retries,
            "clean": len(clean),
            "quarantined": len(quarantined),
            "duplicates_removed": result["duplicates_removed"],
            "total_usd": total_usd,
            "warehouse_rows": warehouse.order_count(),
            "quarantine_rows": warehouse.quarantine_count(),
            "revenue_by_category": warehouse.revenue_by_category(),
            "revenue_by_country": warehouse.revenue_by_country(),
            "daily_revenue": warehouse.daily_revenue(),
            "top_customers": warehouse.top_customers(),
            "quarantine_by_reason": warehouse.quarantine_by_reason(),
            "clean_rows": clean,
            "quarantined_rows": [
                {"order_id": q.get("order_id"), "reason": q["reason"]}
                for q in quarantined
            ],
        }
        summary["alerts"] = evaluate_alerts(summary)
        warehouse.log_run(summary)

    if out is not None:
        public = {k: v for k, v in summary.items() if k != "clean_rows"}
        (out / "summary.json").write_text(
            json.dumps(public, indent=2), encoding="utf-8"
        )
        (out / "report.md").write_text(render_report(summary), encoding="utf-8")
        (out / "n8n-workflow.json").write_text(
            workflow_json(build_workflow()), encoding="utf-8"
        )
    return summary
