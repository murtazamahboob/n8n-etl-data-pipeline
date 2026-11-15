"""Demo: run the full ETL pipeline and print the run report.

Writes outputs/report.md, outputs/summary.json,
outputs/n8n-workflow.json and outputs/warehouse.db.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from etlpipeline import run_pipeline  # noqa: E402
from etlpipeline.report import render_report  # noqa: E402


def main() -> None:
    out_dir = Path(__file__).resolve().parents[1] / "outputs"
    summary = run_pipeline(out_dir=out_dir)
    print(render_report(summary))
    print(f"Wrote {out_dir / 'report.md'}")
    print(f"Wrote {out_dir / 'summary.json'}")
    print(f"Wrote {out_dir / 'n8n-workflow.json'}")
    print(f"Warehouse: {out_dir / 'warehouse.db'}")


if __name__ == "__main__":
    main()
