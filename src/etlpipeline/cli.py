"""Command-line interface: run the ETL, or export the n8n workflow."""
from __future__ import annotations

import argparse
import json
import sys

from .pipeline import run_pipeline
from .workflow import build_workflow, validate_workflow, workflow_json


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="etlpipeline",
        description="n8n ETL data pipeline: API orders to warehouse.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("run", help="Run the full ETL pipeline.")
    p.add_argument("--db", default=None, help="SQLite warehouse path.")
    p.add_argument("--out-dir", default=None,
                   help="Write summary.json, report.md and n8n-workflow.json here.")
    p.add_argument("--json", action="store_true",
                   help="Print the summary as JSON.")

    p = sub.add_parser("workflow", help="Print the n8n workflow JSON.")
    p.add_argument("--output", default=None, help="Write the JSON here.")
    p.add_argument("--validate", action="store_true",
                   help="Validate the workflow and report problems.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "run":
        summary = run_pipeline(db_path=args.db, out_dir=args.out_dir)
        if args.json:
            public = {k: v for k, v in summary.items() if k != "clean_rows"}
            print(json.dumps(public, indent=2))
        else:
            print(f"Extracted {summary['extracted']} rows"
                  f" ({summary['retries']} retr"
                  f"{'y' if summary['retries'] == 1 else 'ies'})")
            print(f"Loaded {summary['clean']} clean rows"
                  f" — ${summary['total_usd']:.2f} total revenue")
            print(f"Quarantined {summary['quarantined']} rows,"
                  f" removed {summary['duplicates_removed']} duplicate(s)")
            for alert in summary["alerts"]:
                print(f"[{alert['severity'].upper()}] {alert['message']}")
        return 0
    if args.command == "workflow":
        workflow = build_workflow()
        problems = validate_workflow(workflow)
        if args.validate:
            if problems:
                for problem in problems:
                    print(f"INVALID: {problem}", file=sys.stderr)
                return 1
            print("Workflow valid: "
                  f"{len(workflow['nodes'])} nodes, "
                  f"{len(workflow['connections'])} connections")
            return 0
        text = workflow_json(workflow)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(text)
            print(f"Wrote {args.output}")
        else:
            print(text)
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
