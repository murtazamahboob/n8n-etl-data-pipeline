# n8n ETL Data Pipeline — API Orders to Warehouse, with Alerts

**Extract orders from a paginated API, clean and validate them, load the clean rows into a SQLite warehouse, quarantine the bad rows instead of dropping them, raise data-quality alerts — and export the same pipeline as an importable n8n workflow. Fully offline, stdlib-only, deterministic.**

Most ETL demos either drop bad rows silently or crash on the first flaky API page. This one does neither: a page that fails once is retried, a duplicate order is resolved by keeping the latest update, and every invalid row lands in a quarantine table with its reason and raw payload — so nothing is ever lost, and the run report tells you exactly what happened.

## Results (measured, not claimed)

One run over the bundled 20 orders (19 unique — `ORD-007` appears twice):

| Stage | Result |
|---|---:|
| Extracted | 20 rows, 3 pages, **1 retry** (page 2 fails once, then recovers) |
| Clean rows loaded | **15** |
| Quarantined | **4** (1 invalid quantity, 1 unknown product, 1 unknown currency, 1 unknown customer) |
| Duplicates removed | **1** (`ORD-007`, kept the later update: quantity 3) |
| Total revenue | **$762.22** |

Revenue by category: Electronics **$402.17**, Home **$277.48**, Stationery **$82.57**. Top customer: Bilal Ahmed (`CUST-002`) — **$172.19** across 2 orders. Loading the same run twice leaves exactly 15 rows — the load is idempotent.

Alerts raised on the demo run: quarantine rate **20% exceeds the 15% threshold** (high), one alert per quarantine reason, 1 duplicate removed, 1 upstream retry, and 1 high-value order (`ORD-002`, $97.19).

## Features

- **Extract with retries** — a paginated mock orders API; page 2 fails once with a transient error on every run, so retry logic is exercised, not just unit-tested.
- **Transform: dedupe, validate, convert** — dedupe by `order_id` keeping the latest `updated_at`; validation for unknown customer / product / currency, non-positive quantity or price, missing order id; email lower-cased and stripped; totals converted to USD at pinned FX rates (EUR 1.08, GBP 1.27).
- **Load: SQLite warehouse** — `orders` fact table, `quarantine` table, `runs` log; `INSERT OR REPLACE` makes re-runs idempotent; revenue by category, country and day, plus top customers, straight from SQL.
- **Alerts** — quarantine-rate threshold, per-reason quarantine alerts, duplicate, upstream-retry and high-value-order rules; plain dicts, so the CLI, the report and the n8n Slack node all use the same list.
- **n8n workflow export** — `etlpipeline workflow` prints (or writes) an importable n8n workflow JSON: Schedule Trigger → HTTP Request → Code (transform) → IF (valid?) → SQLite load → Slack alert, with a built-in validator that checks node names, types and connections.
- **CLI** — `run` and `workflow` subcommands, `--json` output for scripting.

## Quickstart

```bash
pip install -r requirements.txt
pip install -e .

# Run the full pipeline (prints the summary)
etlpipeline run

# Same run as JSON, writing report + warehouse + workflow to outputs/
etlpipeline run --json --out-dir outputs

# Export the n8n workflow (import it in n8n, point the HTTP node at your real API)
etlpipeline workflow --output n8n-workflow.json
etlpipeline workflow --validate

# Run the full demo
python examples/demo.py
```

## Python API

```python
from etlpipeline import run_pipeline, build_workflow, validate_workflow

summary = run_pipeline(out_dir="outputs")
summary["clean"]        # 15
summary["total_usd"]    # 762.22
summary["alerts"]       # quarantine-rate, per-reason, duplicate, retry, high-value
summary["revenue_by_category"]  # {'Electronics': 402.17, 'Home': 277.48, ...}

workflow = build_workflow()
validate_workflow(workflow)  # [] — no problems
```

## How it works

1. **Extract** — `MockOrdersApi.fetch_all()` pages through the bundled orders (7 per page), retrying a transient failure up to 3 times per page.
2. **Transform** — `transform_orders()` dedupes, validates and converts each row; invalid rows are quarantined with a reason and their raw payload, never dropped.
3. **Load** — `Warehouse` upserts clean rows into SQLite, stores quarantined rows, and logs the run; aggregates (revenue by category/country/day, top customers) are SQL queries.
4. **Alert** — `evaluate_alerts()` runs the data-quality rules over the run summary.
5. **Export** — `build_workflow()` renders the same five stages as n8n nodes and connections, ready to import.

## Why this is different from the n8n repos already on the profile

The profile already has an n8n automation hub and a self-healing workflow watchdog. This is neither: it is a **data pipeline** — API → transform → warehouse — with data-quality quarantine and alerts, plus a workflow *exporter* that generates the n8n JSON from code. It complements the hub and watchdog instead of repeating them.

## Project structure

```
src/etlpipeline/
  source.py      # paginated mock orders API with a transient failure
  transform.py   # dedupe, validate, convert to USD
  warehouse.py   # SQLite warehouse: orders, quarantine, runs
  alerts.py      # data-quality alert rules
  workflow.py    # n8n workflow builder + validator
  pipeline.py    # run_pipeline(): extract → transform → load → alerts
  report.py      # Markdown run report
  cli.py         # etlpipeline run / workflow
examples/demo.py # full demo run, writes outputs/
tests/           # 41 tests: source, transform, warehouse, alerts, workflow, CLI
```

## License

MIT — see [LICENSE](LICENSE).
