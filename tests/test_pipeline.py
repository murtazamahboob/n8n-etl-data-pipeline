"""Tests for alerts, the n8n workflow export, the report and the CLI."""
from __future__ import annotations

import json

from etlpipeline.alerts import evaluate_alerts
from etlpipeline.cli import main
from etlpipeline.pipeline import run_pipeline
from etlpipeline.report import render_report
from etlpipeline.workflow import build_workflow, validate_workflow


def _summary():
    return run_pipeline()


def test_quarantine_rate_alert_fires_at_20_percent():
    alerts = evaluate_alerts({
        "extracted": 20, "quarantined": 4, "duplicates_removed": 0,
        "retries": 0, "quarantine_by_reason": {}, "clean_rows": [],
    })
    assert any(a["rule"] == "QUARANTINE_RATE" and a["severity"] == "high"
               for a in alerts)


def test_quarantine_rate_alert_silent_below_threshold():
    alerts = evaluate_alerts({
        "extracted": 100, "quarantined": 5, "duplicates_removed": 0,
        "retries": 0, "quarantine_by_reason": {}, "clean_rows": [],
    })
    assert not any(a["rule"] == "QUARANTINE_RATE" for a in alerts)


def test_each_quarantine_reason_gets_an_alert():
    alerts = evaluate_alerts({
        "extracted": 20, "quarantined": 4, "duplicates_removed": 0,
        "retries": 0,
        "quarantine_by_reason": {"UNKNOWN_CUSTOMER": 1, "INVALID_QTY": 1},
        "clean_rows": [],
    })
    rules = {a["rule"] for a in alerts}
    assert "QUARANTINED_UNKNOWN_CUSTOMER" in rules
    assert "QUARANTINED_INVALID_QTY" in rules


def test_duplicate_and_retry_alerts():
    alerts = evaluate_alerts({
        "extracted": 20, "quarantined": 0, "duplicates_removed": 1,
        "retries": 1, "quarantine_by_reason": {}, "clean_rows": [],
    })
    rules = {a["rule"] for a in alerts}
    assert rules == {"DUPLICATES_REMOVED", "UPSTREAM_RETRY"}


def test_high_value_order_alert():
    alerts = evaluate_alerts({
        "extracted": 1, "quarantined": 0, "duplicates_removed": 0,
        "retries": 0, "quarantine_by_reason": {},
        "clean_rows": [{"order_id": "ORD-002", "total_usd": 97.19,
                        "customer_name": "Bilal Ahmed",
                        "product_name": "Mechanical Keyboard"}],
    })
    assert any(a["rule"] == "HIGH_VALUE_ORDER" and "ORD-002" in a["message"]
               for a in alerts)


def test_full_run_summary_numbers():
    summary = _summary()
    assert summary["extracted"] == 20
    assert summary["retries"] == 1
    assert summary["clean"] == 15
    assert summary["quarantined"] == 4
    assert summary["duplicates_removed"] == 1
    assert summary["total_usd"] == 762.22
    assert summary["warehouse_rows"] == 15
    assert summary["quarantine_rows"] == 4


def test_full_run_alerts():
    summary = _summary()
    rules = {a["rule"] for a in summary["alerts"]}
    assert "QUARANTINE_RATE" in rules
    assert "QUARANTINED_UNKNOWN_CUSTOMER" in rules
    assert "DUPLICATES_REMOVED" in rules
    assert "UPSTREAM_RETRY" in rules
    assert "HIGH_VALUE_ORDER" in rules


def test_run_is_deterministic():
    first = _summary()
    second = _summary()
    for key in ("extracted", "clean", "quarantined", "total_usd",
                "revenue_by_category", "alerts"):
        assert first[key] == second[key]


def test_run_writes_outputs(tmp_path):
    summary = run_pipeline(out_dir=tmp_path)
    assert (tmp_path / "summary.json").exists()
    assert (tmp_path / "report.md").exists()
    assert (tmp_path / "n8n-workflow.json").exists()
    assert (tmp_path / "warehouse.db").exists()
    saved = json.loads((tmp_path / "summary.json").read_text())
    assert saved["total_usd"] == 762.22
    assert saved["clean"] == 15
    workflow = json.loads((tmp_path / "n8n-workflow.json").read_text())
    assert validate_workflow(workflow) == []


def test_workflow_structure_and_validation():
    workflow = build_workflow()
    assert workflow["name"] == "Orders ETL — API to Warehouse"
    assert len(workflow["nodes"]) == 8
    types = [n["type"] for n in workflow["nodes"]]
    assert "n8n-nodes-base.scheduleTrigger" in types
    assert "n8n-nodes-base.httpRequest" in types
    assert "n8n-nodes-base.sqlite" in types
    assert "n8n-nodes-base.slack" in types
    assert validate_workflow(workflow) == []


def test_workflow_validation_catches_broken_connections():
    workflow = build_workflow()
    workflow["connections"]["Schedule Trigger"]["main"][0][0]["node"] = "Nope"
    problems = validate_workflow(workflow)
    assert any("Nope" in p for p in problems)


def test_workflow_validation_catches_missing_trigger():
    workflow = build_workflow()
    workflow["nodes"] = [n for n in workflow["nodes"]
                         if "scheduleTrigger" not in n["type"]]
    assert any("Schedule Trigger" in p for p in validate_workflow(workflow))


def test_report_contains_key_numbers():
    report = render_report(_summary())
    assert "762.22" in report
    assert "Electronics" in report and "$402.17" in report
    assert "ORD-011" in report and "INVALID_QTY" in report
    assert "Bilal Ahmed" in report
    assert "QUARANTINE" in report.upper()


def test_cli_run_prints_summary(capsys):
    assert main(["run"]) == 0
    out = capsys.readouterr().out
    assert "Extracted 20 rows" in out
    assert "$762.22" in out
    assert "Quarantined 4 rows" in out


def test_cli_run_json(capsys):
    assert main(["run", "--json"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert data["clean"] == 15 and data["total_usd"] == 762.22


def test_cli_workflow_validate(capsys):
    assert main(["workflow", "--validate"]) == 0
    assert "Workflow valid: 8 nodes" in capsys.readouterr().out


def test_cli_workflow_writes_file(tmp_path):
    target = tmp_path / "wf.json"
    assert main(["workflow", "--output", str(target)]) == 0
    workflow = json.loads(target.read_text())
    assert validate_workflow(workflow) == []
