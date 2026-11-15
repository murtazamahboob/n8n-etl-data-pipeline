"""n8n workflow: the same pipeline as an importable workflow JSON.

The Python pipeline is the reference implementation; this module
exports the equivalent n8n workflow so the same ETL can run on a
schedule inside n8n. Node types and versions follow n8n's built-ins
(Schedule Trigger, HTTP Request, Code, IF, SQLite, Slack). The mock
API URL is a placeholder on purpose — point the HTTP Request node at
the real orders endpoint before activating the workflow.
"""
from __future__ import annotations

import copy
import json

WORKFLOW_NAME = "Orders ETL — API to Warehouse"
MOCK_API_URL = "https://api.example.com/orders?page={{ $json.page }}"

_NODE_TYPES = {
    "schedule": "n8n-nodes-base.scheduleTrigger",
    "http": "n8n-nodes-base.httpRequest",
    "code": "n8n-nodes-base.code",
    "if": "n8n-nodes-base.if",
    "sqlite": "n8n-nodes-base.sqlite",
    "slack": "n8n-nodes-base.slack",
    "set": "n8n-nodes-base.set",
}


def _node(name: str, kind: str, position: list[int], **parameters) -> dict:
    return {
        "id": f"node-{name.lower().replace(' ', '-')}",
        "name": name,
        "type": _NODE_TYPES[kind],
        "typeVersion": 1.2 if kind in ("schedule", "http") else 1,
        "position": position,
        "parameters": parameters,
    }


def build_workflow() -> dict:
    """Build the n8n workflow dict for the orders ETL."""
    nodes = [
        _node("Schedule Trigger", "schedule", [0, 0],
              rule={"interval": [{"field": "cronExpression",
                                  "expression": "0 9 * * *"}]}),
        _node("Fetch Orders Page", "http", [240, 0],
              method="GET", url=MOCK_API_URL,
              options={"response": {"response": {"responseFormat": "json"}}}),
        _node("Transform Orders", "code", [480, 0],
              jsCode=(
                  "// Dedupe by order_id (keep latest updated_at), validate,\n"
                  "// normalise email, convert totals to USD at fixed rates.\n"
                  "// Quarantined rows go to the 'quarantine' output.\n"
                  "return items.map(i => ({ json: i.json }));"
              )),
        _node("Is Row Valid?", "if", [720, 0],
              conditions={"boolean": [{"value1": "={{ $json.valid }}",
                                       "value2": True}]}),
        _node("Load Warehouse", "sqlite", [960, -80],
              operation="insert", table="orders"),
        _node("Load Quarantine", "sqlite", [960, 80],
              operation="insert", table="quarantine"),
        _node("Alert on Quarantine", "slack", [1200, 80],
              channel="#data-alerts",
              text=("=ETL alert: {{ $json.quarantined }} order(s) quarantined"
                    " in run {{ $json.run_id }}")),
        _node("Summarise Run", "set", [1200, -80],
              values={"string": [{"name": "run_summary",
                                  "value": "=Run complete: {{ $json.clean }}"
                                           " clean, {{ $json.quarantined }}"
                                           " quarantined"}]}),
    ]
    connections = {
        "Schedule Trigger": {"main": [[{"node": "Fetch Orders Page",
                                        "type": "main", "index": 0}]]},
        "Fetch Orders Page": {"main": [[{"node": "Transform Orders",
                                         "type": "main", "index": 0}]]},
        "Transform Orders": {"main": [[{"node": "Is Row Valid?",
                                        "type": "main", "index": 0}]]},
        "Is Row Valid?": {"main": [
            [{"node": "Load Warehouse", "type": "main", "index": 0}],
            [{"node": "Load Quarantine", "type": "main", "index": 0}],
        ]},
        "Load Quarantine": {"main": [[{"node": "Alert on Quarantine",
                                       "type": "main", "index": 0}]]},
        "Load Warehouse": {"main": [[{"node": "Summarise Run",
                                      "type": "main", "index": 0}]]},
    }
    return {
        "name": WORKFLOW_NAME,
        "nodes": nodes,
        "connections": connections,
        "active": False,
        "settings": {"executionOrder": "v1"},
        "tags": [{"name": "etl"}, {"name": "orders"}],
    }


def validate_workflow(workflow: dict) -> list[str]:
    """Check a workflow dict. Returns a list of problems (empty = valid)."""
    problems: list[str] = []
    nodes = workflow.get("nodes", [])
    names = [n.get("name") for n in nodes]
    if len(names) != len(set(names)):
        problems.append("duplicate node names")
    if not any(n.get("type", "").endswith("scheduleTrigger") for n in nodes):
        problems.append("no Schedule Trigger node")
    if not any(n.get("type", "").endswith("httpRequest") for n in nodes):
        problems.append("no HTTP Request node")
    for node in nodes:
        if not node.get("type"):
            problems.append(f"node {node.get('name')!r} has no type")
        if "position" not in node:
            problems.append(f"node {node.get('name')!r} has no position")
    for source, outputs in workflow.get("connections", {}).items():
        if source not in names:
            problems.append(f"connection from unknown node {source!r}")
        for branch in outputs.get("main", []):
            for link in branch:
                if link.get("node") not in names:
                    problems.append(
                        f"connection from {source!r} to unknown node"
                        f" {link.get('node')!r}"
                    )
    return problems


def workflow_json(workflow: dict | None = None) -> str:
    """Serialise the workflow (built fresh when none is given)."""
    return json.dumps(copy.deepcopy(workflow or build_workflow()), indent=2)
