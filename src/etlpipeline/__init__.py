"""n8n ETL Data Pipeline — API orders into a warehouse, with alerts.

Extracts orders from a paginated source API (bundled mock, so the
pipeline runs fully offline), cleans and validates them, loads the
clean rows into a SQLite warehouse, quarantines bad rows instead of
dropping them, raises alerts on data-quality problems, and exports
the same pipeline as an importable n8n workflow JSON.
"""

from .alerts import evaluate_alerts
from .pipeline import run_pipeline
from .source import MockOrdersApi, TransientError
from .transform import transform_orders
from .warehouse import Warehouse
from .workflow import build_workflow, validate_workflow

__version__ = "1.0.0"
__all__ = [
    "MockOrdersApi",
    "TransientError",
    "Warehouse",
    "build_workflow",
    "evaluate_alerts",
    "run_pipeline",
    "transform_orders",
    "validate_workflow",
]
