"""Report: a Markdown summary of one pipeline run."""
from __future__ import annotations


def render_report(summary: dict) -> str:
    """Render the run summary as Markdown."""
    lines = [
        "# Orders ETL — Run Report",
        "",
        f"- Extracted: **{summary['extracted']}** rows"
        f" ({summary['retries']} retr{'y' if summary['retries'] == 1 else 'ies'}"
        " after a transient API failure)",
        f"- Clean rows loaded: **{summary['clean']}**",
        f"- Quarantined: **{summary['quarantined']}**",
        f"- Duplicates removed: **{summary['duplicates_removed']}**",
        f"- Total revenue: **${summary['total_usd']:.2f}**",
        "",
        "## Revenue by category",
        "",
        "| Category | Revenue (USD) |",
        "|---|---:|",
    ]
    for category, total in summary["revenue_by_category"].items():
        lines.append(f"| {category} | ${total:.2f} |")
    lines += [
        "",
        "## Revenue by country",
        "",
        "| Country | Revenue (USD) |",
        "|---|---:|",
    ]
    for country, total in summary["revenue_by_country"].items():
        lines.append(f"| {country} | ${total:.2f} |")
    lines += [
        "",
        "## Top customers",
        "",
        "| Customer | Orders | Revenue (USD) |",
        "|---|---:|---:|",
    ]
    for customer in summary["top_customers"]:
        lines.append(
            f"| {customer['customer_name']} ({customer['customer_id']})"
            f" | {customer['orders']} | ${customer['total_usd']:.2f} |"
        )
    lines += [
        "",
        "## Quarantined rows",
        "",
        "| Order | Reason |",
        "|---|---|",
    ]
    for row in summary["quarantined_rows"]:
        lines.append(f"| {row['order_id']} | {row['reason']} |")
    lines += [
        "",
        "## Alerts",
        "",
    ]
    if summary["alerts"]:
        for alert in summary["alerts"]:
            lines.append(
                f"- **[{alert['severity'].upper()}]** {alert['message']}"
            )
    else:
        lines.append("No alerts.")
    lines.append("")
    return "\n".join(lines)
