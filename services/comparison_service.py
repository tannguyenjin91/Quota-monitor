from pathlib import Path

import pandas as pd

from models import QuotaSummaryRow
from services.file_service import build_output_paths


def build_comparison_table(base_upload, current_upload, section):
    previous_rows = _summary_lookup(base_upload.id, section)
    current_rows = _summary_lookup(current_upload.id, section)
    categories = list(dict.fromkeys(list(previous_rows.keys()) + list(current_rows.keys())))

    comparison_rows = []
    for category in categories:
        previous = previous_rows.get(category)
        current = current_rows.get(category)
        if not previous and not current:
            continue
        current_actual = current.actual_n if current else 0
        previous_actual = previous.actual_n if previous else 0
        delta_actual = current_actual - previous_actual
        status = current.status if current else "Not started"
        completion_pct = current.completion_pct if current else 0
        remaining_n = current.remaining_n if current else (previous.remaining_n if previous else 0)
        comparison_rows.append(
            {
                "category": category,
                "target": current.target_n if current else previous.target_n,
                "previous_actual": previous_actual,
                "current_actual": current_actual,
                "delta_actual": delta_actual,
                "delta_display": format_delta(delta_actual),
                "current_remaining": remaining_n,
                "current_completion_pct": completion_pct,
                "status": status,
                "is_parent": current.is_parent if current else previous.is_parent,
                "display_order": current.display_order if current else previous.display_order,
            }
        )
    return sorted(comparison_rows, key=lambda row: row["display_order"])


def format_delta(value: int) -> str:
    if value > 0:
        return f"+{value}"
    if value == 0:
        return "0"
    return str(value)


def build_comparison_workbook(base_upload, current_upload, app_config):
    output_paths = build_output_paths(current_upload.id, app_config["OUTPUT_FOLDER"])
    file_path = Path(output_paths["comparison_summary_path"])

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        for section, sheet_name in (("gender", "Gender Comparison"), ("region", "Region Comparison"), ("overall", "Overall")):
            pd.DataFrame(build_comparison_table(base_upload, current_upload, section)).to_excel(
                writer, sheet_name=sheet_name, index=False
            )
    return str(file_path)


def _summary_lookup(upload_id: int, section: str):
    rows = QuotaSummaryRow.query.filter_by(upload_run_id=upload_id, section=section).all()
    return {row.category: row for row in rows}
