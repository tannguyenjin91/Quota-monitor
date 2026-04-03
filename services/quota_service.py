from collections import Counter

import yaml

from models import QuotaSummaryRow
from services.quota_engine_service import build_banner_table, dataframe_from_cleaned_export, load_quota_engine_config


def load_mappings(path: str):
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def summarize_actuals(valid_df, mappings):
    actuals = {"gender": Counter(), "region": Counter(), "overall": Counter()}

    for gender in valid_df["gender_std"]:
        if gender:
            actuals["gender"][gender] += 1
    actuals["gender"]["Total"] = len(valid_df)

    for category in valid_df["region_quota_parent"]:
        if category:
            actuals["region"][category] += 1
    for category in valid_df["region_quota_detail"]:
        if category:
            actuals["region"][category] += 1
    actuals["region"]["Total"] = len(valid_df)
    actuals["overall"]["Total"] = len(valid_df)

    return actuals


def calculate_status(target_n: int, actual_n: int):
    remaining_n = max(target_n - actual_n, 0)
    overfill_n = max(actual_n - target_n, 0)
    completion_pct = (actual_n / target_n * 100) if target_n > 0 else 0

    if actual_n == 0:
        status = "Not started"
    elif actual_n < target_n:
        status = "Under quota"
    elif actual_n == target_n:
        status = "On target"
    else:
        status = "Over quota"

    return {
        "remaining_n": remaining_n,
        "overfill_n": overfill_n,
        "completion_pct": completion_pct,
        "status": status,
    }


def build_summary_rows(actuals, mappings):
    rows = []

    for section in ("gender", "region"):
        for display_order, category in enumerate(mappings["display_order"][section], start=1):
            target_n = mappings["targets"][section][category]
            actual_n = actuals[section].get(category, 0)
            status_payload = calculate_status(target_n, actual_n)
            rows.append(
                {
                    "section": section,
                    "category": category,
                    "is_parent": category in mappings.get("parent_categories", {}).get(section, []),
                    "display_order": display_order,
                    "target_n": target_n,
                    "actual_n": actual_n,
                    "remaining_n": status_payload["remaining_n"],
                    "overfill_n": status_payload["overfill_n"],
                    "completion_pct": status_payload["completion_pct"],
                    "status": status_payload["status"],
                }
            )

    overall_target = mappings["targets"]["overall"]
    overall_actual = actuals["overall"].get("Total", 0)
    overall_payload = calculate_status(overall_target, overall_actual)
    rows.append(
        {
            "section": "overall",
            "category": "Total",
            "is_parent": True,
            "display_order": 1,
            "target_n": overall_target,
            "actual_n": overall_actual,
            "remaining_n": overall_payload["remaining_n"],
            "overfill_n": overall_payload["overfill_n"],
            "completion_pct": overall_payload["completion_pct"],
            "status": overall_payload["status"],
        }
    )

    return rows


def compute_run_metrics(working_df, rejected_df, invalid_df, valid_df, mappings):
    overall_target = mappings["targets"]["overall"]
    return {
        "raw_row_count": len(working_df),
        "rejected_row_count": len(rejected_df),
        "accepted_row_count": int(working_df["is_accepted_for_quota"].sum()),
        "invalid_accepted_row_count": len(invalid_df),
        "valid_quota_count": len(valid_df),
        "overall_completion_pct": (len(valid_df) / overall_target * 100) if overall_target else 0,
    }


def build_dashboard_context(upload, app_config=None, metric_view=None):
    quota_rows = (
        QuotaSummaryRow.query.filter_by(upload_run_id=upload.id)
        .order_by(QuotaSummaryRow.section.asc(), QuotaSummaryRow.display_order.asc())
        .all()
    )

    gender_rows = [row for row in quota_rows if row.section == "gender"]
    region_rows = [row for row in quota_rows if row.section == "region"]
    overall_row = next((row for row in quota_rows if row.section == "overall"), None)

    banner_table = None
    banner_error = None
    if app_config and upload.cleaned_data_path:
        try:
            banner_config = load_quota_engine_config(app_config["QUOTA_ENGINE_CONFIG_PATH"])
            metric_modes = resolve_metric_modes(metric_view, banner_config["output_table"].get("metric_modes", ["count"]))
            cleaned_df = dataframe_from_cleaned_export(upload.cleaned_data_path)
            banner_table = build_banner_table(cleaned_df, banner_config, metric_modes=metric_modes)
        except Exception as exc:  # pragma: no cover - defensive UI fallback
            banner_error = str(exc)

    return {
        "upload": upload,
        "metrics": {
            "Raw Rows": upload.raw_row_count,
            "Rejected Rows": upload.rejected_row_count,
            "Accepted Rows": upload.accepted_row_count,
            "Invalid Accepted Rows": upload.invalid_accepted_row_count,
            "Valid Quota Completes": upload.valid_quota_count,
            "Overall Target": overall_row.target_n if overall_row else 0,
            "Overall Completion %": round(upload.overall_completion_pct, 1),
            "Overall Remaining": overall_row.remaining_n if overall_row else 0,
        },
        "gender_rows": gender_rows,
        "region_rows": region_rows,
        "overall_row": overall_row,
        "banner_table": banner_table,
        "banner_error": banner_error,
        "selected_metric_view": metric_view or "default",
    }


def resolve_metric_modes(metric_view, default_metric_modes):
    if metric_view == "count":
        return ["count"]
    if metric_view == "percent":
        return ["percent"]
    if metric_view == "both":
        return ["count", "percent"]
    return default_metric_modes
