from pathlib import Path

import pandas as pd

from services.config_service import load_yaml_config, save_yaml_config
from services.quota_engine_service import build_banner_table, dataframe_from_cleaned_export
from utils.text_utils import normalize_text


INTERNAL_REPORT_FIELDS = {
    "respondent_id",
    "tu_choi_raw",
    "tu_choi_normalized",
    "is_rejected",
    "is_accepted_for_quota",
    "validation_status",
    "validation_note",
}


def get_report_config_path(upload_id: int, output_folder: str) -> Path:
    run_dir = Path(output_folder) / f"upload_{upload_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir / "report_builder.yaml"


def detect_report_fields(cleaned_path: str):
    df = dataframe_from_cleaned_export(cleaned_path)
    fields = []

    for column in df.columns:
        if column in INTERNAL_REPORT_FIELDS:
            continue
        series = df[column].fillna("").map(normalize_text)
        non_empty = series[series != ""]
        unique_values = list(dict.fromkeys(non_empty.tolist()))
        fields.append(
            {
                "key": column,
                "label": prettify_field_label(column),
                "sample_values": unique_values[:8],
                "unique_count": len(unique_values),
            }
        )
    return fields


def prettify_field_label(field_key: str):
    label = field_key.replace("raw__", "").replace("_std", "").replace("_", " ")
    return " ".join(part.capitalize() for part in label.split())


def load_or_create_report_config(upload, app_config):
    config_path = get_report_config_path(upload.id, app_config["OUTPUT_FOLDER"])
    if config_path.exists():
        return load_yaml_config(str(config_path))

    fields = detect_report_fields(upload.cleaned_data_path)
    config = {
        "upload_id": upload.id,
        "orientation": "horizontal",
        "metric_modes": ["count"],
        "show_question_text": True,
        "show_row_labels": True,
        "fields": [],
    }
    default_roles = {
        "gender_std": "quota",
        "distance_std": "column",
        "direction_std": "column",
        "region_quota_detail": "column",
        "validation_status": "ignore",
    }
    for index, field in enumerate(fields, start=1):
        config["fields"].append(
            {
                "key": field["key"],
                "label": field["label"],
                "role": default_roles.get(field["key"], "ignore"),
                "visible": field["key"] in {"gender_std", "distance_std", "direction_std", "region_quota_detail"},
                "order": index,
            }
        )
    save_yaml_config(str(config_path), config)
    return config


def save_report_config(upload, app_config, form_data):
    config = load_or_create_report_config(upload, app_config)
    config["orientation"] = form_data.get("orientation", "horizontal")
    metric_mode = form_data.get("metric_mode", "count")
    config["metric_modes"] = ["count", "percent"] if metric_mode == "both" else [metric_mode]
    config["show_question_text"] = bool(form_data.get("show_question_text"))
    config["show_row_labels"] = bool(form_data.get("show_row_labels"))

    updated_fields = []
    for field in config["fields"]:
        key = field["key"]
        updated_fields.append(
            {
                "key": key,
                "label": form_data.get(f"label__{key}", field["label"]).strip() or field["label"],
                "role": form_data.get(f"role__{key}", field["role"]),
                "visible": bool(form_data.get(f"visible__{key}")),
                "order": int(form_data.get(f"order__{key}", field["order"]) or field["order"]),
            }
        )
    config["fields"] = sorted(updated_fields, key=lambda item: item["order"])
    save_yaml_config(str(get_report_config_path(upload.id, app_config["OUTPUT_FOLDER"])), config)
    return config


def build_runtime_config_from_report(cleaned_path: str, report_config: dict):
    df = dataframe_from_cleaned_export(cleaned_path)
    selected_fields = [field for field in report_config.get("fields", []) if field.get("visible") and field.get("role") != "ignore"]

    quota_groups = []
    row_definitions = []
    group_order = []

    for field in sorted(selected_fields, key=lambda item: item["order"]):
        values = distinct_values(df, field["key"])
        if field["role"] in {"quota", "column"}:
            group_id = field["key"]
            quota_groups.append(
                {
                    "id": group_id,
                    "key": field["label"],
                    "label": field["label"],
                    "level": 2,
                    "visible": True,
                    "denominator_key": "total_accepted",
                    "buckets": build_buckets(field["key"], values),
                }
            )
            group_order.append(field["label"])
        elif field["role"] == "row":
            for value in values:
                row_definitions.append(
                    {
                        "id": f"{field['key']}__{sanitize_value(value)}",
                        "question_key": field["key"],
                        "question_label": field["label"],
                        "category_key": sanitize_value(value),
                        "category_label": value,
                        "visible": True,
                        "filter": {"operator": "equals", "field": field["key"], "value": value},
                        "percent_denominator": {"mode": "overall_total"},
                    }
                )

    if not row_definitions:
        row_definitions = [
            {
                "id": "valid_quota",
                "question_key": "quota_progress",
                "question_label": "Quota Progress",
                "category_key": "valid_quota",
                "category_label": "Valid Quota Completes",
                "visible": True,
                "filter": {"operator": "equals", "field": "validation_status", "value": "valid"},
                "percent_denominator": {"mode": "overall_total"},
            }
        ]

    runtime_config = {
        "quota_groups": quota_groups,
        "row_definitions": row_definitions,
        "denominators": {
            "total_accepted": {
                "label": "Total accepted base",
                "filter": {"operator": "equals", "field": "is_accepted_for_quota", "value": True},
            }
        },
        "output_table": {
            "show_question_text": report_config.get("show_question_text", True),
            "show_row_labels": report_config.get("show_row_labels", True),
            "group_order": group_order,
            "hidden_group_keys": [],
            "hidden_bucket_keys": [],
            "hidden_question_keys": [],
            "metric_modes": report_config.get("metric_modes", ["count"]),
        },
    }
    return runtime_config, df


def build_buckets(field_key: str, values):
    buckets = [
        {
            "key": "total",
            "label": "Total",
            "visible": True,
            "rule": {"operator": "not_empty", "field": field_key},
        }
    ]
    for value in values:
        buckets.append(
            {
                "key": sanitize_value(value),
                "label": value,
                "visible": True,
                "rule": {"operator": "equals", "field": field_key, "value": value},
            }
        )
    return buckets


def distinct_values(df: pd.DataFrame, field_key: str):
    if field_key not in df.columns:
        return []
    values = df[field_key].fillna("").map(normalize_text)
    return [value for value in dict.fromkeys(values.tolist()) if value]


def sanitize_value(value: str):
    return normalize_text(value).lower().replace("%", "pct").replace(" ", "_").replace("-", "_")


def build_report_output(upload, app_config):
    report_config = load_or_create_report_config(upload, app_config)
    runtime_config, df = build_runtime_config_from_report(upload.cleaned_data_path, report_config)
    table = build_banner_table(df, runtime_config, metric_modes=report_config.get("metric_modes", ["count"]))
    if report_config.get("orientation") == "vertical":
        return transpose_banner_table(table)
    return {"mode": "horizontal", "table": table, "config": report_config}


def transpose_banner_table(table_payload: dict):
    table = table_payload
    transposed_rows = []
    for index, column in enumerate(table["columns"]):
        row = {
            "row_label": f"{column['metric_label']} / {column['group_label']} / {column['bucket_label']}",
            "cells": [],
        }
        for source_row in table["rows"]:
            row["cells"].append({"display": source_row["cells"][index]["display"]})
        transposed_rows.append(row)

    return {
        "mode": "vertical",
        "table": table,
        "transposed_rows": transposed_rows,
        "transposed_columns": [row["category_label"] for row in table["rows"]],
        "config": None,
    }
