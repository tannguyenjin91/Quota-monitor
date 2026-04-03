from copy import deepcopy

import pandas as pd

from services.config_service import load_yaml_config
from utils.text_utils import make_data_key, normalize_text


SUPPORTED_OPERATORS = {"always_true", "equals", "in", "not_empty", "all_of", "any_of"}


def load_quota_engine_config(path: str):
    config = load_yaml_config(path)
    validate_quota_engine_config(config)
    return config


def validate_quota_engine_config(config: dict):
    errors = []

    if not config.get("quota_groups"):
        errors.append("quota_groups is required")
    if not config.get("row_definitions"):
        errors.append("row_definitions is required")
    if not config.get("output_table"):
        errors.append("output_table is required")

    group_keys = set()
    bucket_refs = set()
    for group in config.get("quota_groups", []):
        if not group.get("key"):
            errors.append("Each quota group needs a key")
            continue
        group_keys.add(group["key"])
        if not isinstance(group.get("buckets", []), list) or not group.get("buckets"):
            errors.append(f"Quota group {group['key']} needs buckets")
            continue
        for bucket in group["buckets"]:
            if not bucket.get("key"):
                errors.append(f"Quota group {group['key']} has a bucket without key")
            operator = bucket.get("rule", {}).get("operator")
            if operator not in SUPPORTED_OPERATORS:
                errors.append(f"Unsupported operator '{operator}' in {group['key']}.{bucket.get('key', '')}")
            bucket_refs.add(f"{group['id']}.{bucket.get('key', '')}")

    output_table = config.get("output_table", {})
    for group_key in output_table.get("group_order", []):
        if group_key not in group_keys:
            errors.append(f"Unknown group_order key: {group_key}")
    for bucket_key in output_table.get("hidden_bucket_keys", []):
        if bucket_key not in bucket_refs:
            errors.append(f"Unknown hidden bucket key: {bucket_key}")
    for row in config.get("row_definitions", []):
        operator = row.get("filter", {}).get("operator")
        if operator not in SUPPORTED_OPERATORS:
            errors.append(f"Unsupported row filter operator '{operator}' in row {row.get('id', '')}")

    if errors:
        raise ValueError("Invalid quota engine config:\n- " + "\n- ".join(errors))


def enrich_dataframe_for_quota_engine(df: pd.DataFrame, config: dict):
    enriched = df.copy()

    for field in config.get("derived_fields", []):
        key = field["key"]
        field_type = field.get("type")
        default_value = field.get("default_value", "")
        if field_type == "range_bucket":
            derived_series = enriched.apply(lambda row: derive_range_bucket(row, field), axis=1)
        elif field_type == "concat":
            sources = field.get("sources", [])
            separator = field.get("separator", " | ")
            derived_series = enriched.apply(
                lambda row: separator.join(
                    [normalize_text(row.get(source, "")) for source in sources if normalize_text(row.get(source, ""))]
                )
                or default_value,
                axis=1,
            )
        else:
            derived_series = pd.Series([default_value] * len(enriched), index=enriched.index)

        if key in enriched.columns:
            enriched[key] = enriched[key].where(enriched[key].fillna("").map(lambda value: bool(normalize_text(value))), derived_series)
        else:
            enriched[key] = derived_series

    return enriched


def derive_range_bucket(row, field_config: dict):
    source_key = field_config.get("source_key")
    raw_value = row.get(source_key, "")
    numeric_value = extract_float(raw_value)
    if numeric_value is None:
        return field_config.get("default_value", "")

    for bucket in field_config.get("buckets", []):
        min_value = bucket.get("min")
        max_value = bucket.get("max")
        if min_value is not None and numeric_value < min_value:
            continue
        if max_value is not None and numeric_value > max_value:
            continue
        return bucket["key"]
    return field_config.get("default_value", "")


def extract_float(value):
    text = normalize_text(value).lower().replace(",", ".")
    number_chars = []
    started = False
    for char in text:
        if char.isdigit() or char == ".":
            number_chars.append(char)
            started = True
        elif started:
            break
    if not number_chars:
        return None
    try:
        return float("".join(number_chars))
    except ValueError:
        return None


def build_banner_table(df: pd.DataFrame, config: dict, metric_modes=None):
    data = enrich_dataframe_for_quota_engine(df, config)
    output_table = config["output_table"]
    visible_groups = get_visible_groups(config)
    visible_rows = get_visible_rows(config)
    metric_modes = metric_modes or output_table.get("metric_modes", ["count"])

    columns = []
    for metric_mode in metric_modes:
        for group in visible_groups:
            visible_buckets = get_visible_buckets(group, output_table)
            for bucket in visible_buckets:
                columns.append(
                    {
                        "metric_mode": metric_mode,
                        "metric_label": metric_mode.title(),
                        "group_key": group["key"],
                        "group_label": group.get("label", group["key"]),
                        "bucket_key": bucket["key"],
                        "bucket_label": bucket.get("label", bucket["key"]),
                        "bucket_ref": f"{group['id']}.{bucket['key']}",
                    }
                )

    rows = []
    for row in visible_rows:
        row_entry = {
            "question_key": row["question_key"],
            "question_label": row.get("question_label", row["question_key"]),
            "category_key": row["category_key"],
            "category_label": row.get("category_label", row["category_key"]),
            "visible": row.get("visible", True),
            "cells": [],
        }
        for column in columns:
            group = next(group for group in visible_groups if group["key"] == column["group_key"])
            bucket = next(bucket for bucket in group["buckets"] if bucket["key"] == column["bucket_key"])
            numerator_mask = evaluate_rule(data, row["filter"]) & evaluate_rule(data, bucket["rule"])
            numerator = int(numerator_mask.sum())
            value = numerator
            if column["metric_mode"] == "percent":
                denominator = resolve_denominator(data, config, row, group, bucket)
                value = round((numerator / denominator * 100) if denominator else 0, 1)
            row_entry["cells"].append(
                {
                    "metric_mode": column["metric_mode"],
                    "group_key": column["group_key"],
                    "bucket_key": column["bucket_key"],
                    "value": value,
                    "display": f"{value:.1f}%" if column["metric_mode"] == "percent" else str(value),
                }
            )
        rows.append(row_entry)

    return {
        "columns": columns,
        "rows": rows,
        "metric_modes": metric_modes,
        "show_question_text": output_table.get("show_question_text", True),
        "show_row_labels": output_table.get("show_row_labels", True),
        "group_headers": build_group_headers(columns),
        "metric_headers": build_metric_headers(columns),
    }


def get_visible_groups(config: dict):
    output_table = config["output_table"]
    hidden_groups = set(output_table.get("hidden_group_keys", []))
    order_lookup = {key: index for index, key in enumerate(output_table.get("group_order", []))}
    groups = [
        deepcopy(group)
        for group in config.get("quota_groups", [])
        if group.get("visible", True) and group["key"] not in hidden_groups
    ]
    return sorted(groups, key=lambda group: order_lookup.get(group["key"], 10_000))


def get_visible_buckets(group: dict, output_table: dict):
    hidden_bucket_keys = set(output_table.get("hidden_bucket_keys", []))
    return [
        bucket
        for bucket in group.get("buckets", [])
        if bucket.get("visible", True) and f"{group['id']}.{bucket['key']}" not in hidden_bucket_keys
    ]


def get_visible_rows(config: dict):
    hidden_questions = set(config["output_table"].get("hidden_question_keys", []))
    return [
        row
        for row in config.get("row_definitions", [])
        if row.get("visible", True) and row.get("question_key") not in hidden_questions
    ]


def build_metric_headers(columns):
    headers = []
    current_mode = None
    span = 0
    for column in columns:
        if column["metric_mode"] != current_mode:
            if current_mode is not None:
                headers.append({"label": current_mode.title(), "span": span})
            current_mode = column["metric_mode"]
            span = 1
        else:
            span += 1
    if current_mode is not None:
        headers.append({"label": current_mode.title(), "span": span})
    return headers


def build_group_headers(columns):
    headers = []
    current_signature = None
    span = 0
    for column in columns:
        signature = (column["metric_mode"], column["group_key"], column["group_label"])
        if signature != current_signature:
            if current_signature is not None:
                headers.append({"metric_mode": current_signature[0], "label": current_signature[2], "span": span})
            current_signature = signature
            span = 1
        else:
            span += 1
    if current_signature is not None:
        headers.append({"metric_mode": current_signature[0], "label": current_signature[2], "span": span})
    return headers


def evaluate_rule(df: pd.DataFrame, rule: dict):
    operator = (rule or {}).get("operator")
    if operator == "always_true":
        return pd.Series([True] * len(df), index=df.index)
    if operator == "equals":
        field = rule["field"]
        return df.get(field, pd.Series([""] * len(df), index=df.index)).fillna("") == rule.get("value")
    if operator == "in":
        field = rule["field"]
        return df.get(field, pd.Series([""] * len(df), index=df.index)).isin(rule.get("values", []))
    if operator == "not_empty":
        field = rule["field"]
        return df.get(field, pd.Series([""] * len(df), index=df.index)).fillna("").map(lambda value: bool(normalize_text(value)))
    if operator == "all_of":
        result = pd.Series([True] * len(df), index=df.index)
        for nested_rule in rule.get("rules", []):
            result &= evaluate_rule(df, nested_rule)
        return result
    if operator == "any_of":
        result = pd.Series([False] * len(df), index=df.index)
        for nested_rule in rule.get("rules", []):
            result |= evaluate_rule(df, nested_rule)
        return result
    raise ValueError(f"Unsupported rule operator: {operator}")


def resolve_denominator(df: pd.DataFrame, config: dict, row: dict, group: dict, bucket: dict):
    percent_config = row.get("percent_denominator", {"mode": "overall_total"})
    mode = percent_config.get("mode", "overall_total")

    if mode == "overall_total":
        return len(df)
    if mode == "bucket_total":
        return int(evaluate_rule(df, bucket["rule"]).sum())
    if mode == "group_total":
        group_mask = pd.Series([False] * len(df), index=df.index)
        for group_bucket in group.get("buckets", []):
            group_mask |= evaluate_rule(df, group_bucket["rule"])
        return int(group_mask.sum())
    if mode == "denominator_key":
        denominator_key = percent_config.get("value") or group.get("denominator_key")
        denominator_rule = config.get("denominators", {}).get(denominator_key, {}).get("filter", {"operator": "always_true"})
        denominator_mask = evaluate_rule(df, denominator_rule) & evaluate_rule(df, bucket["rule"])
        return int(denominator_mask.sum())
    if mode == "row_base":
        denominator_mask = evaluate_rule(df, percent_config.get("filter", {"operator": "always_true"}))
        return int(denominator_mask.sum())
    raise ValueError(f"Unsupported percent denominator mode: {mode}")


def dataframe_from_cleaned_export(cleaned_path: str):
    df = pd.read_excel(cleaned_path)
    alias_columns = {}
    for column in df.columns:
        normalized_key = make_data_key(column)
        if normalized_key and normalized_key not in df.columns:
            alias_columns[normalized_key] = df[column]
    if not alias_columns:
        return df
    return pd.concat([df, pd.DataFrame(alias_columns)], axis=1)
