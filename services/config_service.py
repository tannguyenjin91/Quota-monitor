from copy import deepcopy

import yaml


def load_yaml_config(path: str):
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def save_yaml_config(path: str, payload: dict):
    with open(path, "w", encoding="utf-8") as file:
        yaml.safe_dump(payload, file, allow_unicode=True, sort_keys=False)


def update_output_table_config(config: dict, form_payload: dict):
    updated = deepcopy(config)
    output_table = updated.setdefault("output_table", {})

    output_table["show_question_text"] = bool(form_payload.get("show_question_text"))
    output_table["show_row_labels"] = bool(form_payload.get("show_row_labels"))
    output_table["group_order"] = parse_multiline_list(form_payload.get("group_order", ""))
    output_table["hidden_group_keys"] = parse_multiline_list(form_payload.get("hidden_group_keys", ""))
    output_table["hidden_bucket_keys"] = parse_multiline_list(form_payload.get("hidden_bucket_keys", ""))
    output_table["hidden_question_keys"] = parse_multiline_list(form_payload.get("hidden_question_keys", ""))
    output_table["metric_modes"] = parse_multiline_list(form_payload.get("metric_modes", "")) or ["count"]
    return updated


def parse_multiline_list(value: str):
    return [item.strip() for item in (value or "").splitlines() if item.strip()]
