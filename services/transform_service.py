from pathlib import Path

import pandas as pd

from models import QuotaSummaryRow, UploadRun, db
from services.header_service import detect_header_position, find_best_matching_column
from services.quota_service import build_summary_rows, compute_run_metrics, load_mappings, summarize_actuals
from utils.id_utils import build_respondent_id
from utils.text_utils import make_data_key, normalize_text


def process_upload(upload_run_id: int, app_config: dict):
    mappings = load_mappings(app_config["MAPPINGS_PATH"])
    upload_run = UploadRun.query.get_or_404(upload_run_id)
    file_path = Path(app_config["UPLOAD_FOLDER"]) / upload_run.stored_file_name

    preview_df = pd.read_excel(file_path, header=None, nrows=10, engine="openpyxl")
    preview_rows = preview_df.fillna("").values.tolist()
    header_row_index = detect_header_position(preview_rows, mappings["required_column_candidates"])

    raw_df = pd.read_excel(file_path, header=None, engine="openpyxl")
    header_values = raw_df.iloc[header_row_index].fillna("").tolist()
    merged_headers = [normalize_text(value) for value in header_values]
    data_df = raw_df.iloc[header_row_index + 1 :].reset_index(drop=True).copy()
    data_df.columns = merged_headers[: len(data_df.columns)]
    data_df = data_df.loc[:, [col for col in data_df.columns if normalize_text(col)]]
    data_df = data_df.dropna(how="all").reset_index(drop=True)

    required_indices = {}
    for key, candidates in mappings["required_column_candidates"].items():
        idx, _ = find_best_matching_column(list(data_df.columns), candidates)
        required_indices[key] = idx

    matched_columns = {key: data_df.columns[idx] for key, idx in required_indices.items()}
    required_data = data_df[list(matched_columns.values())].copy()
    required_mask = required_data.apply(lambda column: column.map(lambda value: bool(normalize_text(value)))).any(axis=1)
    data_df = data_df.loc[required_mask].reset_index(drop=True)

    working_df = pd.DataFrame(
        {
            "respondent_id": [build_respondent_id(upload_run_id, i + 1) for i in range(len(data_df))],
            "tu_choi_raw": data_df[matched_columns["rejection"]],
            "gender_raw": data_df[matched_columns["gender"]],
            "distance_raw": data_df[matched_columns["distance"]],
            "direction_raw": data_df[matched_columns["direction"]],
        }
    )

    for column in data_df.columns:
        column_key = make_data_key(column)
        if column_key and column_key not in working_df.columns:
            working_df[column_key] = data_df[column]

    working_df["tu_choi_normalized"] = working_df["tu_choi_raw"].apply(normalize_rejection_value)
    rejected_marker = mappings["rejection_rules"]["rejected_marker"]
    working_df["is_rejected"] = working_df["tu_choi_normalized"] == rejected_marker
    working_df["is_accepted_for_quota"] = ~working_df["is_rejected"]

    working_df["gender_std"] = working_df["gender_raw"].apply(lambda v: standardize_gender(v, mappings))
    working_df["distance_std"] = working_df["distance_raw"].apply(standardize_distance)
    working_df["direction_std"] = working_df["direction_raw"].apply(normalize_text)
    working_df["region_quota_parent"] = working_df["distance_std"].apply(lambda v: derive_region_parent(v, mappings))
    working_df["region_quota_detail"] = working_df.apply(lambda row: derive_region_detail(row, mappings), axis=1)
    validation = working_df.apply(validate_row, axis=1, result_type="expand")
    working_df["validation_status"] = validation[0]
    working_df["validation_note"] = validation[1]

    rejected_df = working_df[working_df["is_rejected"]].copy()
    invalid_df = working_df[
        (working_df["is_accepted_for_quota"]) & (working_df["validation_status"] == "invalid_accepted")
    ].copy()
    valid_df = working_df[
        (working_df["is_accepted_for_quota"]) & (working_df["validation_status"] == "valid")
    ].copy()

    actuals = summarize_actuals(valid_df, mappings)
    metrics = compute_run_metrics(working_df, rejected_df, invalid_df, valid_df, mappings)
    summary_rows = build_summary_rows(actuals, mappings)

    QuotaSummaryRow.query.filter_by(upload_run_id=upload_run_id).delete()
    for row in summary_rows:
        db.session.add(QuotaSummaryRow(upload_run_id=upload_run_id, **row))
    db.session.commit()

    audit_df = working_df[
        [
            "respondent_id",
            "tu_choi_raw",
            "tu_choi_normalized",
            "is_rejected",
            "is_accepted_for_quota",
            "gender_raw",
            "gender_std",
            "distance_raw",
            "direction_raw",
            "region_quota_parent",
            "region_quota_detail",
            "validation_status",
            "validation_note",
        ]
    ].copy()

    exceptions_df = pd.concat([rejected_df.assign(validation_status="rejected"), invalid_df], ignore_index=True)

    return {
        "cleaned_df": working_df,
        "exceptions_df": exceptions_df,
        "audit_df": audit_df,
        "summary_rows": summary_rows,
        "metrics": metrics,
        "actuals": actuals,
    }


def normalize_rejection_value(value):
    return normalize_text(value).lower()


def standardize_gender(value, mappings):
    normalized = normalize_text(value)
    return mappings["gender_mapping"].get(normalized, "")


def standardize_distance(value):
    normalized = normalize_text(value)
    if not normalized:
        return ""
    if normalized in {"0 - 2.49 km", "2.5 - 5 km", "5.1 - 7 km"}:
        return normalized

    numeric = extract_numeric_distance(normalized)
    if numeric is None:
        return normalized
    if numeric <= 2.49:
        return "0 - 2.49 km"
    if numeric <= 5:
        return "2.5 - 5 km"
    if numeric <= 7:
        return "5.1 - 7 km"
    return ""


def extract_numeric_distance(value):
    cleaned = (
        value.lower()
        .replace("km", "")
        .replace("–", "-")
        .replace("—", "-")
        .replace(",", ".")
        .strip()
    )
    number_chars = []
    for char in cleaned:
        if char.isdigit() or char == ".":
            number_chars.append(char)
        elif number_chars:
            break
    if not number_chars:
        return None
    try:
        return float("".join(number_chars))
    except ValueError:
        return None


def derive_region_parent(distance_value, mappings):
    return mappings["distance_parent_mapping"].get(distance_value, "")


def derive_region_detail(row, mappings):
    distance = row.get("distance_std", "")
    direction = row.get("direction_std", "")
    return mappings["distance_direction_mapping"].get(distance, {}).get(direction, "")


def validate_row(row):
    if row["is_rejected"]:
        return "rejected", "Rejected before quota calculations"
    if not row["gender_std"]:
        return "invalid_accepted", "Gender did not map to a quota category"
    if not row["region_quota_parent"]:
        return "invalid_accepted", "Distance did not map to a quota parent"
    if not row["region_quota_detail"]:
        return "invalid_accepted", "Distance and direction did not map to a quota detail"
    return "valid", "Accepted and quota-mapped"
