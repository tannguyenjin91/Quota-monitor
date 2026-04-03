from datetime import datetime

import pandas as pd

from utils.id_utils import build_respondent_id
from utils.text_utils import is_blank, normalize_answer_code, normalize_key, normalize_rejection_value, normalize_text


def parse_data_sheet(file_path: str, sheet_name: str, upload_run_id: int, source_file_name: str, mappings: dict, question_metadata: dict):
    header_row_idx = mappings["parser_options"]["data_header_row"] - 1
    data_start_idx = mappings["parser_options"]["data_start_row"] - 1

    raw_df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, engine="openpyxl")
    header_values = [normalize_text(value) for value in raw_df.iloc[header_row_idx].tolist()]
    unique_header_values = make_unique_headers(header_values)
    data_df = raw_df.iloc[data_start_idx:].reset_index(drop=True).copy()
    data_df.columns = unique_header_values[: len(data_df.columns)]
    data_df = data_df.loc[:, [column for column in data_df.columns if normalize_text(column)]]
    data_df = data_df.dropna(how="all").reset_index(drop=True)

    rejection_column = find_rejection_column(data_df.columns.tolist(), mappings["rejection_column_candidates"])
    processing_timestamp = datetime.utcnow().isoformat()
    cleaned_df = pd.DataFrame(
        {
            "respondent_id": [build_respondent_id(upload_run_id, data_start_idx + 1 + row_offset) for row_offset in range(len(data_df))],
            "source_row_number": [data_start_idx + 1 + row_offset for row_offset in range(len(data_df))],
            "source_file_name": source_file_name,
            "processing_timestamp": processing_timestamp,
        }
    )

    if rejection_column:
        tu_choi_raw_series = data_df[rejection_column]
    else:
        tu_choi_raw_series = pd.Series([""] * len(data_df))

    tu_choi_normalized_series = tu_choi_raw_series.map(normalize_rejection_value)
    is_rejected_series = tu_choi_normalized_series.eq("x")

    cleaned_df["tu_choi_raw"] = tu_choi_raw_series.values
    cleaned_df["tu_choi_normalized"] = tu_choi_normalized_series.values
    cleaned_df["is_rejected"] = is_rejected_series.values
    cleaned_df["is_accepted_for_quota"] = (~is_rejected_series).values

    decoded_columns = {}
    for column_index, column in enumerate(data_df.columns):
        variable_code = normalize_text(column)
        coded_series = data_df.iloc[:, column_index]
        decoded_columns[f"coded__{variable_code}"] = coded_series.values
        decoded_columns[f"decoded__{variable_code}"] = decode_series(variable_code, coded_series, question_metadata).values

    if decoded_columns:
        cleaned_df = pd.concat([cleaned_df, pd.DataFrame(decoded_columns)], axis=1)

    return {
        "respondent_data_clean": cleaned_df,
        "rejection_column_found": bool(rejection_column),
        "rejection_column_name": rejection_column,
        "data_headers": data_df.columns.tolist(),
    }


def find_rejection_column(headers, candidates):
    normalized_candidates = {normalize_key(candidate) for candidate in candidates}
    for header in headers:
        if normalize_key(header) in normalized_candidates:
            return header
    return None


def decode_value(variable_code: str, coded_value, question_metadata: dict):
    if is_blank(coded_value):
        return ""

    metadata = resolve_question_metadata(variable_code, question_metadata)
    if not metadata:
        return normalize_text(coded_value)

    answer_code = normalize_answer_code(coded_value)
    return metadata["answer_map"].get(answer_code, normalize_text(coded_value))


def decode_series(variable_code: str, coded_series: pd.Series, question_metadata: dict) -> pd.Series:
    metadata = resolve_question_metadata(variable_code, question_metadata)
    normalized_text_series = coded_series.map(normalize_text)
    if not metadata:
        return normalized_text_series

    normalized_code_series = coded_series.map(normalize_answer_code)
    mapped_series = normalized_code_series.map(metadata["answer_map"])
    return mapped_series.fillna(normalized_text_series)


def resolve_question_metadata(variable_code: str, question_metadata: dict):
    if variable_code in question_metadata:
        return question_metadata[variable_code]
    if "_" in variable_code:
        base_code = variable_code.split("_")[0]
        return question_metadata.get(base_code)
    return None


def make_unique_headers(headers):
    counts = {}
    unique_headers = []
    for raw_header in headers:
        header = normalize_text(raw_header)
        if not header:
            unique_headers.append(header)
            continue
        count = counts.get(header, 0) + 1
        counts[header] = count
        unique_headers.append(header if count == 1 else f"{header}_dup{count}")
    return unique_headers
