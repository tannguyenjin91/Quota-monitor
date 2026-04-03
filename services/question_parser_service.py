import pandas as pd

from utils.text_utils import normalize_answer_code, normalize_key, normalize_text


def parse_question_sheet(file_path: str, sheet_name: str, mappings: dict):
    header_row = mappings["parser_options"]["question_header_row"] - 1
    raw_df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row, engine="openpyxl")
    raw_df = raw_df.fillna("")

    normalized_columns = {column: normalize_key(column) for column in raw_df.columns}
    column_lookup = {value: key for key, value in normalized_columns.items()}

    name_column = column_lookup.get("name_of_items")
    type_column = column_lookup.get("question_type")
    matrix_column = column_lookup.get("question_matrix")
    normal_column = column_lookup.get("question_normal")

    if not name_column:
        raise ValueError("Question sheet does not contain 'Name of items'")

    numeric_answer_columns = [
        column
        for column in raw_df.columns
        if normalize_text(column).isdigit()
    ]

    question_dictionary = []
    question_metadata = {}

    for _, row in raw_df.iterrows():
        variable_code = normalize_text(row.get(name_column))
        if not variable_code:
            continue

        question_type = normalize_text(row.get(type_column))
        question_matrix_label = normalize_text(row.get(matrix_column)) if matrix_column else ""
        question_label = normalize_text(row.get(normal_column)) or question_matrix_label or variable_code

        answers = []
        for display_order, answer_column in enumerate(numeric_answer_columns, start=1):
            answer_label = normalize_text(row.get(answer_column))
            answer_code = normalize_answer_code(answer_column)
            if not answer_label:
                continue
            answers.append(
                {
                    "question_code": variable_code,
                    "question_type": question_type,
                    "question_label": question_label,
                    "question_matrix_label": question_matrix_label,
                    "answer_code": answer_code,
                    "answer_label": answer_label,
                    "answer_display_order": display_order,
                    "source_sheet": sheet_name,
                }
            )

        question_dictionary.extend(answers)
        question_metadata[variable_code] = {
            "question_code": variable_code,
            "question_type": question_type,
            "question_label": question_label,
            "question_matrix_label": question_matrix_label,
            "answer_map": {entry["answer_code"]: entry["answer_label"] for entry in answers},
            "answer_labels_in_order": [entry["answer_label"] for entry in answers],
            "answer_codes_in_order": [entry["answer_code"] for entry in answers],
            "source_sheet": sheet_name,
        }

    return {
        "question_dictionary": question_dictionary,
        "question_metadata": question_metadata,
    }
