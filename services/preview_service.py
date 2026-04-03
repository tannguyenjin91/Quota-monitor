from utils.text_utils import normalize_text


def build_decoded_preview(cleaned_df, variable_catalog, max_rows: int = 10):
    quota_eligible_variables = [item for item in variable_catalog if item.get("quota_eligible")]
    preferred_codes = [code for code in ("S2", "S3") if f"decoded__{code}" in cleaned_df.columns]

    selected_variables = []
    for code in preferred_codes:
        if code not in selected_variables:
            selected_variables.append(code)

    for item in quota_eligible_variables:
        code = item["variable_code"]
        if code not in selected_variables and f"decoded__{code}" in cleaned_df.columns:
            selected_variables.append(code)
        if len(selected_variables) >= 5:
            break

    preview_columns = [
        {"key": "respondent_id", "label": "respondent_id"},
        {"key": "tu_choi_raw", "label": "tu_choi_raw"},
        {"key": "is_rejected", "label": "is_rejected"},
    ]
    for code in selected_variables:
        preview_columns.append({"key": code, "label": code})

    rows = []
    preview_df = cleaned_df.head(max_rows)
    for _, row in preview_df.iterrows():
        rendered_row = {}
        rendered_row["respondent_id"] = row.get("respondent_id", "")
        rendered_row["tu_choi_raw"] = row.get("tu_choi_raw", "")
        rendered_row["is_rejected"] = row.get("is_rejected", "")
        for code in selected_variables:
            raw_value = normalize_text(row.get(f"coded__{code}", ""))
            decoded_value = normalize_text(row.get(f"decoded__{code}", ""))
            if decoded_value and raw_value and decoded_value != raw_value:
                rendered_row[code] = f"{decoded_value} (code: {raw_value})"
            else:
                rendered_row[code] = decoded_value or raw_value
        rows.append(rendered_row)

    return {"columns": preview_columns, "rows": rows}
