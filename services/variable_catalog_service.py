from utils.text_utils import normalize_key, normalize_text


def build_variable_catalog(data_headers, question_metadata, respondent_data_clean, mappings, data_sheet_name):
    catalog = []
    counts_by_type = {"SA": 0, "MA": 0, "FT": 0}

    for variable_code in data_headers:
        metadata = question_metadata.get(variable_code) or question_metadata.get(variable_code.split("_")[0], {})
        question_type = normalize_text(metadata.get("question_type", ""))
        question_label = normalize_text(metadata.get("question_label", "")) or variable_code
        decoded_column = f"decoded__{variable_code}"
        distinct_values = []
        if decoded_column in respondent_data_clean.columns:
            distinct_values = [
                value
                for value in respondent_data_clean[decoded_column].fillna("").map(normalize_text).tolist()
                if value
            ]
            distinct_values = list(dict.fromkeys(distinct_values))

        available_codes = metadata.get("answer_codes_in_order", []) or distinct_values
        available_labels = metadata.get("answer_labels_in_order", []) or distinct_values

        quota_eligible = is_quota_eligible(variable_code, question_type, distinct_values, mappings)
        if question_type in counts_by_type:
            counts_by_type[question_type] += 1

        catalog.append(
            {
                "variable_code": variable_code,
                "question_label": question_label,
                "question_type": question_type or "UNKNOWN",
                "available_codes": available_codes,
                "available_labels": available_labels,
                "distinct_count_in_data": len(distinct_values),
                "quota_eligible": quota_eligible,
                "category_sort_mode": "code_order" if metadata.get("answer_labels_in_order") else "observed_order",
                "source_sheet": data_sheet_name,
            }
        )

    return {"variable_catalog": catalog, "counts_by_type": counts_by_type}


def filter_variable_catalog(variable_catalog, search_text="", question_type="ALL", eligibility_mode="eligible_only", distinct_band="ALL", decoded_only=False, sort_key="default"):
    filtered = list(variable_catalog)

    if eligibility_mode == "eligible_only":
        filtered = [item for item in filtered if item.get("quota_eligible")]

    if question_type != "ALL":
        filtered = [item for item in filtered if item.get("question_type") == question_type]

    if distinct_band == "2_5":
        filtered = [item for item in filtered if 2 <= item.get("distinct_count_in_data", 0) <= 5]
    elif distinct_band == "6_10":
        filtered = [item for item in filtered if 6 <= item.get("distinct_count_in_data", 0) <= 10]
    elif distinct_band == "11_plus":
        filtered = [item for item in filtered if item.get("distinct_count_in_data", 0) >= 11]

    if decoded_only:
        filtered = [item for item in filtered if item.get("available_labels")]

    if search_text:
        needle = normalize_text(search_text).lower()
        filtered = [
            item for item in filtered
            if needle in normalize_text(item.get("variable_code", "")).lower()
            or needle in normalize_text(item.get("question_label", "")).lower()
            or needle in normalize_text(item.get("question_type", "")).lower()
            or needle in ", ".join(item.get("available_labels", [])[:6]).lower()
        ]

    return sort_variable_catalog(filtered, sort_key=sort_key)


def sort_variable_catalog(variable_catalog, sort_key="default"):
    if sort_key == "question_label":
        return sorted(variable_catalog, key=lambda item: (normalize_text(item.get("question_label", "")).lower(), item.get("variable_code", "")))
    if sort_key == "question_type":
        return sorted(variable_catalog, key=lambda item: (item.get("question_type", ""), item.get("variable_code", "")))
    if sort_key == "distinct_count_in_data":
        return sorted(variable_catalog, key=lambda item: (item.get("distinct_count_in_data", 0), item.get("variable_code", "")))
    if sort_key == "variable_code":
        return sorted(variable_catalog, key=lambda item: item.get("variable_code", ""))
    type_priority = {"SA": 0, "MA": 1, "FT": 2}
    return sorted(
        variable_catalog,
        key=lambda item: (
            0 if item.get("quota_eligible") else 1,
            type_priority.get(item.get("question_type"), 9),
            item.get("variable_code", ""),
        ),
    )


def is_quota_eligible(variable_code: str, question_type: str, distinct_values, mappings: dict):
    quota_rules = mappings["quota_eligibility"]
    if not quota_rules.get(question_type, quota_rules.get("unknown", False)):
        return False
    if len(distinct_values) < 2:
        return False
    if len(distinct_values) > mappings["field_rules"]["max_distinct_for_quota"]:
        return False
    key = normalize_key(variable_code)
    if any(keyword in key for keyword in mappings["field_rules"]["id_like_keywords"]):
        return False
    return True
