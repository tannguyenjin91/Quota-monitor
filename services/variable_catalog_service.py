import re

from utils.text_utils import normalize_key, normalize_text


def build_variable_catalog(data_headers, question_metadata, respondent_data_clean, mappings, data_sheet_name):
    catalog = []
    counts_by_type = {"SA": 0, "MA": 0, "FT": 0}

    for variable_code in data_headers:
        metadata = question_metadata.get(variable_code) or question_metadata.get(variable_code.split("_")[0], {})
        question_type = normalize_text(metadata.get("question_type", ""))
        question_label = normalize_text(metadata.get("question_label", "")) or variable_code

        if question_type == "MA":
            ma_label = resolve_ma_sub_label(variable_code, metadata)
            if ma_label:
                question_label = ma_label
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

    ma_groups = build_ma_groups(catalog)
    catalog_with_groups = catalog + ma_groups

    return {"variable_catalog": catalog_with_groups, "counts_by_type": counts_by_type}


def resolve_ma_sub_label(variable_code: str, base_metadata: dict) -> str | None:
    """Derive the per-option answer label for an MA sub-variable.

    Q15_1  → suffix "1"  → answer_map["1"] → "VietnamNet"
    Q15_o15 → suffix "o15" → "Other" option
    """
    match = re.match(r"^(.+?)_([^_]+)$", variable_code)
    if not match:
        return None

    suffix = match.group(2)
    answer_map = base_metadata.get("answer_map", {})
    answer_labels = base_metadata.get("answer_labels_in_order", [])
    answer_codes = base_metadata.get("answer_codes_in_order", [])

    # Direct match: suffix "1" in answer_map
    if suffix in answer_map:
        return answer_map[suffix]

    # "o" prefix = Other option (e.g., "o15", "o4")
    if suffix.startswith("o"):
        numeric_part = suffix[1:]
        if numeric_part in answer_map:
            return answer_map[numeric_part]
        # Find label containing "Other" or "Khác"
        for label in answer_labels:
            lower = label.lower()
            if "other" in lower or "khác" in lower or "khac" in lower:
                return label
        return f"Other ({suffix})"

    # Positional fallback: suffix "3" → 3rd answer
    try:
        idx = int(suffix) - 1
        if 0 <= idx < len(answer_labels):
            return answer_labels[idx]
    except ValueError:
        pass

    return None


def build_ma_groups(catalog):
    """Detect MA sub-variables (Q15_1, Q15_2, ...) and create a virtual MA_GROUP entry (Q15).

    Also marks individual MA sub-variables with 'ma_base_code' so they can be
    hidden from the selector regardless of whether a group is created.
    """

    ma_items = [item for item in catalog if item.get("question_type") == "MA"]
    if not ma_items:
        return []

    groups = {}
    for item in ma_items:
        code = item["variable_code"]
        match = re.match(r"^(.+?)_[^_]+$", code)
        if not match:
            continue
        base_code = match.group(1)
        if base_code not in groups:
            groups[base_code] = []
        groups[base_code].append(item)

    existing_codes = {item["variable_code"] for item in catalog}
    existing_non_ma = {
        item["variable_code"]
        for item in catalog
        if item.get("question_type") not in ("MA",)
    }

    ma_group_entries = []
    for base_code, members in groups.items():
        if base_code in existing_non_ma:
            continue
        if base_code in existing_codes:
            continue
        first = members[0]
        sub_labels = []
        for member in members:
            label = member.get("question_label", member["variable_code"])
            if label not in sub_labels:
                sub_labels.append(label)
        ma_group_entries.append({
            "variable_code": base_code,
            "question_label": first.get("question_label", base_code),
            "question_type": "MA_GROUP",
            "available_codes": [m["variable_code"] for m in members],
            "available_labels": sub_labels,
            "distinct_count_in_data": len(members),
            "quota_eligible": True,
            "category_sort_mode": "code_order",
            "source_sheet": first.get("source_sheet", ""),
        })
    return ma_group_entries


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
