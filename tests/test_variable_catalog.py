from services.variable_catalog_service import build_variable_catalog


def test_variable_catalog_marks_sa_as_quota_eligible():
    question_metadata = {
        "S2": {
            "question_type": "SA",
            "question_label": "Distance",
            "answer_codes_in_order": ["1", "2", "3"],
            "answer_labels_in_order": ["0 - 2.49 km", "2.5 - 5 km", "5.1 - 7 km"],
        },
        "OPEN": {
            "question_type": "FT",
            "question_label": "Open text",
            "answer_codes_in_order": [],
            "answer_labels_in_order": [],
        },
    }
    respondent_data_clean = __import__("pandas").DataFrame(
        {
            "decoded__S2": ["0 - 2.49 km", "2.5 - 5 km"],
            "decoded__OPEN": ["foo", "bar"],
        }
    )
    mappings = {
        "quota_eligibility": {"SA": True, "MA": False, "FT": False, "unknown": False},
        "field_rules": {"max_distinct_for_quota": 20, "id_like_keywords": ["id", "time"]},
    }
    result = build_variable_catalog(["S2", "OPEN"], question_metadata, respondent_data_clean, mappings, "Data")
    lookup = {item["variable_code"]: item for item in result["variable_catalog"]}
    assert lookup["S2"]["quota_eligible"] is True
    assert lookup["OPEN"]["quota_eligible"] is False
    assert lookup["OPEN"]["available_labels"] == ["foo", "bar"]
    assert [item["variable_code"] for item in result["variable_catalog"]] == ["S2", "OPEN"]
