import pandas as pd

from services.preview_service import build_decoded_preview
from services.variable_catalog_service import filter_variable_catalog


def test_preview_returns_max_10_rows_and_decoded_labels():
    cleaned_df = pd.DataFrame(
        [
            {
                "respondent_id": f"R{i}",
                "tu_choi_raw": "",
                "is_rejected": False,
                "coded__S2": 1,
                "decoded__S2": "0 - 2.49 km",
                "coded__S3": 2,
                "decoded__S3": "Đông",
            }
            for i in range(12)
        ]
    )
    variable_catalog = [
        {"variable_code": "S2", "quota_eligible": True},
        {"variable_code": "S3", "quota_eligible": True},
    ]
    preview = build_decoded_preview(cleaned_df, variable_catalog, max_rows=10)
    assert len(preview["rows"]) == 10
    assert preview["rows"][0]["S2"] == "0 - 2.49 km (code: 1)"


def test_search_and_filters_work():
    variables = [
        {"variable_code": "S2", "question_label": "Distance", "question_type": "SA", "available_labels": ["Near", "Far"], "distinct_count_in_data": 2, "quota_eligible": True},
        {"variable_code": "S3", "question_label": "Direction", "question_type": "SA", "available_labels": ["Bắc", "Đông"], "distinct_count_in_data": 2, "quota_eligible": True},
        {"variable_code": "QTXT", "question_label": "Open text", "question_type": "FT", "available_labels": [], "distinct_count_in_data": 50, "quota_eligible": False},
    ]
    assert [item["variable_code"] for item in filter_variable_catalog(variables, search_text="Distance")] == ["S2"]
    assert [item["variable_code"] for item in filter_variable_catalog(variables, search_text="S3")] == ["S3"]
    assert [item["variable_code"] for item in filter_variable_catalog(variables, question_type="FT", eligibility_mode="all")] == ["QTXT"]
    assert all(item["quota_eligible"] for item in filter_variable_catalog(variables, eligibility_mode="eligible_only"))
