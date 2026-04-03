from services.ai_assist_service import suggest_quota_setup


def test_heuristic_ai_suggests_axes_and_filters():
    variables = [
        {
            "variable_code": "GENDER",
            "question_label": "Giới tính",
            "question_type": "SA",
            "available_labels": ["Nam", "Nữ"],
            "quota_eligible": True,
        },
        {
            "variable_code": "S3",
            "question_label": "Hướng phỏng vấn",
            "question_type": "SA",
            "available_labels": ["Bắc", "Đông", "Tây", "Nam", "Đông Bắc", "Tây Nam"],
            "quota_eligible": True,
        },
        {
            "variable_code": "S2",
            "question_label": "Khoảng cách",
            "question_type": "SA",
            "available_labels": ["0 - 2.49 km", "2.5 - 5 km", "5.1 - 7 km"],
            "quota_eligible": True,
        },
        {
            "variable_code": "Q9",
            "question_label": "Tuổi",
            "question_type": "SA",
            "available_labels": ["18-29", "30-39", "40-49"],
            "quota_eligible": True,
        },
    ]
    prompt = "Bao nhieu nguoi nam o huong Dong Bac, co khoang cach S2 0 - 2.49 km, o Q9 30-39 tuoi"
    suggestion = suggest_quota_setup(prompt, variables, display_mode="Count", percent_mode="total_percent")

    assert suggestion["horizontal_variable"] == "GENDER"
    assert suggestion["vertical_variable"] == "S3"
    assert {"variable_code": "S2", "value": "0 - 2.49 km"} in suggestion["selected_filters"]
    assert {"variable_code": "Q9", "value": "30-39"} in suggestion["selected_filters"]


def test_empty_prompt_returns_empty_suggestion():
    suggestion = suggest_quota_setup("", [], display_mode="Count", percent_mode="total_percent")
    assert suggestion["horizontal_variable"] == ""
    assert suggestion["vertical_variable"] == ""
    assert suggestion["selected_filters"] == []
