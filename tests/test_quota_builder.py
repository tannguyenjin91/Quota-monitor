import pandas as pd

from services.quota_builder_service import apply_additional_filters, build_banner_table, build_drilldown_table, build_quota_table


def test_quota_builder_preserves_question_order():
    cleaned_df = pd.DataFrame(
        [
            {"is_accepted_for_quota": True, "decoded__S2": "2.5 - 5 km", "decoded__S3": "Đông"},
            {"is_accepted_for_quota": True, "decoded__S2": "0 - 2.49 km", "decoded__S3": "Bắc"},
            {"is_accepted_for_quota": True, "decoded__S2": "0 - 2.49 km", "decoded__S3": "Đông"},
        ]
    )
    variable_catalog_lookup = {
        "S2": {"available_labels": ["0 - 2.49 km", "2.5 - 5 km", "5.1 - 7 km"]},
        "S3": {"available_labels": ["Bắc", "Đông", "Tây"]},
    }
    result = build_quota_table(cleaned_df, variable_catalog_lookup, "S2", "S3", "Count", "total_percent")
    assert result["row_categories"] == ["0 - 2.49 km", "2.5 - 5 km"]
    assert result["column_categories"] == ["Bắc", "Đông"]
    assert result["rows"][0]["cells"][0]["display"] == "1"


def test_additional_filters_slice_dataset():
    cleaned_df = pd.DataFrame(
        [
            {"decoded__S2": "0 - 2.49 km", "decoded__Q9": "30-39", "decoded__S3": "Đông Bắc"},
            {"decoded__S2": "2.5 - 5 km", "decoded__Q9": "30-39", "decoded__S3": "Đông Bắc"},
            {"decoded__S2": "0 - 2.49 km", "decoded__Q9": "18-29", "decoded__S3": "Đông Bắc"},
        ]
    )
    filtered = apply_additional_filters(
        cleaned_df,
        [
            {"variable_code": "S2", "value": "0 - 2.49 km"},
            {"variable_code": "Q9", "value": "30-39"},
        ],
    )
    assert len(filtered) == 1


def test_count_and_percent_mode_returns_two_tables():
    cleaned_df = pd.DataFrame(
        [
            {"is_accepted_for_quota": True, "decoded__S2": "0 - 2.49 km", "decoded__S3": "Bắc"},
            {"is_accepted_for_quota": True, "decoded__S2": "0 - 2.49 km", "decoded__S3": "Đông"},
        ]
    )
    variable_catalog_lookup = {
        "S2": {"available_labels": ["0 - 2.49 km"]},
        "S3": {"available_labels": ["Bắc", "Đông"]},
    }
    result = build_quota_table(cleaned_df, variable_catalog_lookup, "S2", "S3", "Count + Percent", "total_percent")
    assert len(result["table_views"]) == 2
    assert result["table_views"][0]["mode"] == "Count"
    assert result["table_views"][1]["mode"] == "Percent"


def test_drilldown_table_breaks_selected_cell_into_s2():
    cleaned_df = pd.DataFrame(
        [
            {"is_accepted_for_quota": True, "decoded__BIDANH": "Dang Hoang Loc", "decoded__S3": "Dong", "decoded__S2": "0 - 2.49 km"},
            {"is_accepted_for_quota": True, "decoded__BIDANH": "Dang Hoang Loc", "decoded__S3": "Dong", "decoded__S2": "2.5 - 5 km"},
            {"is_accepted_for_quota": True, "decoded__BIDANH": "Dang Hoang Loc", "decoded__S3": "Dong", "decoded__S2": "0 - 2.49 km"},
            {"is_accepted_for_quota": True, "decoded__BIDANH": "Nguyen Thi A", "decoded__S3": "Dong", "decoded__S2": "0 - 2.49 km"},
        ]
    )
    variable_catalog_lookup = {
        "S2": {"question_label": "Khoang cach", "available_labels": ["0 - 2.49 km", "2.5 - 5 km", "5.1 - 7 km"]},
        "S3": {"question_label": "Huong"},
        "BIDANH": {"question_label": "Bi danh"},
    }
    result = build_drilldown_table(
        cleaned_df=cleaned_df,
        variable_catalog_lookup=variable_catalog_lookup,
        row_variable="BIDANH",
        row_value="Dang Hoang Loc",
        column_variable="S3",
        column_value="Dong",
        breakdown_variable="S2",
        display_mode="Count",
        percent_mode="total_percent",
    )
    assert result["rows"][0]["label"] == "0 - 2.49 km"
    assert result["rows"][0]["cell"]["display"] == "2"
    assert result["rows"][1]["cell"]["display"] == "1"
    assert result["total"]["display"] == "3"


def test_banner_table_builds_grouped_columns_and_sections():
    cleaned_df = pd.DataFrame(
        [
            {"is_accepted_for_quota": True, "decoded__S1": "Ward 1", "decoded__GENDER": "Male", "decoded__S3": "Dong"},
            {"is_accepted_for_quota": True, "decoded__S1": "Ward 1", "decoded__GENDER": "Female", "decoded__S3": "Dong"},
            {"is_accepted_for_quota": True, "decoded__S1": "Ward 2", "decoded__GENDER": "Male", "decoded__S3": "Bac"},
        ]
    )
    variable_catalog_lookup = {
        "S1": {"question_label": "Current address", "available_labels": ["Ward 1", "Ward 2"]},
        "GENDER": {"question_label": "Gender", "available_labels": ["Male", "Female"]},
        "S3": {"question_label": "Direction", "available_labels": ["Bac", "Dong"]},
    }
    result = build_banner_table(
        cleaned_df=cleaned_df,
        variable_catalog_lookup=variable_catalog_lookup,
        row_variables=["S1"],
        banner_variables=["GENDER", "S3"],
        display_mode="Count",
        percent_mode="total_percent",
    )
    assert result["report_type"] == "banner_table"
    assert result["column_groups"][1]["question_label"] == "Gender"
    assert result["column_groups"][2]["question_label"] == "Direction"
    assert result["banner_views"][0]["sections"][0]["question_label"] == "Current address"
    assert result["banner_views"][0]["sections"][0]["rows"][1]["category_label"] == "Ward 1"


def test_banner_table_supports_ma_row_options():
    cleaned_df = pd.DataFrame(
        [
            {"is_accepted_for_quota": True, "decoded__S3": "Dong", "decoded__Q11_1": 1, "decoded__Q11_2": 0},
            {"is_accepted_for_quota": True, "decoded__S3": "Dong", "decoded__Q11_1": 0, "decoded__Q11_2": 1},
            {"is_accepted_for_quota": True, "decoded__S3": "Bac", "decoded__Q11_1": 1, "decoded__Q11_2": 1},
        ]
    )
    variable_catalog_lookup = {
        "Q11": {"question_label": "Reason list", "question_type": "MA"},
        "Q11_1": {"question_label": "Reason A"},
        "Q11_2": {"question_label": "Reason B"},
        "S3": {"question_label": "Direction", "available_labels": ["Bac", "Dong"]},
    }
    result = build_banner_table(
        cleaned_df=cleaned_df,
        variable_catalog_lookup=variable_catalog_lookup,
        row_variables=["Q11"],
        banner_variables=["S3"],
        display_mode="Count",
        percent_mode="total_percent",
    )
    section_rows = result["banner_views"][0]["sections"][0]["rows"]
    assert section_rows[1]["category_label"] == "Reason A"
    assert section_rows[2]["category_label"] == "Reason B"
    # Overall total column for Reason A should be 2
    assert section_rows[1]["cells"][0]["display"] == "2"


def test_banner_tree_order_is_applied_for_column_building():
    cleaned_df = pd.DataFrame(
        [
            {"is_accepted_for_quota": True, "decoded__CITY": "HCM", "decoded__GENDER": "Male", "decoded__AGE": "18-24", "decoded__Q1": "Ward 1"},
            {"is_accepted_for_quota": True, "decoded__CITY": "HCM", "decoded__GENDER": "Female", "decoded__AGE": "25-32", "decoded__Q1": "Ward 1"},
        ]
    )
    variable_catalog_lookup = {
        "Q1": {"question_label": "Address", "available_labels": ["Ward 1"]},
        "CITY": {"question_label": "City", "available_labels": ["HCM"]},
        "GENDER": {"question_label": "Gender", "available_labels": ["Male", "Female"]},
        "AGE": {"question_label": "Age", "available_labels": ["18-24", "25-32"]},
    }
    result = build_banner_table(
        cleaned_df=cleaned_df,
        variable_catalog_lookup=variable_catalog_lookup,
        row_variables=["Q1"],
        banner_variables=["AGE", "CITY"],
        banner_tree=[{"variable_code": "CITY"}, {"variable_code": "GENDER"}, {"variable_code": "AGE"}],
        banner_layout_mode="tree",
        display_mode="Count",
        percent_mode="total_percent",
    )
    assert result["selected_banner_variables"] == ["CITY", "GENDER", "AGE"]
    assert result["column_groups"][1]["question_label"] == "HCM"


def test_mixed_tree_flat_header_matches_body_columns():
    cleaned_df = pd.DataFrame(
        [
            {"is_accepted_for_quota": True, "decoded__ROW": "A", "decoded__S3": "Dong", "decoded__S2": "0-2", "decoded__GENDER": "Nam"},
            {"is_accepted_for_quota": True, "decoded__ROW": "B", "decoded__S3": "Bac", "decoded__S2": "2-5", "decoded__GENDER": "Nu"},
        ]
    )
    variable_catalog_lookup = {
        "ROW": {"question_label": "Row", "available_labels": ["A", "B"]},
        "S3": {"question_label": "Direction", "available_labels": ["Bac", "Dong"]},
        "S2": {"question_label": "Distance", "available_labels": ["0-2", "2-5"]},
        "GENDER": {"question_label": "Gender", "available_labels": ["Nam", "Nu"]},
    }
    result = build_banner_table(
        cleaned_df=cleaned_df,
        variable_catalog_lookup=variable_catalog_lookup,
        row_variables=["ROW"],
        banner_variables=["S3", "S2", "GENDER"],
        banner_tree=[{"variable_code": "S3"}, {"variable_code": "S2"}],
        banner_layout_mode="tree",
        display_mode="Count",
        percent_mode="total_percent",
    )
    assert len(result["header_rows"]) == 2
    data_col_count = len(result["flat_columns"])
    header_top_data_sum = sum(cell.get("colspan", 1) for cell in result["header_rows"][0]) - 2
    header_bottom_count = len(result["header_rows"][1])
    row_cell_count = len(result["banner_views"][0]["sections"][0]["rows"][0]["cells"])
    assert header_top_data_sum == data_col_count
    assert header_bottom_count == data_col_count
    assert row_cell_count == data_col_count
