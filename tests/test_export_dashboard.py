from openpyxl import load_workbook

from services.export_service import export_quota_dashboard


def test_export_preserves_dashboard_state(tmp_path):
    app_config = {"OUTPUT_FOLDER": str(tmp_path)}
    dashboard_payload = {
        "source_file_name": "sample.xlsx",
        "processed_at": None,
        "horizontal_code": "S3",
        "horizontal_label": "Direction",
        "vertical_code": "S2",
        "vertical_label": "Distance",
        "display_mode": "Count + Percent",
        "percent_mode": "row_percent",
        "accepted_base_size": 3,
        "column_categories": ["Bac", "Dong"],
        "rows": [{"label": "0 - 2.49 km", "cells": [{"display": "1"}, {"display": "1"}], "total": {"display": "2"}}],
        "total_row": {"label": "Total", "cells": [{"display": "1"}, {"display": "1"}], "total": {"display": "3"}},
        "table_views": [
            {
                "mode": "Count",
                "rows": [{"label": "0 - 2.49 km", "cells": [{"display": "1"}, {"display": "1"}], "total": {"display": "2"}}],
                "total_row": {"label": "Total", "cells": [{"display": "1"}, {"display": "1"}], "total": {"display": "3"}},
            },
            {
                "mode": "Percent",
                "rows": [{"label": "0 - 2.49 km", "cells": [{"display": "50.0%"}, {"display": "50.0%"}], "total": {"display": "100.0%"}}],
                "total_row": {"label": "Total", "cells": [{"display": "33.3%"}, {"display": "33.3%"}], "total": {"display": "100.0%"}},
            },
        ],
    }

    file_path = export_quota_dashboard(1, dashboard_payload, app_config)
    workbook = load_workbook(file_path)
    sheet = workbook["Quota Dashboard"]

    assert sheet["A1"].value == "Source file name"
    assert sheet["B3"].value == "S3"
    assert sheet["A11"].value == "Count table"
    assert sheet["A12"].value == "Distance"
    assert sheet["B13"].value == "1"
    assert sheet["A16"].value == "Percent table"
    assert sheet["B18"].value == "50.0%"
    assert sheet["D19"].value == "100.0%"
