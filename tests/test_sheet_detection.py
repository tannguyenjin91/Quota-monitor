from openpyxl import Workbook

from services.sheet_detection_service import detect_workbook_sheets


def test_sheet_detection(tmp_path):
    workbook = Workbook()
    question_sheet = workbook.active
    question_sheet.title = "Question"
    workbook.create_sheet("Data")
    file_path = tmp_path / "test.xlsx"
    workbook.save(file_path)

    mappings = {
        "sheet_detection": {
            "question_sheet_candidates": ["Question", "Questions"],
            "data_sheet_candidates": ["Data", "Raw Data"],
        }
    }
    result = detect_workbook_sheets(str(file_path), mappings)
    assert result["question_sheet_name"] == "Question"
    assert result["data_sheet_name"] == "Data"
