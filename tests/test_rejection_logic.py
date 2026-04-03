import pandas as pd

from services.raw_data_parser_service import find_rejection_column
from utils.text_utils import normalize_rejection_value


def test_rejection_normalization():
    assert normalize_rejection_value(" x ") == "x"
    assert normalize_rejection_value("X") == "x"
    assert normalize_rejection_value("") == ""


def test_rejection_column_detection():
    headers = ["ID", "Từ chối", "S1"]
    assert find_rejection_column(headers, ["Từ chối", "Tu choi"]) == "Từ chối"
