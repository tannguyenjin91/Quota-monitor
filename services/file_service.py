from pathlib import Path

import pandas as pd
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename


def ensure_directories(paths):
    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.lower().endswith(".xlsx")


def save_upload(uploaded_file: FileStorage, upload_id: int, upload_folder: str) -> str:
    filename = secure_filename(uploaded_file.filename or f"upload_{upload_id}.xlsx")
    stored_file_name = f"upload_{upload_id}_{filename}"
    destination = Path(upload_folder) / stored_file_name
    uploaded_file.save(destination)
    return stored_file_name


def build_output_paths(upload_id: int, output_folder: str):
    run_dir = Path(output_folder) / f"upload_{upload_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return {
        "run_dir": run_dir,
        "cleaned_data_path": run_dir / "cleaned_data.xlsx",
        "cleaned_data_pickle_path": run_dir / "cleaned_data.pkl",
        "question_dictionary_path": run_dir / "question_dictionary.xlsx",
        "mapping_audit_path": run_dir / "mapping_audit.xlsx",
        "quota_dashboard_path": run_dir / "quota_dashboard.xlsx",
    }


def load_cleaned_dataset(cleaned_data_path: str) -> pd.DataFrame:
    excel_path = Path(cleaned_data_path)
    pickle_path = excel_path.with_suffix(".pkl")
    if pickle_path.exists():
        return pd.read_pickle(pickle_path)
    return pd.read_excel(excel_path)
