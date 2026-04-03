def build_respondent_id(upload_run_id: int, source_row_number: int) -> str:
    return f"U{upload_run_id:04d}-R{source_row_number:05d}"
