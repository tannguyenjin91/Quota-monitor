from datetime import datetime

from flask_sqlalchemy import SQLAlchemy


db = SQLAlchemy()


class UploadRun(db.Model):
    __tablename__ = "upload_runs"

    id = db.Column(db.Integer, primary_key=True)
    original_file_name = db.Column(db.String(255), nullable=False)
    stored_file_name = db.Column(db.String(255), nullable=False)
    uploaded_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    processed_at = db.Column(db.DateTime, nullable=True)
    note = db.Column(db.Text, nullable=True)
    processing_status = db.Column(db.String(50), default="pending", nullable=False)
    error_message = db.Column(db.Text, nullable=True)

    question_sheet_name = db.Column(db.String(255), nullable=True)
    data_sheet_name = db.Column(db.String(255), nullable=True)
    raw_row_count = db.Column(db.Integer, default=0, nullable=False)
    accepted_row_count = db.Column(db.Integer, default=0, nullable=False)
    rejected_row_count = db.Column(db.Integer, default=0, nullable=False)
    invalid_accepted_row_count = db.Column(db.Integer, default=0, nullable=False)
    valid_quota_count = db.Column(db.Integer, default=0, nullable=False)
    overall_completion_pct = db.Column(db.Float, default=0, nullable=False)
    parsed_variable_count = db.Column(db.Integer, default=0, nullable=False)
    sa_variable_count = db.Column(db.Integer, default=0, nullable=False)
    ma_variable_count = db.Column(db.Integer, default=0, nullable=False)
    ft_variable_count = db.Column(db.Integer, default=0, nullable=False)
    tu_choi_found = db.Column(db.Boolean, default=False, nullable=False)

    cleaned_data_path = db.Column(db.String(500), nullable=True)
    exceptions_path = db.Column(db.String(500), nullable=True)
    quota_summary_path = db.Column(db.String(500), nullable=True)
    question_dictionary_path = db.Column(db.String(500), nullable=True)
    mapping_audit_path = db.Column(db.String(500), nullable=True)
    quota_dashboard_path = db.Column(db.String(500), nullable=True)

    parsed_variables = db.relationship(
        "ParsedVariable",
        backref="upload_run",
        lazy=True,
        cascade="all, delete-orphan",
    )
    saved_quota_configs = db.relationship(
        "SavedQuotaConfig",
        backref="upload_run",
        lazy=True,
        cascade="all, delete-orphan",
    )


class ParsedVariable(db.Model):
    __tablename__ = "parsed_variables"

    id = db.Column(db.Integer, primary_key=True)
    upload_run_id = db.Column(db.Integer, db.ForeignKey("upload_runs.id"), nullable=False, index=True)
    variable_code = db.Column(db.String(255), nullable=False)
    question_label = db.Column(db.Text, nullable=True)
    question_type = db.Column(db.String(50), nullable=True)
    available_codes = db.Column(db.JSON, nullable=True)
    available_labels = db.Column(db.JSON, nullable=True)
    distinct_count_in_data = db.Column(db.Integer, default=0, nullable=False)
    quota_eligible = db.Column(db.Boolean, default=False, nullable=False)
    category_sort_mode = db.Column(db.String(50), default="code_order", nullable=False)
    source_sheet = db.Column(db.String(100), nullable=False)


class SavedQuotaConfig(db.Model):
    __tablename__ = "saved_quota_configs"

    id = db.Column(db.Integer, primary_key=True)
    upload_run_id = db.Column(db.Integer, db.ForeignKey("upload_runs.id"), nullable=False, index=True)
    report_type = db.Column(db.String(50), nullable=False, default="simple_crosstab")
    selected_horizontal_variable = db.Column(db.String(255), nullable=False)
    selected_vertical_variable = db.Column(db.String(255), nullable=False)
    selected_display_mode = db.Column(db.String(50), nullable=False)
    selected_percent_mode = db.Column(db.String(50), nullable=True)
    selected_filters_json = db.Column(db.JSON, nullable=True)
    banner_row_variables_json = db.Column(db.JSON, nullable=True)
    banner_column_variables_json = db.Column(db.JSON, nullable=True)
    banner_tree_json = db.Column(db.JSON, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
