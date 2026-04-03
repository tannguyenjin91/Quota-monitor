from pathlib import Path
import os

from flask import Flask
from dotenv import load_dotenv
from sqlalchemy import inspect, text
from sqlalchemy.exc import OperationalError

from models import db
from routes.web import web_bp
from services.file_service import ensure_directories


def create_app() -> Flask:
    base_dir = Path(__file__).resolve().parent
    load_dotenv(base_dir / ".env")
    app = Flask(__name__)

    app.config["BASE_DIR"] = str(base_dir)
    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "quota-dashboard-dev-key")
    app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv(
        "DATABASE_URL",
        f"sqlite:///{base_dir / 'instance' / 'app.db'}",
    )
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["UPLOAD_FOLDER"] = str(base_dir / "uploads")
    app.config["OUTPUT_FOLDER"] = str(base_dir / "outputs")
    app.config["MAPPINGS_PATH"] = str(base_dir / "config" / "mappings.yaml")
    app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024
    app.config["PORT"] = int(os.getenv("PORT", "5000"))
    app.config["DEBUG"] = os.getenv("FLASK_DEBUG", "0") == "1"

    ensure_directories(
        [
            Path(app.config["UPLOAD_FOLDER"]),
            Path(app.config["OUTPUT_FOLDER"]),
            base_dir / "instance",
        ]
    )

    db.init_app(app)
    app.register_blueprint(web_bp)

    with app.app_context():
        try:
            db.create_all()
        except OperationalError as exc:  # pragma: no cover - local sqlite compatibility guard
            if "already exists" not in str(exc).lower():
                raise
        ensure_upload_run_columns()
        ensure_saved_quota_config_columns()

    return app


def ensure_upload_run_columns():
    inspector = inspect(db.engine)
    if "upload_runs" not in inspector.get_table_names():
        return

    existing_columns = {column["name"] for column in inspector.get_columns("upload_runs")}
    required_columns = {
        "question_sheet_name": "ALTER TABLE upload_runs ADD COLUMN question_sheet_name VARCHAR(255)",
        "data_sheet_name": "ALTER TABLE upload_runs ADD COLUMN data_sheet_name VARCHAR(255)",
        "parsed_variable_count": "ALTER TABLE upload_runs ADD COLUMN parsed_variable_count INTEGER DEFAULT 0 NOT NULL",
        "sa_variable_count": "ALTER TABLE upload_runs ADD COLUMN sa_variable_count INTEGER DEFAULT 0 NOT NULL",
        "ma_variable_count": "ALTER TABLE upload_runs ADD COLUMN ma_variable_count INTEGER DEFAULT 0 NOT NULL",
        "ft_variable_count": "ALTER TABLE upload_runs ADD COLUMN ft_variable_count INTEGER DEFAULT 0 NOT NULL",
        "tu_choi_found": "ALTER TABLE upload_runs ADD COLUMN tu_choi_found BOOLEAN DEFAULT 0 NOT NULL",
        "question_dictionary_path": "ALTER TABLE upload_runs ADD COLUMN question_dictionary_path VARCHAR(500)",
        "quota_dashboard_path": "ALTER TABLE upload_runs ADD COLUMN quota_dashboard_path VARCHAR(500)",
    }

    for column_name, ddl in required_columns.items():
        if column_name not in existing_columns:
            db.session.execute(text(ddl))
    db.session.commit()


def ensure_saved_quota_config_columns():
    inspector = inspect(db.engine)
    if "saved_quota_configs" not in inspector.get_table_names():
        return

    existing_columns = {column["name"] for column in inspector.get_columns("saved_quota_configs")}
    required_columns = {
        "selected_filters_json": "ALTER TABLE saved_quota_configs ADD COLUMN selected_filters_json JSON",
        "report_type": "ALTER TABLE saved_quota_configs ADD COLUMN report_type VARCHAR(50) DEFAULT 'simple_crosstab' NOT NULL",
        "banner_row_variables_json": "ALTER TABLE saved_quota_configs ADD COLUMN banner_row_variables_json JSON",
        "banner_column_variables_json": "ALTER TABLE saved_quota_configs ADD COLUMN banner_column_variables_json JSON",
        "banner_tree_json": "ALTER TABLE saved_quota_configs ADD COLUMN banner_tree_json JSON",
    }
    for column_name, ddl in required_columns.items():
        if column_name not in existing_columns:
            db.session.execute(text(ddl))
    db.session.commit()
