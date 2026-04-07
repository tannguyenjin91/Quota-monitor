from datetime import datetime
import json
from pathlib import Path
import shutil

import pandas as pd
import yaml
from flask import Blueprint, current_app, flash, jsonify, redirect, render_template, request, send_file, url_for

from models import ParsedVariable, SavedQuotaConfig, UploadRun, db
try:
    from services.ai_assist_service import suggest_quota_setup
except ImportError:
    suggest_quota_setup = None
from services.export_service import export_parse_outputs, export_quota_dashboard
from services.file_service import allowed_file, load_cleaned_dataset, save_upload
from services.preview_service import build_decoded_preview
from services.question_parser_service import parse_question_sheet
from services.quota_builder_service import apply_additional_filters, build_banner_table, build_drilldown_table, build_quota_table
from services.raw_data_parser_service import parse_data_sheet
from services.sheet_detection_service import detect_workbook_sheets
from services.variable_catalog_service import build_variable_catalog, filter_variable_catalog


web_bp = Blueprint("web", __name__)


def wants_json_response() -> bool:
    return request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.form.get("ajax") == "1"


@web_bp.route("/")
def index():
    return redirect(url_for("web.upload"))


@web_bp.route("/upload", methods=["GET", "POST"])
def upload():
    if request.method == "POST":
        uploaded_file = request.files.get("file")
        note = request.form.get("note", "").strip() or None
        if not uploaded_file or uploaded_file.filename == "":
            if wants_json_response():
                return jsonify({"ok": False, "error": "Select an .xlsx workbook to upload."}), 400
            flash("Select an .xlsx workbook to upload.", "danger")
            return redirect(url_for("web.upload"))
        if not allowed_file(uploaded_file.filename):
            if wants_json_response():
                return jsonify({"ok": False, "error": "Only .xlsx files are supported."}), 400
            flash("Only .xlsx files are supported.", "danger")
            return redirect(url_for("web.upload"))

        upload_run = UploadRun(
            original_file_name=uploaded_file.filename,
            stored_file_name="pending.xlsx",
            note=note,
            processing_status="processing",
        )
        db.session.add(upload_run)
        db.session.commit()

        try:
            upload_run.stored_file_name = save_upload(uploaded_file, upload_run.id, current_app.config["UPLOAD_FOLDER"])
            db.session.commit()

            parse_result = process_workbook(upload_run)
            export_paths = export_parse_outputs(upload_run.id, parse_result, current_app.config)

            upload_run.processed_at = datetime.utcnow()
            upload_run.processing_status = "completed"
            upload_run.question_sheet_name = parse_result["sheet_detection"]["question_sheet_name"]
            upload_run.data_sheet_name = parse_result["sheet_detection"]["data_sheet_name"]
            upload_run.raw_row_count = parse_result["raw_row_count"]
            upload_run.accepted_row_count = parse_result["accepted_row_count"]
            upload_run.rejected_row_count = parse_result["rejected_row_count"]
            upload_run.parsed_variable_count = len(parse_result["variable_catalog"])
            upload_run.sa_variable_count = parse_result["counts_by_type"]["SA"]
            upload_run.ma_variable_count = parse_result["counts_by_type"]["MA"]
            upload_run.ft_variable_count = parse_result["counts_by_type"]["FT"]
            upload_run.tu_choi_found = parse_result["rejection_column_found"]
            upload_run.cleaned_data_path = export_paths["cleaned_data_path"]
            upload_run.question_dictionary_path = export_paths["question_dictionary_path"]
            upload_run.mapping_audit_path = export_paths["mapping_audit_path"]

            ParsedVariable.query.filter_by(upload_run_id=upload_run.id).delete()
            for entry in parse_result["variable_catalog"]:
                db.session.add(ParsedVariable(upload_run_id=upload_run.id, **entry))

            db.session.commit()
            if wants_json_response():
                return jsonify({"ok": True, "redirect_url": url_for("web.upload", upload_id=upload_run.id), "upload_id": upload_run.id})
            return redirect(url_for("web.upload", upload_id=upload_run.id))
        except Exception as exc:  # pragma: no cover
            upload_run.processed_at = datetime.utcnow()
            upload_run.processing_status = "failed"
            upload_run.error_message = str(exc)
            db.session.commit()
            if wants_json_response():
                return jsonify({"ok": False, "error": f"Upload failed: {exc}"}), 500
            flash(f"Upload failed: {exc}", "danger")
            return redirect(url_for("web.upload"))

    upload_id = request.args.get("upload_id", type=int)
    current_upload = UploadRun.query.get(upload_id) if upload_id else None
    uploads = UploadRun.query.order_by(UploadRun.id.desc()).limit(10).all()
    preview = None
    quota_eligible_count = 0
    if current_upload and current_upload.cleaned_data_path:
        cleaned_df = load_cleaned_dataset(current_upload.cleaned_data_path)
        variable_rows = [to_variable_dict(item) for item in ParsedVariable.query.filter_by(upload_run_id=current_upload.id).all()]
        quota_eligible_count = len([item for item in variable_rows if item["quota_eligible"]])
        preview = build_decoded_preview(cleaned_df, variable_rows, max_rows=10)
    return render_template("upload.html", uploads=uploads, current_upload=current_upload, preview=preview, quota_eligible_count=quota_eligible_count)


@web_bp.route("/upload/clear-history", methods=["POST"])
def clear_upload_history():
    uploads = UploadRun.query.order_by(UploadRun.id.desc()).all()
    upload_folder = Path(current_app.config["UPLOAD_FOLDER"])
    output_folder = Path(current_app.config["OUTPUT_FOLDER"])

    for upload in uploads:
        SavedQuotaConfig.query.filter_by(upload_run_id=upload.id).delete()
        ParsedVariable.query.filter_by(upload_run_id=upload.id).delete()

        stored_file = upload_folder / (upload.stored_file_name or "")
        if upload.stored_file_name and stored_file.exists():
            stored_file.unlink()

        upload_output_dir = output_folder / f"upload_{upload.id}"
        if upload_output_dir.exists():
            shutil.rmtree(upload_output_dir, ignore_errors=True)

        db.session.delete(upload)

    db.session.commit()
    flash("Old upload history and cached output files were cleared.", "success")
    return redirect(url_for("web.upload"))


@web_bp.route("/configure-quota/<int:upload_id>", methods=["GET", "POST"])
def configure_quota(upload_id):
    upload_run = UploadRun.query.get_or_404(upload_id)
    mappings = load_mappings(current_app.config)
    all_variables = [to_variable_dict(item) for item in ParsedVariable.query.filter_by(upload_run_id=upload_id).all()]
    selector_variables = build_selector_variables(all_variables)
    existing_config = SavedQuotaConfig.query.filter_by(upload_run_id=upload_id).order_by(SavedQuotaConfig.id.desc()).first()
    selected_horizontal = request.args.get("selected_horizontal") or (existing_config.selected_horizontal_variable if existing_config else "")
    selected_vertical = request.args.get("selected_vertical") or (existing_config.selected_vertical_variable if existing_config else "")
    existing_filters = existing_config.selected_filters_json if existing_config and existing_config.selected_filters_json else []
    selected_row_variables = list(existing_config.banner_row_variables_json or ([selected_vertical] if selected_vertical else [])) if existing_config else ([selected_vertical] if selected_vertical else [])
    selected_banner_variables = list(existing_config.banner_column_variables_json or ([selected_horizontal] if selected_horizontal else [])) if existing_config else ([selected_horizontal] if selected_horizontal else [])
    selected_banner_tree = list(existing_config.banner_tree_json or [{"variable_code": code, "label": code} for code in selected_banner_variables]) if existing_config else [{"variable_code": code, "label": code} for code in selected_banner_variables]
    selected_banner_layout_mode = "tree" if selected_banner_tree else "flat"
    selected_display_mode = existing_config.selected_display_mode if existing_config else mappings["default_display_mode"]
    selected_percent_mode = existing_config.selected_percent_mode if existing_config and existing_config.selected_percent_mode else mappings["default_percent_mode"]
    ai_prompt = ""
    ai_suggestion = None

    if request.method == "POST":
        action = request.form.get("action", "save")
        banner_rows = [item.strip() for item in request.form.getlist("banner_row_variables") if item.strip()]
        banner_columns = [item.strip() for item in request.form.getlist("banner_column_variables") if item.strip()]
        banner_flat_columns = [item.strip() for item in request.form.getlist("banner_flat_variables") if item.strip()]
        raw_banner_tree = request.form.get("banner_tree_json", "").strip()
        parsed_banner_tree = []
        if raw_banner_tree:
            try:
                tree_data = json.loads(raw_banner_tree)
                if isinstance(tree_data, list):
                    parsed_banner_tree = [item for item in tree_data if isinstance(item, dict) and item.get("variable_code")]
            except json.JSONDecodeError:
                parsed_banner_tree = []
        display_mode = request.form.get("display_mode", mappings["default_display_mode"])
        percent_mode = request.form.get("percent_mode", mappings["default_percent_mode"])
        selected_filters = parse_selected_filters(request.form)
        if banner_rows:
            selected_row_variables = banner_rows
        # Combine tree + flat into banner variables
        tree_codes = [item["variable_code"] for item in parsed_banner_tree] if parsed_banner_tree else []
        all_banner_codes = tree_codes + [c for c in banner_flat_columns if c not in tree_codes]
        if all_banner_codes:
            selected_banner_variables = all_banner_codes
        elif banner_columns:
            selected_banner_variables = banner_columns
        selected_banner_tree = parsed_banner_tree
        banner_layout_mode = "tree" if parsed_banner_tree else "flat"
        selected_vertical = selected_row_variables[0] if selected_row_variables else selected_vertical
        selected_horizontal = selected_banner_variables[0] if selected_banner_variables else selected_horizontal
        selected_banner_layout_mode = banner_layout_mode
        selected_display_mode = display_mode
        selected_percent_mode = percent_mode
        existing_filters = selected_filters or existing_filters

        if action == "ai_suggest":
            ai_prompt = request.form.get("ai_prompt", "").strip()
            ai_suggestion = suggest_quota_setup(
                ai_prompt,
                selector_variables,
                display_mode=display_mode,
                percent_mode=percent_mode,
            )
            if ai_suggestion["horizontal_variable"]:
                selected_banner_variables = [ai_suggestion["horizontal_variable"]]
                selected_horizontal = ai_suggestion["horizontal_variable"]
            if ai_suggestion["vertical_variable"]:
                selected_row_variables = [ai_suggestion["vertical_variable"]]
                selected_vertical = ai_suggestion["vertical_variable"]
            existing_filters = ai_suggestion["selected_filters"] or existing_filters
            if selected_banner_variables or selected_row_variables or ai_suggestion["selected_filters"]:
                flash(f"Suggestion ready via {ai_suggestion['engine']}. Review the proposal below before continuing.", "info")
            else:
                flash("No strong suggestion was found from the prompt. Try describing the table in more detail.", "warning")
            return render_template(
                "configure_quota.html",
                upload=upload_run,
                existing_config=existing_config,
                accepted_base=upload_run.accepted_row_count,
                variable_lookup={item["variable_code"]: item for item in all_variables},
                selected_horizontal=selected_horizontal,
                selected_vertical=selected_vertical,
                selected_row_variables=selected_row_variables,
                selected_banner_variables=selected_banner_variables,
                selected_banner_tree=selected_banner_tree,
                selected_banner_layout_mode=selected_banner_layout_mode,
                selector_variables=selector_variables,
                existing_filters=existing_filters,
                selected_display_mode=selected_display_mode,
                selected_percent_mode=selected_percent_mode,
                ai_prompt=ai_prompt,
                ai_suggestion=ai_suggestion,
            )

        if not selected_row_variables or not selected_banner_variables:
            flash("Choose at least one row question and one banner column group.", "danger")
            return redirect(url_for("web.configure_quota", upload_id=upload_id))

        saved_config = SavedQuotaConfig(
            upload_run_id=upload_id,
            report_type="banner_table",
            selected_horizontal_variable=selected_banner_variables[0],
            selected_vertical_variable=selected_row_variables[0],
            selected_display_mode=display_mode,
            selected_percent_mode=percent_mode,
            selected_filters_json=selected_filters,
            banner_row_variables_json=selected_row_variables,
            banner_column_variables_json=selected_banner_variables,
            banner_tree_json=selected_banner_tree,
        )
        db.session.add(saved_config)
        db.session.commit()
        return redirect(url_for("web.dashboard", upload_id=upload_id))

    accepted_base = upload_run.accepted_row_count
    return render_template(
        "configure_quota.html",
        upload=upload_run,
        existing_config=existing_config,
        accepted_base=accepted_base,
        variable_lookup={item["variable_code"]: item for item in all_variables},
        selected_horizontal=selected_horizontal,
        selected_vertical=selected_vertical,
        selected_row_variables=selected_row_variables,
        selected_banner_variables=selected_banner_variables,
        selected_banner_tree=selected_banner_tree,
        selected_banner_layout_mode=selected_banner_layout_mode,
        selector_variables=selector_variables,
        existing_filters=existing_filters,
        selected_display_mode=selected_display_mode,
        selected_percent_mode=selected_percent_mode,
        ai_prompt=ai_prompt,
        ai_suggestion=ai_suggestion,
    )


@web_bp.route("/dashboard/<int:upload_id>")
def dashboard(upload_id):
    upload_run = UploadRun.query.get_or_404(upload_id)
    saved_config = SavedQuotaConfig.query.filter_by(upload_run_id=upload_id).order_by(SavedQuotaConfig.id.desc()).first()
    if not saved_config:
        flash("Configure a quota layout before opening the dashboard.", "warning")
        return redirect(url_for("web.configure_quota", upload_id=upload_id))

    cleaned_df = load_cleaned_dataset(upload_run.cleaned_data_path)
    cleaned_df = apply_additional_filters(cleaned_df, saved_config.selected_filters_json)
    variables = ParsedVariable.query.filter_by(upload_run_id=upload_id).all()
    variable_lookup = {variable.variable_code: variable for variable in variables}
    variable_catalog_lookup = {key: to_variable_dict(value) for key, value in variable_lookup.items()}

    percent_mode = saved_config.selected_percent_mode or load_mappings(current_app.config)["default_percent_mode"]
    report_type = saved_config.report_type or "simple_crosstab"
    if report_type == "banner_table":
        row_variables = list(saved_config.banner_row_variables_json or ([saved_config.selected_vertical_variable] if saved_config.selected_vertical_variable else []))
        banner_variables = list(saved_config.banner_column_variables_json or ([saved_config.selected_horizontal_variable] if saved_config.selected_horizontal_variable else []))
        banner_tree = list(saved_config.banner_tree_json or [])
        table_payload = build_banner_table(
            cleaned_df=cleaned_df,
            variable_catalog_lookup=variable_catalog_lookup,
            row_variables=row_variables,
            banner_variables=banner_variables,
            banner_tree=banner_tree,
            banner_layout_mode="tree" if banner_tree else "flat",
            display_mode=saved_config.selected_display_mode,
            percent_mode=percent_mode,
        )
    else:
        table_payload = build_quota_table(
            cleaned_df=cleaned_df,
            variable_catalog_lookup=variable_catalog_lookup,
            row_variable=saved_config.selected_vertical_variable,
            column_variable=saved_config.selected_horizontal_variable,
            display_mode=saved_config.selected_display_mode,
            percent_mode=percent_mode,
        )

    drill_variable_choices = [
        item
        for item in variable_catalog_lookup.values()
        if item["variable_code"] not in {saved_config.selected_vertical_variable, saved_config.selected_horizontal_variable}
    ]
    default_drill_variable = choose_default_drill_variable(drill_variable_choices)
    drill_variable = request.args.get("drill_variable", "").strip() or default_drill_variable
    if drill_variable and drill_variable not in {item["variable_code"] for item in drill_variable_choices}:
        drill_variable = default_drill_variable
    drill_row_value = request.args.get("drill_row", "").strip()
    drill_column_value = request.args.get("drill_column", "").strip()
    drilldown = None
    if report_type != "banner_table" and drill_variable and drill_row_value and drill_column_value:
        drilldown = build_drilldown_table(
            cleaned_df=cleaned_df,
            variable_catalog_lookup=variable_catalog_lookup,
            row_variable=saved_config.selected_vertical_variable,
            row_value=drill_row_value,
            column_variable=saved_config.selected_horizontal_variable,
            column_value=drill_column_value,
            breakdown_variable=drill_variable,
            display_mode=saved_config.selected_display_mode,
            percent_mode=saved_config.selected_percent_mode or load_mappings(current_app.config)["default_percent_mode"],
        )

    dashboard_payload = {
        **table_payload,
        "source_file_name": upload_run.original_file_name,
        "processed_at": upload_run.processed_at,
        "horizontal_code": saved_config.selected_horizontal_variable,
        "horizontal_label": variable_lookup[saved_config.selected_horizontal_variable].question_label,
        "vertical_code": saved_config.selected_vertical_variable,
        "vertical_label": variable_lookup[saved_config.selected_vertical_variable].question_label,
        "report_type": report_type,
        "selected_row_variables": list(saved_config.banner_row_variables_json or ([saved_config.selected_vertical_variable] if saved_config.selected_vertical_variable else [])),
        "selected_banner_variables": list(saved_config.banner_column_variables_json or ([saved_config.selected_horizontal_variable] if saved_config.selected_horizontal_variable else [])),
        "selected_banner_tree": list(saved_config.banner_tree_json or []),
        "selected_banner_layout_mode": "tree" if (saved_config.banner_tree_json or []) else "flat",
        "display_mode": saved_config.selected_display_mode,
        "percent_mode": percent_mode,
        "selected_filters": saved_config.selected_filters_json or [],
        "drilldown": drilldown,
        "drill_variable": drill_variable,
        "default_drill_variable": default_drill_variable,
        "drill_row_value": drill_row_value,
        "drill_column_value": drill_column_value,
    }
    upload_run.quota_dashboard_path = export_quota_dashboard(upload_id, dashboard_payload, current_app.config)
    db.session.commit()

    return render_template(
        "dashboard.html",
        upload=upload_run,
        config=saved_config,
        dashboard=dashboard_payload,
        drill_variable_choices=drill_variable_choices,
    )


@web_bp.route("/history")
def history():
    uploads = UploadRun.query.order_by(UploadRun.id.desc()).all()
    return render_template("history.html", uploads=uploads)


@web_bp.route("/exports/<int:upload_id>/<string:export_type>")
def download_export(upload_id, export_type):
    upload_run = UploadRun.query.get_or_404(upload_id)
    export_map = {
        "cleaned_data": upload_run.cleaned_data_path,
        "question_dictionary": upload_run.question_dictionary_path,
        "mapping_audit": upload_run.mapping_audit_path,
        "quota_dashboard": upload_run.quota_dashboard_path,
    }
    file_path = export_map.get(export_type)
    if not file_path or not Path(file_path).exists():
        flash("Requested export is not available.", "warning")
        return redirect(url_for("web.dashboard", upload_id=upload_id))
    return send_file(file_path, as_attachment=True)


def process_workbook(upload_run: UploadRun):
    mappings = load_mappings(current_app.config)
    file_path = str(Path(current_app.config["UPLOAD_FOLDER"]) / upload_run.stored_file_name)

    sheet_detection = detect_workbook_sheets(file_path, mappings)
    question_result = parse_question_sheet(file_path, sheet_detection["question_sheet_name"], mappings)
    data_result = parse_data_sheet(
        file_path=file_path,
        sheet_name=sheet_detection["data_sheet_name"],
        upload_run_id=upload_run.id,
        source_file_name=upload_run.original_file_name,
        mappings=mappings,
        question_metadata=question_result["question_metadata"],
    )
    variable_result = build_variable_catalog(
        data_headers=data_result["data_headers"],
        question_metadata=question_result["question_metadata"],
        respondent_data_clean=data_result["respondent_data_clean"],
        mappings=mappings,
        data_sheet_name=sheet_detection["data_sheet_name"],
    )

    respondent_data_clean = data_result["respondent_data_clean"]
    return {
        "sheet_detection": sheet_detection,
        "question_dictionary": question_result["question_dictionary"],
        "question_metadata": question_result["question_metadata"],
        "respondent_data_clean": respondent_data_clean,
        "variable_catalog": variable_result["variable_catalog"],
        "counts_by_type": variable_result["counts_by_type"],
        "rejection_column_found": data_result["rejection_column_found"],
        "raw_row_count": len(respondent_data_clean),
        "accepted_row_count": int(respondent_data_clean["is_accepted_for_quota"].sum()),
        "rejected_row_count": int(respondent_data_clean["is_rejected"].sum()),
    }


def load_mappings(app_config):
    with open(app_config["MAPPINGS_PATH"], "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def to_variable_dict(parsed_variable: ParsedVariable):
    return {
        "variable_code": parsed_variable.variable_code,
        "question_label": parsed_variable.question_label,
        "question_type": parsed_variable.question_type,
        "available_codes": parsed_variable.available_codes or [],
        "available_labels": parsed_variable.available_labels or [],
        "distinct_count_in_data": parsed_variable.distinct_count_in_data,
        "quota_eligible": parsed_variable.quota_eligible,
        "category_sort_mode": parsed_variable.category_sort_mode,
        "source_sheet": parsed_variable.source_sheet,
    }

def parse_selected_filters(form_data):
    filters = []
    for index in range(1, 4):
        variable_code = form_data.get(f"filter_variable_{index}", "").strip()
        selected_values = form_data.getlist(f"filter_values_{index}")
        selected_values = [v.strip() for v in selected_values if v.strip()]
        if variable_code and selected_values:
            filters.append({"variable_code": variable_code, "values": selected_values})
    return filters


def build_selector_variables(all_variables):
    """Filter variables for Step 2 selector: hide MA sub-variables, show MA_GROUPs."""
    import re

    ma_group_codes = {item["variable_code"] for item in all_variables if item["question_type"] == "MA_GROUP"}
    all_codes = {item["variable_code"] for item in all_variables}
    ma_sub_codes = set()
    for item in all_variables:
        if item["question_type"] != "MA":
            continue
        code = item["variable_code"]
        match = re.match(r"^(.+?)_[^_]+$", code)
        if not match:
            continue
        base_code = match.group(1)
        if base_code in ma_group_codes or base_code in all_codes:
            ma_sub_codes.add(code)
    return [item for item in all_variables if item["variable_code"] not in ma_sub_codes]


def choose_default_drill_variable(variable_choices):
    if not variable_choices:
        return ""
    preferred_codes = ["S2", "Q9"]
    for preferred_code in preferred_codes:
        for item in variable_choices:
            if item["variable_code"] == preferred_code:
                return preferred_code
    return variable_choices[0]["variable_code"]
