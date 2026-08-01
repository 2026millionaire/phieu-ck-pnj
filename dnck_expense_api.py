# -*- coding: utf-8 -*-
"""Flask blueprint for ĐNCK thanh toán chi phí khác Phase 1."""

import json
import os
from pathlib import Path

from flask import Blueprint, jsonify, render_template, request, url_for

from dnck_expense_manifest import file_sha256, write_manifest
from dnck_expense_models import (
    FIXED_PAYMENT_METHOD,
    STATE_FILES_GENERATED,
    STATE_NEEDS_REVIEW,
    STATE_READY_TO_SUBMIT,
    canonical_json,
    normalize_source_payload,
    utc_now_iso,
)
from dnck_expense_source import SourceUnavailable, read_source
from dnck_expense_template import TemplateValidationError, build_template_tt_bytes
from dnck_expense_validation import has_blockers, result, source_sap_document, validate_source_and_supplement


SOURCE_MODE = "thanh_toan_chi_phi_khac"


def _json_error(code, message, status=400, extra=None):
    payload = {"ok": False, "code": code, "message": message}
    if extra:
        payload.update(extra)
    return jsonify(payload), status


def _admin_required(is_admin):
    if not is_admin():
        return _json_error("FORBIDDEN", "Bạn không có quyền thao tác ĐNCK thanh toán chi phí khác.", 403)
    return None


def _detail_json_from_hd(hd_rows):
    detail = []
    for row in hd_rows:
        note_parts = []
        if row.get("lookup_url"):
            note_parts.append(str(row.get("lookup_url")))
        if row.get("lookup_code"):
            note_parts.append("Mã: " + str(row.get("lookup_code")))
        detail.append({
            "label": row.get("description", ""),
            "amount": int(row.get("amount") or 0),
            "document": row.get("invoice_no_full", ""),
            "identity": row.get("supplier_tax_id", ""),
            "note": " | ".join(note_parts),
        })
    return detail


def _normalize_account_number(value):
    return "".join(ch for ch in str(value or "") if ch.isalnum()).upper()


def _lookup_payment_object(db, object_code):
    code = str(object_code or "").strip().upper()
    if not code:
        return None, [result("object_code", "error", "fail", "Thiếu mã đối tượng ở ND cột F.")]
    rows = db.execute("""
        SELECT *
        FROM dnck_object_lookup
        WHERE UPPER(object_code) = ?
        ORDER BY is_primary DESC, id ASC
    """, (code,)).fetchall()
    if not rows:
        return None, [result("object_lookup", "error", "fail", "Không tìm thấy mã đối tượng trong Dữ liệu đối tượng.", {"object_code": code})]

    primary_rows = [row for row in rows if row["is_primary"]]
    checks = []
    if len(primary_rows) != 1:
        checks.append(result(
            "object_primary_account",
            "error",
            "fail",
            "Mã đối tượng không có đúng một STK chính.",
            {"object_code": code, "primary_count": len(primary_rows)},
        ))
    primary = primary_rows[0] if primary_rows else rows[0]
    account_number = _normalize_account_number(primary["account_number"])
    if not account_number:
        checks.append(result("object_account_number", "error", "fail", "Dữ liệu đối tượng thiếu STK nhận CK.", {"object_code": code}))
    if not primary["bank"]:
        checks.append(result("object_bank", "error", "fail", "Dữ liệu đối tượng thiếu ngân hàng.", {"object_code": code}))
    if not primary["bank_eoffice_code"]:
        checks.append(result("object_bank_eoffice_code", "error", "fail", "Dữ liệu đối tượng thiếu mã ngân hàng eOffice.", {"object_code": code}))
    if not primary["identity_value"]:
        checks.append(result("object_identity", "error", "fail", "Dữ liệu đối tượng thiếu CCCD/MST.", {"object_code": code}))
    if not checks:
        checks.append(result("object_lookup", "info", "pass", "Đã tra được STK chính từ Dữ liệu đối tượng.", {"object_code": code}))
    return {
        "object_code": primary["object_code"],
        "object_name": primary["object_name"],
        "identity_value": primary["identity_value"],
        "account_number": account_number,
        "account_name": primary["object_name"],
        "bank": primary["bank"],
        "bank_eoffice_code": primary["bank_eoffice_code"],
        "is_primary": bool(primary["is_primary"]),
        "source": primary["source"],
        "account_count": len(rows),
    }, checks


def _validation_with_object(db, source, supplement, require_live=True):
    validation = validate_source_and_supplement(source, supplement, require_live=require_live)
    object_data, object_results = _lookup_payment_object(db, (source.get("nd") or {}).get("object_code"))
    source["object_lookup"] = object_data
    validation["results"].extend(object_results)
    validation["status"] = "needs_review" if has_blockers(validation["results"]) else "validated"
    return validation


def _insert_source_snapshot(db, draft_id, source):
    nd = source["nd"]
    db.execute("""
        INSERT INTO dnck_expense_source_nd
            (draft_id, spreadsheet_id, sheet_name, range_a1, read_at, source_hash,
             stt, plant, cost_center, expense_account, period, object_code,
             object_name, supplier_tax_id, nd_description, nd_amount, raw_values_json)
        VALUES (?, ?, 'ND', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        draft_id,
        source.get("spreadsheet_id", ""),
        source.get("nd_range", ""),
        source.get("read_at", ""),
        source.get("source_hash", ""),
        nd.get("stt", ""),
        nd.get("plant", ""),
        nd.get("cost_center", ""),
        nd.get("expense_account", ""),
        nd.get("period", ""),
        nd.get("object_code", ""),
        nd.get("object_name", ""),
        nd.get("supplier_tax_id", ""),
        nd.get("description", ""),
        int(nd.get("amount") or 0),
        canonical_json(nd),
    ))
    for row in source["hd"]:
        db.execute("""
            INSERT INTO dnck_expense_source_hd
                (draft_id, nd_stt, spreadsheet_id, sheet_name, row_number, range_a1,
                 invoice_date, hd_description, amount, invoice_no_full, supplier_tax_id,
                 lookup_code, lookup_url, document_type, expense_account, cost_center,
                 adjustment_ref, raw_values_json, source_hash)
            VALUES (?, ?, ?, 'HD', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            draft_id,
            nd.get("stt", ""),
            source.get("spreadsheet_id", ""),
            int(row.get("row_number") or 0),
            source.get("hd_range", ""),
            row.get("invoice_date", ""),
            row.get("description", ""),
            int(row.get("amount") or 0),
            row.get("invoice_no_full", ""),
            row.get("supplier_tax_id", ""),
            row.get("lookup_code", ""),
            row.get("lookup_url", ""),
            row.get("document_type", ""),
            row.get("expense_account", ""),
            row.get("cost_center", ""),
            row.get("adjustment_ref", ""),
            canonical_json(row),
            source.get("source_hash", ""),
        ))


def _insert_audit(db, draft_id, event_type, old_status, new_status, source, validation, user_id, message=""):
    db.execute("""
        INSERT INTO dnck_expense_audit
            (draft_id, event_type, old_status, new_status, spreadsheet_id, sheet_name,
             range_a1, nd_snapshot_json, hd_snapshot_json, validation_result_json,
             message, created_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        draft_id,
        event_type,
        old_status or "",
        new_status or "",
        source.get("spreadsheet_id", ""),
        "ND/HD",
        f"{source.get('nd_range', '')}; {source.get('hd_range', '')}",
        canonical_json(source.get("nd") or {}),
        canonical_json(source.get("hd") or []),
        canonical_json(validation.get("results") or []),
        message,
        user_id,
    ))


def _insert_output(db, draft_id, output_type, path, validation_status):
    return db.execute("""
        INSERT INTO dnck_expense_output
            (draft_id, output_type, production_file_name, output_path, sha256, validation_status)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        draft_id,
        output_type,
        os.path.basename(path),
        str(path),
        file_sha256(path),
        validation_status,
    ))


def create_dnck_expense_blueprint(get_db, current_user_id, is_admin, app_root, static_folder, get_settings=None):
    bp = Blueprint("dnck_expense", __name__)
    app_root = Path(app_root)
    static_folder = Path(static_folder)

    @bp.route("/dnck/expense")
    def expense_page():
        if not is_admin():
            return "Bạn không có quyền truy cập trang này.", 403
        return render_template("dnck_expense.html")

    @bp.route("/api/dnck-expense/config")
    def api_config():
        denied = _admin_required(is_admin)
        if denied:
            return denied
        settings = get_settings() if get_settings else {}
        ktpt_raw = settings.get("dnck_expense_ktpt_options", "[]")
        try:
            ktpt_options = json.loads(ktpt_raw)
        except (TypeError, ValueError):
            ktpt_options = [item.strip() for item in str(ktpt_raw or "").splitlines() if item.strip()]
        return jsonify({
            "ok": True,
            "payment_method": FIXED_PAYMENT_METHOD,
            "payment_methods": [FIXED_PAYMENT_METHOD],
            "ktpt_options": ktpt_options,
        })

    @bp.route("/api/dnck-expense/source")
    def api_source():
        denied = _admin_required(is_admin)
        if denied:
            return denied
        try:
            source = read_source(
                plant=request.args.get("plant", "1305"),
                period=request.args.get("period", ""),
                nd_stt=request.args.get("nd_stt", ""),
                fixture=request.args.get("fixture", ""),
            )
        except SourceUnavailable as exc:
            return _json_error("LIVE_SOURCE_UNAVAILABLE", str(exc), 503)
        source["suggested_sap_document"] = source_sap_document(source)
        validation = _validation_with_object(get_db(), source, {}, require_live=True)
        return jsonify({"ok": True, "source": source, "validation": validation})

    @bp.route("/api/dnck-expense/validate", methods=["POST"])
    def api_validate():
        denied = _admin_required(is_admin)
        if denied:
            return denied
        if not request.is_json:
            return _json_error("INVALID_REQUEST", "Yêu cầu phải là JSON.")
        data = request.get_json(silent=True) or {}
        source = normalize_source_payload(data.get("source") or {})
        supplement = data.get("supplement") or {}
        validation = _validation_with_object(get_db(), source, supplement, require_live=True)
        return jsonify({"ok": True, "status": validation["status"], "validation": validation})

    @bp.route("/api/dnck-expense/drafts", methods=["POST"])
    def api_create_draft():
        denied = _admin_required(is_admin)
        if denied:
            return denied
        if not request.is_json:
            return _json_error("INVALID_REQUEST", "Yêu cầu phải là JSON.")
        data = request.get_json(silent=True) or {}
        if not data.get("user_confirmed"):
            return _json_error("USER_CONFIRMATION_REQUIRED", "Cần user xác nhận preview cuối trước khi tạo draft.")

        source = normalize_source_payload(data.get("source") or {})
        supplement = data.get("supplement") or {}
        db = get_db()
        validation = _validation_with_object(db, source, supplement, require_live=True)
        status = STATE_NEEDS_REVIEW if has_blockers(validation["results"]) else STATE_FILES_GENERATED
        user_id = current_user_id()
        nd = source["nd"]
        hd_rows = source["hd"]
        object_data = source.get("object_lookup") or {}
        detail_json = _detail_json_from_hd(hd_rows)
        total = validation["total_hd"]
        sap_document = str(supplement.get("sap_document") or "").strip() or source_sap_document(source)

        cursor = db.execute("""
            INSERT INTO dnck_expense_draft
                (status, da_trinh, source_type, spreadsheet_id, nd_range, hd_range,
                 source_read_at, source_hash, user_confirmed_at, payment_method,
                 desired_payment_date, ktpt, sap_document, advance_amount,
                 request_content, internal_note, cost_group, validation_json, created_by)
            VALUES (?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)
        """, (
            status,
            source.get("source_type", ""),
            source.get("spreadsheet_id", ""),
            source.get("nd_range", ""),
            source.get("hd_range", ""),
            source.get("read_at", ""),
            source.get("source_hash", ""),
            utc_now_iso(),
            supplement.get("payment_method", ""),
            supplement.get("desired_payment_date", ""),
            supplement.get("ktpt", ""),
            sap_document,
            supplement.get("request_content", ""),
            supplement.get("internal_note", ""),
            supplement.get("cost_group", "Khác"),
            canonical_json(validation["results"]),
            user_id,
        ))
        draft_id = cursor.lastrowid
        _insert_source_snapshot(db, draft_id, source)

        dnck_cursor = db.execute("""
            INSERT INTO dnck
                (created_at, object_type, object_code, object_name, identity_value, phone,
                 account_number, account_name, bank, purpose, approval_level, expense_type,
                 cost_group, request_content, sap_document, amount, payment_tag,
                 approver_option, detail_json, hashtags_json, reference_note,
                 reference_links_json, cost_limit_ref, da_trinh, user_id, source_mode, source_id)
            VALUES (CURRENT_TIMESTAMP, 'vendor', ?, ?, ?, '', ?, ?, ?, ?, ?, '', ?,
                    ?, ?, ?, 'Thanh Toán Chi Phí Khác', 'none', ?, '[]', ?, '[]', '',
                    0, ?, ?, ?)
        """, (
            object_data.get("object_code") or nd.get("object_code", ""),
            object_data.get("object_name") or nd.get("object_name", ""),
            object_data.get("identity_value") or nd.get("supplier_tax_id", ""),
            object_data.get("account_number") or "",
            object_data.get("account_name") or object_data.get("object_name") or nd.get("object_name", ""),
            object_data.get("bank") or "",
            "Thanh toán cho nhà cung cấp",
            supplement.get("approval_level", "Cấp cửa hàng"),
            supplement.get("cost_group", "Khác"),
            supplement.get("request_content") or nd.get("description", ""),
            sap_document,
            total,
            json.dumps(detail_json, ensure_ascii=False),
            f"DNCK chi phí khác source_id={draft_id}; source_hash={source.get('source_hash', '')}",
            user_id,
            SOURCE_MODE,
            draft_id,
        ))
        dnck_id = dnck_cursor.lastrowid
        db.execute("UPDATE dnck_expense_draft SET dnck_id = ? WHERE id = ?", (dnck_id, draft_id))
        _insert_audit(db, draft_id, "create_draft", "", status, source, validation, user_id)

        outputs = []
        output_dir = app_root / "tmp" / "dnck_expense" / str(draft_id)
        output_dir.mkdir(parents=True, exist_ok=True)
        template_error = ""
        try:
            excel_blob = build_template_tt_bytes(static_folder / "template_tt.xlsx", hd_rows)
            excel_path = output_dir / f"DNCK_TTCP_{draft_id}_template_tt.xlsx"
            excel_path.write_bytes(excel_blob)
            _insert_output(db, draft_id, "excel_tt", excel_path, status)
            outputs.append({"output_type": "excel_tt", "output_path": str(excel_path), "sha256": file_sha256(excel_path)})
        except TemplateValidationError as exc:
            template_error = str(exc)
            status = STATE_NEEDS_REVIEW
            db.execute(
                "UPDATE dnck_expense_draft SET status = ?, validation_json = ? WHERE id = ?",
                (status, canonical_json(validation["results"] + [{
                    "rule_key": "template_tt",
                    "severity": "error",
                    "status": "fail",
                    "message": template_error,
                    "evidence": {},
                }]), draft_id),
            )

        manifest_path = output_dir / f"DNCK_TTCP_{draft_id}_manifest.md"
        manifest_written, manifest_hash = write_manifest(manifest_path, draft_id, source, validation, outputs)
        _insert_output(db, draft_id, "manifest_md", manifest_written, status)
        outputs.append({"output_type": "manifest_md", "output_path": manifest_written, "sha256": manifest_hash})
        db.commit()

        return jsonify({
            "ok": True,
            "id": draft_id,
            "dnck_id": dnck_id,
            "status": status,
            "da_trinh": 0,
            "template_error": template_error,
            "outputs": outputs,
            "eoffice_url": url_for("eoffice_dnck_page", dnck_id=dnck_id),
            "validation": validation,
        })

    @bp.route("/api/dnck-expense/<int:draft_id>/mark-ready", methods=["POST"])
    def api_mark_ready(draft_id):
        denied = _admin_required(is_admin)
        if denied:
            return denied
        db = get_db()
        draft = db.execute("SELECT * FROM dnck_expense_draft WHERE id = ?", (draft_id,)).fetchone()
        if not draft:
            return _json_error("NOT_FOUND", "Không tìm thấy draft.", 404)
        if draft["status"] != STATE_FILES_GENERATED:
            return _json_error("NOT_READY", "Draft chưa đạt điều kiện files_generated/validated.", 409)
        if not draft["source_read_at"] or draft["source_type"] != "google_sheet_live":
            return _json_error("LIVE_SOURCE_REQUIRED", "ready_to_submit yêu cầu nguồn live vừa đọc lại.", 409)
        db.execute(
            "UPDATE dnck_expense_draft SET status = ?, da_trinh = 0, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (STATE_READY_TO_SUBMIT, draft_id),
        )
        db.commit()
        return jsonify({"ok": True, "id": draft_id, "status": STATE_READY_TO_SUBMIT, "da_trinh": 0})

    return bp
