# -*- coding: utf-8 -*-
"""Routes và nghiệp vụ của phân hệ ĐỀ XUẤT."""

import json
import re
from datetime import datetime

from flask import Blueprint, abort, jsonify, render_template, request


PLACEHOLDER_RE = re.compile(r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}")
VALID_STATUSES = {"draft", "completed", "exported"}


def _json_loads(value, default):
    try:
        return json.loads(value or "")
    except (TypeError, ValueError):
        return default


def _template_dict(row):
    if row is None:
        return None
    result = dict(row)
    result["fields"] = _json_loads(result.pop("fields_json", "[]"), [])
    return result


def _proposal_dict(row):
    if row is None:
        return None
    result = dict(row)
    result["form_data"] = _json_loads(result.pop("form_data_json", "{}"), {})
    return result


def render_pattern(pattern, values, empty_value="……"):
    """Điền biến mẫu; giữ dấu chấm cho trường chưa có dữ liệu."""
    def replace(match):
        value = values.get(match.group(1))
        if value is None or str(value).strip() == "":
            return empty_value
        return str(value).strip()

    return PLACEHOLDER_RE.sub(replace, pattern or "")


def _normalize_amount(value):
    cleaned = re.sub(r"[^\d-]", "", str(value or ""))
    try:
        return max(0, int(cleaned))
    except ValueError:
        return 0


def create_de_xuat_blueprint(get_db, current_user_id, is_admin, send_pdf, pdf_filename):
    bp = Blueprint(
        "de_xuat",
        __name__,
        url_prefix="/de-xuat",
        template_folder="../templates/de_xuat",
        static_folder="../static/de_xuat",
        static_url_path="/assets",
    )

    def get_template(template_id=None, slug=None):
        db = get_db()
        if template_id is not None:
            row = db.execute(
                "SELECT * FROM de_xuat_templates WHERE id = ? AND active = 1",
                (template_id,),
            ).fetchone()
        else:
            row = db.execute(
                "SELECT * FROM de_xuat_templates WHERE slug = ? AND active = 1",
                (slug,),
            ).fetchone()
        return _template_dict(row)

    def get_proposal(proposal_id):
        row = get_db().execute("""
            SELECT d.*, t.name AS template_name, t.slug AS template_slug,
                   t.fields_json, t.title_template, t.content_template, t.reason_template
            FROM de_xuat d
            JOIN de_xuat_templates t ON t.id = d.template_id
            WHERE d.id = ?
        """, (proposal_id,)).fetchone()
        proposal = _proposal_dict(row)
        if proposal is None:
            abort(404)
        if proposal["user_id"] != current_user_id() and not is_admin():
            abort(403)
        proposal["fields"] = _json_loads(proposal.pop("fields_json", "[]"), [])
        return proposal

    @bp.get("/")
    def index():
        db = get_db()
        templates = [
            _template_dict(row)
            for row in db.execute("""
                SELECT * FROM de_xuat_templates
                WHERE active = 1
                ORDER BY sort_order, name
            """).fetchall()
        ]
        if is_admin():
            recent_rows = db.execute("""
                SELECT d.*, t.name AS template_name
                FROM de_xuat d
                JOIN de_xuat_templates t ON t.id = d.template_id
                ORDER BY d.updated_at DESC LIMIT 6
            """).fetchall()
        else:
            recent_rows = db.execute("""
                SELECT d.*, t.name AS template_name
                FROM de_xuat d
                JOIN de_xuat_templates t ON t.id = d.template_id
                WHERE d.user_id = ?
                ORDER BY d.updated_at DESC LIMIT 6
            """, (current_user_id(),)).fetchall()
        return render_template(
            "de_xuat/index.html",
            templates=templates,
            recent=[dict(row) for row in recent_rows],
        )

    @bp.get("/new")
    def new():
        template = get_template(slug=request.args.get("template", "de-xuat-chung"))
        if template is None:
            abort(404)
        initial = {
            "id": None,
            "template_id": template["id"],
            "status": "draft",
            "title": render_pattern(template["title_template"], {}),
            "proposal_content": render_pattern(template["content_template"], {}),
            "reason_content": render_pattern(template["reason_template"], {}),
            "approval_level": "",
            "organization": "PNJ NEXT 27 Hà Nội - Huế (1305)",
            "attachments": "",
            "form_data": {},
        }
        return render_template("de_xuat/editor.html", template=template, proposal=initial)

    @bp.get("/<int:proposal_id>/edit")
    def edit(proposal_id):
        proposal = get_proposal(proposal_id)
        template = get_template(template_id=proposal["template_id"])
        return render_template("de_xuat/editor.html", template=template, proposal=proposal)

    @bp.get("/history")
    def history():
        db = get_db()
        query = """
            SELECT d.*, t.name AS template_name, t.group_name
            FROM de_xuat d
            JOIN de_xuat_templates t ON t.id = d.template_id
        """
        params = []
        if not is_admin():
            query += " WHERE d.user_id = ?"
            params.append(current_user_id())
        query += " ORDER BY d.updated_at DESC LIMIT 200"
        rows = [dict(row) for row in db.execute(query, params).fetchall()]
        return render_template("de_xuat/history.html", proposals=rows)

    @bp.post("/api/save")
    def save():
        payload = request.get_json(silent=True) or {}
        template = get_template(template_id=payload.get("template_id"))
        if template is None:
            return jsonify({"ok": False, "error": "Mẫu đề xuất không hợp lệ."}), 400

        form_data = payload.get("form_data")
        if not isinstance(form_data, dict):
            return jsonify({"ok": False, "error": "Dữ liệu biểu mẫu không hợp lệ."}), 400

        missing = []
        for field in template["fields"]:
            if field.get("required") and not str(form_data.get(field["key"], "")).strip():
                missing.append(field["label"])
        if missing:
            return jsonify({
                "ok": False,
                "error": "Vui lòng nhập: " + ", ".join(missing),
            }), 400

        status = str(payload.get("status") or "draft")
        if status not in VALID_STATUSES:
            status = "draft"
        title = str(payload.get("title") or "").strip()
        proposal_content = str(payload.get("proposal_content") or "").strip()
        reason_content = str(payload.get("reason_content") or "").strip()
        if not title or not proposal_content:
            return jsonify({"ok": False, "error": "Thiếu tiêu đề hoặc nội dung đề xuất."}), 400

        amount = _normalize_amount(form_data.get("amount"))
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        values = (
            template["id"], status, title, proposal_content, reason_content, amount,
            str(payload.get("approval_level") or "").strip(),
            str(payload.get("organization") or "").strip(),
            str(payload.get("attachments") or "").strip(),
            json.dumps(form_data, ensure_ascii=False),
        )
        db = get_db()
        proposal_id = payload.get("id")
        if proposal_id:
            try:
                existing = get_proposal(int(proposal_id))
            except (TypeError, ValueError):
                return jsonify({"ok": False, "error": "Mã đề xuất không hợp lệ."}), 400
            db.execute("""
                UPDATE de_xuat
                SET updated_at = ?, template_id = ?, status = ?, title = ?,
                    proposal_content = ?, reason_content = ?, amount = ?,
                    approval_level = ?, organization = ?, attachments = ?,
                    form_data_json = ?
                WHERE id = ?
            """, (now, *values, existing["id"]))
            proposal_id = existing["id"]
        else:
            cursor = db.execute("""
                INSERT INTO de_xuat
                    (created_at, updated_at, user_id, template_id, status, title,
                     proposal_content, reason_content, amount, approval_level,
                     organization, attachments, form_data_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (now, now, current_user_id(), *values))
            proposal_id = cursor.lastrowid

        snapshot = {
            "status": status,
            "title": title,
            "proposal_content": proposal_content,
            "reason_content": reason_content,
            "amount": amount,
            "approval_level": values[6],
            "organization": values[7],
            "attachments": values[8],
            "form_data": form_data,
        }
        db.execute("""
            INSERT INTO de_xuat_versions (de_xuat_id, user_id, source, snapshot_json)
            VALUES (?, ?, 'manual', ?)
        """, (proposal_id, current_user_id(), json.dumps(snapshot, ensure_ascii=False)))
        db.commit()
        return jsonify({
            "ok": True,
            "id": proposal_id,
            "edit_url": f"/de-xuat/{proposal_id}/edit",
            "preview_url": f"/de-xuat/{proposal_id}/preview",
            "pdf_url": f"/de-xuat/{proposal_id}/pdf",
        })

    @bp.post("/<int:proposal_id>/duplicate")
    def duplicate(proposal_id):
        source = get_proposal(proposal_id)
        db = get_db()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor = db.execute("""
            INSERT INTO de_xuat
                (created_at, updated_at, user_id, template_id, status, title,
                 proposal_content, reason_content, amount, approval_level,
                 organization, attachments, form_data_json)
            VALUES (?, ?, ?, ?, 'draft', ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            now, now, current_user_id(), source["template_id"],
            source["title"], source["proposal_content"], source["reason_content"],
            source["amount"], source["approval_level"], source["organization"],
            source["attachments"], json.dumps(source["form_data"], ensure_ascii=False),
        ))
        db.commit()
        return jsonify({"ok": True, "edit_url": f"/de-xuat/{cursor.lastrowid}/edit"})

    def render_print(proposal_id, download=False):
        proposal = get_proposal(proposal_id)
        html = render_template("de_xuat/print.html", proposal=proposal, download=download)
        return proposal, html

    @bp.get("/<int:proposal_id>/preview")
    def preview(proposal_id):
        _proposal, html = render_print(proposal_id)
        return html

    @bp.get("/<int:proposal_id>/pdf")
    def pdf(proposal_id):
        _proposal, html = render_print(proposal_id, download=True)
        response = send_pdf(html, pdf_filename("De xuat"))
        if getattr(response, "status_code", 500) == 200:
            get_db().execute(
                "UPDATE de_xuat SET status = 'exported', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (proposal_id,),
            )
            get_db().commit()
        return response

    return bp
