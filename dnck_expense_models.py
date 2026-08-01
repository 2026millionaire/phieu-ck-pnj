# -*- coding: utf-8 -*-
"""Schema and data helpers for ĐNCK thanh toán chi phí khác."""

import hashlib
import json
import re
from datetime import datetime, timezone


STATE_SOURCE_SELECTED = "draft_source_selected"
STATE_SOURCE_LOADED = "source_loaded"
STATE_SOURCE_LOCKED = "source_locked"
STATE_NEEDS_REVIEW = "needs_review"
STATE_VALIDATED = "validated"
STATE_FILES_GENERATED = "files_generated"
STATE_FINAL_PREVIEWED = "final_previewed"
STATE_READY_TO_SUBMIT = "ready_to_submit"
STATE_SUBMITTED = "submitted"

ALL_STATES = {
    STATE_SOURCE_SELECTED,
    STATE_SOURCE_LOADED,
    STATE_SOURCE_LOCKED,
    STATE_NEEDS_REVIEW,
    STATE_VALIDATED,
    STATE_FILES_GENERATED,
    STATE_FINAL_PREVIEWED,
    STATE_READY_TO_SUBMIT,
    STATE_SUBMITTED,
}

ADJUSTMENT_DESCRIPTION = "Phần chênh lệch hóa đơn không thanh toán"
CHI_PHI_SPREADSHEET_ID = "1EuTTamSDs1BD5kzBsoGeSQpn9E9s7EGaK7CPkHgvpuc"
CHI_PHI_ND_SHEET = "ND"
CHI_PHI_HD_SHEET = "HD"
CHI_PHI_ND_HEADER_ROW = 4
CHI_PHI_HD_HEADER_ROW = 3
FIXED_PAYMENT_METHOD = "Chuyển khoản"

VIP_T07_FIXTURE = {
    "source_type": "test_fixture",
    "live": False,
    "spreadsheet_id": CHI_PHI_SPREADSHEET_ID,
    "nd_range": "ND!A167:H167",
    "hd_range": "HD!A352:N354",
    "nd": {
        "stt": "173",
        "plant": "1305",
        "cost_center": "1305",
        "expense_account": "64180280",
        "period": "2026-07",
        "object_code": "E01F7743",
        "object_name": "",
        "supplier_tax_id": "3300535435",
        "description": "Chi phí phòng chờ khách VIP T07.2026-CH 27 Hà Nội",
        "amount": 1575118,
        "sap_document": "",
        "frequency": "Tháng",
    },
    "hd": [
        {
            "row_number": 352,
            "invoice_date": "2026-07-06",
            "description": "Chi phí phòng chờ khách VIP T07.2026-CH 27 Hà Nội",
            "amount": 597859,
            "invoice_no_full": "1C26MCM-254889",
            "supplier_tax_id": "3300535435",
            "lookup_code": "13PBN922AWL",
            "lookup_url": "https://saigoncoop.einvoice.com.vn/",
            "document_type": "Hóa đơn",
            "expense_account": "64180280",
            "cost_center": "1305",
            "sap_document": "",
        },
        {
            "row_number": 353,
            "invoice_date": "2026-07-20",
            "description": "Chi phí phòng chờ khách VIP T07.2026-CH 27 Hà Nội",
            "amount": 562179,
            "invoice_no_full": "1C26MCM-274006",
            "supplier_tax_id": "3300535435",
            "lookup_code": "13ILR5ZQDS2",
            "lookup_url": "https://saigoncoop.einvoice.com.vn/",
            "document_type": "Hóa đơn",
            "expense_account": "64180280",
            "cost_center": "1305",
            "sap_document": "",
        },
        {
            "row_number": 354,
            "invoice_date": "2026-07-30",
            "description": "Chi phí phòng chờ khách VIP T07.2026-CH 27 Hà Nội",
            "amount": 415080,
            "invoice_no_full": "1C26MCM-286917",
            "supplier_tax_id": "3300535435",
            "lookup_code": "13G6BCU2EXB",
            "lookup_url": "https://saigoncoop.einvoice.com.vn/",
            "document_type": "Hóa đơn",
            "expense_account": "64180280",
            "cost_center": "1305",
            "sap_document": "2400032967",
        },
    ],
}


def utc_now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def canonical_json(data):
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def source_hash(payload):
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def normalize_amount(value):
    if value is None or value == "":
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    cleaned = re.sub(r"[^\d-]", "", str(value))
    return int(cleaned or 0)


def normalize_text(value):
    return str(value or "").strip()


def normalize_hd_row(row):
    data = dict(row or {})
    data["row_number"] = int(data.get("row_number") or 0)
    data["invoice_date"] = normalize_text(data.get("invoice_date"))
    data["description"] = normalize_text(data.get("description") or data.get("hd_description"))
    data["amount"] = normalize_amount(data.get("amount"))
    data["invoice_no_full"] = normalize_text(data.get("invoice_no_full")).upper()
    data["supplier_tax_id"] = re.sub(r"\D", "", normalize_text(data.get("supplier_tax_id")))
    data["lookup_code"] = normalize_text(data.get("lookup_code"))
    data["lookup_url"] = normalize_text(data.get("lookup_url"))
    data["document_type"] = normalize_text(data.get("document_type") or "Hóa đơn")
    data["expense_account"] = normalize_text(data.get("expense_account"))
    data["cost_center"] = normalize_text(data.get("cost_center"))
    data["adjustment_ref"] = normalize_text(data.get("adjustment_ref"))
    data["sap_document"] = normalize_text(data.get("sap_document"))
    return data


def normalize_nd_row(row):
    data = dict(row or {})
    data["stt"] = normalize_text(data.get("stt"))
    data["plant"] = normalize_text(data.get("plant") or "1305")
    data["cost_center"] = normalize_text(data.get("cost_center"))
    data["expense_account"] = normalize_text(data.get("expense_account"))
    data["period"] = normalize_text(data.get("period"))
    data["object_code"] = normalize_text(data.get("object_code"))
    data["object_name"] = normalize_text(data.get("object_name"))
    data["supplier_tax_id"] = re.sub(r"\D", "", normalize_text(data.get("supplier_tax_id")))
    data["description"] = normalize_text(data.get("description"))
    data["amount"] = normalize_amount(data.get("amount"))
    data["sap_document"] = normalize_text(data.get("sap_document"))
    data["frequency"] = normalize_text(data.get("frequency"))
    return data


def normalize_source_payload(payload):
    raw = dict(payload or {})
    nd = normalize_nd_row(raw.get("nd") or {})
    hd = [normalize_hd_row(item) for item in raw.get("hd") or []]
    source = {
        "source_type": normalize_text(raw.get("source_type") or "local_fixture"),
        "live": bool(raw.get("live")),
        "spreadsheet_id": normalize_text(raw.get("spreadsheet_id")),
        "nd_range": normalize_text(raw.get("nd_range")),
        "hd_range": normalize_text(raw.get("hd_range")),
        "read_at": normalize_text(raw.get("read_at") or utc_now_iso()),
        "nd": nd,
        "hd": hd,
    }
    source["source_hash"] = source_hash({
        "source_type": source["source_type"],
        "spreadsheet_id": source["spreadsheet_id"],
        "nd_range": source["nd_range"],
        "hd_range": source["hd_range"],
        "nd": nd,
        "hd": hd,
    })
    return source


def initialize_dnck_expense_schema(conn):
    for statement in (
        "ALTER TABLE dnck ADD COLUMN source_mode TEXT DEFAULT ''",
        "ALTER TABLE dnck ADD COLUMN source_id INTEGER DEFAULT 0",
    ):
        try:
            conn.execute(statement)
        except Exception:
            pass

    conn.execute("""
        CREATE TABLE IF NOT EXISTS dnck_expense_draft (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dnck_id INTEGER,
            status TEXT NOT NULL DEFAULT 'draft_source_selected',
            da_trinh INTEGER NOT NULL DEFAULT 0,
            source_type TEXT DEFAULT '',
            spreadsheet_id TEXT DEFAULT '',
            nd_range TEXT DEFAULT '',
            hd_range TEXT DEFAULT '',
            source_read_at TEXT DEFAULT '',
            source_hash TEXT DEFAULT '',
            user_confirmed_at TEXT DEFAULT '',
            payment_method TEXT DEFAULT '',
            desired_payment_date TEXT DEFAULT '',
            ktpt TEXT DEFAULT '',
            sap_document TEXT DEFAULT '',
            advance_amount INTEGER NOT NULL DEFAULT 0,
            request_content TEXT DEFAULT '',
            internal_note TEXT DEFAULT '',
            cost_group TEXT DEFAULT '',
            validation_json TEXT DEFAULT '[]',
            created_by INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dnck_expense_source_nd (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            draft_id INTEGER NOT NULL,
            spreadsheet_id TEXT DEFAULT '',
            sheet_name TEXT DEFAULT 'ND',
            range_a1 TEXT DEFAULT '',
            cell_refs_json TEXT DEFAULT '{}',
            read_at TEXT DEFAULT '',
            source_hash TEXT DEFAULT '',
            stt TEXT DEFAULT '',
            plant TEXT DEFAULT '',
            cost_center TEXT DEFAULT '',
            expense_account TEXT DEFAULT '',
            period TEXT DEFAULT '',
            object_code TEXT DEFAULT '',
            object_name TEXT DEFAULT '',
            supplier_tax_id TEXT DEFAULT '',
            nd_description TEXT DEFAULT '',
            nd_amount INTEGER DEFAULT 0,
            raw_values_json TEXT DEFAULT '{}'
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dnck_expense_source_hd (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            draft_id INTEGER NOT NULL,
            nd_stt TEXT DEFAULT '',
            spreadsheet_id TEXT DEFAULT '',
            sheet_name TEXT DEFAULT 'HD',
            row_number INTEGER DEFAULT 0,
            range_a1 TEXT DEFAULT '',
            invoice_date TEXT DEFAULT '',
            hd_description TEXT DEFAULT '',
            amount INTEGER DEFAULT 0,
            invoice_no_full TEXT DEFAULT '',
            supplier_tax_id TEXT DEFAULT '',
            lookup_code TEXT DEFAULT '',
            lookup_url TEXT DEFAULT '',
            document_type TEXT DEFAULT '',
            expense_account TEXT DEFAULT '',
            cost_center TEXT DEFAULT '',
            adjustment_ref TEXT DEFAULT '',
            raw_values_json TEXT DEFAULT '{}',
            source_hash TEXT DEFAULT ''
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dnck_expense_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            draft_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            old_status TEXT DEFAULT '',
            new_status TEXT DEFAULT '',
            spreadsheet_id TEXT DEFAULT '',
            sheet_name TEXT DEFAULT '',
            range_a1 TEXT DEFAULT '',
            cell_refs_json TEXT DEFAULT '{}',
            nd_snapshot_json TEXT DEFAULT '{}',
            hd_snapshot_json TEXT DEFAULT '[]',
            validation_result_json TEXT DEFAULT '[]',
            message TEXT DEFAULT '',
            created_by INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dnck_expense_attachment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            draft_id INTEGER NOT NULL,
            attachment_type TEXT NOT NULL,
            file_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            sha256 TEXT DEFAULT '',
            mime_type TEXT DEFAULT '',
            size_bytes INTEGER DEFAULT 0,
            source_note TEXT DEFAULT '',
            created_by INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dnck_expense_output (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            draft_id INTEGER NOT NULL,
            output_type TEXT NOT NULL,
            production_file_name TEXT NOT NULL,
            output_path TEXT NOT NULL,
            sha256 TEXT DEFAULT '',
            generated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            validation_status TEXT DEFAULT ''
        )
    """)
