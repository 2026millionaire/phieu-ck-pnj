# -*- coding: utf-8 -*-

import io
import html
import json
import sqlite3
import tempfile
import unittest
import zipfile
from pathlib import Path

import app as app_module
from dnck_expense_models import ADJUSTMENT_DESCRIPTION, VIP_T07_FIXTURE, normalize_source_payload
from dnck_expense_template import build_template_tt_bytes
from dnck_expense_validation import build_limit_adjustment_rows, validate_source_and_supplement


class DnckExpensePhase1Tests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = app_module.DB_PATH
        app_module.DB_PATH = str(Path(self.temp_dir.name) / "phieu.db")
        app_module.init_db()
        app_module.app.config.update(TESTING=True)
        self.client = app_module.app.test_client()

    def tearDown(self):
        app_module.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def login(self, role="admin", user_id=1):
        with self.client.session_transaction() as session:
            session["user_id"] = user_id
            session["user_name"] = "ADMIN TEST"
            session["role"] = role

    def source_payload(self):
        return normalize_source_payload(VIP_T07_FIXTURE)

    def valid_supplement(self):
        return {
            "payment_method": "Chuyển khoản",
            "desired_payment_date": "2026-08-05",
            "ktpt": "KTPT TEST",
            "sap_document": "",
            "request_content": "Chi phí phòng chờ khách VIP T07.2026-CH 27 Hà Nội",
            "cost_group": "Khác",
            "advance_amount": 0,
            "limit_verified": True,
        }

    def test_schema_adds_expense_tables_and_legacy_source_columns(self):
        conn = sqlite3.connect(app_module.DB_PATH)
        try:
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertIn("dnck_expense_draft", tables)
            self.assertIn("dnck_expense_source_nd", tables)
            self.assertIn("dnck_expense_source_hd", tables)
            self.assertIn("dnck_expense_audit", tables)
            self.assertIn("dnck_expense_output", tables)
            dnck_cols = {row[1] for row in conn.execute("PRAGMA table_info(dnck)")}
            self.assertIn("source_mode", dnck_cols)
            self.assertIn("source_id", dnck_cols)
        finally:
            conn.close()

    def test_vip_fixture_total_is_needs_review_without_live_connector(self):
        self.login()
        response = self.client.get("/api/dnck-expense/source?fixture=schema_173")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertTrue(data["ok"])
        self.assertFalse(data["source"]["live"])
        self.assertEqual(data["source"]["spreadsheet_id"], "1EuTTamSDs1BD5kzBsoGeSQpn9E9s7EGaK7CPkHgvpuc")
        self.assertEqual(data["source"]["nd"]["stt"], "173")
        self.assertEqual(data["source"]["nd"]["object_code"], "E01F7743")
        self.assertEqual(data["source"]["object_lookup"]["object_code"], "E01F7743")
        self.assertEqual(data["source"]["object_lookup"]["account_number"], "106873221304")
        self.assertEqual(data["source"]["suggested_sap_document"], "2400032967")
        self.assertEqual(data["validation"]["total_hd"], 1575118)
        self.assertEqual(data["validation"]["total_nd"], 1575118)
        self.assertEqual(data["validation"]["status"], "needs_review")
        self.assertTrue(any(item["rule_key"] == "live_source" for item in data["validation"]["results"]))

    def test_validate_requires_qt82_fields_and_limit_review(self):
        source = self.source_payload()
        validation = validate_source_and_supplement(source, {}, require_live=False)
        rules = {item["rule_key"] for item in validation["results"] if item["status"] != "pass"}
        self.assertIn("limit_verified", rules)
        self.assertNotIn("missing_sap_document", rules)
        self.assertIn("missing_ktpt", rules)
        self.assertIn("missing_payment_method", rules)
        self.assertIn("missing_desired_payment_date", rules)

    def test_live_source_without_connector_reports_blocker(self):
        self.login()
        response = self.client.get("/api/dnck-expense/source?nd_stt=173")
        self.assertEqual(response.status_code, 503)
        data = response.get_json()
        self.assertEqual(data["code"], "LIVE_SOURCE_UNAVAILABLE")

    def test_payment_method_is_fixed_transfer(self):
        source = self.source_payload()
        supplement = self.valid_supplement()
        supplement["payment_method"] = "Tiền mặt"
        validation = validate_source_and_supplement(source, supplement, require_live=False)
        failed = {item["rule_key"] for item in validation["results"] if item["status"] != "pass"}
        self.assertIn("payment_method_fixed", failed)

    def test_duplicate_invoice_and_lookup_are_blocked(self):
        source = self.source_payload()
        duplicate = dict(source["hd"][0])
        duplicate["row_number"] = 99
        source["hd"].append(duplicate)
        source["nd"]["amount"] += duplicate["amount"]
        validation = validate_source_and_supplement(source, self.valid_supplement(), require_live=False)
        failed = {item["rule_key"] for item in validation["results"] if item["status"] != "pass"}
        self.assertIn("duplicate_invoice_tax_amount", failed)
        self.assertIn("duplicate_lookup_tax", failed)

    def test_wrong_total_is_needs_review(self):
        source = self.source_payload()
        source["nd"]["amount"] = 1
        validation = validate_source_and_supplement(source, self.valid_supplement(), require_live=False)
        self.assertEqual(validation["status"], "needs_review")
        self.assertTrue(any(item["rule_key"] == "total_nd_hd" for item in validation["results"]))

    def test_limit_adjustment_keeps_original_rows_and_adds_negative_rows(self):
        source = self.source_payload()
        rows = build_limit_adjustment_rows(source["hd"], 1000000)
        self.assertEqual(len(source["hd"]), 3)
        self.assertGreater(len(rows), 3)
        original_amounts = [row["amount"] for row in rows[:3]]
        self.assertEqual(original_amounts, [597859, 562179, 415080])
        negatives = [row for row in rows if row["amount"] < 0]
        self.assertTrue(negatives)
        self.assertTrue(all(row["description"].startswith(ADJUSTMENT_DESCRIPTION) for row in negatives))
        self.assertTrue(all(row["adjustment_ref"] for row in negatives))
        validation = validate_source_and_supplement(
            {"nd": {"stt": "1", "plant": "1305", "cost_center": "1305", "expense_account": "64180280", "period": "2026-07", "description": "Test", "amount": sum(row["amount"] for row in rows)}, "hd": rows, "live": True},
            self.valid_supplement(),
            require_live=True,
        )
        self.assertFalse(any(item["rule_key"] == "negative_template_support" for item in validation["results"]))

    def test_template_preserves_structure_and_unicode(self):
        source = self.source_payload()
        blob = build_template_tt_bytes(Path(app_module.app.static_folder) / "template_tt.xlsx", source["hd"])
        with zipfile.ZipFile(io.BytesIO(blob), "r") as archive:
            names = set(archive.namelist())
            self.assertIn("xl/workbook.xml", names)
            self.assertIn("xl/styles.xml", names)
            self.assertIn("xl/worksheets/sheet1.xml", names)
            workbook = archive.read("xl/workbook.xml").decode("utf-8", errors="replace")
            if "Sheet2" in workbook:
                self.assertIn('state="hidden"', workbook)
            text = html.unescape(archive.read("xl/worksheets/sheet1.xml").decode("utf-8", errors="replace"))
            if "xl/sharedStrings.xml" in names:
                text += html.unescape(archive.read("xl/sharedStrings.xml").decode("utf-8", errors="replace"))
            self.assertIn("Chi phí phòng chờ khách VIP", text)
            self.assertIn("1C26MCM-254889", text)

    def test_create_draft_sets_legacy_source_mode_and_da_trinh_zero(self):
        self.login()
        source = self.source_payload()
        response = self.client.post(
            "/api/dnck-expense/drafts",
            json={"source": source, "supplement": self.valid_supplement(), "user_confirmed": True},
        )
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["da_trinh"], 0)
        self.assertEqual(data["status"], "needs_review")

        conn = sqlite3.connect(app_module.DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            draft = conn.execute("SELECT * FROM dnck_expense_draft WHERE id = ?", (data["id"],)).fetchone()
            legacy = conn.execute("SELECT * FROM dnck WHERE id = ?", (data["dnck_id"],)).fetchone()
            self.assertEqual(draft["da_trinh"], 0)
            self.assertEqual(legacy["da_trinh"], 0)
            self.assertEqual(legacy["source_mode"], "thanh_toan_chi_phi_khac")
            self.assertEqual(legacy["source_id"], data["id"])
            self.assertEqual(legacy["payment_tag"], "Thanh Toán Chi Phí Khác")
            self.assertEqual(legacy["object_code"], "E01F7743")
            self.assertEqual(legacy["account_number"], "106873221304")
            self.assertEqual(legacy["bank"], "Vietinbank")
            self.assertEqual(legacy["sap_document"], "2400032967")
            detail = json.loads(legacy["detail_json"])
            self.assertEqual(sum(item["amount"] for item in detail), 1575118)
        finally:
            conn.close()

    def test_mark_ready_requires_live_source_and_does_not_change_da_trinh(self):
        self.login()
        response = self.client.post(
            "/api/dnck-expense/drafts",
            json={"source": self.source_payload(), "supplement": self.valid_supplement(), "user_confirmed": True},
        )
        draft_id = response.get_json()["id"]
        ready = self.client.post(f"/api/dnck-expense/{draft_id}/mark-ready", json={})
        self.assertIn(ready.status_code, (409, 400))
        conn = sqlite3.connect(app_module.DB_PATH)
        try:
            da_trinh = conn.execute("SELECT da_trinh FROM dnck_expense_draft WHERE id = ?", (draft_id,)).fetchone()[0]
            self.assertEqual(da_trinh, 0)
        finally:
            conn.close()

    def test_admin_only(self):
        self.login(role="user", user_id=2)
        self.assertEqual(self.client.get("/dnck/expense").status_code, 403)
        self.assertEqual(self.client.get("/api/dnck-expense/source").status_code, 403)

    def test_config_exposes_payment_method_and_ktpt_options(self):
        self.login()
        response = self.client.get("/api/dnck-expense/config")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertEqual(data["payment_method"], "Chuyển khoản")
        self.assertIn("ktpt_options", data)
