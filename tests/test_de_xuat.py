# -*- coding: utf-8 -*-

import os
import tempfile
import unittest
from unittest import mock
from pathlib import Path

import app as app_module


class DeXuatTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = app_module.DB_PATH
        self.original_require_login = app_module.REQUIRE_LOGIN
        app_module.DB_PATH = str(Path(self.temp_dir.name) / "de_xuat_test.db")
        app_module.REQUIRE_LOGIN = False
        app_module.init_db()
        app_module.app.config.update(TESTING=True)
        self.client = app_module.app.test_client()

    def tearDown(self):
        app_module.DB_PATH = self.original_db_path
        app_module.REQUIRE_LOGIN = self.original_require_login
        self.temp_dir.cleanup()

    def valid_payload(self):
        with app_module.app.app_context():
            template = app_module.get_db().execute(
                "SELECT id FROM de_xuat_templates WHERE slug = 'hoan-coc'"
            ).fetchone()
        return {
            "template_id": template["id"],
            "status": "draft",
            "title": "ĐỀ XUẤT HOÀN CỌC KHÁCH HÀNG",
            "proposal_content": "Đề xuất hoàn tiền cọc cho khách hàng Nguyễn Văn A.",
            "reason_content": "Giao dịch không tiếp tục.",
            "approval_level": "GĐCN",
            "organization": "PNJ NEXT 27 Hà Nội - Huế (1305)",
            "attachments": "Biên nhận cọc",
            "form_data": {
                "ten_kh": "NGUYỄN VĂN A",
                "ma_kh": "100000001",
                "so_bien_nhan": "1600012345",
                "ngay_dat_coc": "2026-07-25",
                "amount": "1.500.000",
                "tai_khoan_nhan": "NGUYỄN VĂN A - 0123456789 - TCB",
            },
        }

    def test_home_lists_seeded_templates(self):
        response = self.client.get("/de-xuat/")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Nạp mực / sửa chữa máy in", html)
        self.assertIn("Hoàn cọc khách hàng", html)
        self.assertIn("Trích lục hóa đơn", html)

    def test_save_and_reopen_proposal(self):
        response = self.client.post("/de-xuat/api/save", json=self.valid_payload())
        self.assertEqual(response.status_code, 200)
        result = response.get_json()
        self.assertTrue(result["ok"])
        self.assertEqual(result["edit_url"], f"/de-xuat/{result['id']}/edit")

        edit = self.client.get(result["edit_url"])
        self.assertEqual(edit.status_code, 200)
        html = edit.get_data(as_text=True)
        self.assertIn("NGUYỄN VĂN A", html)
        self.assertIn("1.500.000", html)

        preview = self.client.get(result["preview_url"])
        self.assertEqual(preview.status_code, 200)
        self.assertIn("ĐỀ XUẤT HOÀN CỌC KHÁCH HÀNG", preview.get_data(as_text=True))

    def test_required_template_fields_are_validated(self):
        payload = self.valid_payload()
        payload["form_data"]["amount"] = ""
        response = self.client.post("/de-xuat/api/save", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertIn("Số tiền hoàn cọc", response.get_json()["error"])

    def test_failed_pdf_render_does_not_mark_proposal_as_exported(self):
        saved = self.client.post("/de-xuat/api/save", json=self.valid_payload()).get_json()
        with mock.patch.object(
            app_module,
            "make_pdf_from_print_html",
            side_effect=RuntimeError("PDF_RENDERER_MISSING"),
        ):
            response = self.client.get(saved["pdf_url"])
        self.assertEqual(response.status_code, 503)
        with app_module.app.app_context():
            status = app_module.get_db().execute(
                "SELECT status FROM de_xuat WHERE id = ?",
                (saved["id"],),
            ).fetchone()["status"]
        self.assertEqual(status, "draft")

    def test_de_xuat_redirects_to_bk_login_with_next(self):
        app_module.REQUIRE_LOGIN = True
        response = self.client.get("/de-xuat/history")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/bk/login?next=%2Fde-xuat%2Fhistory", response.location)

    def test_login_next_rejects_external_url(self):
        self.assertEqual(app_module.safe_login_next("https://example.com"), "")
        self.assertEqual(app_module.safe_login_next("//example.com/path"), "")
        self.assertEqual(
            app_module.safe_login_next("/de-xuat/12/edit"),
            "/de-xuat/12/edit",
        )


if __name__ == "__main__":
    unittest.main()
