# -*- coding: utf-8 -*-

import io
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import app as app_module
from customer_identity import CustomerIdentityStore
from customer_lookup import CustomerLookupStore
from employee_lookup import EmployeeLookupStore
from openpyxl import Workbook


class CustomerUpdateApiTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.original_db_path = app_module.DB_PATH
        self.original_store = app_module._customer_lookup_store
        self.original_identity_store = app_module._customer_identity_store
        self.original_employee_store = app_module._employee_lookup_store
        app_module.DB_PATH = str(self.root / "phieu.db")
        app_module.init_db()

        self.store = CustomerLookupStore(self.root / "lookup.db", bytes(range(32)))
        source = self.root / "source.tsv"
        source.write_text(
            "\tSearchTerm\tCty\tPostalCode\tCity\tName 1\tCustomer\tCoCd\tDelF\n"
            "\t0900000000\tVN\t\tCITY\tTEN HE THONG\t100000000\t10\n",
            encoding="utf-8-sig",
        )
        self.store.import_files([source])
        app_module._customer_lookup_store = self.store
        self.identity_store = CustomerIdentityStore(self.root / "identity.db", bytes(range(32)))
        self.identity_store.initialize()
        app_module._customer_identity_store = self.identity_store
        self.employee_store = EmployeeLookupStore(self.root / "employee.db", bytes(range(32)))
        self.employee_store.initialize()
        app_module._employee_lookup_store = self.employee_store
        app_module.app.config.update(TESTING=True)
        self.client = app_module.app.test_client()

    def tearDown(self):
        app_module.DB_PATH = self.original_db_path
        app_module._customer_lookup_store = self.original_store
        app_module._customer_identity_store = self.original_identity_store
        app_module._employee_lookup_store = self.original_employee_store
        self.temp_dir.cleanup()

    def login(self, role="admin", user_id=1):
        with self.client.session_transaction() as session:
            session["user_id"] = user_id
            session["user_name"] = "ADMIN TEST"
            session["role"] = role

    def printed_payload(self, status):
        return {
            "status": status,
            "ma_kh": "100000000",
            "ten_kh": "TEN TVV NHAP",
            "sdt": "0912345678",
            "cccd": "012345678901",
            "tvv_code": "E000001",
            "tvv_name_real": "TVV TEST",
            "so_bk": "440300001",
            "tong_ck": 1000,
            "chung_tu": [],
        }

    def identity_xlsx(self):
        stream = io.BytesIO()
        workbook = Workbook()
        sheet = workbook.active
        sheet.append(["Inv.Date", "Vendor", "Tên Vendor", "CMND", "Plant"])
        sheet.append(["17.07.2026", "100000000", "TEN XAC MINH", "", "1305"])
        sheet.append(["16.07.2026", "100000000", "TEN XAC MINH", "012345678901", "1305"])
        workbook.save(stream)
        return stream.getvalue()

    def mixed_identity_xlsx(self):
        stream = io.BytesIO()
        workbook = Workbook()
        sheet = workbook.active
        sheet.append(["Inv.Date", "Vendor", "Tên Vendor", "CMND", "Plant"])
        sheet.append(["17.07.2026", "100000000", "TEN KH", "012345678901", "1305"])
        sheet.append(["17.07.2026", "E0100001", "TEN NHAN VIEN", "012345678902", "1305"])
        workbook.save(stream)
        return stream.getvalue()

    def insert_identity_record(self, customer_code="100000000", identity_value="012345678901", name="TEN KH"):
        key = self.identity_store.lookup_key(customer_code)
        payload = {
            "vendor": customer_code,
            "identity_value": identity_value,
            "source_name": name,
            "verified_name": name,
            "plant": "1305",
            "source_date": "2026-07-17",
        }
        nonce, ciphertext, digest = self.identity_store._encrypt(payload, key)
        with closing(self.identity_store.connect()) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO identity_records
                (lookup_key, payload_nonce, payload_ciphertext, payload_digest, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (key, nonce, ciphertext, digest, time.time()),
            )
            connection.commit()

    def test_draft_does_not_create_candidates_but_printed_does(self):
        self.login()
        draft = self.client.post("/api/save", json=self.printed_payload("draft"))
        self.assertEqual(draft.status_code, 200)
        self.assertEqual(self.store.list_candidate_report("pending")["total"], 0)

        printed_payload = self.printed_payload("printed")
        printed_payload["so_bk"] = "440300002"
        printed = self.client.post("/api/save", json=printed_payload)
        self.assertEqual(printed.status_code, 200)
        self.assertEqual(self.store.list_candidate_report("pending")["total"], 3)

    def test_admin_can_list_and_review_but_normal_user_cannot(self):
        candidate_id = self.store.record_tvv_values(
            customer_code="100000000",
            values={"name": "TEN MOI"},
            user_id=2,
            phieu_id=99,
        )[0]

        self.login(role="user", user_id=2)
        self.assertEqual(self.client.get("/customer-updates").status_code, 403)
        self.assertEqual(self.client.get("/api/customer-updates").status_code, 403)
        self.assertEqual(
            self.client.post(
                f"/api/customer-updates/{candidate_id}/review",
                json={"action": "approve"},
            ).status_code,
            403,
        )

        self.login(role="admin", user_id=1)
        self.assertEqual(self.client.get("/customer-updates").status_code, 200)
        with patch.object(
            app_module.shared_auth,
            "get_user",
            return_value={"username": "admin", "full_name": "ADMIN TEST"},
        ):
            report = self.client.get("/api/customer-updates")
        self.assertEqual(report.status_code, 200)
        self.assertEqual(report.get_json()["data"]["total"], 1)

        reviewed = self.client.post(
            f"/api/customer-updates/{candidate_id}/review",
            json={"action": "approve"},
            headers={"Origin": "http://localhost"},
        )
        self.assertEqual(reviewed.status_code, 200)
        self.assertEqual(
            self.store.get_suggestions("100000000", "name"),
            [{"value": "TEN MOI", "source": "approved"}],
        )

    def test_settings_shows_pending_review_count_for_admin(self):
        self.store.record_tvv_values(
            customer_code="100000000",
            values={"phone": "0912345678"},
            user_id=2,
            phieu_id=100,
        )
        self.login(role="admin", user_id=1)
        response = self.client.get("/settings")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Cài đặt (1)", response.get_data(as_text=True))

    def test_cccd_suggestion_api_returns_candidate_list(self):
        self.login()
        self.store.record_tvv_values(
            customer_code="100000000",
            values={"cccd": "012345678901"},
            user_id=1,
            phieu_id=77,
        )
        response = self.client.post(
            "/api/customer-suggestion",
            json={"customer_code": "100000000", "field": "cccd"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json()["suggestions"],
            [{"value": "012345678901"}],
        )

    def test_verified_identity_cccd_is_not_reported_as_tvv_update(self):
        self.insert_identity_record(identity_value="012345678901")
        self.login(role="admin", user_id=1)
        payload = self.printed_payload("printed")
        payload["so_bk"] = "440300099"
        response = self.client.post("/api/save", json=payload)
        self.assertEqual(response.status_code, 200)

        report = self.store.list_candidate_report("pending")
        self.assertEqual({item["field"] for item in report["items"]}, {"name", "phone"})
        self.assertNotIn("cccd", {item["field"] for item in report["items"]})

    def test_customer_update_report_uses_verified_identity_as_current_cccd(self):
        candidate_id = self.store.record_tvv_values(
            customer_code="100000000",
            values={"cccd": "012345678901"},
            user_id=2,
            phieu_id=99,
        )[0]
        self.insert_identity_record(identity_value="046183000623")
        self.login(role="admin", user_id=1)

        response = self.client.get("/api/customer-updates?status=pending")
        self.assertEqual(response.status_code, 200)
        item = next(item for item in response.get_json()["data"]["items"] if item["id"] == candidate_id)
        self.assertEqual(item["original_value"], "046183000623")

    def test_admin_previews_then_applies_encrypted_identity_file(self):
        self.login(role="admin", user_id=1)
        settings_page = self.client.get("/settings")
        self.assertIn(b"btnManageCentralPlants", settings_page.data)
        self.assertIn(b"centralPlantsModal", settings_page.data)
        self.assertIn(b"btnCopyCentralPlants", settings_page.data)
        self.assertNotIn(b"btnCopyCentralPlants", self.client.get("/").data)
        with self.client.session_transaction() as session:
            csrf = session["customer_import_csrf"]
        content = self.identity_xlsx()
        preview = self.client.post(
            "/api/customer-identity-import/preview",
            data={"file": (io.BytesIO(content), "du-lieu-hue-khong-can-dung-ten.dat")},
            headers={"X-CSRF-Token": csrf, "Origin": "http://localhost"},
            content_type="multipart/form-data",
        )
        self.assertEqual(preview.status_code, 200, preview.get_json())
        self.assertEqual(preview.get_json()["data"]["with_identity"], 1)
        self.assertEqual(self.identity_store.get_summary()["record_count"], 0)

        applied = self.client.post(
            "/api/customer-identity-import/apply",
            data={
                "confirmed": "yes",
                "file": (io.BytesIO(content), "du-lieu-hue-khong-can-dung-ten.dat"),
            },
            headers={"X-CSRF-Token": csrf, "Origin": "http://localhost"},
            content_type="multipart/form-data",
        )
        self.assertEqual(applied.status_code, 200, applied.get_json())
        self.assertEqual(self.identity_store.get_summary()["record_count"], 1)

        suggestion = self.client.post(
            "/api/customer-suggestion",
            json={"customer_code": "100000000", "field": "cccd"},
        )
        self.assertEqual(suggestion.status_code, 200)
        self.assertEqual(suggestion.get_json()["suggestion"], "012345678901")

    def test_identity_import_is_admin_only(self):
        self.login(role="user", user_id=2)
        self.assertEqual(self.client.get("/api/customer-identity-import/summary").status_code, 403)
        self.assertEqual(self.client.post("/api/customer-identity-import/preview").status_code, 403)
        self.assertEqual(self.client.post("/api/customer-identity-import/apply").status_code, 403)

    def test_central_plants_are_seeded_and_admin_can_manage_them(self):
        seeded = self.client.get("/api/central-plants")
        self.assertEqual(seeded.status_code, 200)
        plants = seeded.get_json()["data"]
        self.assertEqual(len(plants), 59)
        self.assertIn({"province": "Huế", "plant": "1305"}, [
            {"province": item["province"], "plant": item["plant"]} for item in plants
        ])

        self.login(role="user", user_id=2)
        self.assertEqual(
            self.client.post("/api/central-plants", json={"province": "Test", "plant": "2000"}).status_code,
            403,
        )

        self.login(role="admin", user_id=1)
        self.client.get("/settings")
        with self.client.session_transaction() as session:
            csrf = session["customer_import_csrf"]
        headers = {"X-CSRF-Token": csrf, "Origin": "http://localhost"}
        added = self.client.post(
            "/api/central-plants", json={"province": "Test", "plant": "2000"}, headers=headers
        )
        self.assertEqual(added.status_code, 200)
        added_id = next(
            item["id"] for item in self.client.get("/api/central-plants").get_json()["data"]
            if item["plant"] == "2000"
        )
        updated = self.client.put(
            f"/api/central-plants/{added_id}",
            json={"province": "Test mới", "plant": "2001"},
            headers=headers,
        )
        self.assertEqual(updated.status_code, 200)
        self.assertEqual(self.client.delete(f"/api/central-plants/{added_id}", headers=headers).status_code, 200)

    def test_identity_summary_includes_employee_statistics(self):
        self.login(role="admin", user_id=1)
        response = self.client.get("/api/customer-identity-import/summary")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["employee"]["record_count"], 0)

    def test_identity_import_routes_e01_codes_to_employee_database(self):
        self.login(role="admin", user_id=1)
        self.client.get("/settings")
        with self.client.session_transaction() as session:
            csrf = session["customer_import_csrf"]
        content = self.mixed_identity_xlsx()
        preview = self.client.post(
            "/api/customer-identity-import/preview",
            data={"file": (io.BytesIO(content), "bang-ke.xlsx")},
            headers={"X-CSRF-Token": csrf, "Origin": "http://localhost"},
            content_type="multipart/form-data",
        )
        self.assertEqual(preview.status_code, 200, preview.get_json())
        self.assertEqual(preview.get_json()["data"]["with_identity"], 1)
        self.assertEqual(preview.get_json()["data"]["employee_with_identity"], 1)

        applied = self.client.post(
            "/api/customer-identity-import/apply",
            data={"confirmed": "yes", "file": (io.BytesIO(content), "bang-ke.xlsx")},
            headers={"X-CSRF-Token": csrf, "Origin": "http://localhost"},
            content_type="multipart/form-data",
        )
        self.assertEqual(applied.status_code, 200, applied.get_json())
        self.assertEqual(self.identity_store.get_summary()["record_count"], 1)
        employee = self.employee_store.get_record("E0100001")
        self.assertEqual(employee["cccd"], "012345678902")
        name_suggestion = self.client.post(
            "/api/customer-suggestion",
            json={"customer_code": "e0100001", "field": "name"},
            headers={"Origin": "http://localhost"},
        )
        self.assertEqual(name_suggestion.status_code, 200)
        self.assertEqual(name_suggestion.get_json()["suggestion"], "TEN NHAN VIEN")

    def test_admin_can_upload_arbitrarily_named_sap_file(self):
        self.login(role="admin", user_id=1)
        self.client.get("/settings")
        with self.client.session_transaction() as session:
            csrf = session["customer_import_csrf"]
        content = (
            "\tSearchTerm\tCty\tPostalCode\tCity\tName 1\tCustomer\tCoCd\tDelF\n"
            "\t0911111111\tVN\t\tCITY\tTEN MOI\t100000010\t10\n"
        ).encode("utf-8-sig")
        response = self.client.post(
            "/api/customer-import",
            data={
                "confirmed": "yes",
                "files": (io.BytesIO(content), "du lieu moi khong theo khoang.dat"),
            },
            headers={"X-CSRF-Token": csrf, "Origin": "http://localhost"},
            content_type="multipart/form-data",
        )
        self.assertEqual(response.status_code, 202)
        job_id = response.get_json()["job"]["id"]
        deadline = time.time() + 5
        job = None
        while time.time() < deadline:
            job = self.client.get(f"/api/customer-import/{job_id}").get_json()["job"]
            if job["status"] in ("completed", "failed"):
                break
            time.sleep(0.05)
        self.assertEqual(job["status"], "completed", job)
        while time.time() < deadline and app_module._get_active_customer_import_job() is not None:
            time.sleep(0.01)
        self.assertEqual(job["result"]["inserted_rows"], 1)
        self.assertEqual(job["result"]["dataset"]["max_customer"], "100000010")
        self.assertIsNotNone(self.store.get_record("100000010"))
        self.assertEqual(list((self.root / "pending-imports").glob("*")), [])
        self.assertEqual(len(list((self.root / "backups").glob("*.db"))), 1)

    def test_customer_import_requires_admin_and_csrf(self):
        self.login(role="user", user_id=2)
        self.assertEqual(self.client.get("/settings").status_code, 403)
        self.assertEqual(self.client.get("/api/customer-import/summary").status_code, 403)
        self.assertEqual(self.client.post("/api/customer-import").status_code, 403)

        self.login(role="admin", user_id=1)
        self.assertIn(b"customerImportFiles", self.client.get("/settings").data)
        response = self.client.post(
            "/api/customer-import",
            data={"confirmed": "yes"},
            content_type="multipart/form-data",
        )
        self.assertEqual(response.status_code, 403)

    def test_settings_and_default_sap_state_follow_server_role(self):
        self.login(role="user", user_id=2)
        user_index = self.client.get("/")
        user_html = user_index.get_data(as_text=True)
        self.assertEqual(user_index.status_code, 200)
        self.assertNotIn('href="/settings"', user_html)
        self.assertNotIn('id="btnOcrClip"', user_html)
        self.assertNotIn("appUrl('/api/ocr-bk')", user_html)
        self.assertIn('id="bankDropdown" class="dropdown-menu search-dropup-menu w-100"', user_html)
        self.assertIn('id="tvvDropdown" class="dropdown-menu search-dropup-menu w-100"', user_html)
        self.assertIn('placeholder="Nhập thông tin TVV"', user_html)
        self.assertNotIn("<h5>PNJ 1305</h5>", user_html)
        self.assertNotIn("<small>Phiếu xác nhận CK</small>", user_html)
        self.assertNotIn('aria-controls="sapDataCollapse"', user_html)
        self.assertNotIn('id="sapDataCollapse"', user_html)
        self.assertNotIn("Tự nhập dữ liệu SAP", user_html)

        settings_page = self.client.get("/settings")
        settings_get = self.client.get("/api/settings")
        settings_post = self.client.post(
            "/api/settings",
            json={"mb_password": "khong-duoc-ghi"},
        )
        self.assertEqual(settings_page.status_code, 403)
        self.assertEqual(settings_get.status_code, 403)
        self.assertEqual(settings_post.status_code, 403)
        self.assertEqual(self.client.post("/api/ocr-bk").status_code, 403)
        self.assertEqual(settings_get.headers.get("Cache-Control"), "no-store, max-age=0")
        connection = sqlite3.connect(self.root / "phieu.db")
        try:
            stored = connection.execute(
                "SELECT value FROM settings WHERE key = 'mb_password'"
            ).fetchone()
        finally:
            connection.close()
        self.assertTrue(stored is None or stored[0] != "khong-duoc-ghi")

        self.login(role="admin", user_id=1)
        admin_index = self.client.get("/")
        admin_html = admin_index.get_data(as_text=True)
        self.assertIn('href="/settings"', admin_html)
        self.assertIn('id="btnOcrClip"', admin_html)
        self.assertIn("appUrl('/api/ocr-bk')", admin_html)
        self.assertIn('aria-expanded="true" aria-controls="sapDataCollapse"', admin_html)
        self.assertIn('<div class="collapse show" id="sapDataCollapse">', admin_html)

        settings_page = self.client.get("/settings")
        self.assertEqual(settings_page.status_code, 200)
        self.assertEqual(settings_page.headers.get("Cache-Control"), "no-store, max-age=0")
        self.assertIn(b"customerImportFiles", settings_page.data)
        self.assertIn(b"settingsAppUrl", settings_page.data)
        self.assertIn(b"readIdentityImportResponse", settings_page.data)
        self.assertIn(
            "Kiểm tra file quá thời gian chờ của máy chủ.",
            settings_page.get_data(as_text=True),
        )
        self.assertEqual(self.client.get("/api/settings").status_code, 200)
        rejected = self.client.post(
            "/api/settings",
            json={"plant": "9999"},
            headers={"Origin": "https://khong-hop-le.example"},
        )
        self.assertEqual(rejected.status_code, 400)
        saved = self.client.post(
            "/api/settings",
            json={"plant": "1305"},
            headers={"Origin": "http://localhost"},
        )
        self.assertEqual(saved.status_code, 200)

    def test_bank_api_sends_eoffice_code_to_admin_only(self):
        self.login(role="user", user_id=2)
        response = self.client.get("/api/banks")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["data"])
        self.assertTrue(
            all("eoffice" not in bank for bank in response.get_json()["data"])
        )

        self.login(role="admin", user_id=1)
        response = self.client.get("/api/banks")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(
            any("eoffice" in bank for bank in response.get_json()["data"])
        )


if __name__ == "__main__":
    unittest.main()
