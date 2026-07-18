# -*- coding: utf-8 -*-

import json
import io
import re
import tempfile
import unittest
import zipfile
from pathlib import Path

import app as app_module
from openpyxl import load_workbook


class EofficeQt82Tests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.original_db_path = app_module.DB_PATH
        app_module.DB_PATH = str(self.root / "phieu.db")
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

    def create_phieu(self, doc_num=""):
        payload = {
            "status": "draft",
            "ngay_lap": "2026-07-16",
            "ma_kh": "100000000",
            "ten_kh": "KHACH HANG TEST",
            "sdt": "0900000000",
            "cccd": "012345678901",
            "so_tk": "123456789",
            "ten_tk": "KHACH HANG TEST",
            "ngan_hang": "OCB",
            "so_bk": "4403000001",
            "plant": "1305",
            "tong_ck": 1500000,
            "chung_tu": [
                {
                    "loai": "Bảng kê",
                    "so_ct": "4403000001",
                    "doc_num": doc_num,
                    "gia_tri": 1500000,
                    "gio": "16/07/2026 10:00",
                }
            ],
        }
        response = self.client.post("/api/save", json=payload)
        self.assertEqual(response.status_code, 200)
        return response.get_json()["id"]

    @staticmethod
    def payload_from_html(html):
        match = re.search(
            r'<script type="application/json" id="qt82DraftPayload">(.*?)</script>',
            html,
            flags=re.DOTALL,
        )
        if not match:
            raise AssertionError("Không tìm thấy payload QT82 trong HTML.")
        return json.loads(match.group(1))

    def test_eoffice_and_template_are_really_admin_only(self):
        self.login(role="admin")
        phieu_id = self.create_phieu(doc_num="2500000001")

        self.login(role="user", user_id=2)
        self.assertEqual(self.client.get("/eoffice").status_code, 403)
        self.assertEqual(self.client.get(f"/eoffice/{phieu_id}").status_code, 403)
        self.assertEqual(self.client.get(f"/api/template-tt/{phieu_id}").status_code, 403)
        self.assertEqual(self.client.get("/api/qt82-extension").status_code, 403)
        self.assertNotIn("eOffice QT82", self.client.get("/").get_data(as_text=True))
        history_html = self.client.get("/history").get_data(as_text=True)
        self.assertNotIn('title="eOffice QT82"', history_html)
        self.assertNotIn('title="Tải template TT"', history_html)

    def test_admin_can_download_current_qt82_extension_zip(self):
        self.login(role="admin")
        response = self.client.get("/api/qt82-extension")
        current_manifest = json.loads(
            (app_module.QT82_EXTENSION_DIR / "manifest.json").read_text(encoding="utf-8")
        )
        current_version = current_manifest["version"]

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("Cache-Control"), "no-store, max-age=0")
        self.assertEqual(response.headers.get("X-Content-Type-Options"), "nosniff")
        self.assertIn(
            f"PNJ-QT82-Draft-Helper-v{current_version}.zip",
            response.headers.get("Content-Disposition", ""),
        )

        with zipfile.ZipFile(io.BytesIO(response.data)) as archive:
            names = set(archive.namelist())
            expected = {
                f"PNJ-QT82-Draft-Helper/{name}"
                for name in app_module.QT82_EXTENSION_FILES
            }
            self.assertEqual(names, expected)
            manifest = json.loads(
                archive.read("PNJ-QT82-Draft-Helper/manifest.json").decode("utf-8")
            )
            self.assertEqual(manifest["version"], current_version)

    def test_extension_uses_workflow_catalog_before_filling_qt82(self):
        extension_dir = app_module.QT82_EXTENSION_DIR
        background = (extension_dir / "background.js").read_text(encoding="utf-8")
        filler = (extension_dir / "eoffice-fill.js").read_text(encoding="utf-8")
        manifest = json.loads((extension_dir / "manifest.json").read_text(encoding="utf-8"))

        self.assertEqual(manifest["version"], "0.1.16")
        self.assertIn("/workflow/sitepages/createworkflow.aspx?rcid=8&rscid=0&wid=0", background)
        self.assertIn('message.draft.openMode === "deeplink" ? message.draft.formUrl : CREATE_WORKFLOW_URL', background)
        self.assertIn('message.type === "DISABLE_HELPER"', background)
        self.assertIn('text === "qt82 quy trinh thanh toan"', filler)
        self.assertIn("sessionStorage.setItem(HUB_CLICK_NONCE_KEY, nonce)", filler)
        self.assertIn("function isQt82FormPage()", filler)
        self.assertIn("function redirectForDraft(draft)", filler)
        self.assertIn("function directHrefFromTarget(target)", filler)
        self.assertIn("function dispatchDoubleClick(target)", filler)
        self.assertIn('disable.textContent = "Tắt helper"', filler)
        self.assertIn("handleDraftOnCurrentPage(activeDraft, false)", filler)
        html = self.client.get(f"/eoffice/{self.create_phieu()}").get_data(as_text=True)
        self.assertIn("preparingQt82 = false;", html)
        self.assertIn("if (prepareButton && qt82Draft && qt82Draft.ready) prepareButton.disabled = false;", html)

    def test_eoffice_index_redirects_to_latest_existing_phieu(self):
        self.login(role="admin")
        older_id = self.create_phieu(doc_num="2500000001")
        latest_id = self.create_phieu(doc_num="2500000002")

        response = self.client.get("/eoffice", follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith(f"/eoffice/{latest_id}"))

        self.client.delete(f"/api/delete/{latest_id}")
        response = self.client.get("/eoffice", follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith(f"/eoffice/{older_id}"))

    def test_missing_sap_document_uses_1234_without_falling_back_to_bk(self):
        self.login(role="admin")
        phieu_id = self.create_phieu(doc_num="")
        response = self.client.get(f"/eoffice/{phieu_id}")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("Cache-Control"), "no-store, max-age=0")
        payload = self.payload_from_html(response.get_data(as_text=True))

        self.assertEqual(payload["sapDocument"], "1234")
        self.assertTrue(payload["sapPlaceholder"])
        self.assertEqual(payload["desiredDateMode"], "browser_today")
        self.assertNotEqual(payload["sapDocument"], "4403000001")
        self.assertEqual(payload["bankQuery"], "79333001-")
        self.assertTrue(payload["ready"])

    def test_explicit_sap_document_and_qt82_defaults_are_preserved(self):
        self.login(role="admin")
        phieu_id = self.create_phieu(doc_num="2500000001")
        payload = self.payload_from_html(
            self.client.get(f"/eoffice/{phieu_id}").get_data(as_text=True)
        )

        self.assertEqual(payload["sapDocument"], "2500000001")
        self.assertFalse(payload["sapPlaceholder"])
        self.assertEqual(payload["purpose"], "Thanh toán cho khách hàng(Mua lại)")
        self.assertEqual(payload["currency"], "VND")
        self.assertEqual(payload["managerApproval"], "Có")
        self.assertEqual(payload["paymentMethod"], "Bank transfer – Chuyển khoản")
        self.assertEqual(payload["costGroup"], "Hàng hóa(ML)")
        self.assertEqual(payload["storeManagerQuery"], "my.hth")
        self.assertEqual(payload["detailDocuments"], ["4403000001"])
        self.assertEqual(payload["formUrl"], app_module.DEFAULT_QT82_FORM_URL)

    def test_preflight_check_is_rendered_below_customer_and_qt82_actions(self):
        self.login(role="admin")
        phieu_id = self.create_phieu(doc_num="2500000001")
        html = self.client.get(f"/eoffice/{phieu_id}").get_data(as_text=True)

        customer_card = html.index('<i class="bi bi-clipboard-data"></i>')
        prepare_button = html.index('id="btnPrepareQt82"')
        preflight_card = html.index('<strong>Kiểm tra trước khi tạo QT82</strong>')

        self.assertLess(customer_card, prepare_button)
        self.assertLess(prepare_button, preflight_card)

    def test_history_navigation_derives_reverse_proxy_prefix_from_browser_path(self):
        self.login(role="admin")
        phieu_id = self.create_phieu(doc_num="2500000001")
        html = self.client.get(f"/eoffice/{phieu_id}").get_data(as_text=True)

        self.assertIn("window.location.pathname.match", html)
        self.assertIn("const historyApiUrl = appPath('/api/history');", html)
        self.assertIn("link.href = appPath('/eoffice/' + p.id);", html)
        self.assertIn("fetch(appPath('/api/da-trinh/' + e.target.dataset.id)", html)
        self.assertNotIn("link.href = '/eoffice/'", html)
        self.assertNotIn("const historyApiUrl = \"/api/history\";", html)
        self.assertIn("Không tải được lịch sử phiếu", html)

    def test_qt82_form_url_can_change_only_within_pnj_eoffice_workflow(self):
        self.login(role="admin")
        phieu_id = self.create_phieu(doc_num="2500000001")
        new_url = (
            "https://eoffice.pnj.com.vn/workflow/SitePages/NewWorkflow.aspx"
            "?mode=1&LID=NEW-LIST-ID&wid=9999"
        )
        saved = self.client.post(
            "/api/settings",
            json={"qt82_form_url": new_url},
            headers={"Origin": "http://localhost"},
        )
        self.assertEqual(saved.status_code, 200)
        payload = self.payload_from_html(
            self.client.get(f"/eoffice/{phieu_id}").get_data(as_text=True)
        )
        self.assertEqual(payload["formUrl"], new_url)

        rejected = self.client.post(
            "/api/settings",
            json={"qt82_form_url": "https://example.com/workflow/fake"},
            headers={"Origin": "http://localhost"},
        )
        self.assertEqual(rejected.status_code, 400)

    def test_template_tt_is_xlsx_no_store_and_contains_expected_detail(self):
        self.login(role="admin")
        phieu_id = self.create_phieu(doc_num="2500000001")

        response = self.client.get(f"/api/template-tt/{phieu_id}")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("Cache-Control"), "no-store, max-age=0")
        self.assertEqual(response.headers.get("Pragma"), "no-cache")
        self.assertEqual(response.headers.get("X-Content-Type-Options"), "nosniff")
        self.assertTrue(response.data.startswith(b"PK"))
        workbook = load_workbook(io.BytesIO(response.data), data_only=False)
        sheet = workbook["Sheet1"]
        self.assertEqual(sheet.cell(row=5, column=2).value, "Bảng kê")
        self.assertEqual(sheet.cell(row=5, column=3).value, 1500000)
        self.assertEqual(sheet.cell(row=5, column=4).value, "4403000001")
        self.assertEqual(sheet.cell(row=5, column=5).value, "012345678901")


if __name__ == "__main__":
    unittest.main()
