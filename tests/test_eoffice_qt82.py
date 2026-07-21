# -*- coding: utf-8 -*-

import json
import io
import re
import tempfile
import unittest
import zipfile
from pathlib import Path

import app as app_module
from customer_lookup import CustomerLookupStore
from employee_lookup import EmployeeLookupStore
from openpyxl import load_workbook


class EofficeQt82Tests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.original_db_path = app_module.DB_PATH
        self.original_store = app_module._customer_lookup_store
        self.original_employee_store = app_module._employee_lookup_store
        app_module.DB_PATH = str(self.root / "phieu.db")
        app_module.init_db()
        self.store = CustomerLookupStore(self.root / "lookup.db", bytes(range(32)))
        self.store.initialize()
        app_module._customer_lookup_store = self.store
        self.employee_store = EmployeeLookupStore(self.root / "employee.db", bytes(range(32)))
        self.employee_store.initialize()
        app_module._employee_lookup_store = self.employee_store
        app_module.app.config.update(TESTING=True)
        self.client = app_module.app.test_client()

    def tearDown(self):
        app_module.DB_PATH = self.original_db_path
        app_module._customer_lookup_store = self.original_store
        app_module._employee_lookup_store = self.original_employee_store
        self.temp_dir.cleanup()

    def login(self, role="admin", user_id=1):
        with self.client.session_transaction() as session:
            session["user_id"] = user_id
            session["user_name"] = "ADMIN TEST"
            session["role"] = role

    def create_phieu(self, doc_num="", amount=1500000):
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
            "tong_ck": amount,
            "chung_tu": [
                {
                    "loai": "Bảng kê",
                    "so_ct": "4403000001",
                    "doc_num": doc_num,
                    "gia_tri": amount,
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

        self.assertEqual(manifest["version"], "0.1.17")
        self.assertIn("/workflow/sitepages/createworkflow.aspx?rcid=8&rscid=0&wid=0", background)
        self.assertIn('message.draft.openMode === "deeplink" ? message.draft.formUrl : CREATE_WORKFLOW_URL', background)
        self.assertIn('message.type === "DISABLE_HELPER"', background)
        self.assertIn('text === "qt82 quy trinh thanh toan"', filler)
        self.assertIn("sessionStorage.setItem(HUB_CLICK_NONCE_KEY, nonce)", filler)
        self.assertIn("function isQt82FormPage()", filler)
        self.assertIn("function redirectForDraft(draft)", filler)
        self.assertIn("function directHrefFromTarget(target)", filler)
        self.assertIn("function dispatchDoubleClick(target)", filler)
        self.assertIn("function endCurrentSession(panel, disableHelper)", filler)
        self.assertIn('clear.textContent = activeDraft ? "Xóa dữ liệu tạm" : "Xong/Đóng"', filler)
        self.assertIn('disable.textContent = "Tắt helper"', filler)
        self.assertIn("handleDraftOnCurrentPage(activeDraft, false)", filler)
        self.assertNotIn('renderStatus("Tiện ích đã chạy. Đang tìm bản nháp QT82...', filler)
        self.assertNotIn("Không tìm thấy bản nháp QT82. Quay lại website", filler)
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

    def test_admin_can_override_sap_document_for_qt82(self):
        self.login(role="admin")
        phieu_id = self.create_phieu(doc_num="")

        response = self.client.post(
            f"/api/phieu/{phieu_id}/sap-document",
            json={"sap_document": "2500000001"},
            headers={"Origin": "http://localhost"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["sap_document"], "2500000001")

        html = self.client.get(f"/eoffice/{phieu_id}").get_data(as_text=True)
        payload = self.payload_from_html(html)
        self.assertEqual(payload["sapDocument"], "2500000001")
        self.assertFalse(payload["sapPlaceholder"])
        self.assertIn('id="btnSaveSapDocument"', html)
        self.assertIn("tr.addEventListener('dblclick'", html)

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

    def test_clean_html_print_token_hides_row_id_from_print_url(self):
        self.login(role="admin")
        phieu_id = self.create_phieu(doc_num="2500000001")
        api_response = self.client.get("/api/history").get_json()
        phieu = next(item for item in api_response["data"] if item["id"] == phieu_id)
        token = phieu["pdf_token"]

        index_html = self.client.get("/").get_data(as_text=True)
        history_html = self.client.get("/history").get_data(as_text=True)
        self.assertIn("appUrl('/p/' + encodeURIComponent(data.pdf_token) + '?print=1')", index_html)
        self.assertIn("'p/' + encodeURIComponent(p.pdf_token)", history_html)

        self.login(role="user", user_id=2)
        response = self.client.get(f"/p/{token}?print=1")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Phiếu xác nhận thông tin thanh toán chuyển khoản", html)
        self.assertIn(f'href="/api/pdf/{phieu_id}?token=', html)
        self.assertNotIn(f"/api/print/{phieu_id}", f"/p/{token}?print=1")

    def test_print_uses_five_stage_payment_schedule(self):
        self.login(role="admin")
        phieu_id = self.create_phieu(amount=143271123)

        html = self.client.get(f"/api/print/{phieu_id}").get_data(as_text=True)

        self.assertIn("1. T+0/1: 10% - tương ứng số tiền", html)
        self.assertIn("<strong>14,327,112 đồng</strong>", html)
        self.assertIn("2. T+30: 20% - tương ứng số tiền", html)
        self.assertIn("<strong>28,654,225 đồng</strong>", html)
        self.assertIn("3. T+60: 25% - tương ứng số tiền", html)
        self.assertIn("<strong>35,817,781 đồng</strong>", html)
        self.assertIn("4. T+90: 25% - tương ứng số tiền", html)
        self.assertIn("5. T+120: 20% - tương ứng số tiền", html)
        self.assertNotIn("Thanh toán trong ngày (T)", html)
        self.assertNotIn("Thanh toán vào ngày kế tiếp (T+1)", html)


if __name__ == "__main__":
    unittest.main()
