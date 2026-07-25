import json
import io
import os
import tempfile
import unittest
from unittest import mock
from pathlib import Path

import app as app_module
import erp_business_partner


class ErpBusinessPartnerTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.original_fixture = os.environ.get("PNJ_ERP_BP_FIXTURE_PATH")
        self.original_user = os.environ.get("PNJ_ERP_USER")
        self.original_password = os.environ.get("PNJ_ERP_PASSWORD")
        self.original_require_login = app_module.REQUIRE_LOGIN
        self.original_sitekey = app_module.CUSTOMER_LOOKUP_TURNSTILE_SITEKEY
        self.original_secret = app_module.CUSTOMER_LOOKUP_TURNSTILE_SECRET
        os.environ.pop("PNJ_ERP_USER", None)
        os.environ.pop("PNJ_ERP_PASSWORD", None)
        self.client = app_module.app.test_client()
        app_module.app.config.update(TESTING=True)

    def tearDown(self):
        if self.original_fixture is None:
            os.environ.pop("PNJ_ERP_BP_FIXTURE_PATH", None)
        else:
            os.environ["PNJ_ERP_BP_FIXTURE_PATH"] = self.original_fixture
        if self.original_user is None:
            os.environ.pop("PNJ_ERP_USER", None)
        else:
            os.environ["PNJ_ERP_USER"] = self.original_user
        if self.original_password is None:
            os.environ.pop("PNJ_ERP_PASSWORD", None)
        else:
            os.environ["PNJ_ERP_PASSWORD"] = self.original_password
        app_module.REQUIRE_LOGIN = self.original_require_login
        app_module.CUSTOMER_LOOKUP_TURNSTILE_SITEKEY = self.original_sitekey
        app_module.CUSTOMER_LOOKUP_TURNSTILE_SECRET = self.original_secret
        self.temp_dir.cleanup()

    def write_fixture(self, records):
        path = self.root / "business_partners.json"
        path.write_text(json.dumps({"records": records}, ensure_ascii=False), encoding="utf-8")
        os.environ["PNJ_ERP_BP_FIXTURE_PATH"] = str(path)

    def test_maps_business_partner_profile_fields(self):
        profile = erp_business_partner.public_business_partner_profile(
            {
                "BusinessPartner": "100065309",
                "FirstName": "LÊ NGHI GIÁNG",
                "LastName": "HƯƠNG",
                "MobilePhoneNumber": "0983156393",
                "IdentificationNumber": "046166004673",
                "StreetName": "18A TRẦN BÌNH TRỌNG",
                "Ward": "THUẬN HÒA",
                "District": "TP HUẾ",
                "CityName": "THỪA THIÊN HUẾ",
                "RegionName": "Hưng Yên-Xã Ngự Thiên",
                "BirthDate": "1966-08-19",
            }
        )

        self.assertEqual(profile["customer_code"], "100065309")
        self.assertEqual(profile["name"], "LÊ NGHI GIÁNG HƯƠNG")
        self.assertEqual(profile["phone"], "0983156393")
        self.assertEqual(profile["cccd"], "046166004673")
        self.assertEqual(profile["birth_date"], "1966-08-19")
        self.assertEqual(
            profile["address"],
            "18A TRẦN BÌNH TRỌNG, THUẬN HÒA, TP HUẾ, THỪA THIÊN HUẾ",
        )

    def test_uses_district_name_when_district_is_code(self):
        profile = erp_business_partner.public_business_partner_profile(
            {
                "BusinessPartner": "104615653",
                "FullName": "NGUYỄN THỊ PHƯƠNG HIỀN",
                "PhoneNumber": "0935223346",
                "cccd": "044184009314",
                "StreetName": "TỔ 6",
                "Ward": "PHƯỜNG THỦY XUÂN",
                "District": "07",
                "DistrictName": "PHƯỜNG THỦY XUÂN",
                "CityName": "THÀNH PHỐ HUẾ",
            }
        )

        self.assertEqual(profile["district"], "PHƯỜNG THỦY XUÂN")
        self.assertEqual(
            profile["address"],
            "TỔ 6, PHƯỜNG THỦY XUÂN, THÀNH PHỐ HUẾ",
        )

    def test_api_returns_fixture_profile(self):
        self.write_fixture(
            [
                {
                    "BusinessPartner": "100065309",
                    "BusinessPartnerFullName": "LÊ NGHI GIÁNG HƯƠNG",
                    "PhoneNumber": "0983156393",
                    "CCCD": "046166004673",
                    "Street": "18A TRẦN BÌNH TRỌNG",
                    "Ward": "THUẬN HÒA",
                    "DistrictName": "TP HUẾ",
                    "City": "THỪA THIÊN HUẾ",
                }
            ]
        )

        response = self.client.post(
            "/api/erp-business-partner-profile",
            json={"customer_code": "0100065309"},
        )

        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["profile"]["customer_code"], "100065309")
        self.assertEqual(data["profile"]["name"], "LÊ NGHI GIÁNG HƯƠNG")
        self.assertEqual(data["profile"]["cccd"], "046166004673")
        self.assertEqual(data["profile"]["source"], "fixture")

    def test_index_contains_erp_business_partner_flow(self):
        html = self.client.get("/").get_data(as_text=True)

        self.assertIn("/api/erp-business-partner-profile", html)
        self.assertIn("fetchErpBusinessPartnerProfile", html)
        self.assertIn("erpBusinessPartnerProfileCache", html)

    def test_bieu_mau_f1_contains_bp_lookup_without_printing_customer_code(self):
        html = self.client.get("/bieu-mau").get_data(as_text=True)

        self.assertIn('id="f1_ma_kh"', html)
        self.assertNotIn('id="f1BpStatus"', html)
        self.assertIn("/api/erp-business-partner-profile", html)
        self.assertIn("fetchF1BusinessPartnerProfile", html)
        self.assertIn('id="btnPdfF1"', html)
        self.assertIn('id="btnPdfF2"', html)
        self.assertIn('id="btnPdfBBHuy"', html)
        self.assertIn('id="btnPdfCaoHml"', html)
        self.assertIn('id="f1BpCaptchaModal"', html)
        self.assertIn("<Đang tải...>", html)
        self.assertIn("birth_date", html)
        self.assertIn(r"text.match(/^(\d{4})(\d{2})(\d{2})$/)", html)

        print_start = html.index("document.getElementById('btnPrintF1')")
        print_end = html.index("document.getElementById('btnPrintF2')")
        print_handler = html[print_start:print_end]
        self.assertNotIn("f1_ma_kh", print_handler)

    def test_cao_hml_print_uses_wider_product_code_column(self):
        html = self.client.get("/cao-hml/print").get_data(as_text=True)

        self.assertIn(".col-ma { text-align: center; width: 30%; }", html)
        self.assertIn(".col-desc { width: 21%; }", html)

    def test_bieu_mau_is_public_when_login_is_required(self):
        app_module.REQUIRE_LOGIN = True

        response = self.client.get("/bieu-mau")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Biểu Mẫu", response.get_data(as_text=True))

    def test_bieu_mau_bp_lookup_requires_captcha_after_three_unique_codes(self):
        app_module.CUSTOMER_LOOKUP_TURNSTILE_SITEKEY = "site-key"
        app_module.CUSTOMER_LOOKUP_TURNSTILE_SECRET = "secret"

        for code in ("100000001", "100000002", "100000003"):
            response = self.client.post("/api/erp-business-partner-profile", json={"customer_code": code})
            self.assertEqual(response.status_code, 200)

        response = self.client.post("/api/erp-business-partner-profile", json={"customer_code": "100000004"})

        self.assertEqual(response.status_code, 403)
        self.assertTrue(response.get_json()["captcha_required"])

    def test_bieu_mau_pdf_routes_render_from_print_html(self):
        fake_pdf = io.BytesIO(b"%PDF-1.4\n")
        fake_pdf.seek(0)
        with mock.patch.object(app_module, "make_pdf_from_print_html", return_value=fake_pdf) as renderer:
            response = self.client.get("/doi-thongtin/pdf-f1?ten_cu=TRAN%20VAN%20A")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, "application/pdf")
        self.assertGreater(renderer.call_count, 0)
        rendered_html = renderer.call_args.args[0]
        self.assertIn("TRAN VAN A", rendered_html)

    def test_material_proposal_form_and_print_rules(self):
        html = self.client.get("/bieu-mau").get_data(as_text=True)

        self.assertIn('id="rbMaterialProposal"', html)
        self.assertIn('id="btnPrintMaterialProposal"', html)
        self.assertIn('id="btnPdfMaterialProposal"', html)
        self.assertIn("31000404", html)
        self.assertIn("31000403", html)
        self.assertIn("['31000403', 'Vảy hàn 3330 (hội 335)']", html)
        self.assertIn("['31000820', 'Vảy hàn 3330 (hội 334)']", html)
        self.assertIn("['31000204', 'NL tinh màu vàng 4160']", html)
        self.assertIn("['32000090', 'NL tinh bạc 8000']", html)
        self.assertIn("+ ' - ' + item[1]", html)
        self.assertIn("material-quantity-input", html)
        self.assertNotIn("material-weight-input", html)
        self.assertLess(html.index("['31000403', 'Vảy hàn"), html.index("['31000820', 'Vảy hàn"))
        self.assertLess(html.index("['31000820', 'Vảy hàn"), html.index("['31000404', 'Vảy hàn"))
        self.assertLess(html.index("['31000403', 'Vảy hàn"), html.index("['31000204', 'NL tinh"))
        self.assertLess(html.index("['31000204', 'NL tinh"), html.index("['32000090', 'NL tinh bạc"))

        print_html = self.client.get(
            "/de-xuat-nguyen-lieu/print?"
            "purpose=bao_hanh&material_codes=31000204%0A31000403"
            "&quantities=1.2%0A2.3&ngay=25&thang=07&nam=2026"
        ).get_data(as_text=True)

        self.assertIn("size: A5 landscape", print_html)
        self.assertIn("form-header", print_html)
        self.assertIn("/static/logo_pnj.webp", print_html)
        self.assertIn("MS: PNJ-QYĐ-PHC-VC-GNVC-F6", print_html)
        self.assertIn("LSX : 04/00", print_html)
        self.assertIn("NHL: 04/08/2017", print_html)
        self.assertIn("TRANG : 1/1", print_html)
        self.assertIn("Số: ...../1305-2026", print_html)
        self.assertIn("Đề xuất nguyên liệu làm hàng bảo hành.", print_html)
        self.assertIn("Căn cứ theo:</span> Nhu cầu thực tế tại cửa hàng", print_html)
        self.assertIn("Xuất tại kho:</span> 1203", print_html)
        self.assertIn("Kho, đơn vị nhận:</span>1204", print_html)
        self.assertIn("NL tinh màu vàng 4160", print_html)
        self.assertIn("Vảy hàn 3330", print_html)
        self.assertGreaterEqual(print_html.count("<tr>"), 5)
        self.assertIn("<strong>Tổng Cộng</strong>", print_html)
        self.assertIn(">3.5<", print_html)
        self.assertIn("Bằng chữ", print_html)
        self.assertIn("Trưởng đơn vị", print_html)
        self.assertNotIn("Đề xuất nguyên liệu nl tinh", print_html)
        self.assertNotIn("NLT VÀNG VHÀN KQT", print_html)

        xu_ly_html = self.client.get(
            "/de-xuat-nguyen-lieu/print?"
            "purpose=xu_ly&material_codes=31000820%0A32000090"
            "&quantities=1%0A2&ngay=25&thang=07&nam=2026"
        ).get_data(as_text=True)
        self.assertIn("Đề xuất nguyên liệu làm hàng xử lí.", xu_ly_html)
        self.assertIn("Vảy hàn 3330 (hội 334)", xu_ly_html)
        self.assertIn("NL tinh bạc 8000", xu_ly_html)


if __name__ == "__main__":
    unittest.main()
