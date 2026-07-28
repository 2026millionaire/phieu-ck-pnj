import json
import io
import os
import re
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

    def test_maps_business_partner_profile_fields_with_unicode_input(self):
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
        self.assertEqual(profile["region"], "Hưng Yên-Xã Ngự Thiên")
        self.assertEqual(
            profile["address"],
            "18A TRẦN BÌNH TRỌNG, THUẬN HÒA, TP HUẾ, THỪA THIÊN HUẾ, Hưng Yên-Xã Ngự Thiên",
        )

    def test_maps_business_partner_profile_fields(self):
        profile = erp_business_partner.public_business_partner_profile(
            {
                "BusinessPartner": "100065309",
                "FirstName": "LE NGHI GIANG",
                "LastName": "HUONG",
                "MobilePhoneNumber": "0983156393",
                "IdentificationNumber": "046166004673",
                "StreetName": "18A TRAN BINH TRONG",
                "Ward": "THUAN HOA",
                "District": "TP HUE",
                "CityName": "THUA THIEN HUE",
                "RegionName": "HUNG YEN-XA NGU THIEN",
                "BirthDate": "1966-08-19",
            }
        )

        self.assertEqual(profile["customer_code"], "100065309")
        self.assertEqual(profile["name"], "LE NGHI GIANG HUONG")
        self.assertEqual(profile["phone"], "0983156393")
        self.assertEqual(profile["cccd"], "046166004673")
        self.assertEqual(profile["birth_date"], "1966-08-19")
        self.assertEqual(profile["region"], "HUNG YEN-XA NGU THIEN")
        self.assertEqual(
            profile["address"],
            "18A TRAN BINH TRONG, THUAN HOA, TP HUE, THUA THIEN HUE, HUNG YEN-XA NGU THIEN",
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

    def test_maps_nested_identification_number_and_nested_address_parts(self):
        profile = erp_business_partner.public_business_partner_profile(
            {
                "d": {
                    "BusinessPartner": "100000028",
                    "to_Identity": {
                        "results": [
                            {"IdentificationNumber": "0123 4567 8928"}
                        ]
                    },
                    "to_Address": {
                        "results": [
                            {
                                "HouseNumberAndStreet": "18A TRAN BINH TRONG",
                                "DistrictName3": "PHUONG THUAN HOA",
                                "District": "01",
                                "DistrictName": "TP HUE",
                                "City": "THUA THIEN HUE",
                                "Region": "VIET NAM",
                            }
                        ]
                    },
                }
            }
        )

        self.assertEqual(profile["customer_code"], "100000028")
        self.assertEqual(profile["cccd"], "012345678928")
        self.assertEqual(profile["street"], "18A TRAN BINH TRONG")
        self.assertEqual(profile["ward"], "PHUONG THUAN HOA")
        self.assertEqual(profile["district"], "TP HUE")
        self.assertEqual(profile["city"], "THUA THIEN HUE")
        self.assertEqual(profile["region"], "VIET NAM")
        self.assertEqual(
            profile["address"],
            "18A TRAN BINH TRONG, PHUONG THUAN HOA, TP HUE, THUA THIEN HUE, VIET NAM",
        )

    def test_drops_region_when_it_only_repeats_existing_address_parts(self):
        cases = [
            (
                {
                    "StreetName": "00",
                    "Ward": "PHƯỜNG THANH THỦY",
                    "CityName": "THÀNH PHỐ HUẾ",
                    "RegionName": "THÀNH PHỐ HUẾ-PHƯỜNG THANH THỦY",
                },
                "00, PHƯỜNG THANH THỦY, THÀNH PHỐ HUẾ",
            ),
            (
                {
                    "StreetName": "34 AN DƯƠNG VƯƠNG",
                    "Ward": "PHƯỜNG THUẬN HÓA",
                    "CityName": "THÀNH PHỐ HUẾ",
                    "RegionName": "THÀNH PHỐ HUẾ - PHƯỜNG THUẬN HÓA",
                },
                "34 AN DƯƠNG VƯƠNG, PHƯỜNG THUẬN HÓA, THÀNH PHỐ HUẾ",
            ),
            (
                {
                    "StreetName": "THÔN BAO VINH",
                    "Ward": "PHƯỜNG HÓA CHÂU",
                    "CityName": "THÀNH PHỐ HUẾ",
                    "RegionName": "PHƯỜNG HÓA CHÂU, THÀNH PHỐ HUẾ",
                },
                "THÔN BAO VINH, PHƯỜNG HÓA CHÂU, THÀNH PHỐ HUẾ",
            ),
            (
                {
                    "StreetName": "60NGÔ ĐỨC KẾ",
                    "Ward": "PHƯỜNG THUẬN HÓA",
                    "CityName": "THÀNH PHỐ HUẾ",
                    "RegionName": "THÀNH PHỐ HUẾ/PHƯỜNG THUẬN HÓA",
                },
                "60NGÔ ĐỨC KẾ, PHƯỜNG THUẬN HÓA, THÀNH PHỐ HUẾ",
            ),
        ]

        for record, expected in cases:
            with self.subTest(region=record["RegionName"]):
                profile = erp_business_partner.public_business_partner_profile(record)
                self.assertEqual(profile["address"], expected)

    def test_drops_redundant_region_from_nested_address_payload(self):
        profile = erp_business_partner.public_business_partner_profile(
            {
                "d": {
                    "BusinessPartner": "100000029",
                    "to_Address": {
                        "results": [
                            {
                                "HouseNumberAndStreet": "00",
                                "DistrictName3": "PHƯỜNG THANH THỦY",
                                "City": "THÀNH PHỐ HUẾ",
                                "Region": "THÀNH PHỐ HUẾ-PHƯỜNG THANH THỦY",
                            }
                        ]
                    },
                }
            }
        )

        self.assertEqual(profile["address"], "00, PHƯỜNG THANH THỦY, THÀNH PHỐ HUẾ")

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

    def test_provider_uses_erp_session_when_credentials_are_configured(self):
        os.environ["PNJ_ERP_USER"] = "demo-user"
        os.environ["PNJ_ERP_PASSWORD"] = "demo-password"

        payload = {
            "d": {
                "BusinessPartner": "100000028",
                "BusinessPartnerFullName": "KHACH HANG MAU",
                "IdentificationNumber": "012345678928",
                "StreetName": "18A TRAN BINH TRONG",
                "Ward": "THUAN HOA",
                "DistrictName": "TP HUE",
                "CityName": "THUA THIEN HUE",
                "RegionName": "VIET NAM",
            }
        }

        class FakeResponse:
            def raise_for_status(self):
                return None

            def json(self):
                return payload

        class FakeSession:
            def __init__(self):
                self.calls = []

            def get(self, url, timeout=None, headers=None):
                self.calls.append({"url": url, "timeout": timeout, "headers": headers})
                return FakeResponse()

        fake_session = FakeSession()
        with mock.patch.object(erp_business_partner, "login_erp_session", return_value=fake_session):
            profile = erp_business_partner.business_partner_profile("0100000028")

        self.assertEqual(profile["customer_code"], "100000028")
        self.assertEqual(profile["cccd"], "012345678928")
        self.assertEqual(profile["address"], "18A TRAN BINH TRONG, THUAN HOA, TP HUE, THUA THIEN HUE, VIET NAM")
        self.assertEqual(profile["source"], "erp")
        self.assertTrue(fake_session.calls)
        self.assertIn("/sap/opu/odata/sap/ZGW_RE_BPCREATE_SRV/", fake_session.calls[0]["url"])
        self.assertEqual(fake_session.calls[0]["headers"], {"Accept": "application/json"})

    def test_api_returns_503_when_erp_provider_fails(self):
        with mock.patch.object(erp_business_partner, "business_partner_profile", side_effect=RuntimeError("ERP down")):
            response = self.client.post(
                "/api/erp-business-partner-profile",
                json={"customer_code": "100000028"},
            )

        self.assertEqual(response.status_code, 503)
        data = response.get_json()
        self.assertFalse(data["ok"])
        self.assertIn("Business Partner", data["error"])

    def test_index_contains_erp_business_partner_flow(self):
        html = self.client.get("/").get_data(as_text=True)

        self.assertIn("/api/erp-business-partner-profile", html)
        self.assertIn("fetchErpBusinessPartnerProfile", html)
        self.assertIn("erpBusinessPartnerProfileCache", html)
        self.assertIn("if (requestedCode && currentCode !== requestedCode.replace(/^0+/, '')) return;", html)
        self.assertIn("!nameInput.value.trim() && profileName", html)
        self.assertIn("!phoneInput.value.trim() && profile.phone", html)
        self.assertIn("!cccdInput.value.trim() && profile.cccd", html)
        self.assertIn("!addressInput.value.trim() && profile.address", html)
        self.assertIn("nameInput.value = profileName", html)
        self.assertIn("phoneInput.value = profile.phone", html)
        self.assertIn("cccdInput.value = profile.cccd", html)
        self.assertIn("addressInput.value = profile.address", html)

    def test_bieu_mau_f1_contains_bp_lookup_without_printing_customer_code(self):
        html = self.client.get("/bieu-mau").get_data(as_text=True)

        self.assertIn('id="f1_ma_kh"', html)
        self.assertIn('id="f2_ma_kh"', html)
        self.assertNotIn('id="f1BpStatus"', html)
        self.assertIn("/api/erp-business-partner-profile", html)
        self.assertIn("fetchF1BusinessPartnerProfile", html)
        self.assertIn("fetchF2BusinessPartnerProfile", html)
        self.assertIn("setF2BpLoading", html)
        self.assertIn("applyF2BusinessPartnerProfile", html)
        self.assertIn("profile.cccd", html)
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

        f2_section = html[html.index('id="sectionF2"'):html.index('id="btnPrintF2"')]
        self.assertLess(f2_section.index('id="f2_ma_kh"'), f2_section.index('id="f2_cccd"'))
        self.assertLess(f2_section.index('id="f2_cccd"'), f2_section.index('id="f2_sdt"'))
        self.assertLess(f2_section.index('id="f2_sdt"'), f2_section.index('id="f2_ho_ten"'))

        f2_print_start = html.index("function buildF2Params()")
        f2_print_end = html.index("function buildCaoHmlParams()")
        f2_handler = html[f2_print_start:f2_print_end]
        self.assertIn("f2_ma_kh", f2_handler)

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
        def fake_pdf(*_args, **_kwargs):
            pdf = io.BytesIO(b"%PDF-1.4\n")
            pdf.seek(0)
            return pdf

        with mock.patch.object(app_module, "make_pdf_from_print_html", side_effect=fake_pdf) as renderer:
            response = self.client.get("/doi-thongtin/pdf-f1?ten_cu=TRAN%20VAN%20A&ngay=25&thang=07&nam=2026")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, "application/pdf")
        self.assertRegex(
            response.headers.get("Content-Disposition", ""),
            r"F1 XLDL 25\.07\.2026_[A-Za-z0-9]{4}\.pdf",
        )
        self.assertGreater(renderer.call_count, 0)
        rendered_html = renderer.call_args.args[0]
        self.assertIn("TRAN VAN A", rendered_html)

        routes = [
            ("/bb-huy/pdf?ngay=25&thang=07&nam=2026", "BB Huy BK"),
            ("/doi-thongtin/pdf-f2?ngay=25&thang=07&nam=2026", "F2 Khoa DL"),
            ("/cao-hml/pdf?ngay=25&thang=07&nam=2026", "Kiem tra HML"),
            ("/de-xuat-nguyen-lieu/pdf?ngay=25&thang=07&nam=2026", "De xuat NL"),
        ]
        with mock.patch.object(app_module, "make_pdf_from_print_html", side_effect=fake_pdf):
            for route, prefix in routes:
                response = self.client.get(route)
                self.assertEqual(response.status_code, 200)
                self.assertRegex(
                    response.headers.get("Content-Disposition", ""),
                    re.escape(prefix) + r" 25\.07\.2026_[A-Za-z0-9]{4}\.pdf",
                )

    def test_material_proposal_form_and_print_rules(self):
        html = self.client.get("/bieu-mau").get_data(as_text=True)

        self.assertIn('id="rbMaterialProposal"', html)
        self.assertIn('id="btnPrintMaterialProposal"', html)
        self.assertIn('id="btnPdfMaterialProposal"', html)
        self.assertIn("31000404", html)
        self.assertIn("31000425", html)
        self.assertIn("31000403", html)
        self.assertIn("Đề xuất nguyên liệu làm hàng <strong>xử lý</strong>", html)
        self.assertIn("Đề xuất nguyên liệu làm hàng <strong>bảo hành</strong>", html)
        self.assertIn('id="materialPurposeCustom"', html)
        self.assertIn(".................... (tự ghi)", html)
        self.assertIn("normalizeMaterialQuantity", html)
        self.assertIn("material-picker-cell", html)
        self.assertNotIn("Bản in sẽ dùng tên SAP chuẩn", html)
        self.assertIn("buttonName: 'VH 416'", html)
        self.assertIn("buttonName: 'VH bạc'", html)
        self.assertIn("buttonName: 'NLT 333'", html)
        self.assertIn("dropdown-toggle material-pick-btn", html)
        self.assertIn("material-choice-menu", html)
        self.assertIn("material-choice-option", html)
        self.assertIn("material-type-vh-gold", html)
        self.assertIn("material-type-vh-silver", html)
        self.assertIn("material-type-nlt-gold", html)
        self.assertIn("material-mobile-stt", html)
        self.assertIn("material-quantity-input", html)
        self.assertNotIn("material-selected-name", html)
        self.assertNotIn("material-button-grid", html)
        self.assertNotIn("material-code-select", html)
        self.assertNotIn("material-weight-input", html)
        self.assertLess(html.index("code: '31000820'"), html.index("code: '31000204'"))
        self.assertLess(html.index("code: '31000204'"), html.index("code: '31000237'"))
        self.assertLess(html.index("code: '31000243'"), html.index("code: '31000403'"))
        self.assertLess(html.index("code: '31000403'"), html.index("code: '31000404'"))
        self.assertLess(html.index("code: '31000425'"), html.index("code: '32000090'"))

        print_html = self.client.get(
            "/de-xuat-nguyen-lieu/print?"
            "purpose=bao_hanh&material_codes=31000425%0A31000204"
            "&quantities=1.2%0A2.3&ngay=25&thang=07&nam=2026"
        ).get_data(as_text=True)

        self.assertIn("size: A5 landscape", print_html)
        self.assertIn("margin: 8mm 9mm 8mm 14mm", print_html)
        self.assertIn("form-header", print_html)
        self.assertIn("/static/logo_pnj.webp", print_html)
        self.assertIn("MS: PNJ-QYĐ-PHC-VC-GNVC-F6", print_html)
        self.assertIn("LSX : 04/00", print_html)
        self.assertIn("NHL: 04/08/2017", print_html)
        self.assertIn("TRANG : 1/1", print_html)
        self.assertIn("Số: ...../1305-2026", print_html)
        self.assertIn("Đề xuất nguyên liệu làm hàng bảo hành.", print_html)
        self.assertIn("<td class=\"col-unit\">phân</td>", print_html)
        self.assertIn("Căn cứ theo:</span> Nhu cầu thực tế tại cửa hàng", print_html)
        self.assertIn("Xuất tại kho:</span> 1203", print_html)
        self.assertIn("Kho, đơn vị nhận:</span>1204", print_html)
        self.assertIn("31000425 - NLT VÀNG VHÀN KQT TUỔI 7500", print_html)
        self.assertIn("31000204 - NLT VÀNG ĐÚC R MÀU VÀNG TUỔI 4160", print_html)
        self.assertGreaterEqual(print_html.count(">1.200<"), 2)
        self.assertGreaterEqual(print_html.count(">2.300<"), 2)
        self.assertGreaterEqual(print_html.count("<tr>"), 5)
        self.assertIn("<strong>Tổng Cộng</strong>", print_html)
        self.assertGreaterEqual(print_html.count(">3.500<"), 2)
        self.assertIn("total-number", print_html)
        self.assertIn("Bằng chữ", print_html)
        self.assertIn("Trưởng đơn vị", print_html)
        self.assertIn("font-size: 12.5px", print_html)
        self.assertIn(".col-name { width: 39%; }", print_html)
        self.assertIn(".col-value { text-align: center; width: 11%; }", print_html)
        self.assertIn("margin: 11px 0 8px", print_html)
        self.assertIn(".material-table tbody td", print_html)
        self.assertIn("height: 32px", print_html)
        self.assertIn("padding-right: 20mm; text-align: right", print_html)
        self.assertNotIn("Đề xuất nguyên liệu nl tinh", print_html)

        xu_ly_html = self.client.get(
            "/de-xuat-nguyen-lieu/print?"
            "purpose=xu_ly&material_codes=31000820%0A32000090"
            "&quantities=1%0A2&ngay=25&thang=07&nam=2026"
        ).get_data(as_text=True)
        self.assertIn("Đề xuất nguyên liệu làm hàng xử lý.", xu_ly_html)
        self.assertIn("31000820 - NLT VÀNG ĐÚC R M.VÀNG TUỔI 3330 (hội 334)", xu_ly_html)
        self.assertIn("32000090 - NLT BẠC VHÀN KQT TUỔI 8000", xu_ly_html)

        custom_html = self.client.get(
            "/de-xuat-nguyen-lieu/print?"
            "purpose=custom&material_codes=31000820&quantities=1&ngay=25&thang=07&nam=2026"
        ).get_data(as_text=True)
        self.assertIn("Nội dung:</span> ...................................................................................", custom_html)
        self.assertNotIn("Đề xuất nguyên liệu làm hàng .", custom_html)

        missing_quantity_html = self.client.get(
            "/de-xuat-nguyen-lieu/print?"
            "purpose=xu_ly&material_codes=31000820%0A32000090"
            "&quantities=1%0A&ngay=25&thang=07&nam=2026"
        ).get_data(as_text=True)
        self.assertEqual(missing_quantity_html.count(">1.000<"), 2)
        self.assertNotIn(">3.000<", missing_quantity_html)


if __name__ == "__main__":
    unittest.main()
