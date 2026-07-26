# -*- coding: utf-8 -*-

import json
from datetime import datetime
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch

import app as app_module
from uq01 import (
    FORM_CODE,
    PLANT_DIRECTORY,
    apply_sto_data,
    build_uq01_document_identity,
    build_uq01_content,
    build_uq01_context,
    default_uq01_payload,
    normalize_sto_data,
    normalize_uq01_payload,
    plant_context,
    uq01_plant_directory,
    validate_uq01_payload,
)


class UQ01Tests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = app_module.DB_PATH
        self.original_require_login = app_module.REQUIRE_LOGIN
        app_module.DB_PATH = str(Path(self.temp_dir.name) / "uq01_test.db")
        app_module.REQUIRE_LOGIN = False
        app_module.init_db()
        app_module.app.config.update(TESTING=True)
        self.client = app_module.app.test_client()

    def tearDown(self):
        app_module.DB_PATH = self.original_db_path
        app_module.REQUIRE_LOGIN = self.original_require_login
        self.temp_dir.cleanup()

    @staticmethod
    def valid_payload(item_count=1, with_legacy_fields=False):
        items = []
        for index in range(1, item_count + 1):
            items.append(
                {
                    "material_code": f"TEST-MATERIAL-{index:03d}",
                    "batch": f"BATCH-{index:03d}",
                    "description": f"Sản phẩm kiểm thử {index}",
                    "quantity": str(index),
                    "unit": "món",
                    "sale_price": (
                        str(index * 1000) if with_legacy_fields else ""
                    ),
                    "note": "Dữ liệu ghi chú cũ" if with_legacy_fields else "",
                }
            )
        return {
            "template_code": "UQ-01",
            "form_code": FORM_CODE,
            "plant": "1305",
            "document_no": "1305_2026-07-26_15:37",
            "created_at": "2026-07-26T15:37+07:00",
            "issue_place": "Huế",
            "issue_date": "2026-07-26",
            "copies": 2,
            "authorizer": {
                "full_name": "NGUYỄN VĂN MẪU",
                "job_title": "Cửa hàng trưởng",
                "employee_code": "",
                "unit_code": "1305",
                "unit_name": "Đơn vị kiểm thử 1305",
            },
            "authorized_person": {
                "full_name": "TRẦN THỊ THỬ",
                "id_type": "CCCD",
                "id_number": "000000000000",
                "issue_date": "2020-01-01",
                "issue_place": "Cơ quan kiểm thử",
                "job_title": "Nhân viên kiểm thử",
                "employee_code": "E0000000",
                "unit_code": "TEST",
                "unit_name": "Đơn vị nhận kiểm thử",
            },
            "authorization": {
                "authorization_action": "ký/thực hiện nhận gói/hộp niêm phong và giao hàng",
                "pickup_type": "cửa hàng",
                "pickup": {
                    "code": "TEST01",
                    "name": "Điểm nhận thử nghiệm",
                    "address": "Địa chỉ giả lập",
                },
                "destination": {
                    "code": "1305",
                    "name": "Điểm giao thử nghiệm",
                    "address": "",
                },
                "package_count": 1,
                "sealed_package": True,
                "valid_from": "2026-07-26",
                "valid_to": "",
                "responsibility_clause": "Người ủy quyền chịu trách nhiệm về việc ủy quyền này.",
                "additional_notes": "",
                "content_override": "",
                "content_customized": False,
            },
            "sto": {
                "source_mode": "manual",
                "reference_type": "STO",
                "reference_number": "4600000001",
                "approved_date": "2026-07-25",
                "items": items,
            },
        }

    def post_print(self, payload):
        return self.client.post(
            "/uy-quyen-nhan-hang/print",
            data={"payload": json.dumps(payload, ensure_ascii=False)},
        )

    @staticmethod
    def profile_payload(**overrides):
        payload = {
            "full_name": "NGUYỄN THỊ KIỂM THỬ",
            "job_title": "Nhân viên kiểm thử",
            "employee_code": "E0000001",
            "unit_code": "1305",
            "unit_name": "Đơn vị kiểm thử 1305",
            "id_type": "CCCD",
            "id_number": "000000000000",
            "id_issue_date": "2020-01-01",
            "id_issue_place": "Cơ quan kiểm thử",
            "can_authorize": False,
            "can_receive": True,
        }
        payload.update(overrides)
        return payload

    def test_document_identity_is_automatic_stable_and_server_controlled(self):
        fixed_now = datetime(2026, 7, 26, 15, 37)
        identity = build_uq01_document_identity("1305", now=fixed_now)
        self.assertRegex(
            identity["document_no"],
            r"^1305_\d{4}-\d{2}-\d{2}_\d{2}:\d{2}$",
        )
        self.assertEqual(identity["document_no"], "1305_2026-07-26_15:37")
        self.assertEqual(identity["issue_place"], "Huế")
        self.assertEqual(identity["copies"], 2)

        raw = self.valid_payload()
        raw["issue_place"] = "Giá trị client không được dùng"
        raw["issue_date"] = "1999-01-01"
        raw["copies"] = 9
        first = normalize_uq01_payload(raw, today=fixed_now)
        second = normalize_uq01_payload(first, today=datetime(2026, 7, 26, 16, 5))
        self.assertEqual(first["document_no"], second["document_no"])
        self.assertEqual(first["issue_date"], "2026-07-26")
        self.assertEqual(first["issue_place"], "Huế")
        self.assertEqual(first["copies"], 2)

        fallback = normalize_uq01_payload(
            {"plant": "1305", "document_no": "không hợp lệ"},
            today=fixed_now,
        )
        self.assertEqual(fallback["document_no"], "1305_2026-07-26_15:37")

    def test_plant_directory_is_exact_and_drives_issue_place(self):
        expected = {
            "1304": "PNJ 271 Trần Hưng Đạo (Huế)",
            "1305": "PNJ NEXT 27 Hà Nội (Huế)",
            "1398": "PNJ Vincom Huế",
            "1394": "PNJ 29 Mai Thúc Loan (Huế)",
            "1465": "PNJ 186 Hùng Vương (Huế)",
            "1570": "PNJ 1066 Nguyễn Tất Thành (Huế)",
            "1613": "PNJ Aeon Huế",
        }
        self.assertEqual(
            {plant["code"]: plant["name"] for plant in uq01_plant_directory()},
            expected,
        )
        self.assertEqual(
            {code: details["name"] for code, details in PLANT_DIRECTORY.items()},
            expected,
        )
        self.assertNotIn("16313", PLANT_DIRECTORY)
        for code, name in expected.items():
            context = plant_context(code)
            self.assertEqual(context["plant"], code)
            self.assertEqual(context["unit_name"], name)
            self.assertEqual(context["issue_place"], "Huế")
            identity = build_uq01_document_identity(
                code, now=datetime(2026, 7, 26, 15, 37)
            )
            self.assertTrue(identity["document_no"].startswith(f"{code}_"))
            self.assertEqual(identity["issue_place"], "Huế")

        destination = default_uq01_payload()["authorization"]["destination"]
        self.assertEqual(destination["code"], "1305")
        self.assertEqual(destination["name"], "PNJ NEXT 27 Hà Nội (Huế)")

    def test_known_plants_normalize_names_and_manual_locations_remain_compatible(self):
        payload = self.valid_payload()
        payload["authorization"]["pickup"] = {
            "code": "1304",
            "name": "Tên client sai",
            "address": "",
        }
        payload["authorization"]["destination"] = {
            "code": "1613",
            "name": "Tên client sai",
            "address": "",
        }
        normalized = normalize_uq01_payload(payload)
        self.assertEqual(
            normalized["authorization"]["pickup"]["name"],
            "PNJ 271 Trần Hưng Đạo (Huế)",
        )
        self.assertEqual(
            normalized["authorization"]["destination"]["name"],
            "PNJ Aeon Huế",
        )
        content = build_uq01_content(normalized)
        self.assertIn("1304 - PNJ 271 Trần Hưng Đạo (Huế)", content)
        self.assertIn("1613 - PNJ Aeon Huế", content)

        payload["authorization"]["pickup_type"] = "DC"
        payload["authorization"]["pickup"] = {
            "code": "DC-TEST",
            "name": "Điểm DC kiểm thử",
            "address": "Địa chỉ kiểm thử",
        }
        payload["authorization"]["destination"] = {
            "code": "EXT-TEST",
            "name": "Điểm giao ngoài danh mục",
            "address": "",
        }
        manual = normalize_uq01_payload(payload)
        self.assertEqual(manual["authorization"]["pickup"]["code"], "DC-TEST")
        self.assertEqual(
            manual["authorization"]["pickup"]["name"], "Điểm DC kiểm thử"
        )
        self.assertEqual(
            manual["authorization"]["destination"]["name"],
            "Điểm giao ngoài danh mục",
        )

    def test_profile_seeds_are_role_filtered_and_contain_no_identity_number(self):
        authorizers = self.client.get(
            "/api/uq01/personnel-profiles?role=authorizer"
        )
        self.assertEqual(authorizers.status_code, 200)
        self.assertEqual(authorizers.headers["Cache-Control"], "no-store, max-age=0")
        authorizer_profiles = authorizers.get_json()["profiles"]
        self.assertEqual(len(authorizer_profiles), 2)
        self.assertEqual(
            [profile["full_name"] for profile in authorizer_profiles],
            ["HỒ THỊ HÀ MY", "CHÂU ĐĂNG KHOA"],
        )
        self.assertTrue(all(profile["can_authorize"] for profile in authorizer_profiles))
        self.assertTrue(all(not profile["can_receive"] for profile in authorizer_profiles))
        self.assertTrue(all(profile["id_number"] == "" for profile in authorizer_profiles))

        recipients = self.client.get(
            "/api/uq01/personnel-profiles?role=recipient"
        ).get_json()["profiles"]
        self.assertEqual(
            [profile["full_name"] for profile in recipients],
            ["HÀ VĂN RIN", "TRẦN XUÂN HẢI", "TRẦN QUANG TRINH"],
        )
        self.assertTrue(all(profile["can_receive"] for profile in recipients))
        self.assertTrue(all(not profile["can_authorize"] for profile in recipients))
        self.assertTrue(all(not profile["id_number"] for profile in recipients))
        self.assertIn("chưa có số giấy tờ", " ".join(recipients[0]["warnings"]))

    def test_default_role_presets_render_distinct_matching_signatures(self):
        authorizer = self.client.get(
            "/api/uq01/personnel-profiles?role=authorizer"
        ).get_json()["profiles"][0]
        recipient = self.client.get(
            "/api/uq01/personnel-profiles?role=recipient"
        ).get_json()["profiles"][0]
        payload = self.valid_payload()
        payload["authorizer"] = {
            key: authorizer[key]
            for key in (
                "full_name",
                "job_title",
                "employee_code",
                "unit_code",
                "unit_name",
            )
        }
        payload["authorized_person"] = {
            "full_name": recipient["full_name"],
            "id_type": recipient["id_type"],
            "id_number": recipient["id_number"],
            "issue_date": recipient["id_issue_date"],
            "issue_place": recipient["id_issue_place"],
            "job_title": recipient["job_title"],
            "employee_code": recipient["employee_code"],
            "unit_code": recipient["unit_code"],
            "unit_name": recipient["unit_name"],
        }
        html = self.post_print(payload).get_data(as_text=True)
        signatures = html.split('<section class="uq01-signatures">', 1)[1].split(
            "</section>", 1
        )[0]
        self.assertIn("HÀ VĂN RIN", signatures)
        self.assertIn("HỒ THỊ HÀ MY", signatures)
        self.assertLess(signatures.index("HÀ VĂN RIN"), signatures.index("HỒ THỊ HÀ MY"))
        self.assertIn(
            "Chưa nhập số giấy tờ của người được ủy quyền.",
            " ".join(validate_uq01_payload(payload)),
        )

    def test_profile_create_update_persists_after_reload_and_respects_roles(self):
        created = self.client.post(
            "/api/uq01/personnel-profiles",
            json=self.profile_payload(),
            headers={"Origin": "http://localhost"},
            base_url="http://localhost",
        )
        self.assertEqual(created.status_code, 201)
        saved = created.get_json()["profile"]
        self.assertEqual(saved["full_name"], "NGUYỄN THỊ KIỂM THỬ")

        reloaded_client = app_module.app.test_client()
        recipients = reloaded_client.get(
            "/api/uq01/personnel-profiles?role=recipient"
        ).get_json()["profiles"]
        self.assertIn(saved["id"], [profile["id"] for profile in recipients])
        authorizers = reloaded_client.get(
            "/api/uq01/personnel-profiles?role=authorizer"
        ).get_json()["profiles"]
        self.assertNotIn(saved["id"], [profile["id"] for profile in authorizers])

        updated_payload = self.profile_payload(
            job_title="Điều phối nhận hàng",
            can_authorize=True,
        )
        updated = self.client.put(
            f"/api/uq01/personnel-profiles/{saved['id']}",
            json=updated_payload,
            headers={"Origin": "http://localhost"},
            base_url="http://localhost",
        )
        self.assertEqual(updated.status_code, 200)
        self.assertEqual(updated.get_json()["profile"]["job_title"], "Điều phối nhận hàng")
        authorizers_after = self.client.get(
            "/api/uq01/personnel-profiles?role=authorizer"
        ).get_json()["profiles"]
        self.assertIn(saved["id"], [profile["id"] for profile in authorizers_after])

    def test_profile_api_requires_auth_when_login_is_enabled(self):
        app_module.REQUIRE_LOGIN = True
        response = self.client.get("/api/uq01/personnel-profiles")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.headers["Location"])

    def test_non_admin_cannot_render_uq_ui_or_access_any_uq_route(self):
        admin_profile = self.client.post(
            "/api/uq01/personnel-profiles",
            json=self.profile_payload(full_name="HỒ SƠ ADMIN KIỂM THỬ"),
            headers={"Origin": "http://localhost"},
            base_url="http://localhost",
        ).get_json()["profile"]

        app_module.REQUIRE_LOGIN = True
        with self.client.session_transaction() as session_data:
            session_data["user_id"] = 99
            session_data["user_name"] = "Người dùng kiểm thử"
            session_data["role"] = "user"

        with patch.object(app_module, "is_admin", return_value=False):
            page = self.client.get("/bieu-mau?form=uq01")
            self.assertEqual(page.status_code, 200)
            page_html = page.get_data(as_text=True)
            self.assertNotIn('id="rbUQ01"', page_html)
            self.assertNotIn('id="sectionUQ01"', page_html)
            self.assertNotIn("uy_quyen_nhan_hang.js", page_html)
            self.assertNotIn("uy_quyen_nhan_hang.css", page_html)
            self.assertNotIn("uqPlantDirectoryData", page_html)
            self.assertNotIn("PNJ Aeon Huế", page_html)
            self.assertIn('id="rbF1"', page_html)

            api_calls = (
                ("get", "/api/uq01/document-identity", {}),
                ("get", "/api/uq01/personnel-profiles?role=all", {}),
                (
                    "post",
                    "/api/uq01/personnel-profiles",
                    {
                        "json": self.profile_payload(),
                        "headers": {"Origin": "http://localhost"},
                        "base_url": "http://localhost",
                    },
                ),
                (
                    "put",
                    f"/api/uq01/personnel-profiles/{admin_profile['id']}",
                    {
                        "json": self.profile_payload(),
                        "headers": {"Origin": "http://localhost"},
                        "base_url": "http://localhost",
                    },
                ),
            )
            for method, path, kwargs in api_calls:
                with self.subTest(method=method, path=path):
                    response = getattr(self.client, method)(path, **kwargs)
                    self.assertEqual(response.status_code, 403)
                    self.assertFalse(response.get_json()["ok"])
                    self.assertEqual(
                        response.headers["Cache-Control"], "no-store, max-age=0"
                    )

            for path in (
                "/uy-quyen-nhan-hang/print",
                "/uy-quyen-nhan-hang/pdf",
            ):
                with self.subTest(path=path):
                    response = self.client.post(
                        path,
                        data={
                            "payload": json.dumps(
                                self.valid_payload(), ensure_ascii=False
                            )
                        },
                    )
                    self.assertEqual(response.status_code, 403)
                    self.assertIn(
                        "không có quyền",
                        response.get_data(as_text=True).lower(),
                    )

        app_module.REQUIRE_LOGIN = False
        with self.client.session_transaction() as session_data:
            session_data.clear()
        admin_profiles = self.client.get(
            "/api/uq01/personnel-profiles?role=all"
        ).get_json()["profiles"]
        saved_profile = next(
            profile
            for profile in admin_profiles
            if profile["id"] == admin_profile["id"]
        )
        self.assertEqual(saved_profile["full_name"], "HỒ SƠ ADMIN KIỂM THỬ")

    def test_profile_data_is_isolated_by_user_and_plant_scope(self):
        with self.client.session_transaction() as session_data:
            session_data["user_id"] = 2
            session_data["role"] = "admin"
        created = self.client.post(
            "/api/uq01/personnel-profiles",
            json=self.profile_payload(full_name="TRẦN THỊ PHẠM VI HAI"),
            headers={"Origin": "http://localhost"},
            base_url="http://localhost",
        ).get_json()["profile"]

        user_one_client = app_module.app.test_client()
        user_one_profiles = user_one_client.get(
            "/api/uq01/personnel-profiles?role=all"
        ).get_json()["profiles"]
        self.assertNotIn(created["id"], [profile["id"] for profile in user_one_profiles])
        denied_update = user_one_client.put(
            f"/api/uq01/personnel-profiles/{created['id']}",
            json=self.profile_payload(full_name="TRẦN THỊ PHẠM VI HAI"),
            headers={"Origin": "http://localhost"},
            base_url="http://localhost",
        )
        self.assertEqual(denied_update.status_code, 404)

    def test_goods_content_never_generates_cash_wording(self):
        content = build_uq01_content(self.valid_payload())
        self.assertIn("gói/hộp niêm phong chứa hàng hóa", content)
        self.assertIn("STO số 4600000001", content)
        self.assertIn(
            "Người ủy quyền Ông/Bà NGUYỄN VĂN MẪU ủy quyền cho "
            "Người được ủy quyền Ông/Bà TRẦN THỊ THỬ",
            content,
        )
        self.assertNotIn("được duyệt ngày", content)
        self.assertNotIn("số tiền", content.lower())

    def test_print_route_contains_form_identity_footer_and_matching_signatures(self):
        payload = self.valid_payload()
        payload["authorizer"]["salutation"] = "Bà"
        payload["authorized_person"]["salutation"] = "Ông"
        response = self.post_print(payload)
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("MS: PNJ-QYD-PLPLNL-NS-UQ-F1", html)
        self.assertIn("LSX: 00", html)
        self.assertIn("NHL: 14/11/2016", html)
        self.assertIn("TRANG: 1/1", html)
        self.assertIn("Cấp độ bảo mật", html)
        self.assertIn("Thông tin nội bộ", html)
        self.assertIn("logo_pnj.webp", html)
        self.assertGreaterEqual(html.count("NGUYỄN VĂN MẪU"), 2)
        self.assertGreaterEqual(html.count("TRẦN THỊ THỬ"), 2)
        party_html = html.split(
            '<section class="uq01-party">', 1
        )[1].split(
            '<h2 class="uq01-section-title">Nội dung ủy quyền:', 1
        )[0]
        signatures = html.split(
            '<section class="uq01-signatures">', 1
        )[1].split("</section>", 1)[0]
        self.assertNotIn("Ông/Bà", party_html)
        self.assertNotIn("Ông/Bà", signatures)
        self.assertEqual(response.headers["Cache-Control"], "no-store, max-age=0")

    def test_print_uses_only_four_goods_columns_and_ignores_legacy_fields(self):
        payload = self.valid_payload(item_count=5, with_legacy_fields=True)
        payload["sto"]["items"][0].update(
            {
                "batch": "LEGACY-BATCH-HIDDEN",
                "sale_price": "987654321",
                "note": "LEGACY-NOTE-HIDDEN",
            }
        )
        response = self.post_print(payload)
        html = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(html.count("Sản phẩm kiểm thử"), 5)
        self.assertIn("Danh sách hàng hóa:", html)
        table_head = html.split('<table class="uq01-items">', 1)[1].split(
            "</thead>", 1
        )[0]
        headers = re.findall(r"<th[^>]*>(.*?)</th>", table_head, flags=re.S)
        headers = [re.sub(r"<[^>]+>", "", header).strip() for header in headers]
        self.assertEqual(
            headers,
            ["Mã sản phẩm", "Tên sản phẩm", "Số lượng", "Đơn vị"],
        )
        self.assertNotIn("LEGACY-BATCH-HIDDEN", html)
        self.assertNotIn("987654321", html)
        self.assertNotIn("LEGACY-NOTE-HIDDEN", html)
        self.assertNotIn("TỔNG", html)

    def test_manual_override_is_kept_and_structured_text_is_not_substituted(self):
        payload = self.valid_payload()
        payload["authorization"]["content_customized"] = True
        payload["authorization"]["content_override"] = "Nội dung tùy chỉnh kiểm thử."
        context = build_uq01_context(payload)
        self.assertEqual(context["content"], "Nội dung tùy chỉnh kiểm thử.")
        self.assertNotEqual(context["content"], context["generated_content"])

    def test_validation_warns_without_blocking_historical_shapes(self):
        payload = self.valid_payload()
        payload["authorized_person"]["id_number"] = "123"
        payload["sto"]["reference_number"] = "STO-TEST"
        payload["authorization"]["valid_to"] = "2026-07-20"
        payload["authorization"]["destination"] = dict(payload["authorization"]["pickup"])
        payload["sto"]["items"][0]["quantity"] = "0"
        warnings = validate_uq01_payload(payload)
        joined = " ".join(warnings)
        self.assertIn("CCCD", joined)
        self.assertIn("STO", joined)
        self.assertIn("Ngày kết thúc", joined)
        self.assertIn("trùng nhau", joined)
        self.assertIn("số lượng", joined)
        self.assertEqual(self.post_print(payload).status_code, 200)

    def test_sto_adapter_boundary_maps_erp_shaped_fields_without_calling_erp(self):
        raw_sto = {
            "source_mode": "erp",
            "reference_type": "PO",
            "reference_number": "4600000002",
            "items": [
                {
                    "matnr": "TEST-ERP-001",
                    "charg": "BATCH-ERP",
                    "product_name": "Hàng giả lập từ adapter",
                    "menge": "2",
                    "meins": "món",
                    "price": "123456",
                    "remark": "Ghi chú adapter cũ",
                }
            ],
        }
        normalized = normalize_sto_data(raw_sto)
        self.assertEqual(normalized["reference_type"], "PXK")
        self.assertEqual(normalized["items"][0]["material_code"], "TEST-ERP-001")
        self.assertEqual(
            normalized["items"][0]["description"], "Hàng giả lập từ adapter"
        )
        self.assertEqual(normalized["items"][0]["quantity"], "2")
        applied = apply_sto_data(self.valid_payload(), raw_sto)
        self.assertEqual(applied["sto"]["source_mode"], "erp")
        self.assertEqual(applied["sto"]["reference_type"], "PXK")
        html = self.post_print(applied).get_data(as_text=True)
        self.assertIn("PXK số 4600000002", html)
        self.assertIsNone(re.search(r"\bPO\b", html))
        self.assertNotIn("BATCH-ERP", html)
        self.assertNotIn("123456", html)
        self.assertNotIn("Ghi chú adapter cũ", html)

    def test_malformed_or_oversized_payload_is_rejected(self):
        malformed = self.client.post(
            "/uy-quyen-nhan-hang/print",
            data={"payload": "{not-json"},
        )
        self.assertEqual(malformed.status_code, 400)
        oversized = self.client.post(
            "/uy-quyen-nhan-hang/print",
            data={"payload": "x" * (129 * 1024)},
        )
        self.assertEqual(oversized.status_code, 413)

    def test_pdf_uses_same_document_html(self):
        payload = self.valid_payload()
        fake_response = app_module.app.make_response("PDF_OK")
        with patch.object(app_module, "send_print_html_pdf", return_value=fake_response) as sender:
            response = self.client.post(
                "/uy-quyen-nhan-hang/pdf",
                data={"payload": json.dumps(payload, ensure_ascii=False)},
            )
        self.assertEqual(response.status_code, 200)
        html = sender.call_args.args[0]
        filename = sender.call_args.args[1]
        self.assertIn('id="uq01Document"', html)
        self.assertIn("PNJ-QYD-PLPLNL-NS-UQ-F1", html)
        self.assertIn("TRẦN THỊ THỬ", html)
        self.assertEqual(filename, "UQ-01_1305_2026-07-26_15-37.pdf")
        self.assertNotIn(":", filename)
        self.assertEqual(response.headers["Cache-Control"], "no-store, max-age=0")

    def test_bieu_mau_exposes_direct_uq01_entry(self):
        response = self.client.get("/bieu-mau?form=uq01")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn('id="rbUQ01"', html)
        self.assertIn("uy_quyen_nhan_hang.js", html)
        self.assertIn("window.location.pathname.startsWith('/bk/')", html)
        self.assertIn("uq01PreviewFrame", html)
        self.assertIn('id="uqAuthorizerProfile"', html)
        self.assertIn('id="uqRecipientProfile"', html)
        self.assertIn('id="btnManageUqPersonnel"', html)
        self.assertIn('id="uqPersonnelModal"', html)
        self.assertIn('id="uqPickupPlant"', html)
        self.assertIn('id="uqDestinationPlant"', html)
        self.assertIn('id="uqPickupCodeField"', html)
        self.assertIn('id="uqDestinationCodeField"', html)
        self.assertNotIn('id="uqProfileSalutation"', html)
        self.assertNotIn(">Xưng hô<", html)
        self.assertNotIn('id="uqDocumentNo"', html)
        self.assertNotIn('id="uqIssuePlace"', html)
        self.assertNotIn('id="uqCopies"', html)
        self.assertNotIn("Thông tin văn bản", html)
        self.assertIn('<option value="PXK">PXK</option>', html)
        self.assertNotIn('<option value="PO">PO</option>', html)
        self.assertIn("Danh sách hàng hóa", html)
        item_template = html.split('<template id="uqItemTemplate">', 1)[1].split(
            "</template>", 1
        )[0]
        self.assertEqual(item_template.count("data-uq-field="), 4)
        for label in ("Mã sản phẩm", "Tên sản phẩm", "Số lượng", "Đơn vị"):
            self.assertIn(label, item_template)
        for hidden_label in ("Batch", "Giá bán", "Ghi chú"):
            self.assertNotIn(hidden_label, item_template)
        directory_match = re.search(
            r'<script id="uqPlantDirectoryData" type="application/json">(.*?)</script>',
            html,
            flags=re.S,
        )
        self.assertIsNotNone(directory_match)
        embedded_directory = json.loads(directory_match.group(1))
        self.assertEqual(len(embedded_directory), 7)
        self.assertEqual(
            embedded_directory[-1],
            {
                "code": "1613",
                "issue_place": "Huế",
                "name": "PNJ Aeon Huế",
            },
        )
        self.assertNotIn("16313", directory_match.group(1))
        self.assertIn('value="1305"', html)
        self.assertIn('value="PNJ NEXT 27 Hà Nội (Huế)"', html)
        document_match = re.search(
            r'data-document-no="(1305_\d{4}-\d{2}-\d{2}_\d{2}:\d{2})"',
            html,
        )
        self.assertIsNotNone(document_match)


if __name__ == "__main__":
    unittest.main()
