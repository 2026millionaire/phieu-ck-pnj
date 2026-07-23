import json
import os
import tempfile
import unittest
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


if __name__ == "__main__":
    unittest.main()
