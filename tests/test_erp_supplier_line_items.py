import json
import os
import tempfile
import unittest
from pathlib import Path

import app as app_module
import erp_supplier_line_items


class ErpSupplierLineItemsTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.original_fixture = os.environ.get("PNJ_SUPPLIER_LINE_ITEMS_FIXTURE_PATH")
        self.original_user = os.environ.get("PNJ_ERP_USER")
        self.original_password = os.environ.get("PNJ_ERP_PASSWORD")
        os.environ.pop("PNJ_ERP_USER", None)
        os.environ.pop("PNJ_ERP_PASSWORD", None)
        app_module.app.config.update(TESTING=True)
        self.client = app_module.app.test_client()
        with self.client.session_transaction() as session:
            session["user_id"] = 1
            session["user_name"] = "ADMIN TEST"
            session["role"] = "admin"

    def tearDown(self):
        if self.original_fixture is None:
            os.environ.pop("PNJ_SUPPLIER_LINE_ITEMS_FIXTURE_PATH", None)
        else:
            os.environ["PNJ_SUPPLIER_LINE_ITEMS_FIXTURE_PATH"] = self.original_fixture
        if self.original_user is None:
            os.environ.pop("PNJ_ERP_USER", None)
        else:
            os.environ["PNJ_ERP_USER"] = self.original_user
        if self.original_password is None:
            os.environ.pop("PNJ_ERP_PASSWORD", None)
        else:
            os.environ["PNJ_ERP_PASSWORD"] = self.original_password
        self.temp_dir.cleanup()

    def write_fixture(self):
        path = self.root / "supplier_line_items.json"
        records = [
            {
                "Supplier": "100139219",
                "CompanyCode": "1000",
                "PostingDate": "2026-07-22",
                "AccountingDocument": "2500433572",
                "DocumentReferenceID": "000574/07_1305",
                "SpecialGeneralLedgerCode": "",
            },
            {
                "Supplier": "100139219",
                "CompanyCode": "1000",
                "PostingDate": "2026-07-22",
                "AccountingDocument": "2500433411",
                "DocumentReferenceID": "000572/07_1305",
                "SpecialGeneralLedgerCode": "",
            },
            {
                "Supplier": "100139219",
                "CompanyCode": "1000",
                "PostingDate": "2026-07-22",
                "AccountingDocument": "7000006132",
                "DocumentReferenceID": "000572/07_1305",
                "SpecialGeneralLedgerCode": "",
            },
            {
                "Supplier": "100139219",
                "CompanyCode": "1000",
                "PostingDate": "2026-07-22",
                "AccountingDocument": "9999999999",
                "DocumentReferenceID": "not-a-bk-reference",
                "SpecialGeneralLedgerCode": "",
            },
        ]
        path.write_text(json.dumps({"records": records}, ensure_ascii=False), encoding="utf-8")
        os.environ["PNJ_SUPPLIER_LINE_ITEMS_FIXTURE_PATH"] = str(path)

    def test_maps_unique_references_to_purchase_orders_descending(self):
        self.write_fixture()
        result = erp_supplier_line_items.purchase_order_reference_mapping(
            "100139219",
            [
                {"purchase_order": "4403913801", "creation_date": "2026-07-22"},
                {"purchase_order": "4403914037", "creation_date": "2026-07-22"},
            ],
            target_date="2026-07-22",
            lookback_days=0,
        )

        self.assertEqual(
            result["mapping"],
            {
                "4403914037": "000574/07_1305",
                "4403913801": "000572/07_1305",
            },
        )
        self.assertEqual(
            [item["reference"] for item in result["references"]],
            ["000574/07_1305", "000572/07_1305"],
        )

    def test_api_returns_reference_mapping(self):
        self.write_fixture()
        response = self.client.post(
            "/api/purchase-order-references",
            json={
                "customer_code": "100139219",
                "purchase_order_date": "2026-07-22",
                "lookback_days": 0,
                "purchase_orders": [
                    {"purchase_order": "4403913801", "creation_date": "2026-07-22"},
                    {"purchase_order": "4403914037", "creation_date": "2026-07-22"},
                ],
            },
            headers={"Origin": "http://localhost"},
        )

        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["mapping"]["4403914037"], "000574/07_1305")
        self.assertEqual(data["mapping"]["4403913801"], "000572/07_1305")


if __name__ == "__main__":
    unittest.main()
