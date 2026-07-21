# -*- coding: utf-8 -*-

import json
import os
import tempfile
import unittest
from pathlib import Path

import app as app_module
import erp_purchase_orders


class PurchaseOrderSuggestionTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.original_fixture = os.environ.get("PNJ_PURCHASE_ORDER_FIXTURE_PATH")
        self.client = app_module.app.test_client()
        app_module.app.config.update(TESTING=True)

    def tearDown(self):
        if self.original_fixture is None:
            os.environ.pop("PNJ_PURCHASE_ORDER_FIXTURE_PATH", None)
        else:
            os.environ["PNJ_PURCHASE_ORDER_FIXTURE_PATH"] = self.original_fixture
        self.temp_dir.cleanup()

    def write_fixture(self, records):
        path = self.root / "purchase_orders.json"
        path.write_text(json.dumps({"records": records}, ensure_ascii=False), encoding="utf-8")
        os.environ["PNJ_PURCHASE_ORDER_FIXTURE_PATH"] = str(path)

    def test_filters_buyback_purchase_orders_for_two_day_window(self):
        self.write_fixture(
            [
                {
                    "PurchaseOrder": "4403909303",
                    "Supplier": "103345952",
                    "CompanyCode": "1000",
                    "CreationDate": "/Date(1784592000000)/",
                    "PurchaseOrderNetAmount": "9773000",
                    "DocumentCurrency": "VND",
                    "PurchaseOrderType": "Z04",
                    "IsActiveEntity": True,
                },
                {
                    "PurchaseOrder": "4403909228",
                    "Supplier": "103345952",
                    "CompanyCode": "1000",
                    "CreationDate": "2026-07-20",
                    "PurchaseOrderNetAmount": "25,752,000",
                    "DocumentCurrency": "VND",
                    "PurchaseOrderType": "Z04",
                    "IsActiveEntity": True,
                },
                {
                    "PurchaseOrder": "4403909000",
                    "Supplier": "103345952",
                    "CompanyCode": "1000",
                    "CreationDate": "2026-07-19",
                    "PurchaseOrderNetAmount": "1,000",
                    "PurchaseOrderType": "Z04",
                    "IsActiveEntity": True,
                },
                {
                    "PurchaseOrder": "4403908000",
                    "Supplier": "103345952",
                    "CompanyCode": "1000",
                    "CreationDate": "2026-07-21",
                    "PurchaseOrderNetAmount": "2,000",
                    "PurchaseOrderType": "NB",
                    "IsActiveEntity": True,
                },
                {
                    "PurchaseOrder": "4403907000",
                    "Supplier": "103345952",
                    "CompanyCode": "1000",
                    "CreationDate": "2026-07-21",
                    "PurchaseOrderNetAmount": "3,000",
                    "PurchaseOrderType": "Z04",
                    "IsActiveEntity": False,
                },
            ]
        )

        suggestions = erp_purchase_orders.purchase_order_suggestions(
            "103345952", "2026-07-21", lookback_days=1
        )

        self.assertEqual([item["purchase_order"] for item in suggestions], ["4403909303", "4403909228"])
        self.assertEqual(suggestions[0]["amount"], 9773000)
        self.assertEqual(suggestions[1]["amount"], 25752000)
        self.assertTrue(suggestions[0]["same_day"])

    def test_api_returns_purchase_order_suggestions(self):
        self.write_fixture(
            [
                {
                    "PurchaseOrder": "4403909303",
                    "Supplier": "103345952",
                    "CompanyCode": "1000",
                    "CreationDate": "2026-07-21",
                    "PurchaseOrderNetAmount": "9773000",
                    "DocumentCurrency": "VND",
                    "PurchaseOrderType": "Z04",
                    "IsActiveEntity": True,
                }
            ]
        )

        response = self.client.post(
            "/api/purchase-order-suggestions",
            json={
                "customer_code": "103345952",
                "purchase_order_date": "2026-07-21",
                "lookback_days": 1,
                "limit": 10,
            },
        )

        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["suggestions"][0]["purchase_order"], "4403909303")
        self.assertEqual(data["suggestions"][0]["amount"], 9773000)

    def test_index_contains_purchase_order_suggestion_flow(self):
        html = self.client.get("/").get_data(as_text=True)

        self.assertIn("/api/purchase-order-suggestions", html)
        self.assertIn("fetchPurchaseOrderSuggestions", html)
        self.assertIn("purchaseOrderSuggestionCache", html)


if __name__ == "__main__":
    unittest.main()
