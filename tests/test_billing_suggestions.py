# -*- coding: utf-8 -*-

import json
import os
import tempfile
import unittest
from pathlib import Path

import app as app_module
import erp_billing


class BillingSuggestionTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.original_fixture = os.environ.get("PNJ_BILLING_FIXTURE_PATH")
        self.client = app_module.app.test_client()
        app_module.app.config.update(TESTING=True)

    def tearDown(self):
        if self.original_fixture is None:
            os.environ.pop("PNJ_BILLING_FIXTURE_PATH", None)
        else:
            os.environ["PNJ_BILLING_FIXTURE_PATH"] = self.original_fixture
        self.temp_dir.cleanup()

    def write_fixture(self, records):
        path = self.root / "billing.json"
        path.write_text(json.dumps({"records": records}, ensure_ascii=False), encoding="utf-8")
        os.environ["PNJ_BILLING_FIXTURE_PATH"] = str(path)

    def test_filters_customer_901_and_prioritizes_same_day(self):
        self.write_fixture(
            [
                {
                    "billing_document": "9010000001",
                    "customer_code": "0100000000",
                    "billing_date": "2026-07-21",
                    "net_value": "1,000,000",
                },
                {
                    "billing_document": "9010000002",
                    "customer_code": "100000000",
                    "billing_date": "2026-07-20",
                    "net_value": "2,000,000",
                },
                {
                    "billing_document": "8010000001",
                    "customer_code": "100000000",
                    "billing_date": "2026-07-21",
                    "net_value": "3,000,000",
                },
                {
                    "billing_document": "9010000003",
                    "customer_code": "100000001",
                    "billing_date": "2026-07-21",
                    "net_value": "4,000,000",
                },
            ]
        )

        suggestions = erp_billing.billing_suggestions("100000000", "2026-07-21", lookback_days=7)

        self.assertEqual([item["billing_document"] for item in suggestions], ["9010000001", "9010000002"])
        self.assertTrue(suggestions[0]["same_day"])
        self.assertEqual(suggestions[0]["amount"], 1000000)

    def test_excludes_cancelled_and_cancel_documents(self):
        self.write_fixture(
            [
                {
                    "billing_document": "9010000001",
                    "customer_code": "100000000",
                    "billing_date": "2026-07-21",
                    "net_value": "1,000,000",
                    "cancelled": "Yes",
                },
                {
                    "billing_document": "9010000002",
                    "customer_code": "100000000",
                    "billing_date": "2026-07-21",
                    "net_value": "2,000,000",
                    "canceled_bill_doc": "9010000001",
                },
                {
                    "billing_document": "9010000003",
                    "customer_code": "100000000",
                    "billing_date": "2026-07-21",
                    "net_value": "3,000,000",
                    "cancelled": "No",
                },
            ]
        )

        suggestions = erp_billing.billing_suggestions("100000000", "2026-07-21")

        self.assertEqual([item["billing_document"] for item in suggestions], ["9010000003"])

    def test_api_caps_at_ten_suggestions(self):
        self.write_fixture(
            [
                {
                    "billing_document": f"90100000{i:02d}",
                    "customer_code": "100000000",
                    "billing_date": "2026-07-21",
                    "net_value": i * 1000,
                }
                for i in range(12)
            ]
        )

        response = self.client.post(
            "/api/billing-suggestions",
            json={
                "customer_code": "100000000",
                "billing_date": "2026-07-21",
                "lookback_days": 30,
                "limit": 20,
            },
        )

        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(len(data["suggestions"]), 10)

    def test_invoice_suggestion_cards_are_compact_two_lines(self):
        html = self.client.get("/").get_data(as_text=True)

        self.assertIn("sap-invoice-suggestion-doc", html)
        self.assertIn("formatBillingDateShort(item.billing_date)", html)
        self.assertIn("formatNum(item.amount || 0)", html)
        self.assertIn("đ</span>", html)
        self.assertNotIn("sap-invoice-suggestion-meta", html)
        self.assertNotIn("Gần đây", html)
        self.assertNotIn("Cùng ngày", html)


if __name__ == "__main__":
    unittest.main()
