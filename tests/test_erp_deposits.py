# -*- coding: utf-8 -*-

import json
import os
import tempfile
import unittest
from pathlib import Path

import app as app_module
import erp_deposits


class ErpDepositTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.original_fixture = os.environ.get("PNJ_DEPOSIT_FIXTURE_PATH")
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
            os.environ.pop("PNJ_DEPOSIT_FIXTURE_PATH", None)
        else:
            os.environ["PNJ_DEPOSIT_FIXTURE_PATH"] = self.original_fixture
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
        path = self.root / "deposits.json"
        records = [
            {
                "Customer": "1005320208",
                "CompanyCode": "1000",
                "ProfitCenter": "0010241305",
                "PostingDate": "2026-03-04",
                "DocumentNo": "1600524502",
                "DocumentType": "DZ",
                "Amount": "2,000,000-",
                "Text": "Thu cọc ĐH: 2012549282",
            },
            {
                "Customer": "1005320208",
                "CompanyCode": "1000",
                "ProfitCenter": "0010241305",
                "PostingDate": "2026-03-04",
                "DocumentNo": "1600524502",
                "DocumentType": "DZ",
                "Amount": "2,000,000-",
            },
            {
                "Customer": "1005320208",
                "CompanyCode": "1000",
                "ProfitCenter": "0010241305",
                "PostingDate": "2026-03-04",
                "DocumentNo": "1400524502",
                "DocumentType": "DZ",
                "Amount": "2,000,000-",
            },
            {
                "Customer": "1005320208",
                "CompanyCode": "1000",
                "ProfitCenter": "0010241305",
                "PostingDate": "2026-03-04",
                "DocumentNo": "1600524503",
                "DocumentType": "DZ",
                "Amount": "2,000,000",
            },
            {
                "Customer": "1005320209",
                "CompanyCode": "1000",
                "ProfitCenter": "0010241305",
                "PostingDate": "2026-03-04",
                "DocumentNo": "1600524504",
                "DocumentType": "DZ",
                "Amount": "2,000,000-",
            },
        ]
        path.write_text(json.dumps({"records": records}, ensure_ascii=False), encoding="utf-8")
        os.environ["PNJ_DEPOSIT_FIXTURE_PATH"] = str(path)

    def test_returns_unique_negative_16_documents(self):
        self.write_fixture()

        suggestions = erp_deposits.deposit_suggestions("01005320208", "2026-03-04", lookback_days=0)

        self.assertEqual(len(suggestions), 1)
        self.assertEqual(suggestions[0]["deposit_document"], "1600524502")
        self.assertEqual(suggestions[0]["amount"], 2000000)

    def test_api_returns_deposit_suggestions(self):
        self.write_fixture()
        response = self.client.post(
            "/api/deposit-suggestions",
            json={
                "customer_code": "1005320208",
                "deposit_date": "2026-03-04",
                "lookback_days": 0,
            },
            headers={"Origin": "http://localhost"},
        )

        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["suggestions"][0]["deposit_document"], "1600524502")
        self.assertEqual(data["suggestions"][0]["amount"], 2000000)


if __name__ == "__main__":
    unittest.main()
