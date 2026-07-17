# -*- coding: utf-8 -*-

import tempfile
import time
import unittest
from pathlib import Path

from customer_lookup import (
    CustomerLookupError,
    CustomerLookupStore,
    iter_sap_records,
    normalize_customer_code,
    suggestion_for_field,
)


class CustomerLookupTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.store = CustomerLookupStore(self.root / "lookup.db", bytes(range(32)))

    def tearDown(self):
        self.temp_dir.cleanup()

    def write_source(self) -> Path:
        source = self.root / "synthetic.tsv"
        source.write_text(
            "\tSearchTerm\tCty\tPostalCode\tCity\tName 1\tCustomer\tCoCd\tDelF\n"
            "\t0900000000\tVN\t\tCITY\tKHACH HANG THU\t100000000\t10\n"
            "\tINVALID\tVN\t\tCITY\tKHACH HANG XOA\t100000001\t10\t\tX\n",
            encoding="utf-8-sig",
        )
        return source

    def test_import_encrypts_payload_and_keeps_delf(self):
        result = self.store.import_files(
            [self.write_source()], expected_min=100000000, expected_max=100000001
        )
        self.assertEqual(result["source_rows"], 2)
        self.assertEqual(result["delf_rows"], 1)
        record = self.store.get_record("100000001")
        self.assertEqual(record["search_term"], "INVALID")
        self.assertEqual(record["delf"], "X")
        database_bytes = self.store.db_path.read_bytes()
        self.assertNotIn(b"KHACH HANG THU", database_bytes)
        self.assertNotIn(b"0900000000", database_bytes)
        summary = self.store.get_dataset_summary()
        self.assertEqual(summary["record_count"], 2)
        self.assertEqual(summary["max_customer"], "100000001")

    def test_display_rules_are_applied_after_decryption(self):
        self.store.import_files([self.write_source()])
        valid = self.store.get_record("100000000")
        invalid = self.store.get_record("100000001")
        self.assertEqual(suggestion_for_field(valid, "phone"), "0900000000")
        self.assertIsNone(suggestion_for_field(invalid, "phone"))
        self.assertEqual(suggestion_for_field(invalid, "name"), "KHACH HANG XOA")

    def test_customer_code_normalization(self):
        self.assertEqual(normalize_customer_code("100000000"), "100000000")
        self.assertEqual(normalize_customer_code("0100000000"), "100000000")
        self.assertIsNone(normalize_customer_code("100"))

    def test_recovers_shifted_customer_column_and_split_name(self):
        source = self.root / "100000000-100000999.txt"
        source.write_text(
            "\tSearchTerm\tCty\tPostalCode\tCity\tName 1\tCustomer\tCoCd\tDelF\n"
            "\t0900000001\tVN\t\tCITY\tTEN\tKHACH HANG\t100000001\t1000\tX\n",
            encoding="utf-8-sig",
        )
        records = list(iter_sap_records(source))
        self.assertEqual(records[0]["customer"], "100000001")
        self.assertEqual(records[0]["search_term"], "0900000001")
        self.assertEqual(records[0]["name_1"], "TEN KHACH HANG")
        self.assertEqual(records[0]["delf"], "X")
        self.assertEqual(records[0]["_recovered"], "1")

    def test_recovers_whole_row_shift_with_blank_layout_column(self):
        source = self.root / "100000000-100000999.txt"
        source.write_text(
            "\tSearchTerm\tCty\tPostalCode\tCity\tName 1\t\tCustomer\tCoCd\tDelF\n"
            "\t\t0900000002\tVN\t\tCITY\tTEN KHACH\t\t100000002\t1000\n",
            encoding="utf-8-sig",
        )
        records = list(iter_sap_records(source))
        self.assertEqual(records[0]["customer"], "100000002")
        self.assertEqual(records[0]["search_term"], "0900000002")
        self.assertEqual(records[0]["name_1"], "TEN KHACH")
        self.assertEqual(records[0]["delf"], "")
        self.assertEqual(records[0]["_recovered"], "1")

    def test_validates_content_without_requiring_filename_pattern(self):
        source = self.root / "ten file bat ky.data"
        source.write_text(
            "\tSearchTerm\tCty\tPostalCode\tCity\tName 1\tCustomer\tCoCd\tDelF\n"
            "\t0900000003\tVN\t\tCITY\tTEN KHACH\t100000003\t10\n",
            encoding="utf-8-sig",
        )
        validation = self.store.validate_import_files([source])
        self.assertEqual(validation["source_rows"], 1)
        self.assertEqual(validation["max_customer"], 100000003)
        self.store.import_files([source], expected_min=100000003, expected_max=100000003)
        with self.assertRaises(CustomerLookupError):
            self.store.validate_import_files([source])

    def test_sixth_unique_code_requires_captcha(self):
        self.store.initialize()
        session_id = "test-session"
        principal_id = "test-user|127.0.0.1"
        for offset in range(5):
            key = self.store.lookup_key(str(100000000 + offset))
            self.store.record_event(
                session_id=session_id,
                principal_id=principal_id,
                lookup_key=key,
                requested_field="name",
                outcome="suggestion",
                lookup_performed=True,
                record_found=True,
                suggestion_shown=True,
            )
        sixth = self.store.lookup_key("100000005")
        assessment = self.store.assess_risk(session_id, principal_id, sixth)
        self.assertTrue(assessment.requires_captcha)
        self.assertIn("session_unique_burst", assessment.reasons)

    def test_repeated_missing_code_does_not_trigger_captcha(self):
        self.store.initialize()
        session_id = "missing-session"
        principal_id = "test-user|127.0.0.1"
        missing_key = self.store.lookup_key("199999999")
        for _ in range(10):
            self.store.record_event(
                session_id=session_id,
                principal_id=principal_id,
                lookup_key=missing_key,
                requested_field="name",
                outcome="no_suggestion",
                lookup_performed=True,
                record_found=False,
            )
        assessment = self.store.assess_risk(
            session_id, principal_id, self.store.lookup_key("188888888")
        )
        self.assertFalse(assessment.requires_captcha)

    def test_old_unique_codes_do_not_trigger_captcha(self):
        self.store.initialize()
        session_id = "old-session"
        principal_id = "test-user|127.0.0.1"
        for offset in range(5):
            self.store.record_event(
                session_id=session_id,
                principal_id=principal_id,
                lookup_key=self.store.lookup_key(str(100000000 + offset)),
                requested_field="name",
                outcome="suggestion",
                lookup_performed=True,
                record_found=True,
                suggestion_shown=True,
            )
        future = time.time() + 61
        assessment = self.store.assess_risk(
            session_id,
            principal_id,
            self.store.lookup_key("100000005"),
            now=future,
        )
        self.assertFalse(assessment.requires_captcha)

    def test_tvv_candidates_are_encrypted_and_shown_before_system_value(self):
        self.store.import_files([self.write_source()])
        changed = self.store.record_tvv_values(
            customer_code="100000000",
            values={
                "name": "KHÁCH HÀNG MỚI",
                "phone": "0912345678",
                "cccd": "012345678901",
            },
            user_id=7,
            phieu_id=101,
            tvv_code="E012345",
            tvv_name="TVV THỬ",
        )
        self.assertEqual(len(changed), 3)
        self.assertEqual(
            self.store.get_suggestions("100000000", "name"),
            [
                {"value": "KHÁCH HÀNG MỚI", "source": "pending"},
                {"value": "KHACH HANG THU", "source": "system"},
            ],
        )
        self.assertEqual(
            self.store.get_suggestions("100000000", "phone"),
            [
                {"value": "0912345678", "source": "pending"},
                {"value": "0900000000", "source": "system"},
            ],
        )
        self.assertEqual(
            self.store.get_suggestions("100000000", "cccd"),
            [{"value": "012345678901", "source": "pending"}],
        )
        database_bytes = self.store.db_path.read_bytes()
        self.assertNotIn("KHÁCH HÀNG MỚI".encode("utf-8"), database_bytes)
        self.assertNotIn(b"012345678901", database_bytes)

    def test_same_candidate_is_counted_once_and_reported(self):
        self.store.import_files([self.write_source()])
        for phieu_id in (201, 202):
            self.store.record_tvv_values(
                customer_code="100000000",
                values={"name": "TEN MOI"},
                user_id=9,
                phieu_id=phieu_id,
                tvv_code="E000009",
            )
        report = self.store.list_candidate_report("pending")
        self.assertEqual(report["total"], 1)
        self.assertEqual(report["items"][0]["seen_count"], 2)
        self.assertEqual(report["items"][0]["last_phieu_id"], 202)
        self.assertEqual(report["items"][0]["candidate_value"], "TEN MOI")

    def test_approve_replaces_official_and_supersedes_other_pending_values(self):
        self.store.import_files([self.write_source()])
        first_id = self.store.record_tvv_values(
            customer_code="100000000",
            values={"name": "TEN DUYET"},
            user_id=3,
            phieu_id=301,
        )[0]
        self.store.record_tvv_values(
            customer_code="100000000",
            values={"name": "TEN KHAC"},
            user_id=4,
            phieu_id=302,
        )
        self.assertTrue(self.store.review_candidate(first_id, "approve", 1))
        self.assertEqual(
            self.store.get_suggestions("100000000", "name"),
            [{"value": "TEN DUYET", "source": "approved"}],
        )
        self.assertEqual(self.store.list_candidate_report("pending")["total"], 0)
        self.assertEqual(self.store.list_candidate_report("superseded")["total"], 1)

    def test_reject_hides_candidate_and_keeps_system_value(self):
        self.store.import_files([self.write_source()])
        candidate_id = self.store.record_tvv_values(
            customer_code="100000000",
            values={"phone": "0987654321"},
            user_id=5,
            phieu_id=401,
        )[0]
        self.assertTrue(self.store.review_candidate(candidate_id, "reject", 1))
        self.assertEqual(
            self.store.get_suggestions("100000000", "phone"),
            [{"value": "0900000000", "source": "system"}],
        )
        self.assertFalse(self.store.review_candidate(candidate_id, "approve", 1))

    def test_invalid_tvv_values_do_not_create_candidates(self):
        self.store.import_files([self.write_source()])
        changed = self.store.record_tvv_values(
            customer_code="100000000",
            values={"name": "", "phone": "123", "cccd": "046xxx"},
            user_id=6,
            phieu_id=501,
        )
        self.assertEqual(changed, [])
        self.assertEqual(self.store.list_candidate_report("pending")["total"], 0)


if __name__ == "__main__":
    unittest.main()
