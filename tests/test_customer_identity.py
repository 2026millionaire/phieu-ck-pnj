import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook

from customer_identity import CustomerIdentityStore, select_identity_records


HEADERS = ["Inv.Date", "Vendor", "Tên Vendor", "CMND", "Plant"]


def make_workbook(path, rows):
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(HEADERS)
    for row in rows:
        sheet.append(row)
    workbook.save(path)


class CustomerIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = CustomerIdentityStore(self.root / "identity.db", b"I" * 32)
        self.store.initialize()

    def tearDown(self):
        self.temp.cleanup()

    def test_selects_first_nonblank_identity_in_source_order(self):
        path = self.root / "du-lieu-hue.xlsx"
        make_workbook(
            path,
            [
                ["17.07.2026", "100000001", "TEN MOI", "", "1305"],
                ["16.07.2026", "100000001", "TEN MOI", "012345678901", "1305"],
                ["15.07.2026", "100000001", "TEN CU", "999999999999", "1305"],
                ["17.07.2026", "100000002", "TEN HAI", "P1234567", "1394"],
                ["17.07.2026", "100000003", "TEN BA", "", "1465"],
            ],
        )
        records, summary = select_identity_records(path)
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["identity_value"], "012345678901")
        self.assertEqual(records[0]["customer_name"], "TEN MOI")
        self.assertEqual(records[1]["identity_value"], "P1234567")
        self.assertEqual(summary["unique_vendors"], 3)
        self.assertEqual(summary["missing_identity"], 1)
        self.assertEqual(summary["source_date_max"], "2026-07-17")

    def test_preserves_leading_zero_from_numeric_formatted_cell(self):
        path = self.root / "numeric.xlsx"
        workbook = Workbook()
        sheet = workbook.active
        sheet.append(HEADERS)
        sheet.append(["17.07.2026", "100000001", "TEN", 12345678901, "1305"])
        sheet["D2"].number_format = "000000000000"
        workbook.save(path)
        records, _summary = select_identity_records(path)
        self.assertEqual(records[0]["identity_value"], "012345678901")

    def test_accepts_new_plants_without_code_change(self):
        path = self.root / "new-region.xlsx"
        make_workbook(path, [["17.07.2026", "100000001", "TEN", "012345678901", "9999"]])
        records, summary = select_identity_records(path)
        self.assertEqual(summary["with_identity"], 1)
        self.assertEqual(records[0]["plant"], "9999")

    def test_keeps_e_vendor_out_of_customer_identity_store(self):
        path = self.root / "e-vendor.xlsx"
        make_workbook(path, [["17.07.2026", "e01f2345", "TEN E", "P1234567", "1305"]])
        records, summary = select_identity_records(path)
        self.assertEqual(records[0]["vendor"], "E01F2345")
        self.assertEqual(summary["with_identity"], 1)
        preview = self.store.preview_file(path)
        result = self.store.import_file(path, preview["source_sha256"], "initial")
        self.assertEqual(result["inserted_rows"], 0)
        self.assertIsNone(self.store.get_record("E01F2345"))

    def test_reports_and_skips_invalid_vendor_rows(self):
        path = self.root / "invalid-vendor.xlsx"
        make_workbook(
            path,
            [
                ["17.07.2026", "1465", "KHONG CO MA KH", "012345678901", "1465"],
                ["17.07.2026", "100000001", "TEN", "012345678902", "1465"],
            ],
        )
        records, summary = select_identity_records(path)
        self.assertEqual(len(records), 1)
        self.assertEqual(summary["invalid_vendor_rows"], 1)

    def test_initial_then_periodic_import_and_encryption(self):
        first = self.root / "first.xlsx"
        make_workbook(first, [["17.07.2026", "100000001", "TEN CU", "012345678901", "1305"]])
        preview = self.store.preview_file(first)
        self.assertEqual(preview["mode"], "initial")
        result = self.store.import_file(first, preview["source_sha256"], "initial")
        self.assertEqual(result["inserted_rows"], 1)
        record = self.store.get_record("100000001")
        self.assertEqual(record["identity_value"], "012345678901")
        self.assertEqual(record["verified_name"], "")

        second = self.root / "second.xlsx"
        make_workbook(second, [["18.07.2026", "100000001", "TEN DUNG", "A7654321", "1305"]])
        preview = self.store.preview_file(second)
        self.assertEqual(preview["mode"], "periodic")
        result = self.store.import_file(second, preview["source_sha256"], "periodic")
        self.assertEqual(result["updated_rows"], 1)
        self.assertEqual(result["name_updated"], 1)
        record = self.store.get_record("100000001")
        self.assertEqual(record["identity_value"], "A7654321")
        self.assertEqual(record["verified_name"], "TEN DUNG")

        database_bytes = (self.root / "identity.db").read_bytes()
        self.assertNotIn(b"A7654321", database_bytes)
        self.assertNotIn("TEN DUNG".encode("utf-8"), database_bytes)


if __name__ == "__main__":
    unittest.main()
