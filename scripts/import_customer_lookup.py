# -*- coding: utf-8 -*-
"""Nhập file SAP vào CSDL tra cứu mã hóa mà không in dữ liệu khách hàng."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from customer_lookup import CustomerLookupError, CustomerLookupStore


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", type=Path, help="Các file TSV SAP cần nhập")
    parser.add_argument("--expected-min", type=int)
    parser.add_argument("--expected-max", type=int)
    args = parser.parse_args()

    try:
        store = CustomerLookupStore.from_environment(create=True)
        result = store.import_files(
            args.files,
            expected_min=args.expected_min,
            expected_max=args.expected_max,
            progress=lambda count: print(f"Đã xử lý {count:,} bản ghi..."),
        )
        safe_result = {"database": str(store.db_path), **result}
        print(json.dumps(safe_result, ensure_ascii=False, indent=2))
        return 0
    except CustomerLookupError as exc:
        print(f"Không thể nhập dữ liệu: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
