# -*- coding: utf-8 -*-
"""Kho mã NV E01 độc lập, mã hóa tại chỗ và chỉ dùng cho gợi ý BK."""

from __future__ import annotations

import csv
import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import time
from contextlib import closing
from pathlib import Path

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from customer_lookup import CustomerLookupError, default_data_dir, load_master_key


EMPLOYEE_DB_ENV_NAME = "EMPLOYEE_LOOKUP_DB"
EMPLOYEE_CODE_RE = re.compile(r"E01(?:[MF][0-9]{4}|[0-9]{5})", re.IGNORECASE)


def default_employee_db_path() -> Path:
    return Path(os.environ.get(EMPLOYEE_DB_ENV_NAME) or default_data_dir() / "employee_lookup.db")


def normalize_employee_code(value: object) -> str | None:
    raw = str(value or "").strip().upper()
    return raw if EMPLOYEE_CODE_RE.fullmatch(raw) else None


def _keys(master_key: bytes) -> tuple[bytes, bytes, bytes]:
    material = HKDF(
        algorithm=hashes.SHA256(), length=96,
        salt=b"pnj-employee-lookup-v1", info=b"separate employee lookup keys",
    ).derive(master_key)
    return material[:32], material[32:64], material[64:96]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def iter_employee_records(path: Path | str):
    """Đọc TXT SAP; chỉ trả về Customer có mã NV E01 hợp lệ."""
    source_path = Path(path)
    with source_path.open("r", encoding="utf-8-sig", errors="strict", newline="") as source:
        reader = csv.reader(source, delimiter="\t")
        indexes = None
        for row in reader:
            if indexes is None:
                normalized = [re.sub(r"\s+", "", cell).casefold() for cell in row]
                needed = ("searchterm", "name1", "customer", "delf")
                if all(name in normalized for name in needed):
                    indexes = {name: normalized.index(name) for name in needed}
                continue
            if not any(row):
                continue
            def cell(name):
                index = indexes[name]
                return row[index].strip() if index < len(row) else ""
            code = normalize_employee_code(cell("customer"))
            if code is None:
                continue
            yield {
                "employee_code": code,
                "name_1": cell("name1"),
                "search_term": re.sub(r"\s+", "", cell("searchterm")),
                "cccd": "",
                "source_date": "",
                "delf": cell("delf"),
            }
        if indexes is None:
            raise CustomerLookupError("Không tìm thấy tiêu đề SAP hợp lệ của danh sách mã NV.")


class EmployeeLookupStore:
    def __init__(self, db_path: Path | str, master_key: bytes):
        self.db_path = Path(db_path)
        enc, self.hmac_key, self.digest_key = _keys(master_key)
        self.aesgcm = AESGCM(enc)

    @classmethod
    def from_environment(cls, create=False):
        path = default_employee_db_path()
        if create:
            path.parent.mkdir(parents=True, exist_ok=True)
        elif not path.exists():
            raise CustomerLookupError("Chưa có CSDL mã NV.")
        return cls(path, load_master_key(create=create))

    def connect(self):
        connection = sqlite3.connect(self.db_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def initialize(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self.connect()) as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS employee_records (
                    lookup_key BLOB PRIMARY KEY, nonce BLOB NOT NULL,
                    ciphertext BLOB NOT NULL, digest BLOB NOT NULL, updated_at REAL NOT NULL
                ) WITHOUT ROWID;
                CREATE TABLE IF NOT EXISTS employee_import_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, source_sha256 TEXT NOT NULL UNIQUE,
                    imported_at REAL NOT NULL, source_rows INTEGER NOT NULL,
                    inserted_rows INTEGER NOT NULL, updated_rows INTEGER NOT NULL,
                    unchanged_rows INTEGER NOT NULL, latest_new_code TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS employee_metadata (
                    key TEXT PRIMARY KEY, value TEXT NOT NULL
                ) WITHOUT ROWID;
            """)
            connection.commit()

    def lookup_key(self, code: str) -> bytes:
        return hmac.new(self.hmac_key, code.encode("ascii"), hashlib.sha256).digest()

    def _encrypt(self, payload, key):
        plain = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        nonce = secrets.token_bytes(12)
        return nonce, self.aesgcm.encrypt(nonce, plain, b"employee-record-v1|" + key), hmac.new(self.digest_key, plain, hashlib.sha256).digest()

    def _decrypt(self, row):
        try:
            plain = self.aesgcm.decrypt(bytes(row["nonce"]), bytes(row["ciphertext"]), b"employee-record-v1|" + bytes(row["lookup_key"]))
            return json.loads(plain.decode("utf-8"))
        except Exception as exc:
            raise CustomerLookupError("Không thể giải mã bản ghi mã NV.") from exc

    def get_record(self, code):
        canonical = normalize_employee_code(code)
        if canonical is None or not self.db_path.exists(): return None
        key = self.lookup_key(canonical)
        with closing(self.connect()) as connection:
            row = connection.execute("SELECT lookup_key, nonce, ciphertext FROM employee_records WHERE lookup_key=?", (key,)).fetchone()
        return self._decrypt(row) if row else None

    def get_suggestions(self, code, field):
        record = self.get_record(code)
        if not record: return []
        value = str(record.get("name_1" if field == "name" else field if field == "cccd" else "search_term", "")).strip()
        if field == "phone" and not re.fullmatch(r"[0-9]{10}", value): value = ""
        if field == "cccd" and not re.fullmatch(r"[0-9]{12}", value): value = ""
        if field == "name" and (not value or len(value) > 200): value = ""
        return [{"value": value, "source": "employee"}] if value else []

    def get_summary(self):
        self.initialize()
        with closing(self.connect()) as connection:
            meta = {r["key"]: r["value"] for r in connection.execute("SELECT key,value FROM employee_metadata")}
            count = int(connection.execute("SELECT COUNT(*) FROM employee_records").fetchone()[0])
        return {"record_count": count, "latest_new_code": meta.get("latest_new_code", ""), "last_import_at": float(meta.get("last_import_at") or 0)}

    def import_file(self, path):
        self.initialize(); source_path = Path(path); records = list(iter_employee_records(source_path))
        if not records: raise CustomerLookupError("File không có mã NV E01 hợp lệ.")
        codes = [r["employee_code"] for r in records]
        if len(codes) != len(set(codes)): raise CustomerLookupError("File có mã NV trùng; đã hủy nhập.")
        latest = max((c for c in codes if re.fullmatch(r"E01[0-9]{5}", c)), default="")
        sha = _sha256(source_path); inserted = updated = unchanged = 0
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            if connection.execute("SELECT 1 FROM employee_import_batches WHERE source_sha256=?", (sha,)).fetchone():
                raise CustomerLookupError("File mã NV này đã được nhập trước đó.")
            for record in records:
                key = self.lookup_key(record["employee_code"]); nonce, ciphertext, digest = self._encrypt(record, key)
                existing = connection.execute("SELECT digest FROM employee_records WHERE lookup_key=?", (key,)).fetchone()
                if existing is None:
                    connection.execute("INSERT INTO employee_records VALUES (?,?,?,?,?)", (key,nonce,ciphertext,digest,time.time())); inserted += 1
                elif hmac.compare_digest(bytes(existing["digest"]), digest): unchanged += 1
                else:
                    connection.execute("UPDATE employee_records SET nonce=?,ciphertext=?,digest=?,updated_at=? WHERE lookup_key=?", (nonce,ciphertext,digest,time.time(),key)); updated += 1
            now=time.time()
            connection.execute("INSERT INTO employee_import_batches (source_sha256,imported_at,source_rows,inserted_rows,updated_rows,unchanged_rows,latest_new_code) VALUES (?,?,?,?,?,?,?)", (sha,now,len(records),inserted,updated,unchanged,latest))
            previous = connection.execute("SELECT value FROM employee_metadata WHERE key='latest_new_code'").fetchone()
            connection.execute("INSERT OR REPLACE INTO employee_metadata VALUES ('latest_new_code',?)", (max(previous[0] if previous else "", latest),))
            connection.execute("INSERT OR REPLACE INTO employee_metadata VALUES ('last_import_at',?)", (str(now),))
            connection.commit()
        except Exception:
            connection.rollback(); raise
        finally: connection.close()
        return {"source_rows":len(records),"inserted_rows":inserted,"updated_rows":updated,"unchanged_rows":unchanged,"latest_new_code":latest}

    def merge_identity_records(self, identity_records):
        """Nhập CCCD đã có của mã E01 từ kho cũ, sau khi kho NV đã tồn tại."""
        merged = 0
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            for source in identity_records:
                code = normalize_employee_code(source.get("customer_code"))
                if code is None:
                    continue
                key = self.lookup_key(code)
                row = connection.execute("SELECT lookup_key,nonce,ciphertext FROM employee_records WHERE lookup_key=?", (key,)).fetchone()
                record = self._decrypt(row) if row else {"employee_code": code, "name_1": "", "search_term": "", "cccd": "", "delf": ""}
                identity = str(source.get("identity_value") or "").strip()
                if not identity or record.get("cccd") == identity:
                    continue
                record["cccd"] = identity
                if not record.get("name_1"):
                    record["name_1"] = str(source.get("source_name") or "").strip()
                nonce,ciphertext,digest = self._encrypt(record,key)
                if row is None:
                    connection.execute("INSERT INTO employee_records VALUES (?,?,?,?,?)", (key,nonce,ciphertext,digest,time.time()))
                else:
                    connection.execute("UPDATE employee_records SET nonce=?,ciphertext=?,digest=?,updated_at=? WHERE lookup_key=?", (nonce,ciphertext,digest,time.time(),key))
                merged += 1
            connection.commit()
        except Exception:
            connection.rollback(); raise
        finally: connection.close()
        return merged

    def import_identity_records(self, records, source_sha):
        """Cập nhật tên/CCCD từ bảng kê, chỉ thay dữ liệu có ngày mới hơn."""
        selected = [r for r in records if normalize_employee_code(r.get("vendor"))]
        if not selected:
            return {"source_rows": 0, "inserted_rows": 0, "updated_rows": 0, "unchanged_rows": 0}
        inserted = updated = unchanged = 0
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            if connection.execute("SELECT 1 FROM employee_import_batches WHERE source_sha256=?", (source_sha,)).fetchone():
                raise CustomerLookupError("File bảng kê này đã được cập nhật mã NV trước đó.")
            for source in selected:
                code = normalize_employee_code(source["vendor"]); key = self.lookup_key(code)
                row = connection.execute("SELECT lookup_key,nonce,ciphertext,digest FROM employee_records WHERE lookup_key=?", (key,)).fetchone()
                current = self._decrypt(row) if row else {"employee_code": code, "name_1": "", "search_term": "", "cccd": "", "source_date": "", "delf": ""}
                if current.get("source_date") and source.get("source_date") and source["source_date"] < current["source_date"]:
                    unchanged += 1; continue
                candidate = dict(current)
                candidate["employee_code"] = code
                if str(source.get("customer_name") or "").strip(): candidate["name_1"] = str(source["customer_name"]).strip()
                candidate["cccd"] = str(source.get("identity_value") or "").strip()
                candidate["source_date"] = str(source.get("source_date") or "")
                nonce,ciphertext,digest = self._encrypt(candidate,key)
                if row is None:
                    connection.execute("INSERT INTO employee_records VALUES (?,?,?,?,?)", (key,nonce,ciphertext,digest,time.time())); inserted += 1
                elif hmac.compare_digest(bytes(row["digest"]),digest): unchanged += 1
                else:
                    connection.execute("UPDATE employee_records SET nonce=?,ciphertext=?,digest=?,updated_at=? WHERE lookup_key=?", (nonce,ciphertext,digest,time.time(),key)); updated += 1
            now = time.time()
            latest_row = connection.execute(
                "SELECT value FROM employee_metadata WHERE key='latest_new_code'"
            ).fetchone()
            latest = latest_row[0] if latest_row else ""
            connection.execute("INSERT INTO employee_import_batches (source_sha256,imported_at,source_rows,inserted_rows,updated_rows,unchanged_rows,latest_new_code) VALUES (?,?,?,?,?,?,?)", (source_sha,now,len(selected),inserted,updated,unchanged,latest))
            connection.execute("INSERT OR REPLACE INTO employee_metadata VALUES ('last_import_at',?)", (str(now),))
            connection.commit()
        except Exception:
            connection.rollback(); raise
        finally: connection.close()
        return {"source_rows":len(selected),"inserted_rows":inserted,"updated_rows":updated,"unchanged_rows":unchanged}
