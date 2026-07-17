# -*- coding: utf-8 -*-
"""Kho dữ liệu tra cứu khách hàng được mã hóa tại chỗ.

File SQLite chỉ chứa khóa tra cứu HMAC và payload AES-256-GCM. Dữ liệu SAP
nguyên bản không được ghi vào log, terminal hoặc bảng chỉ mục.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import time
import unicodedata
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF


SCHEMA_VERSION = "1"
KEY_ENV_NAME = "CUSTOMER_LOOKUP_MASTER_KEY_B64"
DB_ENV_NAME = "CUSTOMER_LOOKUP_DB"
DATA_DIR_ENV_NAME = "CUSTOMER_LOOKUP_DATA_DIR"


class CustomerLookupError(RuntimeError):
    """Lỗi an toàn của kho tra cứu, không chứa dữ liệu khách hàng."""


@dataclass(frozen=True)
class RiskAssessment:
    requires_captcha: bool
    reasons: tuple[str, ...]
    already_looked_up: bool
    unique_codes_in_session: int


def default_data_dir() -> Path:
    configured = os.environ.get(DATA_DIR_ENV_NAME)
    if configured:
        return Path(configured).expanduser()
    if os.name == "nt":
        local_app_data = os.environ.get("LOCALAPPDATA")
        if not local_app_data:
            raise CustomerLookupError("Không xác định được thư mục LOCALAPPDATA.")
        return Path(local_app_data) / "PNJCustomerLookup"
    return Path.home() / ".local" / "share" / "pnj-customer-lookup"


def default_db_path() -> Path:
    configured = os.environ.get(DB_ENV_NAME)
    if configured:
        return Path(configured).expanduser()
    return default_data_dir() / "customer_lookup.db"


def _decode_env_master_key(value: str) -> bytes:
    try:
        key = base64.b64decode(value, validate=True)
    except Exception as exc:
        raise CustomerLookupError(f"{KEY_ENV_NAME} không phải Base64 hợp lệ.") from exc
    if len(key) != 32:
        raise CustomerLookupError(f"{KEY_ENV_NAME} phải giải mã thành đúng 32 byte.")
    return key


def _load_windows_dpapi_key(key_path: Path, create: bool) -> bytes:
    try:
        import win32crypt
    except ImportError as exc:
        raise CustomerLookupError("Thiếu pywin32 để mở khóa DPAPI trên Windows.") from exc

    if key_path.exists():
        try:
            protected = key_path.read_bytes()
            _description, key = win32crypt.CryptUnprotectData(
                protected, None, None, None, 0
            )
        except Exception as exc:
            raise CustomerLookupError("Không thể mở khóa tra cứu bằng Windows DPAPI.") from exc
        if len(key) != 32:
            raise CustomerLookupError("Khóa DPAPI có độ dài không hợp lệ.")
        return key

    if not create:
        raise CustomerLookupError("Chưa có khóa tra cứu local.")

    key_path.parent.mkdir(parents=True, exist_ok=True)
    master_key = secrets.token_bytes(32)
    try:
        protected = win32crypt.CryptProtectData(
            master_key,
            "PNJCustomerLookup",
            None,
            None,
            None,
            0x01,  # CRYPTPROTECT_UI_FORBIDDEN
        )
        temporary = key_path.with_suffix(".tmp")
        temporary.write_bytes(protected)
        os.replace(temporary, key_path)
    except Exception as exc:
        raise CustomerLookupError("Không thể tạo khóa tra cứu bằng Windows DPAPI.") from exc
    return master_key


def load_master_key(create: bool = False) -> bytes:
    configured = os.environ.get(KEY_ENV_NAME)
    if configured:
        return _decode_env_master_key(configured)
    if os.name != "nt":
        raise CustomerLookupError(
            f"Máy chủ ngoài Windows bắt buộc phải cấu hình {KEY_ENV_NAME}."
        )
    return _load_windows_dpapi_key(default_data_dir() / "master-key.dpapi", create)


def normalize_customer_code(value: object) -> str | None:
    raw = str(value or "").strip()
    if re.fullmatch(r"1[0-9]{8}", raw):
        return raw
    if re.fullmatch(r"0[0-9]{9}", raw):
        return raw[1:]
    return None


def suggestion_for_field(record: dict | None, field: str) -> str | None:
    """Áp dụng quy tắc hiển thị tại web, không thay đổi dữ liệu đã lưu."""
    if not record:
        return None
    if field == "name":
        value = record.get("name_1")
        if not isinstance(value, str) or not value.strip() or len(value) > 200:
            return None
        if any(unicodedata.category(ch).startswith("C") for ch in value):
            return None
        return value
    if field == "phone":
        value = record.get("search_term")
        if isinstance(value, str) and re.fullmatch(r"[0-9]{10}", value):
            return value
    if field == "cccd":
        value = record.get("cccd")
        if isinstance(value, str) and re.fullmatch(r"[0-9]{12}", value):
            return value
    return None


def normalize_tvv_value(field: str, value: object) -> str | None:
    raw = str(value or "")
    if field == "name":
        normalized = re.sub(r"\s+", " ", raw).strip().upper()
        if not normalized or len(normalized) > 200:
            return None
        if any(unicodedata.category(ch).startswith("C") for ch in normalized):
            return None
        return normalized
    normalized = re.sub(r"\s+", "", raw)
    if field == "phone" and re.fullmatch(r"[0-9]{10}", normalized):
        return normalized
    if field == "cccd" and re.fullmatch(r"[0-9]{12}", normalized):
        return normalized
    return None


def _derive_keys(master_key: bytes) -> tuple[bytes, bytes, bytes, bytes]:
    material = HKDF(
        algorithm=hashes.SHA256(),
        length=128,
        salt=b"pnj-customer-lookup-v1",
        info=b"separate lookup encryption digest audit keys",
    ).derive(master_key)
    return material[:32], material[32:64], material[64:96], material[96:128]


def _source_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_header(value: str) -> str:
    return re.sub(r"\s+", "", value).lower()


def _source_customer_range(path: Path) -> tuple[int, int] | None:
    match = re.fullmatch(r"([0-9]{9})-([0-9]{9})\.txt", path.name)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def _recover_shifted_sap_fields(
    path: Path, row: list[str], indexes: dict[str, int]
) -> dict[str, str] | None:
    """Khôi phục dòng TSV bị chèn thêm cột trống/tab nhưng vẫn có đủ neo dữ liệu."""
    source_range = _source_customer_range(path)
    customer_candidates: list[int] = []
    for index in range(indexes["customer"] + 1, len(row)):
        value = row[index].strip()
        if not re.fullmatch(r"[0-9]{9}", value):
            continue
        if source_range and not source_range[0] <= int(value) <= source_range[1]:
            continue
        customer_candidates.append(index)

    country_indexes = [
        index for index, value in enumerate(row) if value.strip().upper() == "VN"
    ]
    if len(customer_candidates) != 1 or len(country_indexes) != 1:
        return None

    customer_index = customer_candidates[0]
    shift = country_indexes[0] - indexes["cty"]
    if shift not in (0, 1):
        return None
    search_index = indexes["searchterm"] + shift
    name_start = indexes["name1"] + shift
    if not (0 <= search_index < len(row) and name_start < customer_index):
        return None

    # Mã KH phải được theo sau bởi cột mã công ty/kênh dạng số.
    if customer_index + 1 >= len(row) or not row[customer_index + 1].strip().isdigit():
        return None
    name_fragments = [
        value.strip() for value in row[name_start:customer_index] if value.strip()
    ]
    if not name_fragments:
        return None

    return {
        "customer": row[customer_index],
        "search_term": row[search_index],
        "name_1": " ".join(name_fragments),
    }


def iter_sap_records(path: Path) -> Iterable[dict[str, str]]:
    """Đọc TSV SAP và giữ nguyên giá trị logic của từng ô nguồn cần dùng."""
    with path.open("r", encoding="utf-8-sig", errors="strict", newline="") as source:
        reader = csv.reader(source, delimiter="\t")
        indexes: dict[str, int] | None = None
        for row in reader:
            if indexes is None:
                normalized = [_normalize_header(cell) for cell in row]
                required = ("searchterm", "cty", "name1", "customer", "delf")
                if all(name in normalized for name in required):
                    indexes = {name: normalized.index(name) for name in required}
                continue

            if not row or not any(cell for cell in row):
                continue

            def cell(name: str) -> str:
                index = indexes[name]
                return row[index] if index < len(row) else ""

            recovered = None
            if not re.fullmatch(r"[0-9]{9}", cell("customer").strip()):
                recovered = _recover_shifted_sap_fields(path, row, indexes)

            # SAP xuất DelF lệch sau một cột trống ở các dòng có cờ X.
            delf = "" if recovered else cell("delf")
            if indexes["delf"] < len(row):
                for candidate in row[indexes["delf"] :]:
                    if candidate.strip().upper() == "X":
                        delf = candidate
                        break

            yield {
                "customer": recovered["customer"] if recovered else cell("customer"),
                "search_term": recovered["search_term"] if recovered else cell("searchterm"),
                "name_1": recovered["name_1"] if recovered else cell("name1"),
                "delf": delf,
                "_recovered": "1" if recovered else "",
            }

        if indexes is None:
            raise CustomerLookupError(f"Không tìm thấy tiêu đề SAP hợp lệ trong {path.name}.")


class CustomerLookupStore:
    def __init__(self, db_path: Path | str, master_key: bytes):
        if len(master_key) != 32:
            raise CustomerLookupError("Khóa chính phải có đúng 32 byte.")
        self.db_path = Path(db_path)
        encryption_key, self.lookup_hmac_key, self.digest_key, self.audit_key = (
            _derive_keys(master_key)
        )
        self.aesgcm = AESGCM(encryption_key)

    @classmethod
    def from_environment(cls, create: bool = False) -> "CustomerLookupStore":
        db_path = default_db_path()
        if create:
            db_path.parent.mkdir(parents=True, exist_ok=True)
        elif not db_path.exists():
            raise CustomerLookupError("Chưa có CSDL tra cứu khách hàng local.")
        return cls(db_path, load_master_key(create=create))

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=15)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self.connect()) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS lookup_customers (
                    lookup_key        BLOB PRIMARY KEY,
                    record_nonce      BLOB NOT NULL,
                    record_ciphertext BLOB NOT NULL,
                    record_digest     BLOB NOT NULL,
                    source_batch      TEXT NOT NULL,
                    updated_at        REAL NOT NULL
                ) WITHOUT ROWID;

                CREATE TABLE IF NOT EXISTS import_batches (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_file    TEXT NOT NULL,
                    source_sha256  TEXT NOT NULL,
                    imported_at    REAL NOT NULL,
                    source_rows    INTEGER NOT NULL,
                    inserted_rows  INTEGER NOT NULL,
                    updated_rows   INTEGER NOT NULL,
                    unchanged_rows INTEGER NOT NULL,
                    delf_rows      INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS lookup_metadata (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                ) WITHOUT ROWID;

                CREATE TABLE IF NOT EXISTS lookup_events (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at       REAL NOT NULL,
                    session_hash     BLOB NOT NULL,
                    principal_hash   BLOB NOT NULL,
                    lookup_key       BLOB NOT NULL,
                    requested_field  TEXT NOT NULL,
                    outcome          TEXT NOT NULL,
                    lookup_performed INTEGER NOT NULL DEFAULT 0,
                    record_found     INTEGER NOT NULL DEFAULT 0,
                    suggestion_shown INTEGER NOT NULL DEFAULT 0,
                    captcha_passed   INTEGER NOT NULL DEFAULT 0
                );

                CREATE INDEX IF NOT EXISTS idx_lookup_events_session_time
                    ON lookup_events(session_hash, created_at);
                CREATE INDEX IF NOT EXISTS idx_lookup_events_principal_time
                    ON lookup_events(principal_hash, created_at);

                CREATE TABLE IF NOT EXISTS customer_field_candidates (
                    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                    lookup_key         BLOB NOT NULL,
                    field_name         TEXT NOT NULL CHECK(field_name IN ('name', 'phone', 'cccd')),
                    value_digest       BLOB NOT NULL,
                    payload_nonce      BLOB NOT NULL,
                    payload_ciphertext BLOB NOT NULL,
                    status             TEXT NOT NULL DEFAULT 'pending'
                                       CHECK(status IN ('pending', 'approved', 'rejected', 'superseded')),
                    seen_count         INTEGER NOT NULL DEFAULT 1,
                    first_seen_at      REAL NOT NULL,
                    last_seen_at       REAL NOT NULL,
                    first_user_id      INTEGER,
                    last_user_id       INTEGER,
                    first_phieu_id     INTEGER,
                    last_phieu_id      INTEGER,
                    reviewed_at        REAL,
                    reviewed_by        INTEGER
                );

                CREATE UNIQUE INDEX IF NOT EXISTS uq_customer_candidate_value
                    ON customer_field_candidates(lookup_key, field_name, value_digest);
                CREATE UNIQUE INDEX IF NOT EXISTS uq_customer_approved_field
                    ON customer_field_candidates(lookup_key, field_name)
                    WHERE status = 'approved';
                CREATE INDEX IF NOT EXISTS idx_customer_candidates_status_time
                    ON customer_field_candidates(status, last_seen_at DESC);
                """
            )
            connection.commit()

    def get_dataset_summary(self) -> dict:
        self.initialize()
        with closing(self.connect()) as connection:
            metadata = {
                row["key"]: row["value"]
                for row in connection.execute(
                    "SELECT key, value FROM lookup_metadata"
                ).fetchall()
            }
            record_count_value = metadata.get("record_count")
            if record_count_value is None:
                record_count = int(
                    connection.execute("SELECT COUNT(*) FROM lookup_customers").fetchone()[0]
                )
                connection.execute(
                    "INSERT OR REPLACE INTO lookup_metadata (key, value) VALUES ('record_count', ?)",
                    (str(record_count),),
                )
                connection.commit()
            else:
                record_count = int(record_count_value)
            last_batch = connection.execute(
                "SELECT imported_at FROM import_batches ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return {
            "record_count": record_count,
            "max_customer": metadata.get("max_customer", ""),
            "last_import_at": float(metadata.get("last_import_at") or (last_batch[0] if last_batch else 0)),
        }

    def seed_max_customer(self, customer_code: str) -> None:
        if not re.fullmatch(r"[0-9]{9}", str(customer_code or "")):
            raise CustomerLookupError("Mã KH lớn nhất không hợp lệ.")
        if self.get_record(customer_code) is None:
            raise CustomerLookupError("Không tìm thấy mã KH lớn nhất trong CSDL.")
        with closing(self.connect()) as connection:
            connection.execute(
                "INSERT OR REPLACE INTO lookup_metadata (key, value) VALUES ('max_customer', ?)",
                (customer_code,),
            )
            connection.commit()

    def create_backup(self, label: str = "before-import") -> Path:
        safe_label = re.sub(r"[^a-zA-Z0-9_-]+", "-", label).strip("-") or "before-import"
        backup_dir = self.db_path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(backup_dir, 0o700)
        except OSError:
            pass
        target = backup_dir / (
            f"customer_lookup-{safe_label}-{time.strftime('%Y%m%d-%H%M%S')}-"
            f"{secrets.token_hex(4)}.db"
        )
        try:
            with closing(sqlite3.connect(self.db_path)) as source, closing(sqlite3.connect(target)) as destination:
                source.backup(destination)
            with closing(sqlite3.connect(target)) as check:
                if check.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise CustomerLookupError("Bản sao lưu không vượt qua kiểm tra toàn vẹn.")
            try:
                os.chmod(target, 0o600)
            except OSError:
                pass
            return target
        except Exception as exc:
            target.unlink(missing_ok=True)
            if isinstance(exc, CustomerLookupError):
                raise
            raise CustomerLookupError("Không thể tạo bản sao lưu trước khi cập nhật.") from exc

    def validate_import_files(self, paths: Iterable[Path | str]) -> dict[str, int]:
        self.initialize()
        source_paths = [Path(path) for path in paths]
        if not source_paths or any(not path.is_file() for path in source_paths):
            raise CustomerLookupError("Danh sách file tải lên không hợp lệ.")

        with closing(self.connect()) as connection:
            imported_hashes = {
                row[0]
                for row in connection.execute(
                    "SELECT source_sha256 FROM import_batches"
                ).fetchall()
            }
        upload_hashes: set[str] = set()
        seen_codes: set[str] = set()
        total = delf_rows = recovered_rows = 0
        minimum: int | None = None
        maximum: int | None = None

        for path in source_paths:
            source_hash = _source_sha256(path)
            if source_hash in imported_hashes or source_hash in upload_hashes:
                raise CustomerLookupError("Có file đã được nhập trước đó hoặc bị tải lên trùng.")
            upload_hashes.add(source_hash)
            for record in iter_sap_records(path):
                if record.get("_recovered") == "1":
                    recovered_rows += 1
                canonical = record["customer"].strip()
                if not re.fullmatch(r"[0-9]{9}", canonical):
                    raise CustomerLookupError("Có dòng không xác định được mã KH 9 chữ số.")
                if canonical in seen_codes:
                    raise CustomerLookupError("Phát hiện mã KH trùng giữa các file tải lên.")
                seen_codes.add(canonical)
                numeric_code = int(canonical)
                minimum = numeric_code if minimum is None else min(minimum, numeric_code)
                maximum = numeric_code if maximum is None else max(maximum, numeric_code)
                total += 1
                if record["delf"].strip().upper() == "X":
                    delf_rows += 1

        if total == 0 or minimum is None or maximum is None:
            raise CustomerLookupError("File tải lên không có bản ghi khách hàng hợp lệ.")
        return {
            "file_count": len(source_paths),
            "source_rows": total,
            "delf_rows": delf_rows,
            "recovered_rows": recovered_rows,
            "min_customer": minimum,
            "max_customer": maximum,
        }

    def lookup_key(self, canonical_customer_code: str) -> bytes:
        return hmac.new(
            self.lookup_hmac_key,
            canonical_customer_code.encode("ascii"),
            hashlib.sha256,
        ).digest()

    def context_hash(self, purpose: str, value: str) -> bytes:
        message = f"{purpose}:{value}".encode("utf-8")
        return hmac.new(self.audit_key, message, hashlib.sha256).digest()

    def _serialize_record(self, record: dict[str, str]) -> bytes:
        return json.dumps(
            record, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")

    def _encrypt_record(self, record: dict[str, str], lookup_key: bytes) -> tuple[bytes, bytes, bytes]:
        plaintext = self._serialize_record(record)
        nonce = secrets.token_bytes(12)
        aad = b"customer-record-v1|" + lookup_key
        ciphertext = self.aesgcm.encrypt(nonce, plaintext, aad)
        digest = hmac.new(self.digest_key, plaintext, hashlib.sha256).digest()
        return nonce, ciphertext, digest

    def _decrypt_record(self, row: sqlite3.Row) -> dict[str, str]:
        aad = b"customer-record-v1|" + bytes(row["lookup_key"])
        try:
            plaintext = self.aesgcm.decrypt(
                bytes(row["record_nonce"]), bytes(row["record_ciphertext"]), aad
            )
            record = json.loads(plaintext.decode("utf-8"))
        except Exception as exc:
            raise CustomerLookupError("Không thể giải mã bản ghi tra cứu.") from exc
        if not isinstance(record, dict):
            raise CustomerLookupError("Payload tra cứu không hợp lệ.")
        return record

    def _candidate_digest(self, field: str, value: str) -> bytes:
        message = f"candidate:{field}\0{value}".encode("utf-8")
        return hmac.new(self.digest_key, message, hashlib.sha256).digest()

    def _encrypt_candidate_payload(
        self,
        payload: dict,
        lookup_key: bytes,
        field: str,
        value_digest: bytes,
    ) -> tuple[bytes, bytes]:
        plaintext = json.dumps(
            payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        nonce = secrets.token_bytes(12)
        aad = b"customer-candidate-v1|" + lookup_key + b"|" + field.encode("ascii") + b"|" + value_digest
        return nonce, self.aesgcm.encrypt(nonce, plaintext, aad)

    def _decrypt_candidate_payload(self, row: sqlite3.Row) -> dict:
        aad = (
            b"customer-candidate-v1|"
            + bytes(row["lookup_key"])
            + b"|"
            + row["field_name"].encode("ascii")
            + b"|"
            + bytes(row["value_digest"])
        )
        try:
            plaintext = self.aesgcm.decrypt(
                bytes(row["payload_nonce"]), bytes(row["payload_ciphertext"]), aad
            )
            payload = json.loads(plaintext.decode("utf-8"))
        except Exception as exc:
            raise CustomerLookupError("Không thể giải mã dữ liệu TVV chờ duyệt.") from exc
        if not isinstance(payload, dict):
            raise CustomerLookupError("Payload dữ liệu TVV không hợp lệ.")
        return payload

    def get_record(self, canonical_customer_code: str) -> dict[str, str] | None:
        key = self.lookup_key(canonical_customer_code)
        with closing(self.connect()) as connection:
            row = connection.execute(
                """
                SELECT lookup_key, record_nonce, record_ciphertext
                FROM lookup_customers
                WHERE lookup_key = ?
                """,
                (key,),
            ).fetchone()
        return self._decrypt_record(row) if row else None

    def _approved_value(self, lookup_key: bytes, field: str) -> str | None:
        with closing(self.connect()) as connection:
            row = connection.execute(
                """
                SELECT * FROM customer_field_candidates
                WHERE lookup_key = ? AND field_name = ? AND status = 'approved'
                LIMIT 1
                """,
                (lookup_key, field),
            ).fetchone()
        if not row:
            return None
        return normalize_tvv_value(field, self._decrypt_candidate_payload(row).get("value"))

    def get_official_value(self, canonical_customer_code: str, field: str) -> str | None:
        key = self.lookup_key(canonical_customer_code)
        approved = self._approved_value(key, field)
        if approved is not None:
            return approved
        baseline = suggestion_for_field(self.get_record(canonical_customer_code), field)
        return normalize_tvv_value(field, baseline)

    def get_suggestions(self, canonical_customer_code: str, field: str) -> list[dict[str, str]]:
        if field not in ("name", "phone", "cccd"):
            return []
        key = self.lookup_key(canonical_customer_code)
        with closing(self.connect()) as connection:
            rows = connection.execute(
                """
                SELECT * FROM customer_field_candidates
                WHERE lookup_key = ? AND field_name = ? AND status IN ('pending', 'approved')
                ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END, last_seen_at DESC
                """,
                (key, field),
            ).fetchall()

        pending: list[dict[str, str]] = []
        approved: str | None = None
        seen_values: set[str] = set()
        for row in rows:
            value = normalize_tvv_value(field, self._decrypt_candidate_payload(row).get("value"))
            if value is None or value in seen_values:
                continue
            seen_values.add(value)
            if row["status"] == "pending":
                pending.append({"value": value, "source": "pending"})
            else:
                approved = value

        suggestions = pending
        if approved is not None:
            if approved not in seen_values or not any(item["value"] == approved for item in suggestions):
                suggestions.append({"value": approved, "source": "approved"})
        else:
            baseline = suggestion_for_field(self.get_record(canonical_customer_code), field)
            if baseline is not None and baseline not in {item["value"] for item in suggestions}:
                suggestions.append({"value": baseline, "source": "system"})
        return suggestions

    def record_tvv_values(
        self,
        *,
        customer_code: object,
        values: dict[str, object],
        user_id: int | None,
        phieu_id: int | None,
        tvv_code: object = "",
        tvv_name: object = "",
    ) -> list[int]:
        canonical = normalize_customer_code(customer_code)
        if canonical is None:
            return []
        self.initialize()
        key = self.lookup_key(canonical)
        now = time.time()
        changed_ids: list[int] = []

        with closing(self.connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                for field in ("name", "phone", "cccd"):
                    value = normalize_tvv_value(field, values.get(field))
                    if value is None:
                        continue
                    official = self.get_official_value(canonical, field)
                    if official == value:
                        continue
                    digest = self._candidate_digest(field, value)
                    existing = connection.execute(
                        """
                        SELECT * FROM customer_field_candidates
                        WHERE lookup_key = ? AND field_name = ? AND value_digest = ?
                        """,
                        (key, field, digest),
                    ).fetchone()

                    if existing:
                        payload = self._decrypt_candidate_payload(existing)
                        payload["last_tvv_code"] = str(tvv_code or "").strip()
                        payload["last_tvv_name"] = str(tvv_name or "").strip()
                        nonce, ciphertext = self._encrypt_candidate_payload(payload, key, field, digest)
                        connection.execute(
                            """
                            UPDATE customer_field_candidates
                            SET payload_nonce = ?, payload_ciphertext = ?, seen_count = seen_count + 1,
                                last_seen_at = ?, last_user_id = ?, last_phieu_id = ?
                            WHERE id = ?
                            """,
                            (nonce, ciphertext, now, user_id, phieu_id, existing["id"]),
                        )
                        if existing["status"] == "pending":
                            changed_ids.append(int(existing["id"]))
                        continue

                    payload = {
                        "customer_code": canonical,
                        "value": value,
                        "original_value": official,
                        "first_tvv_code": str(tvv_code or "").strip(),
                        "first_tvv_name": str(tvv_name or "").strip(),
                        "last_tvv_code": str(tvv_code or "").strip(),
                        "last_tvv_name": str(tvv_name or "").strip(),
                    }
                    nonce, ciphertext = self._encrypt_candidate_payload(payload, key, field, digest)
                    cursor = connection.execute(
                        """
                        INSERT INTO customer_field_candidates (
                            lookup_key, field_name, value_digest, payload_nonce,
                            payload_ciphertext, status, seen_count, first_seen_at,
                            last_seen_at, first_user_id, last_user_id,
                            first_phieu_id, last_phieu_id
                        ) VALUES (?, ?, ?, ?, ?, 'pending', 1, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            key, field, digest, nonce, ciphertext, now, now,
                            user_id, user_id, phieu_id, phieu_id,
                        ),
                    )
                    changed_ids.append(int(cursor.lastrowid))
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return changed_ids

    def review_candidate(self, candidate_id: int, action: str, reviewer_user_id: int) -> bool:
        if action not in ("approve", "reject"):
            raise CustomerLookupError("Thao tác duyệt không hợp lệ.")
        with closing(self.connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM customer_field_candidates WHERE id = ?",
                    (candidate_id,),
                ).fetchone()
                if not row or row["status"] != "pending":
                    connection.rollback()
                    return False
                now = time.time()
                if action == "approve":
                    connection.execute(
                        """
                        UPDATE customer_field_candidates
                        SET status = 'superseded', reviewed_at = ?, reviewed_by = ?
                        WHERE lookup_key = ? AND field_name = ? AND id <> ?
                          AND status IN ('pending', 'approved')
                        """,
                        (now, reviewer_user_id, row["lookup_key"], row["field_name"], candidate_id),
                    )
                    status = "approved"
                else:
                    status = "rejected"
                connection.execute(
                    """
                    UPDATE customer_field_candidates
                    SET status = ?, reviewed_at = ?, reviewed_by = ?
                    WHERE id = ?
                    """,
                    (status, now, reviewer_user_id, candidate_id),
                )
                connection.commit()
                return True
            except Exception:
                connection.rollback()
                raise

    def list_candidate_report(
        self, status: str = "pending", page: int = 1, page_size: int = 50
    ) -> dict:
        allowed = ("pending", "approved", "rejected", "superseded")
        if status not in allowed:
            status = "pending"
        page = max(1, int(page))
        page_size = max(1, min(int(page_size), 50))
        offset = (page - 1) * page_size
        with closing(self.connect()) as connection:
            counts = {
                row["status"]: int(row["total"])
                for row in connection.execute(
                    """
                    SELECT status, COUNT(*) AS total
                    FROM customer_field_candidates GROUP BY status
                    """
                ).fetchall()
            }
            total = int(
                connection.execute(
                    "SELECT COUNT(*) FROM customer_field_candidates WHERE status = ?",
                    (status,),
                ).fetchone()[0]
            )
            rows = connection.execute(
                """
                SELECT * FROM customer_field_candidates
                WHERE status = ?
                ORDER BY last_seen_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (status, page_size, offset),
            ).fetchall()

        items = []
        for row in rows:
            payload = self._decrypt_candidate_payload(row)
            items.append(
                {
                    "id": int(row["id"]),
                    "customer_code": payload.get("customer_code", ""),
                    "field": row["field_name"],
                    "original_value": payload.get("original_value"),
                    "candidate_value": payload.get("value", ""),
                    "status": row["status"],
                    "seen_count": int(row["seen_count"]),
                    "first_seen_at": float(row["first_seen_at"]),
                    "last_seen_at": float(row["last_seen_at"]),
                    "first_user_id": row["first_user_id"],
                    "last_user_id": row["last_user_id"],
                    "first_phieu_id": row["first_phieu_id"],
                    "last_phieu_id": row["last_phieu_id"],
                    "reviewed_at": row["reviewed_at"],
                    "reviewed_by": row["reviewed_by"],
                    "first_tvv_code": payload.get("first_tvv_code", ""),
                    "first_tvv_name": payload.get("first_tvv_name", ""),
                    "last_tvv_code": payload.get("last_tvv_code", ""),
                    "last_tvv_name": payload.get("last_tvv_name", ""),
                }
            )
        return {
            "items": items,
            "counts": {name: counts.get(name, 0) for name in allowed},
            "status": status,
            "page": page,
            "page_size": page_size,
            "total": total,
            "has_next": offset + len(items) < total,
        }

    def import_files(
        self,
        paths: Iterable[Path | str],
        expected_min: int | None = None,
        expected_max: int | None = None,
        progress: Callable[[int], None] | None = None,
    ) -> dict[str, int]:
        self.initialize()
        source_paths = [Path(path) for path in paths]
        if not source_paths or any(not path.is_file() for path in source_paths):
            raise CustomerLookupError("Danh sách file nguồn không hợp lệ.")

        total = inserted = updated = unchanged = delf_rows = recovered_rows = 0
        seen_codes: set[str] = set()
        minimum: int | None = None
        maximum: int | None = None

        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing_count_row = connection.execute(
                "SELECT value FROM lookup_metadata WHERE key = 'record_count'"
            ).fetchone()
            existing_count = (
                int(existing_count_row[0])
                if existing_count_row
                else int(
                    connection.execute("SELECT COUNT(*) FROM lookup_customers").fetchone()[0]
                )
            )
            for path in source_paths:
                batch_rows = batch_inserted = batch_updated = batch_unchanged = 0
                batch_delf = 0
                source_hash = _source_sha256(path)

                for record in iter_sap_records(path):
                    if record.pop("_recovered", ""):
                        recovered_rows += 1
                    canonical = record["customer"].strip()
                    if not re.fullmatch(r"[0-9]{9}", canonical):
                        raise CustomerLookupError(
                            f"{path.name} chứa dòng không có mã KH 9 chữ số; đã hủy toàn bộ giao dịch nhập."
                        )
                    if canonical in seen_codes:
                        raise CustomerLookupError(
                            "Phát hiện mã KH trùng giữa các file nguồn; đã hủy toàn bộ giao dịch nhập."
                        )
                    seen_codes.add(canonical)

                    numeric_code = int(canonical)
                    minimum = numeric_code if minimum is None else min(minimum, numeric_code)
                    maximum = numeric_code if maximum is None else max(maximum, numeric_code)
                    if expected_min is not None and numeric_code < expected_min:
                        raise CustomerLookupError("Có mã KH thấp hơn phạm vi dự kiến.")
                    if expected_max is not None and numeric_code > expected_max:
                        raise CustomerLookupError("Có mã KH cao hơn phạm vi dự kiến.")

                    key = self.lookup_key(canonical)
                    nonce, ciphertext, digest = self._encrypt_record(record, key)
                    existing = connection.execute(
                        "SELECT record_digest FROM lookup_customers WHERE lookup_key = ?",
                        (key,),
                    ).fetchone()
                    now = time.time()
                    if existing is None:
                        connection.execute(
                            """
                            INSERT INTO lookup_customers (
                                lookup_key, record_nonce, record_ciphertext,
                                record_digest, source_batch, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (key, nonce, ciphertext, digest, path.name, now),
                        )
                        inserted += 1
                        batch_inserted += 1
                    elif hmac.compare_digest(bytes(existing["record_digest"]), digest):
                        unchanged += 1
                        batch_unchanged += 1
                    else:
                        connection.execute(
                            """
                            UPDATE lookup_customers
                            SET record_nonce = ?, record_ciphertext = ?, record_digest = ?,
                                source_batch = ?, updated_at = ?
                            WHERE lookup_key = ?
                            """,
                            (nonce, ciphertext, digest, path.name, now, key),
                        )
                        updated += 1
                        batch_updated += 1

                    total += 1
                    batch_rows += 1
                    if record["delf"].strip().upper() == "X":
                        delf_rows += 1
                        batch_delf += 1
                    if progress and total % 50000 == 0:
                        progress(total)

                connection.execute(
                    """
                    INSERT INTO import_batches (
                        source_file, source_sha256, imported_at, source_rows,
                        inserted_rows, updated_rows, unchanged_rows, delf_rows
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        path.name,
                        source_hash,
                        time.time(),
                        batch_rows,
                        batch_inserted,
                        batch_updated,
                        batch_unchanged,
                        batch_delf,
                    ),
                )

            if expected_min is not None and minimum != expected_min:
                raise CustomerLookupError("Không tìm thấy đúng mã KH đầu phạm vi dự kiến.")
            if expected_max is not None and maximum != expected_max:
                raise CustomerLookupError("Không tìm thấy đúng mã KH cuối phạm vi dự kiến.")
            existing_max_row = connection.execute(
                "SELECT value FROM lookup_metadata WHERE key = 'max_customer'"
            ).fetchone()
            existing_max = int(existing_max_row[0]) if existing_max_row else 0
            effective_max = max(existing_max, maximum or 0)
            imported_at = time.time()
            connection.execute(
                "INSERT OR REPLACE INTO lookup_metadata (key, value) VALUES ('max_customer', ?)",
                (str(effective_max),),
            )
            connection.execute(
                "INSERT OR REPLACE INTO lookup_metadata (key, value) VALUES ('last_import_at', ?)",
                (str(imported_at),),
            )
            connection.execute(
                "INSERT OR REPLACE INTO lookup_metadata (key, value) VALUES ('record_count', ?)",
                (str(existing_count + inserted),),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "source_rows": total,
            "inserted_rows": inserted,
            "updated_rows": updated,
            "unchanged_rows": unchanged,
            "delf_rows": delf_rows,
            "recovered_rows": recovered_rows,
            "min_customer": minimum or 0,
            "max_customer": maximum or 0,
        }

    def assess_risk(
        self,
        session_id: str,
        principal_id: str,
        lookup_key: bytes,
        now: float | None = None,
    ) -> RiskAssessment:
        now = now or time.time()
        session_hash = self.context_hash("session", session_id)
        principal_hash = self.context_hash("principal", principal_id)
        reasons: list[str] = []

        with closing(self.connect()) as connection:
            already = connection.execute(
                """
                SELECT 1 FROM lookup_events
                WHERE session_hash = ? AND lookup_key = ? AND lookup_performed = 1
                  AND created_at >= ?
                LIMIT 1
                """,
                (session_hash, lookup_key, now - 60),
            ).fetchone() is not None
            unique_count = connection.execute(
                """
                SELECT COUNT(DISTINCT lookup_key) FROM lookup_events
                WHERE session_hash = ? AND lookup_performed = 1
                  AND created_at >= ?
                """,
                (session_hash, now - 60),
            ).fetchone()[0]
            recent_distinct_codes = connection.execute(
                """
                SELECT COUNT(DISTINCT lookup_key) FROM lookup_events
                WHERE session_hash = ? AND lookup_performed = 1 AND created_at >= ?
                """,
                (session_hash, now - 5),
            ).fetchone()[0]
            principal_stats = connection.execute(
                """
                SELECT COUNT(*) AS requests,
                       COUNT(DISTINCT session_hash) AS sessions,
                       COUNT(DISTINCT lookup_key) AS unique_codes
                FROM lookup_events
                WHERE principal_hash = ? AND lookup_performed = 1 AND created_at >= ?
                """,
                (principal_hash, now - 60),
            ).fetchone()

        if not already and unique_count >= 5:
            reasons.append("session_unique_burst")
        if not already and recent_distinct_codes >= 4:
            reasons.append("robotic_distinct_codes")
        if (
            principal_stats["unique_codes"] >= 30
            and principal_stats["sessions"] >= 8
        ):
            reasons.append("session_rotation")

        return RiskAssessment(bool(reasons), tuple(reasons), already, int(unique_count))

    def record_event(
        self,
        *,
        session_id: str,
        principal_id: str,
        lookup_key: bytes,
        requested_field: str,
        outcome: str,
        lookup_performed: bool,
        record_found: bool = False,
        suggestion_shown: bool = False,
        captcha_passed: bool = False,
    ) -> None:
        session_hash = self.context_hash("session", session_id)
        principal_hash = self.context_hash("principal", principal_id)
        with closing(self.connect()) as connection:
            connection.execute(
                """
                INSERT INTO lookup_events (
                    created_at, session_hash, principal_hash, lookup_key,
                    requested_field, outcome, lookup_performed, record_found,
                    suggestion_shown, captcha_passed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    time.time(),
                    session_hash,
                    principal_hash,
                    lookup_key,
                    requested_field,
                    outcome,
                    int(lookup_performed),
                    int(record_found),
                    int(suggestion_shown),
                    int(captcha_passed),
                ),
            )
            connection.commit()

    def count_customers(self) -> int:
        with closing(self.connect()) as connection:
            return int(connection.execute("SELECT COUNT(*) FROM lookup_customers").fetchone()[0])
