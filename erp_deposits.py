# -*- coding: utf-8 -*-
"""Customer deposit receipt suggestions from ERP customer line items."""

from __future__ import annotations

import json
import os
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import requests

import erp_billing


DEFAULT_LOOKBACK_DAYS = 0
MAX_SUGGESTIONS = 10
ERP_BASE_URL = erp_billing.ERP_BASE_URL
ERP_TIMEOUT_SECONDS = erp_billing.ERP_TIMEOUT_SECONDS
DEFAULT_COMPANY_CODE = "1000"
DEFAULT_PROFIT_CENTER = "0010241305"
DEFAULT_DOCUMENT_TYPE = "DZ"


def normalize_customer_code(value: Any) -> str:
    return erp_billing.normalize_customer_code(value)


def parse_date(value: Any) -> date | None:
    return erp_billing.parse_date(value)


def parse_sap_odata_date(value: Any) -> date | None:
    return erp_billing.parse_sap_odata_date(value)


def erp_credentials() -> tuple[str, str] | None:
    return erp_billing.erp_credentials()


def login_erp_session() -> requests.Session:
    return erp_billing.login_erp_session()


def _sap_datetime_literal(day: date) -> str:
    return f"datetime'{day.isoformat()}T00:00:00'"


def parse_signed_amount(value: Any) -> int:
    if isinstance(value, (int, float)):
        return int(round(value))
    text = str(value or "").strip()
    trailing_negative = text.endswith("-")
    cleaned = re.sub(r"[^\d-]", "", text)
    if trailing_negative:
        cleaned = "-" + re.sub(r"\D+", "", cleaned)
    try:
        return int(cleaned)
    except (TypeError, ValueError):
        return 0


def normalize_deposit_document(value: Any) -> str:
    document = re.sub(r"\D+", "", str(value or ""))
    return document if re.fullmatch(r"16\d{8}", document) else ""


def public_deposit_record(record: dict[str, Any]) -> dict[str, Any] | None:
    document = normalize_deposit_document(
        record.get("DocumentNo")
        or record.get("AccountingDocument")
        or record.get("accounting_document")
        or record.get("deposit_document")
    )
    if not document:
        return None

    amount = parse_signed_amount(
        record.get("Amount")
        or record.get("AmountInCompanyCodeCurrency")
        or record.get("AmountInTransactionCurrency")
        or record.get("amount")
    )
    if amount >= 0:
        return None

    posting_date = parse_sap_odata_date(record.get("PostingDate") or record.get("posting_date") or record.get("date"))
    customer_code = normalize_customer_code(
        record.get("Customer")
        or record.get("customer")
        or record.get("customer_code")
        or record.get("BusinessPartner")
    )
    if not posting_date or not customer_code:
        return None

    return {
        "deposit_document": document,
        "amount": abs(amount),
        "posting_date": posting_date.isoformat(),
        "customer_code": customer_code,
        "company_code": str(record.get("CompanyCode") or record.get("company_code") or "").strip(),
        "profit_center": str(record.get("ProfitCenter") or record.get("profit_center") or "").strip(),
        "document_type": str(
            record.get("AccountingDocumentType") or record.get("DocumentType") or record.get("document_type") or ""
        ).strip().upper(),
        "text": str(record.get("Text") or record.get("DocumentItemText") or record.get("text") or "").strip(),
        "source": str(record.get("source") or "fixture").strip() or "fixture",
    }


def load_erp_deposit_line_items(customer_code: Any, target_date: date, lookback_days: int, top: int = 80) -> list[dict[str, Any]]:
    canonical = normalize_customer_code(customer_code)
    if not canonical:
        return []
    earliest = target_date - timedelta(days=max(0, int(lookback_days or 0)))
    session = login_erp_session()
    url = os.environ.get(
        "PNJ_DEPOSIT_LINE_ITEMS_URL",
        f"{ERP_BASE_URL}/sap/opu/odata/sap/FAP_CUSTOMER_LINE_ITEMS_SRV/Items",
    )
    params = {
        "$format": "json",
        "$top": str(max(10, min(int(top or 80), 200))),
        "$orderby": "PostingDate desc,AccountingDocument desc,AccountingDocumentItem asc",
        "$select": (
            "Customer,CompanyCode,ProfitCenter,PostingDate,AccountingDocument,AccountingDocumentItem,"
            "AccountingDocumentType,AmountInCompanyCodeCurrency,TransactionCurrency,"
            "SpecialGeneralLedgerCode,DocumentItemText"
        ),
        "$filter": (
            f"Customer eq '{canonical}' "
            f"and CompanyCode eq '{DEFAULT_COMPANY_CODE}' "
            f"and ProfitCenter eq '{DEFAULT_PROFIT_CENTER}' "
            f"and PostingDate ge {_sap_datetime_literal(earliest)} "
            f"and PostingDate le {_sap_datetime_literal(target_date)} "
            f"and AccountingDocumentType eq '{DEFAULT_DOCUMENT_TYPE}'"
        ),
    }
    response = session.get(url, params=params, timeout=ERP_TIMEOUT_SECONDS, headers={"Accept": "application/json"})
    response.raise_for_status()
    rows = response.json().get("d", {}).get("results", [])
    for row in rows:
        if isinstance(row, dict):
            row["source"] = "erp"
    return rows if isinstance(rows, list) else []


def load_deposit_fixture() -> list[dict[str, Any]]:
    fixture_path = os.environ.get("PNJ_DEPOSIT_FIXTURE_PATH", "").strip()
    if not fixture_path:
        return []
    path = Path(fixture_path)
    if not path.is_file():
        return []
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    records = raw.get("records", raw) if isinstance(raw, dict) else raw
    return records if isinstance(records, list) else []


def deposit_suggestions(
    customer_code: Any,
    target_date: Any = None,
    lookback_days: Any = DEFAULT_LOOKBACK_DAYS,
    limit: Any = MAX_SUGGESTIONS,
) -> list[dict[str, Any]]:
    canonical_customer = normalize_customer_code(customer_code)
    if not canonical_customer:
        return []
    anchor_date = parse_date(target_date) or date.today()
    try:
        lookback = max(0, min(int(lookback_days), 365))
    except (TypeError, ValueError):
        lookback = DEFAULT_LOOKBACK_DAYS
    try:
        capped_limit = max(1, min(int(limit), MAX_SUGGESTIONS))
    except (TypeError, ValueError):
        capped_limit = MAX_SUGGESTIONS
    earliest = anchor_date - timedelta(days=lookback)

    source_records = (
        load_erp_deposit_line_items(canonical_customer, anchor_date, lookback, top=capped_limit * 4)
        if erp_credentials()
        else load_deposit_fixture()
    )
    matches = []
    seen_documents = set()
    for record in source_records:
        if not isinstance(record, dict):
            continue
        public_record = public_deposit_record(record)
        if public_record is None:
            continue
        if public_record["customer_code"] != canonical_customer:
            continue
        posting_date = parse_date(public_record["posting_date"])
        if not posting_date or posting_date > anchor_date or posting_date < earliest:
            continue
        company_code = public_record["company_code"]
        if company_code and company_code != DEFAULT_COMPANY_CODE:
            continue
        profit_center = public_record["profit_center"]
        if profit_center and profit_center != DEFAULT_PROFIT_CENTER:
            continue
        document_type = public_record["document_type"]
        if document_type and document_type != DEFAULT_DOCUMENT_TYPE:
            continue
        document = public_record["deposit_document"]
        if document in seen_documents:
            continue
        seen_documents.add(document)
        matches.append(public_record)

    matches.sort(key=lambda item: (item["posting_date"], item["deposit_document"]), reverse=True)
    return matches[:capped_limit]
