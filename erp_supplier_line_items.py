# -*- coding: utf-8 -*-
"""Supplier line-item reference lookup for buyback statement numbers."""

from __future__ import annotations

import json
import os
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import requests

import erp_billing


ERP_BASE_URL = erp_billing.ERP_BASE_URL
ERP_TIMEOUT_SECONDS = erp_billing.ERP_TIMEOUT_SECONDS
MAX_REFERENCES = 30
REFERENCE_RE = re.compile(r"^\d{6}/\d{2}_\d{4}$")


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


def normalize_reference(value: Any) -> str:
    text = re.sub(r"\s+", "", str(value or "").strip())
    return text if REFERENCE_RE.fullmatch(text) else ""


def _reference_sort_key(reference: str) -> tuple[int, int, str]:
    match = re.match(r"^(\d{6})/(\d{2})_(\d{4})$", reference)
    if not match:
        return (0, 0, reference)
    return (int(match.group(1)), int(match.group(2)), match.group(3))


def normalize_purchase_order(value: Any) -> str:
    text = re.sub(r"\D+", "", str(value or ""))
    return text if re.fullmatch(r"4403\d{6}", text) else ""


def load_erp_supplier_line_items(customer_code: Any, target_date: date, lookback_days: int) -> list[dict[str, Any]]:
    canonical = normalize_customer_code(customer_code)
    if not canonical:
        return []
    earliest = target_date - timedelta(days=max(0, int(lookback_days or 0)))
    session = login_erp_session()
    url = f"{ERP_BASE_URL}/sap/opu/odata/sap/FAP_VENDOR_LINE_ITEMS_SRV/Items"
    params = {
        "$format": "json",
        "$top": "200",
        "$orderby": "PostingDate desc,AccountingDocument desc,AccountingDocumentItem asc",
        "$select": (
            "Supplier,CompanyCode,PostingDate,AccountingDocument,AccountingDocumentItem,"
            "AccountingDocumentType,AssignmentReference,DocumentReferenceID,"
            "AmountInCompanyCodeCurrency,TransactionCurrency,SpecialGeneralLedgerCode,"
            "FinancialAccountType"
        ),
        "$filter": (
            f"Supplier eq '{canonical}' "
            "and CompanyCode eq '1000' "
            f"and PostingDate ge {_sap_datetime_literal(earliest)} "
            f"and PostingDate le {_sap_datetime_literal(target_date)} "
            "and SpecialGeneralLedgerCode eq ''"
        ),
    }
    response = session.get(url, params=params, timeout=ERP_TIMEOUT_SECONDS, headers={"Accept": "application/json"})
    response.raise_for_status()
    rows = response.json().get("d", {}).get("results", [])
    return rows if isinstance(rows, list) else []


def load_supplier_line_item_fixture() -> list[dict[str, Any]]:
    fixture_path = os.environ.get("PNJ_SUPPLIER_LINE_ITEMS_FIXTURE_PATH", "").strip()
    if not fixture_path:
        return []
    path = Path(fixture_path)
    if not path.is_file():
        return []
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    records = raw.get("records", raw) if isinstance(raw, dict) else raw
    return records if isinstance(records, list) else []


def supplier_line_item_references(
    customer_code: Any,
    target_date: Any = None,
    lookback_days: Any = 0,
) -> list[dict[str, Any]]:
    canonical = normalize_customer_code(customer_code)
    if not canonical:
        return []
    anchor_date = parse_date(target_date) or date.today()
    try:
        lookback = max(0, min(int(lookback_days), 365))
    except (TypeError, ValueError):
        lookback = 0
    earliest = anchor_date - timedelta(days=lookback)
    source_records = (
        load_erp_supplier_line_items(canonical, anchor_date, lookback)
        if erp_credentials()
        else load_supplier_line_item_fixture()
    )
    seen = set()
    references: list[dict[str, Any]] = []
    for record in source_records:
        if not isinstance(record, dict):
            continue
        supplier = normalize_customer_code(record.get("Supplier") or record.get("supplier") or record.get("customer_code"))
        if supplier and supplier != canonical:
            continue
        if str(record.get("CompanyCode") or record.get("company_code") or "").strip() not in ("", "1000"):
            continue
        special_gl = str(record.get("SpecialGeneralLedgerCode") or record.get("special_general_ledger_code") or "").strip()
        if special_gl:
            continue
        posting_date = parse_sap_odata_date(record.get("PostingDate") or record.get("posting_date"))
        if not posting_date or posting_date < earliest or posting_date > anchor_date:
            continue
        reference = normalize_reference(record.get("DocumentReferenceID") or record.get("reference") or record.get("bk_ref"))
        if not reference:
            continue
        key = (posting_date.isoformat(), reference)
        if key in seen:
            continue
        seen.add(key)
        references.append(
            {
                "reference": reference,
                "posting_date": posting_date.isoformat(),
                "accounting_document": re.sub(r"\D+", "", str(record.get("AccountingDocument") or "")),
                "amount": erp_billing.parse_amount(record.get("AmountInCompanyCodeCurrency") or record.get("amount")),
                "source": "erp" if erp_credentials() else "fixture",
            }
        )
    references.sort(key=lambda item: (item["posting_date"], *_reference_sort_key(item["reference"])), reverse=True)
    return references[:MAX_REFERENCES]


def purchase_order_reference_mapping(
    customer_code: Any,
    purchase_orders: list[dict[str, Any]] | None,
    target_date: Any = None,
    lookback_days: Any = 0,
) -> dict[str, Any]:
    references = supplier_line_item_references(customer_code, target_date=target_date, lookback_days=lookback_days)
    po_by_date: dict[str, list[str]] = {}
    for item in purchase_orders or []:
        if not isinstance(item, dict):
            continue
        purchase_order = normalize_purchase_order(item.get("purchase_order") or item.get("so_ct"))
        creation_date = parse_date(item.get("creation_date") or item.get("date") or target_date)
        if not purchase_order or not creation_date:
            continue
        po_by_date.setdefault(creation_date.isoformat(), []).append(purchase_order)

    reference_by_date: dict[str, list[str]] = {}
    for item in references:
        reference_by_date.setdefault(item["posting_date"], []).append(item["reference"])

    mapping: dict[str, str] = {}
    for day, purchase_order_values in po_by_date.items():
        unique_pos = sorted(set(purchase_order_values), reverse=True)
        unique_refs = sorted(set(reference_by_date.get(day, [])), key=_reference_sort_key, reverse=True)
        for purchase_order, reference in zip(unique_pos, unique_refs):
            mapping[purchase_order] = reference

    return {
        "mapping": mapping,
        "references": references,
    }
