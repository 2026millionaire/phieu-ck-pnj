# -*- coding: utf-8 -*-
"""Billing-document suggestion provider for the PNJ ERP/Fiori integration.

The first implementation deliberately reads from a local JSON fixture so the UI
flow can be tested without storing ERP credentials or touching production data.
The provider boundary is intentionally small: replace ``load_billing_documents``
with a real ERP/OData scraper when the authenticated source is ready.
"""

from __future__ import annotations

import json
import os
import re
import html
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests


DEFAULT_LOOKBACK_DAYS = 1
MAX_SUGGESTIONS = 10
ERP_BASE_URL = os.environ.get("PNJ_ERP_BASE_URL", "https://erp.pnj.com.vn").rstrip("/")
ERP_TIMEOUT_SECONDS = 30
VAT_INCLUDED_BILLING_TYPES = {"ZWA", "ZPTG"}


def normalize_customer_code(value: Any) -> str:
    text = re.sub(r"\s+", "", str(value or "")).upper()
    if text.startswith("0") and text[1:].isdigit():
        text = text.lstrip("0")
    return text


def parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    return None


def parse_amount(value: Any) -> int:
    if isinstance(value, (int, float)):
        return int(round(value))
    text = str(value or "")
    cleaned = re.sub(r"[^\d-]", "", text)
    if not cleaned or cleaned == "-":
        return 0
    return int(cleaned)


def billing_document_type(record: dict[str, Any]) -> str:
    return str(
        record.get("billing_type")
        or record.get("BillingDocumentType")
        or record.get("document_type")
        or record.get("DocumentType")
        or ""
    ).strip().upper()


def display_billing_amount(amount: int, billing_type: str, already_adjusted: Any = False) -> int:
    if already_adjusted:
        return int(round(amount))
    if billing_type in VAT_INCLUDED_BILLING_TYPES:
        return int(round(amount * 1.1))
    return int(round(amount))


def parse_sap_odata_date(value: Any) -> date | None:
    text = str(value or "").strip()
    match = re.fullmatch(r"/Date\((-?\d+)\)/", text)
    if match:
        return datetime.utcfromtimestamp(int(match.group(1)) / 1000).date()
    return parse_date(text)


def is_cancelled_value(value: Any) -> bool:
    return str(value or "").strip().lower() in {"yes", "y", "true", "1", "x"}


def has_cancelled_bill_doc(value: Any) -> bool:
    return re.fullmatch(r"90\d{8}", re.sub(r"\D+", "", str(value or ""))) is not None


def public_billing_record(record: dict[str, Any]) -> dict[str, Any] | None:
    document = re.sub(
        r"\D+",
        "",
        str(record.get("billing_document") or record.get("BillingDocument") or record.get("document") or ""),
    )
    if not re.fullmatch(r"90\d{8}", document):
        return None
    billing_type = billing_document_type(record)
    amount = parse_amount(record.get("net_value", record.get("TotalNetAmount", record.get("amount"))))
    amount = display_billing_amount(amount, billing_type, record.get("amount_includes_vat"))
    billing_date = parse_sap_odata_date(record.get("billing_date") or record.get("BillingDocumentDate") or record.get("date"))
    customer_code = normalize_customer_code(
        record.get("customer_code") or record.get("SoldToParty") or record.get("sold_to_party_code") or record.get("sold_to")
    )
    if not billing_date or not customer_code:
        return None
    canceled_bill_doc = re.sub(
        r"\D+", "",
        str(record.get("canceled_bill_doc") or record.get("CancelledBillingDocument") or record.get("canceled_billing_document") or ""),
    )
    cancelled = str(record.get("cancelled") if "cancelled" in record else record.get("BillingDocumentIsCancelled", record.get("canceled", ""))).strip()
    return {
        "billing_document": document,
        "amount": amount,
        "billing_type": billing_type,
        "amount_includes_vat": billing_type in VAT_INCLUDED_BILLING_TYPES or bool(record.get("amount_includes_vat")),
        "billing_date": billing_date.isoformat(),
        "customer_code": customer_code,
        "customer_name": str(record.get("customer_name") or record.get("sold_to_party_name") or "").strip(),
        "canceled_bill_doc": canceled_bill_doc,
        "cancelled": cancelled,
        "source": str(record.get("source") or "fixture").strip() or "fixture",
    }


def erp_credentials() -> tuple[str, str] | None:
    user = os.environ.get("PNJ_ERP_USER", "").strip()
    password = os.environ.get("PNJ_ERP_PASSWORD", "")
    if not user or not password:
        return None
    return user, password


def login_erp_session() -> requests.Session:
    credentials = erp_credentials()
    if not credentials:
        raise RuntimeError("ERP credentials are not configured.")
    user, password = credentials
    session = requests.Session()
    response = session.get(f"{ERP_BASE_URL}/fiori", timeout=ERP_TIMEOUT_SECONDS)
    response.raise_for_status()
    fields = {}
    for match in re.finditer(r"<input([^>]+)>", response.text):
        attrs = dict(re.findall(r'(name|value|type)="([^"]*)"', match.group(1)))
        if "name" in attrs:
            name = html.unescape(attrs["name"])
            value = html.unescape(attrs.get("value", ""))
            if name not in fields or (not fields[name] and value):
                fields[name] = value
    fields.update({"sap-user": user, "sap-password": password, "sap-language": "EN"})
    login_response = session.post(
        f"{ERP_BASE_URL}/fiori",
        data=fields,
        timeout=ERP_TIMEOUT_SECONDS,
        allow_redirects=True,
    )
    login_response.raise_for_status()
    if "sap-password" in login_response.text and "Log On" in login_response.text:
        raise RuntimeError("ERP login was not accepted.")
    return session


def load_erp_billing_documents(customer_code: Any, top: int = 80) -> list[dict[str, Any]]:
    canonical = normalize_customer_code(customer_code)
    if not canonical:
        return []
    session = login_erp_session()
    url = f"{ERP_BASE_URL}/sap/opu/odata/sap/SD_CUSTOMER_INVOICES_MANAGE/C_BillingDocument_F0797"
    params = {
        "$format": "json",
        "$top": str(max(10, min(int(top or 80), 200))),
        "$orderby": "BillingDocumentDate desc,BillingDocument desc",
        "$select": (
            "BillingDocument,BillingDocumentType,SoldToParty,BillingDocumentDate,"
            "TotalNetAmount,TransactionCurrency,BillingDocumentIsCancelled,"
            "CancelledBillingDocument,OverallBillingStatus"
        ),
        "$filter": (
            f"SoldToParty eq '{canonical}' "
            "and BillingDocument ge '9000000000' "
            "and BillingDocument lt '9100000000'"
        ),
    }
    response = session.get(url, params=params, timeout=ERP_TIMEOUT_SECONDS, headers={"Accept": "application/json"})
    response.raise_for_status()
    rows = response.json().get("d", {}).get("results", [])
    for row in rows:
        if isinstance(row, dict):
            row["source"] = "erp"
    return rows


def load_billing_documents() -> list[dict[str, Any]]:
    """Load test records from ``PNJ_BILLING_FIXTURE_PATH``.

    Expected JSON shape can be either a list of records or
    ``{"records": [...]}``. The file must stay outside Git if it contains real
    customer data.
    """
    fixture_path = os.environ.get("PNJ_BILLING_FIXTURE_PATH", "").strip()
    if not fixture_path:
        return []
    path = Path(fixture_path)
    if not path.is_file():
        return []
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    records = raw.get("records", raw) if isinstance(raw, dict) else raw
    if not isinstance(records, list):
        return []
    public_records = []
    for record in records:
        if not isinstance(record, dict):
            continue
        public_record = public_billing_record(record)
        if public_record:
            public_records.append(public_record)
    return public_records


def billing_suggestions(
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

    matches = []
    seen_documents = set()
    source_records = (
        load_erp_billing_documents(canonical_customer)
        if erp_credentials()
        else load_billing_documents()
    )
    for record in source_records:
        public_record = public_billing_record(record)
        if public_record is None:
            continue
        record = public_record
        if record["customer_code"] != canonical_customer:
            continue
        if has_cancelled_bill_doc(record.get("canceled_bill_doc")) or is_cancelled_value(record.get("cancelled")):
            continue
        billing_date = parse_date(record["billing_date"])
        if not billing_date or billing_date > anchor_date or billing_date < earliest:
            continue
        document = record["billing_document"]
        if document in seen_documents:
            continue
        seen_documents.add(document)
        item = dict(record)
        item["same_day"] = billing_date == anchor_date
        item["_sort_date"] = billing_date
        matches.append(item)

    matches.sort(key=lambda item: (not item["same_day"], -item["_sort_date"].toordinal(), item["billing_document"]))
    return [{k: v for k, v in item.items() if not k.startswith("_")} for item in matches[:capped_limit]]
