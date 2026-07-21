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
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any


DEFAULT_LOOKBACK_DAYS = 1
MAX_SUGGESTIONS = 10


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


def is_cancelled_value(value: Any) -> bool:
    return str(value or "").strip().lower() in {"yes", "y", "true", "1", "x"}


def has_cancelled_bill_doc(value: Any) -> bool:
    return re.fullmatch(r"90\d{8}", re.sub(r"\D+", "", str(value or ""))) is not None


def public_billing_record(record: dict[str, Any]) -> dict[str, Any] | None:
    document = re.sub(r"\D+", "", str(record.get("billing_document") or record.get("document") or ""))
    if not re.fullmatch(r"90\d{8}", document):
        return None
    amount = parse_amount(record.get("net_value", record.get("amount")))
    billing_date = parse_date(record.get("billing_date") or record.get("date"))
    customer_code = normalize_customer_code(
        record.get("customer_code") or record.get("sold_to_party_code") or record.get("sold_to")
    )
    if not billing_date or not customer_code:
        return None
    canceled_bill_doc = re.sub(
        r"\D+", "",
        str(record.get("canceled_bill_doc") or record.get("canceled_billing_document") or ""),
    )
    cancelled = str(record.get("cancelled") or record.get("canceled") or "").strip()
    return {
        "billing_document": document,
        "amount": amount,
        "billing_date": billing_date.isoformat(),
        "customer_code": customer_code,
        "customer_name": str(record.get("customer_name") or record.get("sold_to_party_name") or "").strip(),
        "canceled_bill_doc": canceled_bill_doc,
        "cancelled": cancelled,
        "source": str(record.get("source") or "fixture").strip() or "fixture",
    }


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
    for record in load_billing_documents():
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
