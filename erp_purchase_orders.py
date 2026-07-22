# -*- coding: utf-8 -*-
"""Purchase-order suggestion provider for buyback statement numbers."""

from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests

import erp_billing


DEFAULT_LOOKBACK_DAYS = 1
MAX_SUGGESTIONS = 10
ERP_BASE_URL = erp_billing.ERP_BASE_URL
ERP_TIMEOUT_SECONDS = erp_billing.ERP_TIMEOUT_SECONDS
PURCHASE_ORDER_TYPE_BUYBACK = "Z04"
ACTIVE_DRAFT_UUID = "00000000-0000-0000-0000-000000000000"


def normalize_customer_code(value: Any) -> str:
    return erp_billing.normalize_customer_code(value)


def parse_date(value: Any) -> date | None:
    return erp_billing.parse_date(value)


def parse_amount(value: Any) -> int:
    return erp_billing.parse_amount(value)


def parse_sap_odata_date(value: Any) -> date | None:
    return erp_billing.parse_sap_odata_date(value)


def erp_credentials() -> tuple[str, str] | None:
    return erp_billing.erp_credentials()


def login_erp_session() -> requests.Session:
    return erp_billing.login_erp_session()


def public_purchase_order_record(record: dict[str, Any]) -> dict[str, Any] | None:
    document = re.sub(r"\D+", "", str(record.get("PurchaseOrder") or record.get("purchase_order") or ""))
    if not re.fullmatch(r"4403\d{6}", document):
        return None
    purchase_order_type = str(record.get("PurchaseOrderType") or record.get("purchase_order_type") or "").strip().upper()
    if purchase_order_type and purchase_order_type != PURCHASE_ORDER_TYPE_BUYBACK:
        return None
    if record.get("IsActiveEntity") is False or str(record.get("IsActiveEntity", "")).strip().lower() == "false":
        return None

    creation_date = parse_sap_odata_date(record.get("CreationDate") or record.get("creation_date") or record.get("date"))
    customer_code = normalize_customer_code(record.get("Supplier") or record.get("supplier") or record.get("customer_code"))
    if not creation_date or not customer_code:
        return None

    return {
        "purchase_order": document,
        "amount": parse_amount(record.get("PurchaseOrderNetAmount") or record.get("amount")),
        "currency": str(record.get("DocumentCurrency") or record.get("currency") or "VND").strip() or "VND",
        "creation_date": creation_date.isoformat(),
        "customer_code": customer_code,
        "purchase_order_type": purchase_order_type or PURCHASE_ORDER_TYPE_BUYBACK,
        "purchase_order_type_name": str(record.get("PurchaseOrderTypeName") or "").strip(),
        "purchasing_group": str(record.get("PurchasingGroup") or "").strip(),
        "purchasing_organization": str(record.get("PurchasingOrganization") or "").strip(),
        "approval_status": str(record.get("ApprovalStatusName") or "").strip(),
        "approver": str(record.get("ApproverName") or "").strip(),
        "source": str(record.get("source") or "fixture").strip() or "fixture",
    }


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def format_supplier_address(record: dict[str, Any]) -> str:
    """Return a compact customer address from the PO supplier-address payload."""
    parts = [
        _clean_text(record.get("StreetName") or record.get("street_name")),
        _clean_text(record.get("HouseNumber") or record.get("house_number")),
        _clean_text(record.get("CityName") or record.get("city_name")),
    ]
    return ", ".join(part for part in parts if part)


def public_supplier_address_record(record: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(record, dict):
        return {}
    return {
        "name": _clean_text(record.get("FullName") or record.get("name")),
        "address": format_supplier_address(record),
        "street": _clean_text(record.get("StreetName") or record.get("street_name")),
        "house_number": _clean_text(record.get("HouseNumber") or record.get("house_number")),
        "city": _clean_text(record.get("CityName") or record.get("city_name")),
        "country": _clean_text(record.get("Country") or record.get("country")),
        "region": _clean_text(record.get("Region") or record.get("region")),
        "phone": re.sub(r"\D+", "", str(record.get("PhoneNumber") or record.get("phone") or "")),
    }


def _sap_datetime_literal(day: date) -> str:
    return f"datetime'{day.isoformat()}T00:00:00'"


def load_erp_purchase_orders(customer_code: Any, target_date: date, lookback_days: int, top: int = 20) -> list[dict[str, Any]]:
    canonical = normalize_customer_code(customer_code)
    if not canonical:
        return []
    session = login_erp_session()
    url = f"{ERP_BASE_URL}/sap/opu/odata/sap/MM_PUR_PO_MAINT_V2_SRV/C_PurchaseOrderTP"
    earliest = target_date - timedelta(days=max(0, lookback_days))
    date_filter = (
        f"CreationDate ge {_sap_datetime_literal(earliest)} "
        f"and CreationDate le {_sap_datetime_literal(target_date)}"
    )
    params = {
        "$format": "json",
        "$top": str(max(10, min(int(top or 20), 100))),
        "$orderby": "PurchaseOrder desc",
        "$select": (
            "PurchaseOrder,Supplier,CompanyCode,CreationDate,PurchaseOrderNetAmount,"
            "DocumentCurrency,PurchaseOrderType,PurchaseOrderTypeName,PurchasingGroup,"
            "PurchasingOrganization,ApprovalStatusName,ApproverName,IsActiveEntity,DraftUUID"
        ),
        "$filter": (
            f"Supplier eq '{canonical}' "
            "and CompanyCode eq '1000' "
            f"and {date_filter} "
            "and IsActiveEntity eq true "
            f"and PurchaseOrderType eq '{PURCHASE_ORDER_TYPE_BUYBACK}'"
        ),
    }
    response = session.get(url, params=params, timeout=ERP_TIMEOUT_SECONDS, headers={"Accept": "application/json"})
    response.raise_for_status()
    rows = response.json().get("d", {}).get("results", [])
    for row in rows:
        if isinstance(row, dict):
            row["source"] = "erp"
    return rows


def load_erp_purchase_order_address(purchase_order: Any) -> dict[str, Any]:
    document = re.sub(r"\D+", "", str(purchase_order or ""))
    if not re.fullmatch(r"4403\d{6}", document):
        return {}
    session = login_erp_session()
    key = (
        f"C_PurchaseOrderTP(PurchaseOrder='{document}',"
        f"DraftUUID=guid'{ACTIVE_DRAFT_UUID}',IsActiveEntity=true)"
    )
    url = f"{ERP_BASE_URL}/sap/opu/odata/sap/MM_PUR_PO_MAINT_V2_SRV/{key}/to_PurOrdSupplierAddressTP"
    params = {
        "$format": "json",
        "$select": "FullName,StreetName,HouseNumber,PostalCode,CityName,Country,Region,PhoneNumber",
    }
    response = session.get(url, params=params, timeout=ERP_TIMEOUT_SECONDS, headers={"Accept": "application/json"})
    response.raise_for_status()
    return response.json().get("d", {})


def load_purchase_orders() -> list[dict[str, Any]]:
    fixture_path = os.environ.get("PNJ_PURCHASE_ORDER_FIXTURE_PATH", "").strip()
    if not fixture_path:
        return []
    path = Path(fixture_path)
    if not path.is_file():
        return []
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    records = raw.get("records", raw) if isinstance(raw, dict) else raw
    return records if isinstance(records, list) else []


def purchase_order_suggestions(
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
        load_erp_purchase_orders(canonical_customer, anchor_date, lookback, top=capped_limit * 2)
        if erp_credentials()
        else load_purchase_orders()
    )
    matches = []
    seen_documents = set()
    for record in source_records:
        if not isinstance(record, dict):
            continue
        public_record = public_purchase_order_record(record)
        if public_record is None:
            continue
        if public_record["customer_code"] != canonical_customer:
            continue
        creation_date = parse_date(public_record["creation_date"])
        if not creation_date or creation_date > anchor_date or creation_date < earliest:
            continue
        document = public_record["purchase_order"]
        if document in seen_documents:
            continue
        seen_documents.add(document)
        item = dict(public_record)
        item["same_day"] = creation_date == anchor_date
        item["_sort_date"] = creation_date
        matches.append(item)

    matches.sort(key=lambda item: (not item["same_day"], -item["_sort_date"].toordinal(), item["purchase_order"]))
    return [{k: v for k, v in item.items() if not k.startswith("_")} for item in matches[:capped_limit]]


def purchase_order_customer_profile(
    customer_code: Any,
    target_date: Any = None,
    lookback_days: Any = DEFAULT_LOOKBACK_DAYS,
) -> dict[str, Any]:
    """Return customer name/phone/address from the nearest recent buyback PO."""
    suggestions = purchase_order_suggestions(
        customer_code=customer_code,
        target_date=target_date,
        lookback_days=lookback_days,
        limit=1,
    )
    if not suggestions:
        return {}
    purchase_order = suggestions[0]["purchase_order"]
    address_record: dict[str, Any] = {}
    if erp_credentials():
        address_record = load_erp_purchase_order_address(purchase_order)
    else:
        for record in load_purchase_orders():
            public_record = public_purchase_order_record(record) if isinstance(record, dict) else None
            if public_record and public_record["purchase_order"] == purchase_order:
                address_record = (
                    record.get("to_PurOrdSupplierAddressTP")
                    or record.get("supplier_address")
                    or record.get("address")
                    or {}
                )
                break
    profile = public_supplier_address_record(address_record)
    profile.update({
        "customer_code": normalize_customer_code(customer_code),
        "purchase_order": purchase_order,
        "creation_date": suggestions[0].get("creation_date", ""),
        "amount": suggestions[0].get("amount", 0),
        "currency": suggestions[0].get("currency", "VND"),
        "source": "erp" if erp_credentials() else "fixture",
    })
    return {key: value for key, value in profile.items() if value not in ("", None)}
