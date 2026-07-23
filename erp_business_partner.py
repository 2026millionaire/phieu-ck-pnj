# -*- coding: utf-8 -*-
"""Business Partner profile provider for PNJ ERP/Fiori.

The provider mirrors the existing ERP billing / purchase-order modules: use a
local fixture for tests, and use the authenticated ERP session in production
when ``PNJ_ERP_USER`` / ``PNJ_ERP_PASSWORD`` are configured.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import requests

import erp_billing


ERP_BASE_URL = erp_billing.ERP_BASE_URL
ERP_TIMEOUT_SECONDS = erp_billing.ERP_TIMEOUT_SECONDS


def normalize_customer_code(value: Any) -> str:
    return erp_billing.normalize_customer_code(value)


def erp_credentials() -> tuple[str, str] | None:
    return erp_billing.erp_credentials()


def login_erp_session() -> requests.Session:
    return erp_billing.login_erp_session()


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _digits(value: Any) -> str:
    return re.sub(r"\D+", "", str(value or ""))


def _first(record: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = record.get(key)
        if value not in ("", None):
            return _clean_text(value)
    return ""


def _district(record: dict[str, Any]) -> str:
    district = _first(record, "District")
    district_name = _first(record, "DistrictName")
    if district and not district.isdigit():
        return district
    return district_name or district


def _walk_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_dicts(child)


def _flatten_profile_record(payload: dict[str, Any]) -> dict[str, Any]:
    """Merge useful keys from SAP's nested OData payload into one dict."""
    merged: dict[str, Any] = {}
    for record in _walk_dicts(payload):
        if "__metadata" in record and len(record) <= 2:
            continue
        for key, value in record.items():
            if key == "__metadata" or isinstance(value, (dict, list)):
                continue
            if key not in merged or merged.get(key) in ("", None):
                merged[key] = value
    return merged


def format_address(record: dict[str, Any]) -> str:
    parts = [
        _first(record, "StreetName", "Street", "StreetAddressName", "HouseNumberAndStreet"),
        _first(record, "Ward", "DistrictName3"),
        _district(record),
        _first(record, "CityName", "City"),
    ]
    seen = set()
    output = []
    for part in parts:
        key = part.upper()
        if part and key not in seen:
            output.append(part)
            seen.add(key)
    return ", ".join(output)


def public_business_partner_profile(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    record = _flatten_profile_record(payload)
    first_name = _first(record, "FirstName", "NameFirst", "FirstName_BP")
    last_name = _first(record, "LastName", "NameLast", "LastName_BP")
    name = _first(record, "BusinessPartnerFullName", "BusinessPartnerName", "FullName", "Name")
    if not name:
        name = _clean_text(f"{first_name} {last_name}")
    cccd = _digits(_first(record, "CCCD", "cccd", "ZCCCD", "IdentityNumber", "IdentificationNumber", "SearchTerm2"))
    profile = {
        "customer_code": normalize_customer_code(_first(record, "BusinessPartner", "Customer", "Supplier")),
        "name": name,
        "first_name": first_name,
        "last_name": last_name,
        "phone": _digits(_first(record, "PhoneNumber", "MobilePhoneNumber", "MobileNumber", "Telephone")),
        "cccd": cccd,
        "birth_date": _first(record, "BirthDate", "Birthday"),
        "gender": _first(record, "GenderName", "GenderCodeName", "GenderCode"),
        "street": _first(record, "StreetName", "Street", "StreetAddressName", "HouseNumberAndStreet"),
        "ward": _first(record, "Ward", "DistrictName3"),
        "district": _district(record),
        "city": _first(record, "CityName", "City"),
        "region": _first(record, "RegionName", "Region"),
        "address": format_address(record),
    }
    return {key: value for key, value in profile.items() if value not in ("", None)}


def _odata_json(session: requests.Session, path: str) -> dict[str, Any]:
    url = f"{ERP_BASE_URL}{path}"
    response = session.get(url, timeout=ERP_TIMEOUT_SECONDS, headers={"Accept": "application/json"})
    response.raise_for_status()
    data = response.json()
    return data.get("d", data) if isinstance(data, dict) else {}


def load_erp_business_partner(customer_code: Any) -> dict[str, Any]:
    canonical = normalize_customer_code(customer_code)
    if not canonical:
        return {}
    session = login_erp_session()
    candidates = [
        (
            "/sap/opu/odata/sap/ZGW_RE_BPCREATE_SRV/"
            f"ZDDL_RE_BP('{canonical}')?$format=json&sap-client=300&"
            "$expand=to_ProvinceText,to_RegionText"
        ),
        f"/sap/opu/odata/sap/ZGW_RE_BPCREATE_SRV/ZDDL_RE_BP('{canonical}')?$format=json&sap-client=300",
    ]
    last_error: Exception | None = None
    for path in candidates:
        try:
            payload = _odata_json(session, path)
        except Exception as exc:
            last_error = exc
            continue
        profile = public_business_partner_profile(payload)
        if profile:
            profile["customer_code"] = canonical
            profile["source"] = "erp"
            profile["source_path"] = path.split("?", 1)[0]
            return profile
    if last_error:
        raise last_error
    return {}


def load_business_partner_fixture(customer_code: Any) -> dict[str, Any]:
    canonical = normalize_customer_code(customer_code)
    fixture_path = os.environ.get("PNJ_ERP_BP_FIXTURE_PATH", "").strip()
    if not canonical or not fixture_path:
        return {}
    path = Path(fixture_path)
    if not path.is_file():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    records = raw.get("records", raw) if isinstance(raw, dict) else raw
    if isinstance(records, dict):
        records = [records]
    if not isinstance(records, list):
        return {}
    for record in records:
        if not isinstance(record, dict):
            continue
        record_code = normalize_customer_code(
            record.get("BusinessPartner") or record.get("Customer") or record.get("customer_code")
        )
        if record_code == canonical:
            profile = public_business_partner_profile(record)
            profile["customer_code"] = canonical
            profile["source"] = "fixture"
            return profile
    return {}


def business_partner_profile(customer_code: Any) -> dict[str, Any]:
    canonical = normalize_customer_code(customer_code)
    if not canonical:
        return {}
    if erp_credentials():
        return load_erp_business_partner(canonical)
    return load_business_partner_fixture(canonical)
