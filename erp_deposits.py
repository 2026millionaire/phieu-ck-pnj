# -*- coding: utf-8 -*-
"""Customer deposit receipt suggestions from ERP customer line items."""

from __future__ import annotations

import json
import os
import re
from html import unescape
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests

import erp_billing


DEFAULT_LOOKBACK_DAYS = 0
MAX_SUGGESTIONS = 10
ERP_BASE_URL = erp_billing.ERP_BASE_URL
ERP_TIMEOUT_SECONDS = erp_billing.ERP_TIMEOUT_SECONDS
DEFAULT_COMPANY_CODE = "1000"
DEFAULT_PROFIT_CENTER = "0010241305"
DEFAULT_DOCUMENT_TYPE = "DZ"
RESTGUI_TRANSACTION = "ZFIE0029"
RESTGUI_GRID_ID = "C102"
RESTGUI_DEPOSIT_SPECIAL_GL = "A"
RESTGUI_DEPOSIT_ACCOUNT_TYPE = "D"
DEFAULT_RESTGUI_BASE_URL = "https://dc-ep0app01.pnj.com.vn:44300"
RESTGUI_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/149.0.0.0 Safari/537.36"
)
RESTGUI_CLIENT_METRICS = {
    "ClientWidth": "1600",
    "ClientHeight": "950",
    "ScreenOrientation": "landscape",
    "ThemedTableRowHeightStandard": "33",
    "ThemedScrollbarDimension": "0",
    "ThemedDocumentBackgroundColor": "rgb(250, 250, 250)",
    "ThemedRasterHeight": "32",
    "ThemedRasterWidth": "10",
    "ThemedAbapListRasterHeight": "27",
    "ThemedAbapListRasterWidth": "9",
    "sapthemecat": "3",
    "ThemeID": "sap_belize",
    "SapThemeID": "sap_belize",
    "DeviceType": "DESKTOP",
    "Platform": "Windows",
    "ThemedTableRowHeight": "25",
    "sapurisfioritheme": "1",
    "theme": "sap_belize",
    "sapbasetheme": "sap_belize",
    "~webguiScreenWidth": "1600",
    "~webguiScreenHeight": "950",
    "~webguiUserAreaWidth": "1568",
    "~webguiUserAreaHeight": "789",
}


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


def format_restgui_date(day: date) -> str:
    return day.strftime("%d.%m.%Y")


def restgui_customer_code(value: Any) -> str:
    canonical = normalize_customer_code(value)
    return canonical.zfill(10) if canonical.isdigit() and len(canonical) <= 10 else canonical


def _decode_sap_js_text(value: Any) -> str:
    text = unescape(str(value or ""))

    def repl(match: re.Match[str]) -> str:
        return chr(int(match.group(1), 16))

    return re.sub(r"\\x([0-9A-Fa-f]{2})", repl, text)


def _parse_html_attrs(tag: str) -> dict[str, str]:
    attrs = {}
    for match in re.finditer(r'([\w:-]+)=("[^"]*"|\'[^\']*\'|[^\s>]+)', tag or ""):
        attrs[match.group(1)] = unescape(match.group(2).strip("\"'"))
    return attrs


def _extract_form(html_text: str, form_id: str) -> tuple[str, dict[str, str]]:
    form_match = re.search(rf'<form\b([^>]*)\bid="{re.escape(form_id)}"([^>]*)>(.*?)</form>', html_text or "", re.S)
    if not form_match:
        form_match = re.search(rf"<form\b([^>]*)>(?:(?!</form>).)*?</form>", html_text or "", re.S)
        while form_match and f'id="{form_id}"' not in form_match.group(0) and f"id='{form_id}'" not in form_match.group(0):
            next_start = form_match.end()
            form_match = re.search(rf"<form\b([^>]*)>(?:(?!</form>).)*?</form>", (html_text or "")[next_start:], re.S)
    if not form_match:
        raise RuntimeError("RESTGUI start form was not found.")
    form_html = form_match.group(0)
    start_tag = re.search(r"<form\b([^>]*)>", form_html, re.S)
    form_attrs = _parse_html_attrs(start_tag.group(1) if start_tag else "")
    fields = {}
    for input_match in re.finditer(r"<input\b([^>]*)>", form_html, re.S):
        attrs = _parse_html_attrs(input_match.group(1))
        if attrs.get("name"):
            fields[attrs["name"]] = attrs.get("value", "")
    return form_attrs.get("action", ""), fields


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
        "account_type": str(record.get("AccountTy") or record.get("AccountType") or record.get("account_type") or "").strip(),
        "special_gl": str(record.get("SpGL") or record.get("SpG/L") or record.get("SpecialGeneralLedgerCode") or "").strip(),
        "text": str(record.get("Text") or record.get("DocumentItemText") or record.get("text") or "").strip(),
        "source": str(record.get("source") or "fixture").strip() or "fixture",
    }


def build_restgui_deposit_payload(customer_code: Any, target_date: date) -> list[dict[str, str]]:
    """Return RESTGUI batch actions that fill ZFIE0029 and press Post/F8."""
    return [
        {"post": "value/wnd[0]/usr/ctxtGS_SCREEN_0100-BUKRS", "content": DEFAULT_COMPANY_CODE, "logic": "ignore"},
        {"post": "value/wnd[0]/usr/ctxtGS_SCREEN_0100-PRCTR", "content": DEFAULT_PROFIT_CENTER, "logic": "ignore"},
        {"post": "value/wnd[0]/usr/ctxtGS_SCREEN_0100-BUDAT", "content": format_restgui_date(target_date), "logic": "ignore"},
        {"post": "value/wnd[0]/usr/ctxtGS_SCREEN_0100-BLART", "content": DEFAULT_DOCUMENT_TYPE, "logic": "ignore"},
        {"post": "value/wnd[0]/usr/ctxtGS_SCREEN_0100-ACCNR", "content": restgui_customer_code(customer_code), "logic": "ignore"},
        {
            "post": "action/304/wnd[0]/usr/ctxtGS_SCREEN_0100-ACCNR",
            "content": f"position={len(restgui_customer_code(customer_code)) - 1}",
            "logic": "ignore",
        },
        {"post": "focus/wnd[0]/usr/ctxtGS_SCREEN_0100-ACCNR", "logic": "ignore"},
        {"post": "action/3/wnd[0]/tbar[1]/btn[8]"},
        {"get": "state/ur"},
    ]


def _restgui_grid_headers(response_text: str, grid_id: str = RESTGUI_GRID_ID) -> dict[int, str]:
    headers: dict[int, str] = {}
    pattern = re.compile(rf'id="grid#{re.escape(grid_id)}#0,(\d+)#cp"[^>]*>(.*?)</span>', re.S)
    for col, inner in pattern.findall(response_text or ""):
        label = re.sub(r"<[^>]+>", "", inner)
        headers[int(col)] = _decode_sap_js_text(label).strip()
    return headers


def _extract_restgui_value(lsdata: str, inner: str) -> str:
    raw = _decode_sap_js_text(lsdata)
    for pattern in (r"value:'((?:\\'|[^'])*)'", r"5:'((?:\\'|[^'])*)'"):
        match = re.search(pattern, raw)
        if match:
            return _decode_sap_js_text(match.group(1).replace("\\'", "'")).strip()
    text = re.sub(r"<[^>]+>", "", inner or "")
    return _decode_sap_js_text(text).strip()


def parse_restgui_deposit_response(response_text: str, customer_code: Any = "") -> list[dict[str, Any]]:
    """Parse ZFIE0029 RESTGUI multipart response and return candidate deposit rows."""
    canonical = normalize_customer_code(customer_code)
    headers = _restgui_grid_headers(response_text)
    if not headers:
        return []
    column_by_name = {name: col for col, name in headers.items() if name}
    cells: dict[int, dict[int, str]] = {}
    pattern = re.compile(
        rf'id="grid#{re.escape(RESTGUI_GRID_ID)}#(\d+),(\d+)#if"[^>]*lsdata="([^"]*)"[^>]*>(.*?)</span>',
        re.S,
    )
    for row, col, lsdata, inner in pattern.findall(response_text or ""):
        row_idx = int(row)
        col_idx = int(col)
        if row_idx <= 0:
            continue
        cells.setdefault(row_idx, {})[col_idx] = _extract_restgui_value(lsdata, inner)

    records: list[dict[str, Any]] = []
    for row in sorted(cells):
        values = cells[row]
        by_name = {name: values.get(col, "") for name, col in column_by_name.items()}
        document = normalize_deposit_document(by_name.get("DocumentNo"))
        amount_text = by_name.get("Amount") or by_name.get("Amt.in loc.cur.")
        amount = parse_signed_amount(amount_text)
        account_type = str(by_name.get("AccountTy") or "").strip()
        document_type = str(by_name.get("Type") or "").strip().upper()
        special_gl = str(by_name.get("SpG/L") or "").strip()
        if not document or amount >= 0:
            continue
        if account_type and account_type != RESTGUI_DEPOSIT_ACCOUNT_TYPE:
            continue
        if document_type and document_type != DEFAULT_DOCUMENT_TYPE:
            continue
        if special_gl and special_gl != RESTGUI_DEPOSIT_SPECIAL_GL:
            continue
        records.append(
            {
                "Customer": canonical,
                "CompanyCode": DEFAULT_COMPANY_CODE,
                "ProfitCenter": DEFAULT_PROFIT_CENTER,
                "PostingDate": by_name.get("Posting Date") or "",
                "DocumentNo": document,
                "DocumentType": document_type or DEFAULT_DOCUMENT_TYPE,
                "Amount": amount_text,
                "AccountTy": account_type,
                "SpGL": special_gl,
                "Text": by_name.get("Text") or "",
                "source": "restgui",
            }
        )
    return records


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


def _login_direct_restgui_session(base_url: str) -> requests.Session:
    session = requests.Session()
    response = session.get(
        f"{base_url}/sap/bc/gui/sap/its/webgui",
        params={"~transaction": RESTGUI_TRANSACTION, "sap-client": "300", "sap-language": "EN"},
        timeout=ERP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    if "sap-user" not in response.text:
        return session

    credentials = erp_credentials()
    if not credentials:
        raise RuntimeError("ERP credentials are not configured.")
    user, password = credentials
    fields = {}
    for match in re.finditer(r"<input([^>]+)>", response.text):
        attrs = dict(re.findall(r'([\w:-]+)="([^"]*)"', match.group(1)))
        if attrs.get("name"):
            fields[unescape(attrs["name"])] = unescape(attrs.get("value", ""))
    fields.update({"sap-user": user, "sap-password": password, "sap-language": "EN"})
    login_response = session.post(response.url, data=fields, timeout=ERP_TIMEOUT_SECONDS, allow_redirects=True)
    login_response.raise_for_status()
    if "sap-password" in login_response.text and "Log On" in login_response.text:
        raise RuntimeError("ERP RESTGUI login was not accepted.")
    return session


def _start_restgui_transaction(session: requests.Session, base_url: str) -> tuple[str, str]:
    browser_headers = {
        "User-Agent": RESTGUI_USER_AGENT,
        "sec-ch-ua": '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }
    session.headers.update({"User-Agent": RESTGUI_USER_AGENT})
    response = session.get(
        f"{base_url}/sap/bc/gui/sap/its/webgui",
        params={"~transaction": RESTGUI_TRANSACTION, "sap-client": "300", "sap-language": "EN"},
        headers=browser_headers,
        timeout=ERP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    action, fields = _extract_form(response.text, "webguiStartForm")
    action_url = urljoin(response.url, action)
    fields.update(
        {
            "~webguiUserAreaHeight": "900",
            "~webguiUserAreaWidth": "1600",
            "~webguiScreenHeight": "1000",
            "~webguiScreenWidth": "1600",
            "~webguiDynproMetric": "1",
            "SAPURIsFioriTheme": "1",
            "SAPURIsSAPBaseTheme": "sap_belize",
            "~tx": RESTGUI_TRANSACTION,
            "~transaction": RESTGUI_TRANSACTION,
        }
    )
    start_response = session.post(
        action_url,
        data=fields,
        timeout=ERP_TIMEOUT_SECONDS,
        headers={
            **browser_headers,
            "Referer": response.url,
            "Origin": base_url,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Content-Type": "application/x-www-form-urlencoded",
            "Cache-Control": "max-age=0",
            "Upgrade-Insecure-Requests": "1",
        },
    )
    start_response.raise_for_status()
    script_match = re.search(r'src="([^"]*webgui_min\.js[^"]*)"', start_response.text)
    if script_match:
        script_url = urljoin(start_response.url, script_match.group(1))
        session.get(script_url, headers={**browser_headers, "Referer": action_url}, timeout=ERP_TIMEOUT_SECONDS)
    path = fields.get("~path") or re.sub(r"^https?://[^/]+", "", action_url)
    metrics = dict(RESTGUI_CLIENT_METRICS)
    metrics["ThemedSvgLibs"] = (
        f"{base_url}/sap/public/icmandir/its/ls/theming/UR/ls/sap_belize/svg/libs/SAPGUI-icons.svg"
    )
    state_payload = {
        "moin": str(fields.get("moin") or ""),
        "~RG_WEBGUI": "X",
        "sap-statistics": "true",
        "~path": path,
        **metrics,
        "~ci_result": ";".join(f"{key}={value}" for key, value in metrics.items()),
    }
    state_response = session.post(
        action_url.rstrip("/") + "/state/ur",
        data=state_payload,
        timeout=max(ERP_TIMEOUT_SECONDS, 60),
        headers={
            **browser_headers,
            "Referer": action_url,
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    state_response.raise_for_status()
    return action_url, str(fields.get("moin") or "")


def load_erp_deposit_restgui(customer_code: Any, target_date: date) -> list[dict[str, Any]]:
    """Load deposit rows by posting ZFIE0029 through RESTGUI batch/json."""
    base_url = os.environ.get("PNJ_DEPOSIT_RESTGUI_BASE_URL", DEFAULT_RESTGUI_BASE_URL).rstrip("/")
    session = login_erp_session() if base_url == ERP_BASE_URL else _login_direct_restgui_session(base_url)
    action_url, moin = _start_restgui_transaction(session, base_url)
    batch_url = action_url.rstrip("/") + "/batch/json"
    response = session.post(
        batch_url,
        params={"~RG_WEBGUI": "X", "sap-statistics": "true"},
        json=build_restgui_deposit_payload(customer_code, target_date),
        headers={
            "Accept": "multipart/mixed",
            "Content-Type": "application/json;charset=UTF-8",
            "moin": moin,
            "sap-cancel-on-close": "true",
        },
        timeout=max(ERP_TIMEOUT_SECONDS, 120),
    )
    response.raise_for_status()
    return parse_restgui_deposit_response(response.text, customer_code=customer_code)


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
        load_erp_deposit_restgui(canonical_customer, anchor_date)
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
        if public_record["source"] != "restgui":
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
