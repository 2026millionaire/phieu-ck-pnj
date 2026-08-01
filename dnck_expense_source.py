# -*- coding: utf-8 -*-
"""Source adapters for ĐNCK thanh toán chi phí khác."""

import os
import re
from datetime import datetime

from dnck_expense_models import (
    CHI_PHI_HD_HEADER_ROW,
    CHI_PHI_HD_SHEET,
    CHI_PHI_ND_HEADER_ROW,
    CHI_PHI_ND_SHEET,
    CHI_PHI_SPREADSHEET_ID,
    VIP_T07_FIXTURE,
    normalize_amount,
    normalize_source_payload,
)


class SourceUnavailable(RuntimeError):
    pass


def _parse_date(value):
    text = str(value or "").strip()
    for fmt in ("%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            pass
    return text


def _period_from_date(value):
    parsed = _parse_date(value)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", parsed):
        return parsed[:7]
    return ""


def _cell(row, index):
    return row[index] if len(row) > index else ""


class DnckExpenseSourceAdapter:
    """Adapter interface for Google Sheet CHI PHI.

    Live reads require either GOOGLE_APPLICATION_CREDENTIALS service account JSON
    or GOOGLE_API_KEY/DNCK_EXPENSE_GOOGLE_API_KEY for a readable sheet.
    """

    spreadsheet_id = CHI_PHI_SPREADSHEET_ID

    def read(self, plant="", period="", nd_stt="", fixture=""):
        if fixture in ("vip_t07", "schema_173"):
            payload = dict(VIP_T07_FIXTURE)
            payload["live"] = False
            return normalize_source_payload(payload)
        return self.read_google_sheet(plant=plant, period=period, nd_stt=nd_stt)

    def _service(self):
        try:
            from googleapiclient.discovery import build
        except ImportError as exc:
            raise SourceUnavailable("Runtime app chưa có google-api-python-client để đọc Google Sheet live.") from exc

        api_key = os.environ.get("DNCK_EXPENSE_GOOGLE_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
        if credentials_path:
            try:
                from google.oauth2 import service_account
            except ImportError as exc:
                raise SourceUnavailable("Runtime app chưa có google-auth để dùng service account.") from exc
            scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
            credentials = service_account.Credentials.from_service_account_file(credentials_path, scopes=scopes)
            return build("sheets", "v4", credentials=credentials, cache_discovery=False)
        if api_key:
            return build("sheets", "v4", developerKey=api_key, cache_discovery=False)
        raise SourceUnavailable("Chưa cấu hình credential/API key để đọc Google Sheet CHI PHI live.")

    def _values(self, range_a1):
        service = self._service()
        response = service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=range_a1,
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="FORMATTED_STRING",
        ).execute()
        return response.get("values", [])

    def read_google_sheet(self, plant="", period="", nd_stt=""):
        if not nd_stt:
            raise SourceUnavailable("Thiếu STT ND để đọc Google Sheet live.")

        nd_values = self._values(f"{CHI_PHI_ND_SHEET}!A{CHI_PHI_ND_HEADER_ROW + 1}:H1000")
        nd_row_number = None
        nd_row = None
        for offset, row in enumerate(nd_values, start=CHI_PHI_ND_HEADER_ROW + 1):
            if str(_cell(row, 1)).strip() == str(nd_stt).strip():
                nd_row_number = offset
                nd_row = row
                break
        if nd_row is None:
            raise SourceUnavailable(f"Không tìm thấy STT ND {nd_stt} trên tab ND.")

        hd_values = self._values(f"{CHI_PHI_HD_SHEET}!A{CHI_PHI_HD_HEADER_ROW + 1}:N2000")
        hd_rows = []
        for offset, row in enumerate(hd_values, start=CHI_PHI_HD_HEADER_ROW + 1):
            if str(_cell(row, 0)).strip() != str(nd_stt).strip():
                continue
            invoice_date = _parse_date(_cell(row, 1))
            hd_rows.append({
                "row_number": offset,
                "invoice_date": invoice_date,
                "description": _cell(row, 2),
                "amount": normalize_amount(_cell(row, 3)),
                "invoice_no_full": _cell(row, 4),
                "supplier_tax_id": _cell(row, 5),
                "lookup_code": _cell(row, 8),
                "lookup_url": _cell(row, 9),
                "document_type": "Hóa đơn",
                "expense_account": _cell(row, 7),
                "cost_center": str(plant or "1305"),
                "sap_document": _cell(row, 13),
            })
        if not hd_rows:
            raise SourceUnavailable(f"Không tìm thấy dòng HD cho mã ND {nd_stt}.")

        nd_date = _parse_date(_cell(nd_row, 2))
        nd = {
            "stt": str(_cell(nd_row, 1)).strip(),
            "plant": str(plant or "1305"),
            "cost_center": str(plant or "1305"),
            "expense_account": hd_rows[0].get("expense_account", ""),
            "period": period or _period_from_date(nd_date),
            "object_code": str(_cell(nd_row, 5)).strip(),
            "object_name": "",
            "supplier_tax_id": hd_rows[0].get("supplier_tax_id", ""),
            "description": str(_cell(nd_row, 3)).strip(),
            "amount": normalize_amount(_cell(nd_row, 4)),
            "sap_document": str(_cell(nd_row, 6)).strip(),
            "frequency": str(_cell(nd_row, 7)).strip(),
        }
        return normalize_source_payload({
            "source_type": "google_sheet_live",
            "live": True,
            "spreadsheet_id": self.spreadsheet_id,
            "nd_range": f"{CHI_PHI_ND_SHEET}!A{nd_row_number}:H{nd_row_number}",
            "hd_range": f"{CHI_PHI_HD_SHEET}!A{hd_rows[0]['row_number']}:N{hd_rows[-1]['row_number']}",
            "nd": nd,
            "hd": hd_rows,
        })


def read_source(plant="", period="", nd_stt="", fixture=""):
    return DnckExpenseSourceAdapter().read(plant=plant, period=period, nd_stt=nd_stt, fixture=fixture)
