# -*- coding: utf-8 -*-
"""Validation rules for ĐNCK thanh toán chi phí khác."""

import datetime as _dt
import re

from dnck_expense_models import (
    ADJUSTMENT_DESCRIPTION,
    FIXED_PAYMENT_METHOD,
    normalize_amount,
    normalize_hd_row,
    normalize_nd_row,
)


ERROR = "error"
WARNING = "warning"
INFO = "info"


def result(rule_key, severity, status, message, evidence=None):
    return {
        "rule_key": rule_key,
        "severity": severity,
        "status": status,
        "message": message,
        "evidence": evidence or {},
    }


def has_blockers(results):
    return any(item["status"] != "pass" and item["severity"] == ERROR for item in results)


def validation_status(results):
    return "needs_review" if has_blockers(results) else "validated"


def total_hd_amount(hd_rows):
    return sum(normalize_amount(row.get("amount")) for row in hd_rows)


def duplicate_results(hd_rows):
    results = []
    seen_invoice = {}
    seen_lookup = {}
    for row in hd_rows:
        amount = normalize_amount(row.get("amount"))
        invoice_key = (
            str(row.get("invoice_no_full") or "").upper().strip(),
            re.sub(r"\D", "", str(row.get("supplier_tax_id") or "")),
            amount,
        )
        lookup_key = (
            str(row.get("lookup_code") or "").strip().upper(),
            re.sub(r"\D", "", str(row.get("supplier_tax_id") or "")),
        )
        if all(invoice_key):
            seen_invoice.setdefault(invoice_key, []).append(row.get("row_number"))
        if lookup_key[0] and lookup_key[1]:
            seen_lookup.setdefault(lookup_key, []).append(row.get("row_number"))
    for key, rows in seen_invoice.items():
        if len(rows) > 1:
            results.append(result(
                "duplicate_invoice_tax_amount",
                ERROR,
                "fail",
                "Trùng số hóa đơn + MST + số tiền.",
                {"key": key, "rows": rows},
            ))
    for key, rows in seen_lookup.items():
        if len(rows) > 1:
            results.append(result(
                "duplicate_lookup_tax",
                ERROR,
                "fail",
                "Trùng mã tra cứu + MST.",
                {"key": key, "rows": rows},
            ))
    if not results:
        results.append(result("duplicate", INFO, "pass", "Không phát hiện trùng trong nguồn hiện tại."))
    return results


def validate_negative_rows(hd_rows, negative_supported=True):
    results = []
    for row in hd_rows:
        amount = normalize_amount(row.get("amount"))
        if amount >= 0:
            continue
        description = str(row.get("description") or "").strip()
        if not description.startswith(ADJUSTMENT_DESCRIPTION):
            results.append(result(
                "negative_description",
                ERROR,
                "fail",
                "Dòng âm phải có diễn giải đúng phần chênh lệch hóa đơn không thanh toán.",
                {"row_number": row.get("row_number"), "description": description},
            ))
        if not row.get("adjustment_ref"):
            results.append(result(
                "negative_adjustment_ref",
                ERROR,
                "fail",
                "Dòng âm phải có tham chiếu hóa đơn bị điều chỉnh.",
                {"row_number": row.get("row_number")},
            ))
        if not negative_supported:
            results.append(result(
                "negative_template_support",
                ERROR,
                "fail",
                "Chưa xác minh Template/eOffice nhận số âm; giữ needs_review, không tự net.",
                {"row_number": row.get("row_number"), "amount": amount},
            ))
    if not results:
        results.append(result("negative_rows", INFO, "pass", "Không có dòng âm cần rà hoặc dòng âm đã qua rule hiện có."))
    return results


def build_limit_adjustment_rows(hd_rows, allowed_total):
    """Return rows with separate negative adjustments without changing source invoice rows."""
    rows = [normalize_hd_row(row) for row in hd_rows]
    total = total_hd_amount(rows)
    excess = total - normalize_amount(allowed_total)
    if excess <= 0:
        return rows

    sorted_rows = sorted(
        [row for row in rows if normalize_amount(row.get("amount")) > 0],
        key=lambda row: (row.get("invoice_date") or "", row.get("row_number") or 0),
        reverse=True,
    )
    for row in sorted_rows:
        if excess <= 0:
            break
        invoice_amount = normalize_amount(row.get("amount"))
        reduction = min(excess, invoice_amount)
        excess -= reduction
        adjustment = dict(row)
        adjustment["row_number"] = len(rows) + 1
        adjustment["description"] = f"{ADJUSTMENT_DESCRIPTION} - tham chiếu hóa đơn {row.get('invoice_no_full')}"
        adjustment["amount"] = -reduction
        adjustment["adjustment_ref"] = row.get("invoice_no_full")
        rows.append(adjustment)
    return rows


def source_sap_document(source):
    nd = (source or {}).get("nd") or {}
    if nd.get("sap_document"):
        return str(nd.get("sap_document") or "").strip()
    for row in reversed((source or {}).get("hd") or []):
        if row.get("sap_document"):
            return str(row.get("sap_document") or "").strip()
    return ""


def validate_source_and_supplement(source, supplement=None, negative_supported=True, require_live=True):
    supplement = supplement or {}
    nd = normalize_nd_row((source or {}).get("nd") or {})
    hd_rows = [normalize_hd_row(row) for row in (source or {}).get("hd") or []]
    results = []

    if require_live and not (source or {}).get("live"):
        results.append(result(
            "live_source",
            ERROR,
            "fail",
            "Chưa đọc được nguồn live; chỉ được tạo local draft needs_review.",
            {"source_type": (source or {}).get("source_type")},
        ))
    else:
        results.append(result("live_source", INFO, "pass", "Nguồn vừa đọc live."))

    for key, label in (
        ("stt", "STT ND"),
        ("plant", "Plant"),
        ("cost_center", "Cost center"),
        ("expense_account", "Tài khoản chi phí"),
        ("period", "Kỳ thanh toán"),
        ("description", "Diễn giải ND"),
    ):
        if not nd.get(key):
            results.append(result(f"missing_nd_{key}", ERROR, "fail", f"Thiếu {label} từ ND."))

    if not hd_rows:
        results.append(result("missing_hd", ERROR, "fail", "Không có dòng HD."))
    for row in hd_rows:
        for key, label in (
            ("invoice_date", "ngày hóa đơn"),
            ("description", "diễn giải HD"),
            ("invoice_no_full", "số hóa đơn đầy đủ"),
            ("supplier_tax_id", "MST NCC"),
            ("lookup_code", "mã tra cứu"),
            ("lookup_url", "link tra cứu"),
        ):
            if not row.get(key):
                results.append(result(
                    f"missing_hd_{key}",
                    ERROR,
                    "fail",
                    f"Thiếu {label} ở dòng HD.",
                    {"row_number": row.get("row_number")},
                ))

    total_nd = normalize_amount(nd.get("amount"))
    total_hd = total_hd_amount(hd_rows)
    if total_nd != total_hd:
        results.append(result(
            "total_nd_hd",
            ERROR,
            "fail",
            "Tổng ND không khớp SUM HD.",
            {"total_nd": total_nd, "total_hd": total_hd},
        ))
    else:
        results.append(result("total_nd_hd", INFO, "pass", "Tổng ND khớp SUM HD.", {"total": total_nd}))

    results.extend(duplicate_results(hd_rows))
    results.extend(validate_negative_rows(hd_rows, negative_supported=negative_supported))

    if not supplement.get("limit_verified"):
        results.append(result("limit_verified", ERROR, "fail", "Chưa rà/xác minh định mức chi phí."))
    sap_document = str(supplement.get("sap_document") or "").strip() or source_sap_document(source)
    if not sap_document:
        results.append(result("missing_sap_document", ERROR, "fail", "Thiếu mã chứng từ SAP từ nguồn hoặc trường bổ sung."))

    payment_method = str(supplement.get("payment_method") or "").strip()
    if not payment_method:
        results.append(result("missing_payment_method", ERROR, "fail", "Thiếu phương thức nhận tiền."))
    elif payment_method != FIXED_PAYMENT_METHOD:
        results.append(result(
            "payment_method_fixed",
            ERROR,
            "fail",
            "Phương thức nhận tiền của luồng này phải cố định là Chuyển khoản.",
            {"payment_method": payment_method},
        ))
    else:
        results.append(result("payment_method_fixed", INFO, "pass", "Phương thức nhận tiền là Chuyển khoản."))

    for key, label in (
        ("ktpt", "KTPT"),
        ("request_content", "nội dung đề nghị thanh toán"),
    ):
        if not str(supplement.get(key) or "").strip():
            results.append(result(f"missing_{key}", ERROR, "fail", f"Thiếu {label}."))

    if supplement.get("advance_amount") not in (None, "", 0, "0"):
        results.append(result("advance_amount", ERROR, "fail", "Tạm ứng phải luôn bằng 0."))
    else:
        results.append(result("advance_amount", INFO, "pass", "Tạm ứng bằng 0."))

    desired_date = str(supplement.get("desired_payment_date") or "").strip()
    if desired_date:
        try:
            _dt.date.fromisoformat(desired_date)
        except ValueError:
            results.append(result("desired_payment_date", ERROR, "fail", "Ngày mong muốn thanh toán không hợp lệ."))
    else:
        results.append(result("missing_desired_payment_date", ERROR, "fail", "Thiếu ngày mong muốn thanh toán."))

    return {
        "status": validation_status(results),
        "results": results,
        "total_nd": total_nd,
        "total_hd": total_hd,
        "hd_count": len(hd_rows),
    }
