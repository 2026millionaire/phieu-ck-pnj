# -*- coding: utf-8 -*-
"""Manifest/audit helpers for ĐNCK thanh toán chi phí khác."""

import hashlib
from pathlib import Path


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_manifest_markdown(draft_id, source, validation, outputs=None):
    nd = source.get("nd") or {}
    hd_rows = source.get("hd") or []
    lines = [
        f"# Manifest ĐNCK thanh toán chi phí khác #{draft_id}",
        "",
        "## Nguồn dữ liệu",
        f"- Loại nguồn: {source.get('source_type', '')}",
        f"- Live: {'Có' if source.get('live') else 'Không'}",
        f"- Spreadsheet ID: {source.get('spreadsheet_id', '')}",
        f"- Range ND: {source.get('nd_range', '')}",
        f"- Range HD: {source.get('hd_range', '')}",
        f"- Thời điểm đọc: {source.get('read_at', '')}",
        f"- Source hash: {source.get('source_hash', '')}",
        "",
        "## ND",
        f"- STT: {nd.get('stt', '')}",
        f"- Plant: {nd.get('plant', '')}",
        f"- Cost center: {nd.get('cost_center', '')}",
        f"- Tài khoản chi phí: {nd.get('expense_account', '')}",
        f"- Kỳ thanh toán: {nd.get('period', '')}",
        f"- Đối tượng: {nd.get('object_name', '')}",
        f"- Tổng ND: {nd.get('amount', 0)}",
        "",
        "## HD",
    ]
    for row in hd_rows:
        lines.append(
            f"- {row.get('invoice_date', '')} | {row.get('invoice_no_full', '')} | "
            f"{row.get('amount', 0)} | MST {row.get('supplier_tax_id', '')} | "
            f"Mã tra cứu {row.get('lookup_code', '')}"
        )
    lines.extend([
        "",
        "## Kết quả validate",
        f"- Trạng thái: {validation.get('status', '')}",
        f"- Tổng ND: {validation.get('total_nd', 0)}",
        f"- Tổng HD: {validation.get('total_hd', 0)}",
    ])
    for item in validation.get("results") or []:
        lines.append(f"- [{item.get('status')}] {item.get('rule_key')}: {item.get('message')}")
    lines.extend(["", "## File output"])
    for item in outputs or []:
        lines.append(f"- {item.get('output_type')}: {item.get('output_path')} | SHA-256 {item.get('sha256', '')}")
    lines.append("")
    return "\n".join(lines)


def write_manifest(path, draft_id, source, validation, outputs=None):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(build_manifest_markdown(draft_id, source, validation, outputs), encoding="utf-8")
    return str(target), file_sha256(target)
