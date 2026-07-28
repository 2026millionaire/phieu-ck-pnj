#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import tempfile
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import app as app_module


SHORT_ADDRESS = "27 Hà Nội, phường Thuận Hóa, thành phố Huế"
SHORT_BANK = "Ngân hàng TMCP Á Châu (ACB) - CN Huế"
LONG_ADDRESS = (
    "S\u1ed1 128 \u0111\u01b0\u1eddng Tr\u1ea7n H\u01b0ng \u0110\u1ea1o, "
    "ph\u01b0\u1eddng Thu\u1eadn Ho\u00e1, th\u00e0nh ph\u1ed1 Hu\u1ebf, "
    "t\u1ec9nh Th\u1eeba Thi\u00ean Hu\u1ebf, Vi\u1ec7t Nam"
)
LONG_BANK = (
    "Ng\u00e2n h\u00e0ng TMCP Ngo\u1ea1i Th\u01b0\u01a1ng Vi\u1ec7t Nam "
    "(Vietcombank) - VCB chi nh\u00e1nh Th\u1eeba Thi\u00ean Hu\u1ebf"
)


def login_admin(client, user_id=1):
    with client.session_transaction() as session:
        session["user_id"] = user_id
        session["user_name"] = "ADMIN QA"
        session["role"] = "admin"


def build_payload(case):
    return {
        "status": "printed",
        "ngay_lap": "2026-07-28",
        "ma_kh": case["ma_kh"],
        "ten_kh": case["ten_kh"],
        "dia_chi": case["dia_chi"],
        "sdt": case["sdt"],
        "cccd": case["cccd"],
        "so_tk": case["so_tk"],
        "ten_tk": case["ten_tk"],
        "ngan_hang": case["ngan_hang"],
        "so_bk": case["so_bk"],
        "tvv_code": "11358",
        "tvv_name": "NGUYEN TVV",
        "plant": case["plant"],
        "tong_ck": case["tong_ck"],
        "nguoi_ki": "cht",
        "show_payment_dates": True,
        "payment_time_mode": "T120",
        "chung_tu": case["chung_tu"],
    }


def create_cases():
    return [
        {
            "label": "pnj_1305_short",
            "plant": "1305",
            "ma_kh": "100000101",
            "ten_kh": "DOAN ÁI MINH",
            "dia_chi": SHORT_ADDRESS,
            "sdt": "0901000101",
            "cccd": "012345678901",
            "so_tk": "123456789101",
            "ten_tk": "DOAN AI MINH",
            "ngan_hang": SHORT_BANK,
            "so_bk": "4403913051",
            "tong_ck": 6000000,
            "chung_tu": [
                {"loai": "Bảng kê", "so_ct": "4403913051", "bk_ref": "000551/07_1305", "gia_tri": 10000000, "gio": "28/07/2026 10:00"},
                {"loai": "Hóa đơn", "so_ct": "9013913051", "gia_tri": 4000000, "gio": "28/07/2026 10:05"},
            ],
        },
        {
            "label": "pnj_1305_long",
            "plant": "1305",
            "ma_kh": "100000102",
            "ten_kh": "NGUYỄN THỊ NGỌC HUYỀN PHƯƠNG",
            "dia_chi": LONG_ADDRESS,
            "sdt": "0901000102",
            "cccd": "012345678902",
            "so_tk": "123456789102",
            "ten_tk": "NGUYEN THI NGOC HUYEN PHUONG",
            "ngan_hang": LONG_BANK,
            "so_bk": "4403913052",
            "tong_ck": 6000000,
            "chung_tu": [
                {"loai": "Bảng kê", "so_ct": "4403913052", "bk_ref": "000552/07_1305", "gia_tri": 10000000, "gio": "28/07/2026 10:10"},
                {"loai": "Hóa đơn", "so_ct": "9013913052", "gia_tri": 4000000, "gio": "28/07/2026 10:15"},
            ],
        },
        {
            "label": "caf_2122_short",
            "plant": "2122",
            "ma_kh": "100000103",
            "ten_kh": "VÕ THỊ NGỌC DIỆP",
            "dia_chi": SHORT_ADDRESS,
            "sdt": "0901000103",
            "cccd": "012345678903",
            "so_tk": "123456789103",
            "ten_tk": "VO THI NGOC DIEP",
            "ngan_hang": SHORT_BANK,
            "so_bk": "4403921221",
            "tong_ck": 6000000,
            "chung_tu": [
                {"loai": "Bảng kê", "so_ct": "4403921221", "bk_ref": "000551/07_2122", "gia_tri": 10000000, "gio": "28/07/2026 10:20"},
                {"loai": "Hóa đơn", "so_ct": "9013921221", "gia_tri": 4000000, "gio": "28/07/2026 10:25"},
            ],
        },
        {
            "label": "caf_2122_long",
            "plant": "2122",
            "ma_kh": "100000104",
            "ten_kh": "TRẦN THỊ THANH NGÂN NGUYÊN",
            "dia_chi": LONG_ADDRESS,
            "sdt": "0901000104",
            "cccd": "012345678904",
            "so_tk": "123456789104",
            "ten_tk": "TRAN THI THANH NGAN NGUYEN",
            "ngan_hang": LONG_BANK,
            "so_bk": "4403921222",
            "tong_ck": 6000000,
            "chung_tu": [
                {"loai": "Bảng kê", "so_ct": "4403921222", "bk_ref": "000552/07_2122", "gia_tri": 10000000, "gio": "28/07/2026 10:30"},
                {"loai": "Hóa đơn", "so_ct": "9013921222", "gia_tri": 4000000, "gio": "28/07/2026 10:35"},
            ],
        },
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        default="outputs/payment_planning_layout_trial_20260728_v4/trial",
    )
    parser.add_argument(
        "--enable-title-bg",
        action="store_true",
        help="Bật setting màu nền PL1 trước khi render QA.",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    original_db_path = app_module.DB_PATH
    original_customer_store = app_module._customer_lookup_store
    original_employee_store = app_module._employee_lookup_store

    manifest = []
    render_results = []

    try:
        with tempfile.TemporaryDirectory(prefix="payment_planning_layout_qa_") as tmpdir:
            temp_root = Path(tmpdir)
            app_module.DB_PATH = str(temp_root / "phieu.db")
            app_module._customer_lookup_store = None
            app_module._employee_lookup_store = None
            app_module.init_db()
            app_module.app.config.update(TESTING=True)
            client = app_module.app.test_client()
            login_admin(client)
            if args.enable_title_bg:
                settings_response = client.post(
                    "/api/settings",
                    json={"payment_planning_title_bg": "1"},
                    headers={"Origin": "http://localhost"},
                )
                if settings_response.status_code != 200:
                    raise RuntimeError("Không thể bật setting màu nền PL1 cho QA.")

            for case in create_cases():
                response = client.post("/api/save", json=build_payload(case))
                if response.status_code != 200:
                    raise RuntimeError(f"Không thể lưu phiếu cho case {case['label']}: {response.status_code}")
                phieu_id = response.get_json()["id"]

                html_response = client.get(f"/api/payment-planning/{phieu_id}")
                if html_response.status_code != 200:
                    raise RuntimeError(f"Không thể render HTML cho case {case['label']}: {html_response.status_code}")
                html_text = html_response.get_data(as_text=True)

                pdf_response = client.get(f"/api/payment-planning-pdf/{phieu_id}")
                if pdf_response.status_code != 200:
                    raise RuntimeError(f"Không thể render PDF cho case {case['label']}: {pdf_response.status_code}")

                html_path = output_dir / f"{case['label']}.html"
                pdf_path = output_dir / f"{case['label']}.pdf"
                html_path.write_text(html_text, encoding="utf-8")
                pdf_path.write_bytes(pdf_response.data)

                html_rel = str(html_path.resolve().relative_to(ROOT))
                pdf_abs = str(pdf_path.resolve())

                manifest.append(
                    {
                        "label": case["label"],
                        "id": phieu_id,
                        "plant": case["plant"],
                        "name": case["ten_kh"],
                        "html": html_rel,
                    }
                )
                render_results.append(
                    {
                        "label": case["label"],
                        "id": phieu_id,
                        "plant": case["plant"],
                        "name": case["ten_kh"],
                        "html": html_rel,
                        "pdf": pdf_abs,
                    }
                )
    finally:
        app_module.DB_PATH = original_db_path
        app_module._customer_lookup_store = original_customer_store
        app_module._employee_lookup_store = original_employee_store

    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "render-results.json").write_text(
        json.dumps(render_results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(json.dumps({"output_dir": str(output_dir), "cases": [c["label"] for c in render_results]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
