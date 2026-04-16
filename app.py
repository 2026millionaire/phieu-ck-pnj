# -*- coding: utf-8 -*-
"""
Phieu Xac Nhan Chuyen Khoan - PNJ Store 1305
Flask web app for creating transfer confirmation slips.
"""

import json
import os
import re
import sqlite3
import webbrowser
from datetime import datetime, timedelta

from functools import wraps

from flask import Flask, g, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

# ---------------------------------------------------------------------------
# App config
# ---------------------------------------------------------------------------
app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.secret_key = os.environ.get("SECRET_KEY", "pnj1305-xnck-secret-key-2026")

# Auth: only require login when running on server (not localhost)
REQUIRE_LOGIN = os.environ.get("REQUIRE_LOGIN", "0") == "1"

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "phieu_ck.db")
PORT = 5050

# ---------------------------------------------------------------------------
# Load data from Excel
# ---------------------------------------------------------------------------
import pandas as pd

DATA_XLSX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "phieu-ck-data.xlsx")


def load_bank_data():
    """Load bank list from Excel sheet 'bank'."""
    try:
        df = pd.read_excel(DATA_XLSX, sheet_name="bank", dtype=str).fillna("")
        banks = []
        for _, row in df.iterrows():
            banks.append({
                "eoffice": row.get("Mã eoffice", "").strip(),
                "bin": row.get("BIN", "").strip(),
                "ten_tra_cuu": row.get("Tên tra cứu", "").strip(),
                "ten_day_du": row.get("Tên đầy đủ", "").strip(),
                "ten_gd": row.get("Tên GD", "").strip(),
                "code": row.get("Code", "").strip(),
            })
        return banks
    except Exception:
        return []


def load_tvv_data():
    """Load TVV list from Excel sheet 'tvv'."""
    try:
        df = pd.read_excel(DATA_XLSX, sheet_name="tvv", dtype=str).fillna("")
        tvvs = []
        for _, row in df.iterrows():
            tvvs.append({
                "ma": row.get("mã TVV", "").strip(),
                "ten": row.get("tên TVV", "").strip(),
            })
        return tvvs
    except Exception:
        return []


BANK_LIST = load_bank_data()
TVV_LIST = load_tvv_data()

# Legacy BANK_BINS for backward compat (QR generation)
BANK_BINS = {b["ten_gd"]: b["bin"] for b in BANK_LIST}
BANK_BINS.update({b["code"]: b["bin"] for b in BANK_LIST})

# MBBank bankId mapping (BIN → bankId for account lookup)
def load_mb_bank_map():
    try:
        df = pd.read_excel(DATA_XLSX, sheet_name="mb_bank", dtype=str).fillna("")
        return {row["BIN"]: row["bankId_MB"] for _, row in df.iterrows() if row["BIN"]}
    except Exception:
        return {}

MB_BANK_MAP = load_mb_bank_map()

# ---------------------------------------------------------------------------
# Staff defaults (store 1305)
# ---------------------------------------------------------------------------
STAFF = {
    "nv_ke_toan": ["CHÂU ĐĂNG KHOA", "LÊ THỊ MỸ TUYỀN"],
    "cua_hang_truong": "HỒ THỊ HÀ MY",
    "tvv": "NGUYỄN THỊ MỸ UYÊN",
}

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db():
    """Get a database connection for the current request."""
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
    return g.db


@app.teardown_appcontext
def close_db(exception):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    """Create tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS phieu (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at      TEXT NOT NULL,
            ma_kh           TEXT,
            ten_kh          TEXT,
            sdt             TEXT,
            cccd            TEXT,
            so_tk           TEXT,
            ten_tk          TEXT,
            ngan_hang       TEXT,
            so_bk           TEXT,
            tvv_code        TEXT,
            tvv_name        TEXT,
            cht_name        TEXT,
            plant           TEXT DEFAULT '1305',
            chung_tu_json   TEXT,
            tong_ck         REAL DEFAULT 0,
            ngay_tt         TEXT,
            status          TEXT DEFAULT 'draft',
            qr_url          TEXT,
            noi_dung        TEXT,
            nguoi_ki        TEXT DEFAULT 'tvv',
            da_trinh        INTEGER DEFAULT 0
        )
    """)
    # Add columns if missing (for existing DBs)
    for col, ctype, default in [
        ("nguoi_ki", "TEXT", "'tvv'"),
        ("da_trinh", "INTEGER", "0"),
        ("user_id", "INTEGER", "1"),
    ]:
        try:
            conn.execute(f"ALTER TABLE phieu ADD COLUMN {col} {ctype} DEFAULT {default}")
        except Exception:
            pass

    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            name     TEXT DEFAULT ''
        )
    """)
    # Create default admin user if no users exist
    row = conn.execute("SELECT COUNT(*) FROM users").fetchone()
    if row[0] == 0:
        conn.execute(
            "INSERT OR IGNORE INTO users (username, password, name) VALUES (?, ?, ?)",
            ("admin", generate_password_hash("pnj1305"), "Admin"),
        )

    # TVV table (replaces Excel sheet)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tvv (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            ma   TEXT NOT NULL,
            ten  TEXT NOT NULL
        )
    """)
    # Migrate TVV from Excel → DB if table is empty
    row_tvv = conn.execute("SELECT COUNT(*) FROM tvv").fetchone()
    if row_tvv[0] == 0:
        for t in TVV_LIST:
            if t["ma"] and t["ten"]:
                conn.execute("INSERT INTO tvv (ma, ten) VALUES (?, ?)", (t["ma"], t["ten"]))

    # Lý do hủy BK table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lydo_huy (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            noi_dung TEXT NOT NULL
        )
    """)
    row_ld = conn.execute("SELECT COUNT(*) FROM lydo_huy").fetchone()
    if row_ld[0] == 0:
        for ld in [
            "Khách hàng đổi ý.",
            "Sai thông tin khách hàng.",
            "Sai mã sản phẩm.",
            "Sai trọng lượng sản phẩm.",
            "Sai giá trị mua lại.",
        ]:
            conn.execute("INSERT INTO lydo_huy (noi_dung) VALUES (?)", (ld,))

    conn.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    # Default settings
    defaults = {
        "cht_name": "HỒ THỊ HÀ MY",
        "kt1_name": "CHÂU ĐĂNG KHOA",
        "kt2_name": "LÊ THỊ MỸ TUYỀN",
        "thoi_gian_ck": "48",
        "plant": "1305",
        "vietqr_client_id": "",
        "vietqr_api_key": "",
        "mb_username": "",
        "mb_password": "",
        "mb_account": "",
        "bk_prefix": "4403",
    }
    for k, v in defaults.items():
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, v))
    conn.commit()
    conn.close()


def get_settings():
    """Load all settings as dict."""
    db = get_db()
    rows = db.execute("SELECT key, value FROM settings").fetchall()
    return {r["key"]: r["value"] for r in rows}


def row_to_dict(row):
    """Convert a sqlite3.Row to a plain dict."""
    if row is None:
        return None
    return dict(row)

# ---------------------------------------------------------------------------
# Number to Vietnamese words
# ---------------------------------------------------------------------------

def _doc_so_hang(n):
    """Read a number 0-999 in Vietnamese."""
    ones = [
        "", "mot", "hai", "ba", "bon", "nam",
        "sau", "bay", "tam", "chin",
    ]
    vn = [
        "", "m\u1ed9t", "hai", "ba", "b\u1ed1n", "n\u0103m",
        "s\u00e1u", "b\u1ea3y", "t\u00e1m", "ch\u00edn",
    ]

    if n == 0:
        return ""

    tram = n // 100
    chuc = (n % 100) // 10
    donvi = n % 10

    parts = []

    if tram > 0:
        parts.append(f"{vn[tram]} tr\u0103m")
        if chuc == 0 and donvi > 0:
            parts.append(f"linh {vn[donvi]}")
        elif chuc == 1:
            parts.append("m\u01b0\u1eddi")
            if donvi > 0:
                if donvi == 5:
                    parts.append("l\u0103m")
                elif donvi == 1:
                    parts.append("m\u1ed1t")
                else:
                    parts.append(vn[donvi])
        elif chuc > 1:
            parts.append(f"{vn[chuc]} m\u01b0\u01a1i")
            if donvi > 0:
                if donvi == 5:
                    parts.append("l\u0103m")
                elif donvi == 1:
                    parts.append("m\u1ed1t")
                else:
                    parts.append(vn[donvi])
    else:
        # tram == 0
        if chuc == 0:
            parts.append(vn[donvi])
        elif chuc == 1:
            parts.append("m\u01b0\u1eddi")
            if donvi > 0:
                if donvi == 5:
                    parts.append("l\u0103m")
                elif donvi == 1:
                    parts.append("m\u1ed1t")
                else:
                    parts.append(vn[donvi])
        else:
            parts.append(f"{vn[chuc]} m\u01b0\u01a1i")
            if donvi > 0:
                if donvi == 5:
                    parts.append("l\u0103m")
                elif donvi == 1:
                    parts.append("m\u1ed1t")
                else:
                    parts.append(vn[donvi])

    return " ".join(parts)


def so_thanh_chu(n):
    """
    Convert an integer to Vietnamese words.
    E.g. 2302410 -> "hai triệu, ba trăm linh hai nghìn, bốn trăm mười đồng"
    """
    if n is None or n == 0:
        return "kh\u00f4ng \u0111\u1ed3ng"

    n = int(round(n))
    if n < 0:
        return "(\u00e2m) " + so_thanh_chu(-n)

    # Split into groups of 3 digits from right
    units = ["", "ngh\u00ecn", "tri\u1ec7u", "t\u1ef7", "ngh\u00ecn t\u1ef7"]
    groups = []
    temp = n
    while temp > 0:
        groups.append(temp % 1000)
        temp //= 1000

    parts = []
    for i in range(len(groups) - 1, -1, -1):
        if groups[i] == 0:
            continue
        text = _doc_so_hang(groups[i])
        if text:
            unit = units[i] if i < len(units) else ""
            parts.append(f"{text} {unit}".strip())

    result = ", ".join(parts) if parts else "kh\u00f4ng"
    # Capitalize first letter
    result = result[0].upper() + result[1:]
    return result + " \u0111\u1ed3ng"

# ---------------------------------------------------------------------------
# Business logic helpers
# ---------------------------------------------------------------------------

def parse_sap_paste(raw_text):
    """
    Parse tab-separated SAP ZFIE0029 data.

    Columns:
      0: Document Number (14xx=HĐ, 25xx=BK, 16xx=Cọc)
      1: Reference
      2: Amount (trailing '-' = negative = phải CK; positive = phải thu HĐ)
      3: Sp. G/L Trans.Type (usually empty)
      4: Sales Document (90xxxxxxxx for HĐ)
      5: Purchasing Document (optional)

    Logic:
      - Amount positive → Hóa đơn, số CT from col4 (90xx), CK -= amount
      - Amount negative + col0 25xx → Bảng kê, CK += amount
      - Amount negative + col0 16xx → Biên nhận cọc, CK += amount
    """
    records = []
    if not raw_text or not raw_text.strip():
        return records

    for line in raw_text.strip().split("\n"):
        line = line.strip()
        if not line:
            continue

        cols = line.split("\t")
        if len(cols) < 3:
            continue

        doc_num = cols[0].strip()
        so_ref = cols[1].strip() if len(cols) > 1 else ""

        # Parse amount
        amt_str = cols[2].strip() if len(cols) > 2 else "0"
        is_negative = amt_str.endswith("-")
        cleaned = re.sub(r"[.,\s\-]", "", amt_str)
        try:
            amount = abs(float(cleaned))
        except ValueError:
            amount = 0

        # Column 4: Sales Document (for HĐ)
        so_hd = cols[4].strip() if len(cols) > 4 and cols[4].strip() else ""

        # Determine type based on sign and document number
        # so_ct = số hiển thị (90xx cho HĐ, 44xx cho BK lấy từ so_bk input)
        # doc_num = số SAP gốc (14xx, 25xx, 16xx...)
        if not is_negative:
            loai = "Hóa đơn"
            so_ct = so_hd if so_hd else doc_num  # 90xxxxxxxx
            gia_tri = amount
        elif re.match(r"^25\d{8}$", doc_num):
            loai = "Bảng kê"
            so_ct = doc_num  # tạm dùng 25xx, frontend sẽ thay bằng so_bk (44xx)
            gia_tri = amount
        elif re.match(r"^16\d{8}$", doc_num):
            loai = "Biên nhận cọc"
            so_ct = doc_num
            gia_tri = amount
        elif re.match(r"^15\d{8}$", doc_num):
            loai = "HBTL"
            so_ct = doc_num
            gia_tri = amount
        elif re.match(r"^26\d{8}$", doc_num):
            loai = "Phải CK khác"
            so_ct = doc_num
            gia_tri = amount
        else:
            loai = "Phải CK khác" if is_negative else "Phải thu khác"
            so_ct = doc_num
            gia_tri = amount

        records.append({
            "doc_num": doc_num,  # số SAP gốc
            "loai": loai,
            "so_ct": so_ct,
            "gia_tri": gia_tri,
            "gio": "",
        })

    return records


def calc_tong_ck(chung_tu_list):
    """
    Total CK = BK + Cọc - HĐ
    """
    total = 0
    for r in chung_tu_list:
        if r["loai"] in ("Hóa đơn", "Phải thu khác"):
            total -= r["gia_tri"]
        else:
            total += r["gia_tri"]
    return total


def calc_ngay_tt(created_at_str=None):
    """
    Payment date rule:
    - If created before 16:30 on a weekday -> same day
    - If after 16:30 or weekend -> next business day
    """
    if created_at_str:
        now = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S")
    else:
        now = datetime.now()

    cutoff = now.replace(hour=16, minute=30, second=0, microsecond=0)

    if now.weekday() < 5 and now <= cutoff:
        # Weekday before 16:30 -> same day
        return now.strftime("%Y-%m-%d")
    else:
        # Find next business day
        candidate = now + timedelta(days=1)
        while candidate.weekday() >= 5:  # Skip Saturday(5) and Sunday(6)
            candidate += timedelta(days=1)
        return candidate.strftime("%Y-%m-%d")


def build_qr_url(ngan_hang, so_tk, amount=None, memo=None):
    """
    Build VietQR image URL.
    Format: https://img.vietqr.io/image/{BIN}-{account}-qr_only.png
    """
    # Try exact match first, then search keywords in bank name
    bin_code = BANK_BINS.get(ngan_hang, "")
    if not bin_code:
        nh_upper = ngan_hang.upper()
        for key, val in BANK_BINS.items():
            if key.upper() in nh_upper:
                bin_code = val
                break
    if not bin_code or not so_tk:
        return ""

    url = f"https://img.vietqr.io/image/{bin_code}-{so_tk}-qr_only.png"

    params = []
    if amount:
        params.append(f"amount={int(amount)}")
    if memo:
        params.append(f"addInfo={memo}")
    if params:
        url += "?" + "&".join(params)

    return url


def build_noi_dung(plant, so_bk, ngay, ten_kh):
    """
    Build eOffice QT82 noi_dung string.
    Format: '1305 TT PO {so_bk} ngày {date} cho {ten_kh}'
    """
    return f"{plant} TT PO {so_bk} ng\u00e0y {ngay} cho {ten_kh}"

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def login_required(f):
    """Decorator: redirect to login page if not authenticated."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not REQUIRE_LOGIN:
            return f(*args, **kwargs)
        if not session.get("user_id"):
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return decorated


@app.before_request
def check_auth():
    """Global auth check — skip for login/static routes."""
    if not REQUIRE_LOGIN:
        return
    allowed = ("login_page", "login_action", "logout_action", "static")
    if request.endpoint in allowed:
        return
    if not session.get("user_id"):
        return redirect(url_for("login_page"))


@app.route("/login", methods=["GET"])
def login_page():
    if session.get("user_id"):
        return redirect(url_for("index"))
    error = request.args.get("error", "")
    return render_template("login.html", error=error)


@app.route("/login", methods=["POST"])
def login_action():
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")
    remember = request.form.get("remember")

    db = get_db()
    user = db.execute(
        "SELECT * FROM users WHERE LOWER(username) = LOWER(?)", (username,)
    ).fetchone()

    if user and check_password_hash(user["password"], password):
        session.clear()
        session["user_id"] = user["id"]
        session["user_name"] = user["name"] or user["username"]
        if remember:
            session.permanent = True
            app.permanent_session_lifetime = timedelta(days=30)
        return redirect(url_for("index"))

    return redirect(url_for("login_page", error="Sai tên đăng nhập hoặc mật khẩu"))


@app.route("/logout")
def logout_action():
    session.clear()
    return redirect(url_for("login_page"))


def current_user_id():
    """Return logged-in user's ID, default 1 (admin) for local/no-login mode."""
    return session.get("user_id", 1)


def is_admin():
    """Check if current user is admin (username='admin')."""
    uid = current_user_id()
    if uid == 1:
        return True  # ID 1 is always admin
    try:
        db = get_db()
        row = db.execute("SELECT username FROM users WHERE id = ?", (uid,)).fetchone()
        return row and row["username"].lower() == "admin"
    except Exception:
        return False


@app.route("/")
def index():
    """Main page with input form."""
    settings = get_settings()
    bk_prefix = settings.get("bk_prefix", "4403")
    return render_template("index.html", staff=STAFF, bank_list=sorted(BANK_BINS.keys()), settings=settings, bk_prefix=bk_prefix)


@app.route("/history")
def history_page():
    """History page."""
    return render_template("history.html")


@app.route("/bb-huy")
def bb_huy_page():
    """BB Hủy Bảng Kê — form page."""
    settings = get_settings()
    bk_prefix = settings.get("bk_prefix", "4403")
    return render_template("bb_huy.html", settings=settings, bk_prefix=bk_prefix)


@app.route("/bb-huy/print")
def bb_huy_print():
    """BB Hủy Bảng Kê — printable A5 landscape."""
    settings = get_settings()
    so_bk = request.args.get("so_bk", "")
    tvv_name = request.args.get("tvv", "")
    ly_do = request.args.get("ly_do", "")
    kt_name = request.args.get("kt", settings.get("kt1_name", ""))
    cht_name = settings.get("cht_name", "")
    plant = settings.get("plant", "1305")

    now = datetime.now()
    ngay_str = f"Hôm nay, ngày {now.day:02d} tháng {now.month:02d} năm {now.year}, tại Cửa Hàng PNJ NEXT 27 Hà Nội - Huế,"

    return render_template("bb_huy_print.html",
        so_bk=so_bk, tvv_name=tvv_name, ly_do=ly_do,
        kt_name=kt_name, cht_name=cht_name,
        ngay_str=ngay_str, plant=plant)


@app.route("/eoffice")
def eoffice_index():
    """eOffice QT82 page without phieu selected."""
    return render_template("eoffice.html", phieu=None)


@app.route("/eoffice/<int:phieu_id>")
def eoffice_page(phieu_id):
    """eOffice QT82 page with copyable fields."""
    db = get_db()
    row = db.execute("SELECT * FROM phieu WHERE id = ? AND user_id = ?",
                     (phieu_id, current_user_id())).fetchone()
    if not row:
        return render_template("eoffice.html", phieu=None)

    d = row_to_dict(row)
    try:
        d["chung_tu"] = json.loads(d["chung_tu_json"]) if d["chung_tu_json"] else []
    except (json.JSONDecodeError, TypeError):
        d["chung_tu"] = []

    settings = get_settings()
    plant = d.get("plant", settings.get("plant", "1305"))

    # Build eOffice fields
    # Nội dung: "1305 TT PO 2500202418 ngày 2026-04-09 cho HỒ THỊ MỸ VÂN"
    try:
        dt = datetime.strptime(d["created_at"], "%Y-%m-%d %H:%M:%S")
        ngay_str = dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        ngay_str = d["created_at"][:10] if d["created_at"] else ""

    # Số BK: ưu tiên từ form input (44xx), fallback từ chứng từ
    so_bk = d.get("so_bk", "")

    # Số chứng từ SAP gốc (14xx, 25xx, 16xx...)
    doc_nums = [ct.get("doc_num", ct.get("so_ct", "")) for ct in d["chung_tu"] if ct.get("doc_num") or ct.get("so_ct")]
    d["eo_so_ct_sap"] = ", ".join(doc_nums)

    d["eo_ma_kh"] = d.get("ma_kh", "")
    d["eo_noi_dung"] = f"{plant} TT PO {so_bk} ngày {ngay_str} cho {d.get('ten_kh', '')}"
    d["eo_ten_tk"] = d.get("ten_tk", "") or d.get("ten_kh", "")
    d["eo_so_tk"] = re.sub(r"\D", "", d.get("so_tk", ""))
    # Mã NH eOffice: tìm từ BANK_LIST
    eo_ma_nh = ""
    for b in BANK_LIST:
        if b["ten_tra_cuu"] and b["ten_tra_cuu"] in d.get("ngan_hang", ""):
            eo_ma_nh = b["eoffice"]
            break
    if not eo_ma_nh:
        for b in BANK_LIST:
            if b["ten_gd"] and b["ten_gd"].lower() in d.get("ngan_hang", "").lower():
                eo_ma_nh = b["eoffice"]
                break
    d["eo_ma_nh"] = eo_ma_nh
    d["eo_cccd"] = re.sub(r"\D", "", d.get("cccd", ""))
    # Tên file: "1305_BK HO THI MY VAN"
    def remove_diacritics(s):
        import unicodedata
        s = s.replace("đ", "d").replace("Đ", "D")
        return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    d["eo_ten_file"] = f"{plant}_BK {remove_diacritics(d.get('ten_kh', ''))}"

    return render_template("eoffice.html", phieu=d)


@app.route("/settings")
def settings_page():
    """Settings page."""
    settings = get_settings()
    return render_template("settings.html", settings=settings, admin=is_admin())


@app.route("/api/settings", methods=["GET"])
def api_get_settings():
    """Get all settings."""
    return jsonify({"ok": True, "data": get_settings()})


@app.route("/api/settings", methods=["POST"])
def api_save_settings():
    """Save settings."""
    data = request.get_json(force=True)
    db = get_db()
    for k, v in data.items():
        db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (k, str(v)))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/da-trinh/<int:phieu_id>", methods=["POST"])
def api_da_trinh(phieu_id):
    """Toggle da_trinh status."""
    data = request.get_json(force=True)
    db = get_db()
    db.execute("UPDATE phieu SET da_trinh = ? WHERE id = ? AND user_id = ?",
               (1 if data.get("da_trinh") else 0, phieu_id, current_user_id()))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/save", methods=["POST"])
def api_save():
    """Save a phieu to the database. Returns the new ID."""
    data = request.get_json(force=True)

    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Parse chung_tu from SAP paste or from already-parsed JSON
    chung_tu_list = data.get("chung_tu", [])
    if isinstance(chung_tu_list, str):
        # Might be raw SAP paste text
        chung_tu_list = parse_sap_paste(chung_tu_list)

    chung_tu_json = json.dumps(chung_tu_list, ensure_ascii=False)
    tong_ck = data.get("tong_ck") or calc_tong_ck(chung_tu_list)
    tong_ck = float(tong_ck)

    # Tính ngày TT từ giờ BK gần nhất + settings thoi_gian_ck (default 48h)
    settings = get_settings()
    ck_hours = int(settings.get("thoi_gian_ck", "48"))
    ngay_tt = data.get("ngay_tt", "")
    if not ngay_tt:
        # Tìm giờ BK gần nhất trong chung_tu
        bk_times = []
        for ct in chung_tu_list:
            if ct.get("loai") in ("Bảng kê", "Biên nhận cọc") and ct.get("gio"):
                try:
                    t = datetime.strptime(ct["gio"], "%d/%m/%Y %H:%M")
                    bk_times.append(t)
                except ValueError:
                    pass
        if bk_times:
            latest_bk = max(bk_times)
            ngay_tt_dt = latest_bk + timedelta(hours=ck_hours)
            ngay_tt = ngay_tt_dt.strftime("%Y-%m-%d %H:%M")
        else:
            ngay_tt = calc_ngay_tt(created_at)

    plant = data.get("plant", settings.get("plant", "1305"))
    ngan_hang = data.get("ngan_hang", "")
    so_tk = data.get("so_tk", "")
    ten_kh = data.get("ten_kh", "")
    so_bk = data.get("so_bk", "")

    # Build QR URL (only BIN + account, no amount)
    qr_url = build_qr_url(ngan_hang, so_tk)

    # eOffice noi_dung
    ngay_str = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y")
    noi_dung = build_noi_dung(plant, so_bk, ngay_str, ten_kh)

    nguoi_ki = data.get("nguoi_ki", "tvv")

    db = get_db()
    cursor = db.execute("""
        INSERT INTO phieu
            (created_at, ma_kh, ten_kh, sdt, cccd,
             so_tk, ten_tk, ngan_hang, so_bk,
             tvv_code, tvv_name, cht_name, plant,
             chung_tu_json, tong_ck, ngay_tt, status, qr_url, noi_dung, nguoi_ki,
             user_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        created_at,
        data.get("ma_kh", ""),
        ten_kh,
        data.get("sdt", ""),
        data.get("cccd", ""),
        so_tk,
        data.get("ten_tk", ""),
        ngan_hang,
        so_bk,
        data.get("tvv_code", ""),
        data.get("tvv_name_real", "") or data.get("tvv_name", ""),
        data.get("cht_name", STAFF["cua_hang_truong"]),
        plant,
        chung_tu_json,
        tong_ck,
        ngay_tt,
        "draft",
        qr_url,
        noi_dung,
        nguoi_ki,
        current_user_id(),
    ))
    db.commit()
    new_id = cursor.lastrowid

    return jsonify({
        "ok": True,
        "id": new_id,
        "tong_ck": tong_ck,
        "tong_ck_chu": so_thanh_chu(tong_ck),
        "ngay_tt": ngay_tt,
        "qr_url": qr_url,
        "noi_dung": noi_dung,
    })


@app.route("/api/phieu/<int:phieu_id>")
def api_get_phieu(phieu_id):
    """Get a single phieu as JSON."""
    db = get_db()
    row = db.execute("SELECT * FROM phieu WHERE id = ? AND user_id = ?",
                     (phieu_id, current_user_id())).fetchone()
    if not row:
        return jsonify({"ok": False, "error": "Kh\u00f4ng t\u00ecm th\u1ea5y phi\u1ebfu"}), 404

    d = row_to_dict(row)
    d["tong_ck_chu"] = so_thanh_chu(d["tong_ck"])
    # Parse chung_tu_json back to list
    try:
        d["chung_tu"] = json.loads(d["chung_tu_json"]) if d["chung_tu_json"] else []
    except (json.JSONDecodeError, TypeError):
        d["chung_tu"] = []
    return jsonify({"ok": True, "phieu": d})


@app.route("/api/print/<int:phieu_id>")
def api_print(phieu_id):
    """Return printable HTML for a specific phieu."""
    db = get_db()
    row = db.execute("SELECT * FROM phieu WHERE id = ? AND user_id = ?",
                     (phieu_id, current_user_id())).fetchone()
    if not row:
        return "Kh\u00f4ng t\u00ecm th\u1ea5y phi\u1ebfu", 404

    d = row_to_dict(row)
    d["tong_ck_chu"] = so_thanh_chu(d["tong_ck"])
    try:
        d["chung_tu"] = json.loads(d["chung_tu_json"]) if d["chung_tu_json"] else []
    except (json.JSONDecodeError, TypeError):
        d["chung_tu"] = []

    # Format created_at for display: "09/04/2026_17:38"
    try:
        dt = datetime.strptime(d["created_at"], "%Y-%m-%d %H:%M:%S")
        d["created_at_fmt"] = dt.strftime("%H:%M_%d/%m/%Y")
        d["created_at_date"] = dt.strftime("%d/%m/%Y %H:%M")
        d["ngay"] = dt.strftime("%d")
        d["thang"] = dt.strftime("%m")
        d["nam"] = dt.strftime("%Y")
    except (ValueError, TypeError):
        d["created_at_fmt"] = d["created_at"]
        d["created_at_date"] = d["created_at"]
        d["ngay"] = ""
        d["thang"] = ""
        d["nam"] = ""

    # Format SĐT: 4-3-3 (e.g. 0964 667 669)
    raw_sdt = re.sub(r"\D", "", d.get("sdt", ""))
    if len(raw_sdt) == 10:
        d["sdt_fmt"] = f"{raw_sdt[:4]} {raw_sdt[4:7]} {raw_sdt[7:]}"
    else:
        d["sdt_fmt"] = d.get("sdt", "")

    # Format Số TK: groups of 4 (e.g. 0964 6676 69)
    raw_tk = re.sub(r"\D", "", d.get("so_tk", ""))
    d["so_tk_fmt"] = " ".join([raw_tk[i:i+4] for i in range(0, len(raw_tk), 4)]) if raw_tk else d.get("so_tk", "")

    # Format CCCD: groups of 3 (e.g. 046 093 004 708)
    raw_cccd = re.sub(r"\D", "", d.get("cccd", ""))
    d["cccd_fmt"] = " ".join([raw_cccd[i:i+3] for i in range(0, len(raw_cccd), 3)]) if raw_cccd else d.get("cccd", "")

    # Format ngay_tt for display (supports both "YYYY-MM-DD" and "YYYY-MM-DD HH:MM")
    ngay_tt_raw = d.get("ngay_tt", "")
    try:
        if " " in ngay_tt_raw:
            dt_tt = datetime.strptime(ngay_tt_raw, "%Y-%m-%d %H:%M")
            d["ngay_tt_fmt"] = dt_tt.strftime("%d/%m/%Y %H:%M")
        else:
            dt_tt = datetime.strptime(ngay_tt_raw, "%Y-%m-%d")
            dt_ca = datetime.strptime(d["created_at"], "%Y-%m-%d %H:%M:%S")
            d["ngay_tt_fmt"] = dt_tt.strftime("%d/%m/%Y") + " " + dt_ca.strftime("%H:%M")
    except (ValueError, TypeError):
        d["ngay_tt_fmt"] = ngay_tt_raw

    # Số phiếu: dùng giờ BK gần nhất thay vì giờ created_at
    bk_times = []
    for ct in d["chung_tu"]:
        if ct.get("loai") in ("Bảng kê", "Biên nhận cọc") and ct.get("gio"):
            try:
                t = datetime.strptime(ct["gio"], "%d/%m/%Y %H:%M")
                bk_times.append(t)
            except ValueError:
                pass
    if bk_times:
        latest_bk = max(bk_times)
        d["created_at_fmt"] = latest_bk.strftime("%H:%M_%d/%m/%Y")
    # else keep the created_at_fmt from earlier

    # Resolve tên người ký từ nguoi_ki field
    settings = get_settings()
    nguoi_ki = d.get("nguoi_ki", "tvv")
    nguoi_ki_map = {
        "tvv": d.get("tvv_name", ""),
        "cht": settings.get("cht_name", ""),
        "kt1": settings.get("kt1_name", ""),
        "kt2": settings.get("kt2_name", ""),
    }
    d["nguoi_ki_name"] = nguoi_ki_map.get(nguoi_ki, d.get("tvv_name", ""))

    # Mark as printed
    db.execute("UPDATE phieu SET status = 'printed' WHERE id = ? AND user_id = ?",
               (phieu_id, current_user_id()))
    db.commit()

    return render_template("print.html", p=d, staff=STAFF)


@app.route("/api/history")
def api_history():
    """JSON list of all phieu, newest first (filtered by current user)."""
    db = get_db()
    rows = db.execute("SELECT * FROM phieu WHERE user_id = ? ORDER BY id DESC",
                      (current_user_id(),)).fetchall()
    result = []
    for row in rows:
        d = row_to_dict(row)
        d["tong_ck_chu"] = so_thanh_chu(d["tong_ck"])
        try:
            d["chung_tu"] = json.loads(d["chung_tu_json"]) if d["chung_tu_json"] else []
        except (json.JSONDecodeError, TypeError):
            d["chung_tu"] = []
        result.append(d)
    return jsonify({"ok": True, "data": result})


@app.route("/api/delete/<int:phieu_id>", methods=["DELETE"])
def api_delete(phieu_id):
    """Delete a phieu."""
    db = get_db()
    row = db.execute("SELECT id FROM phieu WHERE id = ? AND user_id = ?",
                     (phieu_id, current_user_id())).fetchone()
    if not row:
        return jsonify({"ok": False, "error": "Kh\u00f4ng t\u00ecm th\u1ea5y phi\u1ebfu"}), 404

    db.execute("DELETE FROM phieu WHERE id = ? AND user_id = ?", (phieu_id, current_user_id()))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/parse-sap", methods=["POST"])
def api_parse_sap():
    """Parse SAP paste text and return structured chung_tu + calculated totals."""
    data = request.get_json(force=True)
    raw = data.get("sap_text", "") or data.get("raw_text", "")
    records = parse_sap_paste(raw)
    tong = calc_tong_ck(records)
    return jsonify({
        "ok": True,
        "chung_tu": records,
        "tong_ck": tong,
        "tong_ck_chu": so_thanh_chu(tong),
    })


@app.route("/api/calc-ngay-tt", methods=["POST"])
def api_calc_ngay_tt():
    """Calculate payment date based on current time."""
    ngay_tt = calc_ngay_tt()
    return jsonify({"ok": True, "ngay_tt": ngay_tt})


@app.route("/api/qr-url", methods=["POST"])
def api_qr_url():
    """Generate VietQR URL from bank info."""
    data = request.get_json(force=True)
    url = build_qr_url(
        data.get("ngan_hang", ""),
        data.get("so_tk", ""),
        amount=data.get("amount"),
        memo=data.get("memo"),
    )
    return jsonify({"ok": True, "qr_url": url})


@app.route("/api/so-thanh-chu", methods=["POST"])
def api_so_thanh_chu():
    """Convert number to Vietnamese words."""
    data = request.get_json(force=True)
    n = data.get("number", 0)
    return jsonify({"ok": True, "text": so_thanh_chu(n)})


@app.route("/api/bank-bins")
def api_bank_bins():
    """Return the bank BIN mapping."""
    return jsonify({"ok": True, "banks": BANK_BINS})


@app.route("/api/lookup-account", methods=["POST"])
def api_lookup_account():
    """Tra cứu tên chủ tài khoản qua MBBank interbank lookup."""
    data = request.get_json(force=True)
    bin_code = data.get("bin", "")
    account = data.get("accountNumber", "").strip()
    if not bin_code or not account:
        return jsonify({"ok": False, "error": "Thiếu BIN hoặc số tài khoản"})

    settings = get_settings()
    mb_user = settings.get("mb_username", "")
    mb_pass = settings.get("mb_password", "")
    mb_account = settings.get("mb_account", "")
    if not mb_user or not mb_pass:
        return jsonify({"ok": False, "error": "Chưa cấu hình MBBank. Vào Cài đặt để nhập."})

    # Find MBBank bankId from BIN (loaded from Excel)
    mb_bank_code = MB_BANK_MAP.get(str(bin_code), "")

    try:
        import mbbank
        mb = mbbank.MBBank(username=mb_user, password=mb_pass)

        # Auto-detect debitAccount from MBBank balance API
        debit = mb_account
        if not debit:
            try:
                bal = mb.getBalance()
                if bal.acct_list:
                    debit = bal.acct_list[0].acctNo
            except Exception:
                debit = mb_user

        if not mb_bank_code:
            return jsonify({"ok": False, "error": "Không tìm thấy mã ngân hàng MBBank cho BIN " + str(bin_code)})

        result = mb.getAccountName(
            accountNo=account,
            bankCode=mb_bank_code,
            debitAccount=debit,
        )
        if result.benName:
            return jsonify({"ok": True, "accountName": result.benName})
        else:
            return jsonify({"ok": False, "error": "Không tìm thấy tài khoản"})
    except Exception as e:
        err_msg = str(e)
        if "GW200" in err_msg:
            return jsonify({"ok": False, "error": "Số tài khoản không hợp lệ"})
        return jsonify({"ok": False, "error": f"Lỗi: {err_msg}"})


@app.route("/api/ocr-bk", methods=["POST"])
def api_ocr_bk():
    """OCR ảnh bảng kê SAP → trích xuất Số BK, Mã KH, CCCD, SĐT."""
    import asyncio
    from chrome_lens_py import LensAPI

    if "image" not in request.files:
        return jsonify({"ok": False, "error": "Không có ảnh"})

    img_bytes = request.files["image"].read()
    if not img_bytes:
        return jsonify({"ok": False, "error": "Ảnh trống"})

    # Save temp file
    import tempfile
    tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
    tmp.write(img_bytes)
    tmp.close()

    try:
        import asyncio
        from chrome_lens_py import LensAPI
        api = LensAPI()

        async def do_ocr():
            return await api.process_image(tmp.name, ocr_language="vi", output_format="full_text")

        result = asyncio.run(do_ocr())

        # Extract text from result
        if isinstance(result, str):
            text = result
        elif isinstance(result, dict):
            if "full_text" in result:
                text = result["full_text"]
            elif "word_data" in result:
                # Build text from word_data preserving separators
                parts = []
                for w in result["word_data"]:
                    parts.append(w.get("word", "") + w.get("separator", " "))
                text = "".join(parts)
            else:
                text = str(result)
        else:
            text = str(result)
    except Exception as e:
        return jsonify({"ok": False, "error": f"OCR lỗi: {str(e)}"})
    finally:
        os.unlink(tmp.name)

    if not text:
        return jsonify({"ok": False, "error": "Không đọc được text từ ảnh"})

    so_bk = ""
    ma_kh = ""
    cccd = ""
    sdt = ""
    ten_kh = ""

    # Lấy MST để loại trừ khi tìm SĐT
    mst_match = re.search(r"MST[:\s]*(\d{10,14})", text)
    mst = mst_match.group(1) if mst_match else ""

    # Pattern 1: Số BK (44xxxxxxxx) + Mã KH — dạng "4403701048_100256722" hoặc tách space
    m = re.search(r"\b(44\d{8})[_\s]+(\d{6,12})\b", text)
    if m:
        so_bk = m.group(1)
        ma_kh = m.group(2)
    else:
        m2 = re.search(r"\b(44\d{8})\b", text)
        if m2:
            so_bk = m2.group(1)

    # Pattern 2: CCCD — 12 số liền bắt đầu bằng 0
    cccd_matches = re.findall(r"\b(0\d{11})\b", text)
    if cccd_matches:
        cccd = cccd_matches[0]
    else:
        # CCCD bị tách 2 phần, có thể đảo thứ tự: "459 046194003" hoặc "046194003 459"
        # Tìm số 0xxxxx (9 số) gần số ngắn (3 số) → ghép = 12
        long_parts = re.findall(r"\b(0\d{8})\b", text)
        short_parts = re.findall(r"\b(\d{3})\b", text)
        for lp in long_parts:
            for sp in short_parts:
                combined = lp + sp
                if len(combined) == 12:
                    cccd = combined
                    break
            if cccd:
                break

    # Pattern 3: SĐT — 10 số bắt đầu 07/08/09/03/05, loại MST và CCCD
    sdt_matches = re.findall(r"\b(0[3-9]\d{8})\b", text)
    for s in sdt_matches:
        if s != mst[:10] and s != cccd[:10] and not s.startswith("030"):
            sdt = s
            break

    # Pattern 4: Tên KH — giữa "1" và các từ địa chỉ/số
    # OCR đọc ngang bảng → tên có thể bị đảo: "THƯ NGUYỄN MINH PHƯỜNG"
    # Tên thật: NGUYỄN MINH THƯ
    addr_words = {"PHƯỜNG", "PHONG", "TP", "THÀNH", "SỐ", "ĐƯỜNG",
                  "QUẬN", "HUYỆN", "XÃ", "THÔN", "BÀ", "TRIỆU", "HÓA",
                  "HUẾ", "HUỂ", "THUẬN", "ĐIỆN", "TỔ", "DÂN", "PHỐ",
                  "CHIẾC", "KHU", "TDP", "LÀNG", "NGÕ", "HẺM", "ĐÔNG",
                  "TÂY", "NAM", "BẮC", "KIỆT", "ĐƯỜNG"}
    ho_vn = {"NGUYỄN", "TRẦN", "LÊ", "PHẠM", "HUỲNH", "HOÀNG", "PHAN",
             "VŨ", "VÕ", "ĐẶNG", "BÙI", "ĐỖ", "HỒ", "NGỌC", "DƯƠNG",
             "LÝ", "CHÂU", "ĐOÀN", "TRỊNH", "ĐINH", "LƯU", "VƯƠNG",
             "LƯƠNG", "THẠCH", "TÔN", "MAI", "TỐNG", "LƯƠNG", "HÀ"}

    name_m = re.search(r"\b1\s+([A-ZÀ-Ỹ][A-ZÀ-Ỹa-zà-ỹ\s]+?)(?:\s+\d|,|\s+\d+/)", text)
    if name_m:
        raw_name = name_m.group(1).strip()
        words = raw_name.split()
        # Lọc bỏ từ địa chỉ
        name_words = []
        for w in words:
            if w.upper() in addr_words:
                break
            name_words.append(w)

        # Reorder: nếu từ đầu tiên KHÔNG phải họ VN → có thể bị đảo
        # VD: "THƯ NGUYỄN MINH" → tìm họ VN trong danh sách → đưa lên đầu
        if name_words and name_words[0].upper() not in ho_vn:
            for i, w in enumerate(name_words):
                if w.upper() in ho_vn:
                    # Đưa từ họ lên đầu, từ trước đó xuống cuối (tên)
                    ten_rieng = name_words[:i]
                    ho_dem = name_words[i:]
                    name_words = ho_dem + ten_rieng
                    break

        ten_kh = " ".join(name_words) if name_words else ""

    # Log raw text to file for debugging
    log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ocr_debug.txt")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(text)

    return jsonify({
        "ok": True,
        "so_bk": so_bk,
        "ma_kh": ma_kh,
        "cccd": cccd,
        "sdt": sdt,
        "ten_kh": ten_kh,
        "raw_text": text[:800],
    })


@app.route("/api/template-tt/<int:phieu_id>")
def api_template_tt(phieu_id):
    """Generate filled eOffice QT82 template Excel for a phieu."""
    from openpyxl import load_workbook
    from flask import send_file
    import tempfile

    db = get_db()
    row = db.execute("SELECT * FROM phieu WHERE id = ? AND user_id = ?",
                     (phieu_id, current_user_id())).fetchone()
    if not row:
        return "Không tìm thấy phiếu", 404

    d = row_to_dict(row)
    try:
        chung_tu = json.loads(d["chung_tu_json"]) if d["chung_tu_json"] else []
    except (json.JSONDecodeError, TypeError):
        chung_tu = []

    template_path = os.path.join(app.static_folder, "template_tt.xlsx")
    wb = load_workbook(template_path)
    ws = wb["Sheet1"]

    cccd = re.sub(r"\D", "", d.get("cccd", ""))

    for i, ct in enumerate(chung_tu):
        r = 5 + i  # Row 5 onwards
        ws.cell(row=r, column=1, value=i + 1)

        loai = ct.get("loai", "")
        ws.cell(row=r, column=2, value=loai)

        gia_tri = ct.get("gia_tri", 0)
        if loai == "Hóa đơn":
            ws.cell(row=r, column=3, value=-abs(int(gia_tri)))
        else:
            ws.cell(row=r, column=3, value=abs(int(gia_tri)))

        # Số CT dạng text (giữ số 0 đầu)
        ws.cell(row=r, column=4, value=str(ct.get("so_ct", "")))
        # CCCD dạng text (giữ số 0 đầu)
        ws.cell(row=r, column=5, value=str(cccd))
        ws.cell(row=r, column=7, value=True)

    # Fill remaining STT rows
    for i in range(len(chung_tu), 30):
        r = 5 + i
        ws.cell(row=r, column=1, value=i + 1)
        ws.cell(row=r, column=7, value=True)

    tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    wb.save(tmp.name)
    tmp.close()

    so_bk = d.get("so_bk", "phieu")
    filename = f"Template - TT {so_bk}.xlsx"
    return send_file(tmp.name, as_attachment=True, download_name=filename,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.route("/api/banks")
def api_banks():
    """Return full bank list for dropdown search."""
    return jsonify({"ok": True, "data": BANK_LIST})


@app.route("/api/tvv")
def api_tvv():
    """Return TVV list from database."""
    db = get_db()
    rows = db.execute("SELECT id, ma, ten FROM tvv ORDER BY ma").fetchall()
    data = [{"id": r["id"], "ma": r["ma"], "ten": r["ten"]} for r in rows]
    return jsonify({"ok": True, "data": data})


@app.route("/api/tvv", methods=["POST"])
def api_tvv_add():
    """Add a new TVV (admin only)."""
    if not is_admin():
        return jsonify({"ok": False, "error": "Chỉ admin mới được thao tác"}), 403
    data = request.get_json(force=True)
    ma = data.get("ma", "").strip()
    ten = data.get("ten", "").strip().upper()
    if not ma or not ten:
        return jsonify({"ok": False, "error": "Thiếu mã hoặc tên TVV"})
    db = get_db()
    db.execute("INSERT INTO tvv (ma, ten) VALUES (?, ?)", (ma, ten))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/tvv/<int:tvv_id>", methods=["DELETE"])
def api_tvv_delete(tvv_id):
    """Delete a TVV (admin only)."""
    if not is_admin():
        return jsonify({"ok": False, "error": "Chỉ admin mới được thao tác"}), 403
    db = get_db()
    db.execute("DELETE FROM tvv WHERE id = ?", (tvv_id,))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/lydo-huy")
def api_lydo_huy():
    """Return lý do hủy list from database."""
    db = get_db()
    rows = db.execute("SELECT id, noi_dung FROM lydo_huy ORDER BY id").fetchall()
    data = [{"id": r["id"], "noi_dung": r["noi_dung"]} for r in rows]
    return jsonify({"ok": True, "data": data})


@app.route("/api/lydo-huy", methods=["POST"])
def api_lydo_huy_add():
    """Add a new lý do hủy (admin only)."""
    if not is_admin():
        return jsonify({"ok": False, "error": "Chỉ admin mới được thao tác"}), 403
    data = request.get_json(force=True)
    noi_dung = data.get("noi_dung", "").strip()
    if not noi_dung:
        return jsonify({"ok": False, "error": "Thiếu nội dung lý do"})
    db = get_db()
    db.execute("INSERT INTO lydo_huy (noi_dung) VALUES (?)", (noi_dung,))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/lydo-huy/<int:ld_id>", methods=["DELETE"])
def api_lydo_huy_delete(ld_id):
    """Delete a lý do hủy (admin only)."""
    if not is_admin():
        return jsonify({"ok": False, "error": "Chỉ admin mới được thao tác"}), 403
    db = get_db()
    db.execute("DELETE FROM lydo_huy WHERE id = ?", (ld_id,))
    db.commit()
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# Always init DB when module loads
init_db()

if __name__ == "__main__":
    # Open browser after a short delay (the server needs a moment to start)
    import threading
    threading.Timer(1.5, lambda: webbrowser.open(f"http://localhost:{PORT}")).start()
    app.run(host="127.0.0.1", port=PORT, debug=True, use_reloader=False)
