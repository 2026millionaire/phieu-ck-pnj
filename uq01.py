# -*- coding: utf-8 -*-
"""Data contract and rendering helpers for UQ-01 goods authorization forms."""

from copy import deepcopy
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
import re


TEMPLATE_CODE = "UQ-01"
FORM_CODE = "PNJ-QYD-PLPLNL-NS-UQ-F1"
PLANT_DIRECTORY = {
    "1304": {
        "name": "PNJ 271 Trần Hưng Đạo (Huế)",
        "issue_place": "Huế",
    },
    "1305": {
        "name": "PNJ NEXT 27 Hà Nội (Huế)",
        "issue_place": "Huế",
    },
    "1398": {
        "name": "PNJ Vincom Huế",
        "issue_place": "Huế",
    },
    "1394": {
        "name": "PNJ 29 Mai Thúc Loan (Huế)",
        "issue_place": "Huế",
    },
    "1465": {
        "name": "PNJ 186 Hùng Vương (Huế)",
        "issue_place": "Huế",
    },
    "1570": {
        "name": "PNJ 1066 Nguyễn Tất Thành (Huế)",
        "issue_place": "Huế",
    },
    "1613": {
        "name": "PNJ Aeon Huế",
        "issue_place": "Huế",
    },
}
DEFAULT_PLANT = "1305"
DEFAULT_DESTINATION_CODE = "1305"
DEFAULT_DESTINATION_NAME = PLANT_DIRECTORY[DEFAULT_DESTINATION_CODE]["name"]
DEFAULT_AUTHORIZATION_ACTION = "ký/thực hiện nhận gói/hộp niêm phong và giao hàng"
DEFAULT_RESPONSIBILITY_CLAUSE = "Người ủy quyền hoàn toàn chịu trách nhiệm về việc ủy quyền này."
BANGKOK_TZ = timezone(timedelta(hours=7), name="Asia/Bangkok")
DOCUMENT_NO_RE = re.compile(
    r"^(?P<plant>\d{4})_(?P<date>\d{4}-\d{2}-\d{2})_(?P<hour>\d{2}):(?P<minute>\d{2})$"
)
UQ01_PROFILE_SEEDS = (
    {
        "seed_code": "1305-store-manager",
        "salutation": "",
        "full_name": "HỒ THỊ HÀ MY",
        "job_title": "Cửa Hàng Trưởng",
        "employee_code": "",
        "unit_code": "1305",
        "unit_name": DEFAULT_DESTINATION_NAME,
        "id_type": "CCCD",
        "can_authorize": 1,
        "can_receive": 0,
    },
    {
        "seed_code": "1305-store-accountant",
        "salutation": "",
        "full_name": "CHÂU ĐĂNG KHOA",
        "job_title": "NV Kế Toán CH",
        "employee_code": "",
        "unit_code": "1305",
        "unit_name": DEFAULT_DESTINATION_NAME,
        "id_type": "CCCD",
        "can_authorize": 1,
        "can_receive": 0,
    },
    {
        "seed_code": "1305-security-ha-van-rin",
        "salutation": "",
        "full_name": "HÀ VĂN RIN",
        "job_title": "Nhân Viên An Ninh",
        "employee_code": "",
        "unit_code": "1305",
        "unit_name": DEFAULT_DESTINATION_NAME,
        "id_type": "CCCD",
        "can_authorize": 0,
        "can_receive": 1,
    },
    {
        "seed_code": "1305-security-tran-xuan-hai",
        "salutation": "",
        "full_name": "TRẦN XUÂN HẢI",
        "job_title": "Nhân Viên An Ninh",
        "employee_code": "",
        "unit_code": "1305",
        "unit_name": DEFAULT_DESTINATION_NAME,
        "id_type": "CCCD",
        "can_authorize": 0,
        "can_receive": 1,
    },
    {
        "seed_code": "1305-security-tran-quang-trinh",
        "salutation": "",
        "full_name": "TRẦN QUANG TRINH",
        "job_title": "Nhân Viên An Ninh",
        "employee_code": "",
        "unit_code": "1305",
        "unit_name": DEFAULT_DESTINATION_NAME,
        "id_type": "CCCD",
        "can_authorize": 0,
        "can_receive": 1,
    },
)


def _mapping(value):
    return value if isinstance(value, dict) else {}


def _text(value, max_length=500):
    text = str(value or "").replace("\x00", "").strip()
    return text[:max_length]


def _first_text(source, keys, max_length=500):
    for key in keys:
        value = _text(source.get(key), max_length)
        if value:
            return value
    return ""


def _date_iso(value):
    text = _text(value, 10)
    if not text:
        return ""
    for pattern in ("%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(text, pattern).date().isoformat()
        except ValueError:
            continue
    return text


def _today_iso(today=None):
    if today is None:
        return date.today().isoformat()
    if isinstance(today, datetime):
        return today.date().isoformat()
    if isinstance(today, date):
        return today.isoformat()
    normalized = _date_iso(today)
    return normalized or date.today().isoformat()


def _bangkok_now(now=None):
    if isinstance(now, datetime):
        if now.tzinfo is None:
            return now.replace(tzinfo=BANGKOK_TZ)
        return now.astimezone(BANGKOK_TZ)
    if isinstance(now, date):
        return datetime.combine(now, time.min, tzinfo=BANGKOK_TZ)
    return datetime.now(BANGKOK_TZ)


def plant_context(plant=None):
    plant_code = _text(plant, 20)
    if plant_code not in PLANT_DIRECTORY:
        plant_code = DEFAULT_PLANT
    context = PLANT_DIRECTORY[plant_code]
    return {
        "plant": plant_code,
        "issue_place": context["issue_place"],
        "unit_name": context["name"],
    }


def uq01_plant_directory():
    return [
        {
            "code": code,
            "name": details["name"],
            "issue_place": details["issue_place"],
        }
        for code, details in PLANT_DIRECTORY.items()
    ]


def build_uq01_document_identity(plant=None, now=None):
    context = plant_context(plant)
    created_at = _bangkok_now(now).replace(second=0, microsecond=0)
    return {
        **context,
        "document_no": (
            f"{context['plant']}_{created_at.strftime('%Y-%m-%d_%H:%M')}"
        ),
        "issue_date": created_at.date().isoformat(),
        "created_at": created_at.isoformat(timespec="minutes"),
        "copies": 2,
    }


def normalize_uq01_document_identity(raw, now=None):
    raw = _mapping(raw)
    context = plant_context(raw.get("plant"))
    document_no = _text(raw.get("document_no"), 80)
    match = DOCUMENT_NO_RE.fullmatch(document_no)
    created_at = None
    if match and match.group("plant") == context["plant"]:
        try:
            created_at = datetime.strptime(
                f"{match.group('date')} {match.group('hour')}:{match.group('minute')}",
                "%Y-%m-%d %H:%M",
            ).replace(tzinfo=BANGKOK_TZ)
        except ValueError:
            created_at = None
    if created_at is None:
        return build_uq01_document_identity(context["plant"], now=now)
    return {
        **context,
        "document_no": document_no,
        "issue_date": created_at.date().isoformat(),
        "created_at": created_at.isoformat(timespec="minutes"),
        "copies": 2,
    }


def _bool(value):
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _positive_int(value, default=1, maximum=99):
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError):
        return default
    return max(1, min(number, maximum))


def _party_defaults():
    return {
        "salutation": "",
        "full_name": "",
        "job_title": "",
        "employee_code": "",
        "unit_code": "",
        "unit_name": "",
    }


def default_uq01_payload(today=None, plant=None):
    identity = build_uq01_document_identity(plant=plant, now=today)
    issue_date = identity["issue_date"]
    return {
        "template_code": TEMPLATE_CODE,
        "form_code": FORM_CODE,
        "plant": identity["plant"],
        "document_no": identity["document_no"],
        "created_at": identity["created_at"],
        "issue_place": identity["issue_place"],
        "issue_date": issue_date,
        "copies": 2,
        "authorizer": _party_defaults(),
        "authorized_person": {
            **_party_defaults(),
            "id_type": "CCCD",
            "id_number": "",
            "issue_date": "",
            "issue_place": "",
        },
        "authorization": {
            "authorization_action": DEFAULT_AUTHORIZATION_ACTION,
            "pickup_type": "cửa hàng",
            "pickup": {"code": "", "name": "", "address": ""},
            "destination": {
                "code": DEFAULT_DESTINATION_CODE,
                "name": DEFAULT_DESTINATION_NAME,
                "address": "",
            },
            "package_count": 1,
            "sealed_package": True,
            "valid_from": issue_date,
            "valid_to": "",
            "responsibility_clause": DEFAULT_RESPONSIBILITY_CLAUSE,
            "additional_notes": "",
            "content_override": "",
            "content_customized": False,
        },
        "sto": {
            "source_mode": "manual",
            "reference_type": "STO",
            "reference_number": "",
            "approved_date": "",
            "items": [],
        },
    }


def _normalize_party(raw):
    raw = _mapping(raw)
    salutation = _text(raw.get("salutation"), 10)
    if salutation not in {"Ông", "Bà"}:
        salutation = ""
    return {
        "salutation": salutation,
        "full_name": _text(raw.get("full_name"), 120),
        "job_title": _text(raw.get("job_title"), 120),
        "employee_code": _text(raw.get("employee_code"), 40),
        "unit_code": _text(raw.get("unit_code"), 40),
        "unit_name": _text(raw.get("unit_name"), 180),
    }


def initialize_uq01_schema(conn):
    """Create the user/plant-scoped personnel store without inserting PII."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS uq01_personnel_profiles (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id         INTEGER NOT NULL DEFAULT 1,
            plant           TEXT NOT NULL DEFAULT '1305',
            seed_code       TEXT NOT NULL DEFAULT '',
            salutation      TEXT NOT NULL,
            full_name       TEXT NOT NULL,
            job_title       TEXT NOT NULL,
            employee_code   TEXT NOT NULL DEFAULT '',
            unit_code       TEXT NOT NULL,
            unit_name       TEXT NOT NULL,
            id_type         TEXT NOT NULL DEFAULT 'CCCD',
            id_number       TEXT NOT NULL DEFAULT '',
            id_issue_date   TEXT NOT NULL DEFAULT '',
            id_issue_place  TEXT NOT NULL DEFAULT '',
            can_authorize   INTEGER NOT NULL DEFAULT 0,
            can_receive     INTEGER NOT NULL DEFAULT 0,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_uq01_profiles_scope
        ON uq01_personnel_profiles (user_id, plant, full_name COLLATE NOCASE)
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_uq01_profiles_seed
        ON uq01_personnel_profiles (user_id, plant, seed_code)
        WHERE seed_code <> ''
        """
    )


def ensure_uq01_profile_seeds(conn, user_id, plant=None, now=None):
    """Seed only low-sensitivity names/roles; identity fields remain blank."""
    context = plant_context(plant)
    if context["plant"] != DEFAULT_PLANT:
        return
    timestamp = _bangkok_now(now).isoformat(timespec="seconds")
    # Repair the pre-release local seed once, without overriding a profile
    # that the user has already edited through the management UI.
    conn.execute(
        """
        UPDATE uq01_personnel_profiles
        SET can_receive = 0, updated_at = ?
        WHERE user_id = ? AND plant = ?
              AND seed_code = '1305-store-accountant'
              AND can_receive = 1
              AND created_at = updated_at
        """,
        (timestamp, int(user_id), context["plant"]),
    )
    for seed in UQ01_PROFILE_SEEDS:
        conn.execute(
            """
            INSERT OR IGNORE INTO uq01_personnel_profiles (
                user_id, plant, seed_code, salutation, full_name, job_title,
                employee_code, unit_code, unit_name, id_type, id_number,
                id_issue_date, id_issue_place, can_authorize, can_receive,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', '', ?, ?, ?, ?)
            """,
            (
                int(user_id),
                context["plant"],
                seed["seed_code"],
                seed["salutation"],
                seed["full_name"],
                seed["job_title"],
                seed["employee_code"],
                seed["unit_code"],
                seed["unit_name"],
                seed["id_type"],
                seed["can_authorize"],
                seed["can_receive"],
                timestamp,
                timestamp,
            ),
        )


def normalize_uq01_profile(raw):
    raw = _mapping(raw)
    salutation = _text(raw.get("salutation"), 10)
    if salutation not in {"Ông", "Bà"}:
        salutation = ""

    full_name = re.sub(r"\s+", " ", _text(raw.get("full_name"), 120)).upper()
    if not full_name:
        raise ValueError("Vui lòng nhập họ và tên.")

    job_title = re.sub(r"\s+", " ", _text(raw.get("job_title"), 120))
    if not job_title:
        raise ValueError("Vui lòng nhập chức vụ.")

    unit_code = re.sub(r"\s+", "", _text(raw.get("unit_code"), 40)).upper()
    if not unit_code:
        raise ValueError("Vui lòng nhập mã đơn vị.")
    unit_name = re.sub(r"\s+", " ", _text(raw.get("unit_name"), 180))
    if not unit_name:
        raise ValueError("Vui lòng nhập tên đơn vị.")

    can_authorize = _bool(raw.get("can_authorize"))
    can_receive = _bool(raw.get("can_receive"))
    if not can_authorize and not can_receive:
        raise ValueError("Hồ sơ phải có ít nhất một vai trò sử dụng.")

    id_type_raw = _text(raw.get("id_type"), 30).upper()
    id_type = {"CCCD": "CCCD", "CMND": "CMND", "KHÁC": "Khác"}.get(
        id_type_raw, "CCCD"
    )
    id_number = re.sub(r"\s+", "", _text(raw.get("id_number"), 80))
    issue_date_raw = _text(raw.get("id_issue_date") or raw.get("issue_date"), 20)
    issue_date = _date_iso(issue_date_raw)
    if issue_date_raw:
        try:
            datetime.strptime(issue_date, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("Ngày cấp giấy tờ không hợp lệ.") from exc

    profile = {
        "salutation": salutation,
        "full_name": full_name,
        "job_title": job_title,
        "employee_code": re.sub(
            r"\s+", "", _text(raw.get("employee_code"), 40)
        ).upper(),
        "unit_code": unit_code,
        "unit_name": unit_name,
        "id_type": id_type,
        "id_number": id_number,
        "id_issue_date": issue_date,
        "id_issue_place": re.sub(
            r"\s+", " ", _text(raw.get("id_issue_place") or raw.get("issue_place"), 200)
        ),
        "can_authorize": can_authorize,
        "can_receive": can_receive,
    }
    return profile, uq01_profile_warnings(profile)


def uq01_profile_warnings(profile):
    profile = _mapping(profile)
    warnings = []
    id_number = re.sub(r"\s+", "", _text(profile.get("id_number"), 80))
    id_type = _text(profile.get("id_type"), 30).upper()
    if _bool(profile.get("can_receive")) and not id_number:
        warnings.append("Hồ sơ người được ủy quyền chưa có số giấy tờ.")
    elif id_number and id_type == "CCCD" and not re.fullmatch(r"\d{12}", id_number):
        warnings.append("CCCD thường phải gồm đúng 12 chữ số.")
    elif id_number and id_type == "CMND" and not re.fullmatch(
        r"(?:\d{9}|\d{12})", id_number
    ):
        warnings.append("CMND cần kiểm tra lại; dạng lịch sử thường gồm 9 hoặc 12 chữ số.")
    return warnings


def uq01_profile_from_row(row):
    profile = dict(row)
    profile["can_authorize"] = bool(profile.get("can_authorize"))
    profile["can_receive"] = bool(profile.get("can_receive"))
    profile["warnings"] = uq01_profile_warnings(profile)
    return profile


def _normalize_location(raw):
    raw = _mapping(raw)
    code = _text(raw.get("code"), 40).upper()
    directory_entry = PLANT_DIRECTORY.get(code)
    return {
        "code": code,
        "name": (
            directory_entry["name"]
            if directory_entry
            else _text(raw.get("name"), 180)
        ),
        "address": _text(raw.get("address"), 300),
    }


def normalize_sto_data(raw):
    """Normalize a manual/ERP-shaped STO result without performing an ERP call."""
    raw = _mapping(raw)
    source_mode = _text(raw.get("source_mode"), 20).lower()
    if source_mode not in {"manual", "erp"}:
        source_mode = "manual"

    items = []
    raw_items = raw.get("items")
    if isinstance(raw_items, list):
        for item in raw_items[:40]:
            item = _mapping(item)
            items.append(
                {
                    "material_code": _first_text(
                        item, ("material_code", "material", "matnr"), 80
                    ),
                    "batch": _first_text(item, ("batch", "charg"), 80),
                    "description": _first_text(
                        item,
                        (
                            "description",
                            "product_name",
                            "material_description",
                            "maktx",
                        ),
                        300,
                    ),
                    "quantity": _first_text(
                        item, ("quantity", "qty", "menge"), 40
                    ),
                    "unit": _first_text(item, ("unit", "uom", "meins"), 30) or "món",
                    "sale_price": _first_text(
                        item, ("sale_price", "price", "net_price"), 60
                    ),
                    "note": _first_text(item, ("note", "remark"), 300),
                }
            )

    return {
        "source_mode": source_mode,
        "reference_type": _normalize_reference_type(raw.get("reference_type")),
        "reference_number": _first_text(
            raw, ("reference_number", "sto_number", "ebeln"), 80
        ),
        "approved_date": _date_iso(
            _first_text(raw, ("approved_date", "approval_date"), 20)
        ),
        "items": items,
    }


def _normalize_reference_type(value):
    reference_type = _text(value, 30).upper()
    if reference_type == "PO":
        return "PXK"
    if reference_type in {"STO", "PXK", "KHÁC"}:
        return reference_type
    return "STO" if not reference_type else "KHÁC"


def apply_sto_data(payload, raw_sto):
    """Adapter boundary for applying future ERP data to the UQ-01 contract."""
    normalized = normalize_uq01_payload(payload)
    normalized["sto"] = normalize_sto_data(raw_sto)
    return normalized


def normalize_uq01_payload(raw, today=None):
    raw = _mapping(raw)
    identity = normalize_uq01_document_identity(raw, now=today)
    defaults = default_uq01_payload(today=today, plant=identity["plant"])
    authorization_raw = _mapping(raw.get("authorization"))
    authorized_person_raw = _mapping(raw.get("authorized_person"))

    payload = deepcopy(defaults)
    payload.update(
        {
            "template_code": TEMPLATE_CODE,
            "form_code": FORM_CODE,
            "plant": identity["plant"],
            "document_no": identity["document_no"],
            "created_at": identity["created_at"],
            "issue_place": identity["issue_place"],
            "issue_date": identity["issue_date"],
            "copies": 2,
            "authorizer": _normalize_party(raw.get("authorizer")),
            "authorized_person": {
                **_normalize_party(authorized_person_raw),
                "id_type": _text(authorized_person_raw.get("id_type"), 30).upper() or "CCCD",
                "id_number": _text(authorized_person_raw.get("id_number"), 80),
                "issue_date": _date_iso(authorized_person_raw.get("issue_date")),
                "issue_place": _text(authorized_person_raw.get("issue_place"), 200),
            },
            "sto": normalize_sto_data(raw.get("sto")),
        }
    )

    payload["authorization"] = {
        "authorization_action": (
            _text(authorization_raw.get("authorization_action"), 500)
            or DEFAULT_AUTHORIZATION_ACTION
        ),
        "pickup_type": _text(authorization_raw.get("pickup_type"), 30).lower() or "cửa hàng",
        "pickup": _normalize_location(authorization_raw.get("pickup")),
        "destination": _normalize_location(authorization_raw.get("destination")),
        "package_count": _positive_int(
            authorization_raw.get("package_count"), default=1, maximum=99
        ),
        "sealed_package": _bool(
            authorization_raw.get(
                "sealed_package", defaults["authorization"]["sealed_package"]
            )
        ),
        "valid_from": _date_iso(authorization_raw.get("valid_from")),
        "valid_to": _date_iso(authorization_raw.get("valid_to")),
        "responsibility_clause": (
            _text(authorization_raw.get("responsibility_clause"), 800)
            or DEFAULT_RESPONSIBILITY_CLAUSE
        ),
        "additional_notes": _text(authorization_raw.get("additional_notes"), 1200),
        "content_override": _text(authorization_raw.get("content_override"), 5000),
        "content_customized": _bool(authorization_raw.get("content_customized")),
    }

    destination = payload["authorization"]["destination"]
    if not any(destination.values()):
        destination.update(
            {
                "code": DEFAULT_DESTINATION_CODE,
                "name": DEFAULT_DESTINATION_NAME,
                "address": "",
            }
        )
    return payload


def _date_value(value):
    normalized = _date_iso(value)
    try:
        return datetime.strptime(normalized, "%Y-%m-%d").date()
    except ValueError:
        return None


def format_date_short(value):
    parsed = _date_value(value)
    return parsed.strftime("%d/%m/%Y") if parsed else _text(value, 20)


def format_date_parts(value):
    parsed = _date_value(value) or date.today()
    return {
        "day": f"{parsed.day:02d}",
        "month": f"{parsed.month:02d}",
        "year": str(parsed.year),
    }


def _location_text(location):
    location = _mapping(location)
    lead = " - ".join(
        part for part in (_text(location.get("code"), 40), _text(location.get("name"), 180)) if part
    )
    address = _text(location.get("address"), 300)
    if lead and address:
        return f"{lead}, {address}"
    return lead or address


def build_uq01_content(payload):
    payload = normalize_uq01_payload(payload)
    authorization = payload["authorization"]
    sto = payload["sto"]
    authorizer = _text(payload["authorizer"].get("full_name"), 120) or "[chưa chọn]"
    authorized_person = (
        _text(payload["authorized_person"].get("full_name"), 120) or "[chưa chọn]"
    )
    action = authorization["authorization_action"].rstrip(".")
    package_count = authorization["package_count"]
    package_text = (
        f"{package_count} gói/hộp niêm phong chứa hàng hóa"
        if authorization["sealed_package"]
        else f"{package_count} kiện hàng hóa"
    )
    reference_text = ""
    if sto["reference_number"]:
        reference_text = f" theo {sto['reference_type']} số {sto['reference_number']}"

    pickup = _location_text(authorization["pickup"]) or "địa điểm nhận hàng"
    destination = _location_text(authorization["destination"]) or "địa điểm giao hàng"
    return (
        f"Người ủy quyền Ông/Bà {authorizer} ủy quyền cho Người được ủy quyền "
        f"Ông/Bà {authorized_person} thay mặt Người ủy quyền {action}. "
        f"Phạm vi ủy quyền gồm {package_text}{reference_text}; hàng hóa được nhận tại "
        f"{pickup} và giao về {destination}."
    )


def _decimal(value):
    text = _text(value, 60).replace(" ", "")
    if not text:
        return None
    if re.fullmatch(r"-?\d{1,3}(?:\.\d{3})+", text):
        text = text.replace(".", "")
    elif "," in text and "." not in text:
        text = text.replace(",", ".")
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _format_decimal(value):
    if value is None:
        return ""
    if value == value.to_integral():
        return f"{int(value):,}".replace(",", ".")
    return format(value.normalize(), "f").replace(".", ",")


def _item_has_data(item):
    meaningful = (
        item.get("material_code"),
        item.get("description"),
    )
    if any(_text(value) for value in meaningful):
        return True
    if _text(item.get("quantity")) not in {"", "1"}:
        return True
    return _text(item.get("unit")).lower() not in {"", "món"}


def _append_warning(warnings, message):
    if message not in warnings:
        warnings.append(message)


def validate_uq01_payload(payload):
    payload = normalize_uq01_payload(payload)
    warnings = []
    authorizer = payload["authorizer"]
    authorized_person = payload["authorized_person"]
    authorization = payload["authorization"]
    sto = payload["sto"]

    for label, party in (
        ("người ủy quyền", authorizer),
        ("người được ủy quyền", authorized_person),
    ):
        if not party["full_name"]:
            _append_warning(warnings, f"Chưa chọn hồ sơ {label}.")
            continue
        if not party["job_title"]:
            _append_warning(warnings, f"Chưa nhập chức vụ của {label}.")
        if not (party["unit_code"] or party["unit_name"]):
            _append_warning(warnings, f"Chưa nhập đơn vị của {label}.")

    id_number = authorized_person["id_number"].replace(" ", "")
    if not id_number:
        _append_warning(warnings, "Chưa nhập số giấy tờ của người được ủy quyền.")
    elif authorized_person["id_type"] == "CCCD" and not re.fullmatch(r"\d{12}", id_number):
        _append_warning(warnings, "CCCD thường phải gồm đúng 12 chữ số.")
    elif authorized_person["id_type"] == "CMND" and not re.fullmatch(r"(?:\d{9}|\d{12})", id_number):
        _append_warning(warnings, "CMND cần kiểm tra lại; dạng lịch sử thường gồm 9 hoặc 12 chữ số.")

    if not sto["reference_number"]:
        _append_warning(warnings, "Chưa nhập số STO/tham chiếu nhận hàng.")
    elif sto["reference_type"] == "STO" and not re.fullmatch(r"\d{10}", sto["reference_number"]):
        _append_warning(warnings, "Số STO quan sát thường gồm 10 chữ số; vui lòng kiểm tra lại.")

    pickup_text = _location_text(authorization["pickup"])
    destination_text = _location_text(authorization["destination"])
    if not pickup_text:
        _append_warning(warnings, "Chưa nhập nơi nhận hàng.")
    if not destination_text:
        _append_warning(warnings, "Chưa nhập nơi giao hàng.")

    pickup_code = authorization["pickup"]["code"].strip().casefold()
    destination_code = authorization["destination"]["code"].strip().casefold()
    pickup_name = authorization["pickup"]["name"].strip().casefold()
    destination_name = authorization["destination"]["name"].strip().casefold()
    if (
        (pickup_code and destination_code and pickup_code == destination_code)
        or (pickup_name and destination_name and pickup_name == destination_name)
    ):
        _append_warning(warnings, "Nơi nhận và nơi giao đang trùng nhau; vui lòng kiểm tra.")

    valid_from = _date_value(authorization["valid_from"])
    valid_to = _date_value(authorization["valid_to"])
    if not valid_from and not valid_to:
        _append_warning(warnings, "Chưa nhập ngày hiệu lực ủy quyền.")
    if valid_from and valid_to and valid_to < valid_from:
        _append_warning(warnings, "Ngày kết thúc hiệu lực không được trước ngày bắt đầu.")

    items = [item for item in sto["items"] if _item_has_data(item)]
    if not items:
        _append_warning(warnings, "Chưa có danh sách hàng hóa.")
    for index, item in enumerate(items, start=1):
        if not (item["material_code"] or item["description"]):
            _append_warning(
                warnings, f"Dòng hàng {index}: thiếu mã hoặc tên sản phẩm."
            )
        quantity = _decimal(item["quantity"])
        if quantity is None or quantity <= 0:
            _append_warning(warnings, f"Dòng hàng {index}: số lượng phải lớn hơn 0.")

    if authorization["content_customized"] and not authorization["content_override"]:
        _append_warning(warnings, "Nội dung đã đánh dấu tùy chỉnh nhưng đang để trống.")
    return warnings


def build_uq01_context(raw_payload, today=None):
    payload = normalize_uq01_payload(raw_payload, today=today)
    generated_content = build_uq01_content(payload)
    authorization = payload["authorization"]
    content = (
        authorization["content_override"]
        if authorization["content_customized"]
        else generated_content
    )

    item_rows = []
    for item in payload["sto"]["items"]:
        if not _item_has_data(item):
            continue
        quantity = _decimal(item["quantity"])
        item_rows.append(
            {
                **item,
                "display_quantity": _format_decimal(quantity) or item["quantity"],
            }
        )

    return {
        "payload": payload,
        "content": content,
        "generated_content": generated_content,
        "warnings": validate_uq01_payload(payload),
        "issue_date_parts": format_date_parts(payload["issue_date"]),
        "authorized_person_issue_date": format_date_short(
            payload["authorized_person"]["issue_date"]
        ),
        "valid_from_text": format_date_short(authorization["valid_from"]),
        "valid_to_text": format_date_short(authorization["valid_to"]),
        "approved_date_text": format_date_short(payload["sto"]["approved_date"]),
        "item_rows": item_rows,
    }
