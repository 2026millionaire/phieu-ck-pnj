# -*- coding: utf-8 -*-
"""Các mẫu ĐỀ XUẤT khởi tạo cho phiên bản MVP."""

TEMPLATE_DEFINITIONS = [
    {
        "slug": "may-in",
        "name": "Nạp mực / sửa chữa máy in",
        "group_name": "Chi phí vận hành",
        "description": "Đề xuất nạp mực, thay linh kiện hoặc sửa chữa máy in tại cửa hàng.",
        "icon": "bi-printer",
        "sort_order": 10,
        "fields": [
            {"key": "hinh_thuc", "label": "Hình thức", "type": "select", "required": True,
             "options": ["Nạp mực máy in", "Sửa chữa máy in", "Thay linh kiện máy in"]},
            {"key": "thiet_bi", "label": "Thiết bị", "type": "text", "required": True,
             "placeholder": "Máy in văn phòng / mã tài sản"},
            {"key": "nha_cung_cap", "label": "Nhà cung cấp", "type": "text", "required": False,
             "placeholder": "Tên đơn vị cung cấp"},
            {"key": "so_hoa_don", "label": "Số hóa đơn", "type": "text", "required": False,
             "placeholder": "Số hóa đơn/chứng từ"},
            {"key": "ngay_hoa_don", "label": "Ngày hóa đơn", "type": "date", "required": False},
            {"key": "amount", "label": "Số tiền đề xuất", "type": "money", "required": True,
             "placeholder": "0"},
        ],
        "title_template": "ĐỀ XUẤT {{hinh_thuc}}",
        "content_template": (
            "Cửa hàng PNJ NEXT 27 Hà Nội - Huế đề xuất thanh toán chi phí {{hinh_thuc}} "
            "cho {{thiet_bi}}, do {{nha_cung_cap}} thực hiện, theo hóa đơn/chứng từ số "
            "{{so_hoa_don}} ngày {{ngay_hoa_don}}. Giá trị đề xuất: {{amount}} đồng."
        ),
        "reason_template": (
            "Thiết bị phục vụ hoạt động thường xuyên tại cửa hàng và cần được xử lý "
            "để đảm bảo công việc không bị gián đoạn."
        ),
    },
    {
        "slug": "hoan-tien-thua",
        "name": "Hoàn trả tiền chuyển khoản thừa",
        "group_name": "Hoàn trả giao dịch",
        "description": "Hoàn lại phần tiền khách hàng đã chuyển thừa so với giá trị phải thanh toán.",
        "icon": "bi-arrow-counterclockwise",
        "sort_order": 20,
        "fields": [
            {"key": "ten_kh", "label": "Tên khách hàng", "type": "text", "required": True},
            {"key": "ma_kh", "label": "Mã khách hàng", "type": "text", "required": True,
             "placeholder": "10xxxxxxx"},
            {"key": "ngay_giao_dich", "label": "Ngày giao dịch", "type": "date", "required": True},
            {"key": "so_tien_da_chuyen", "label": "Số tiền đã chuyển", "type": "money", "required": True},
            {"key": "so_tien_phai_thu", "label": "Số tiền phải thu", "type": "money", "required": True},
            {"key": "amount", "label": "Số tiền hoàn lại", "type": "money", "required": True},
            {"key": "tai_khoan_nhan", "label": "Tài khoản nhận hoàn", "type": "text", "required": True,
             "placeholder": "Chủ TK - STK - Ngân hàng"},
        ],
        "title_template": "ĐỀ XUẤT HOÀN TRẢ TIỀN CHUYỂN KHOẢN THỪA",
        "content_template": (
            "Ngày {{ngay_giao_dich}}, khách hàng {{ten_kh}} (mã KH {{ma_kh}}) đã chuyển "
            "{{so_tien_da_chuyen}} đồng, trong khi giá trị phải thu là {{so_tien_phai_thu}} đồng. "
            "Cửa hàng đề xuất hoàn lại số tiền chênh lệch {{amount}} đồng vào tài khoản "
            "{{tai_khoan_nhan}}."
        ),
        "reason_template": "Khách hàng chuyển khoản thừa so với giá trị giao dịch thực tế.",
    },
    {
        "slug": "hoan-coc",
        "name": "Hoàn cọc khách hàng",
        "group_name": "Hoàn trả giao dịch",
        "description": "Đề xuất hoàn tiền cọc khi giao dịch không tiếp tục hoặc cần xử lý theo chính sách.",
        "icon": "bi-wallet2",
        "sort_order": 30,
        "fields": [
            {"key": "ten_kh", "label": "Tên khách hàng", "type": "text", "required": True},
            {"key": "ma_kh", "label": "Mã khách hàng", "type": "text", "required": True},
            {"key": "so_bien_nhan", "label": "Số biên nhận cọc", "type": "text", "required": True},
            {"key": "ngay_dat_coc", "label": "Ngày đặt cọc", "type": "date", "required": True},
            {"key": "amount", "label": "Số tiền hoàn cọc", "type": "money", "required": True},
            {"key": "tai_khoan_nhan", "label": "Tài khoản nhận hoàn", "type": "text", "required": True,
             "placeholder": "Chủ TK - STK - Ngân hàng"},
        ],
        "title_template": "ĐỀ XUẤT HOÀN CỌC KHÁCH HÀNG",
        "content_template": (
            "Cửa hàng đề xuất hoàn tiền cọc cho khách hàng {{ten_kh}} (mã KH {{ma_kh}}), "
            "theo biên nhận cọc số {{so_bien_nhan}} ngày {{ngay_dat_coc}}, với số tiền "
            "{{amount}} đồng. Tài khoản nhận hoàn: {{tai_khoan_nhan}}."
        ),
        "reason_template": "Giao dịch đặt cọc cần hoàn lại theo hồ sơ và chính sách đã được kiểm tra.",
    },
    {
        "slug": "chenh-lech-hoa-don",
        "name": "Hoàn chênh lệch hóa đơn / bảng kê",
        "group_name": "Hoàn trả giao dịch",
        "description": "Hoàn phần chênh lệch phát sinh giữa hóa đơn, bảng kê và số tiền khách đã thanh toán.",
        "icon": "bi-receipt",
        "sort_order": 40,
        "fields": [
            {"key": "ten_kh", "label": "Tên khách hàng", "type": "text", "required": True},
            {"key": "ma_kh", "label": "Mã khách hàng", "type": "text", "required": True},
            {"key": "so_hoa_don", "label": "Số hóa đơn", "type": "text", "required": True},
            {"key": "so_bang_ke", "label": "Số bảng kê", "type": "text", "required": False},
            {"key": "dien_giai_chenh_lech", "label": "Diễn giải chênh lệch", "type": "textarea",
             "required": True, "placeholder": "Nêu các khoản trước/sau và nguyên nhân phát sinh"},
            {"key": "amount", "label": "Số tiền hoàn", "type": "money", "required": True},
        ],
        "title_template": "ĐỀ XUẤT HOÀN TIỀN CHÊNH LỆCH HÓA ĐƠN/BẢNG KÊ",
        "content_template": (
            "Cửa hàng đề xuất hoàn tiền cho khách hàng {{ten_kh}} (mã KH {{ma_kh}}), "
            "liên quan hóa đơn số {{so_hoa_don}} và bảng kê số {{so_bang_ke}}. "
            "{{dien_giai_chenh_lech}} Số tiền đề xuất hoàn: {{amount}} đồng."
        ),
        "reason_template": "Phát sinh chênh lệch sau khi đối chiếu hóa đơn, bảng kê và khoản khách hàng đã thanh toán.",
    },
    {
        "slug": "trich-luc-hoa-don",
        "name": "Trích lục hóa đơn",
        "group_name": "Hóa đơn",
        "description": "Đề xuất hỗ trợ trích lục hóa đơn khi khách hàng không còn chứng từ gốc.",
        "icon": "bi-file-earmark-search",
        "sort_order": 50,
        "fields": [
            {"key": "ten_kh", "label": "Tên khách hàng", "type": "text", "required": True},
            {"key": "ma_kh", "label": "Mã khách hàng", "type": "text", "required": True},
            {"key": "so_hoa_don", "label": "Số hóa đơn", "type": "text", "required": True},
            {"key": "ngay_hoa_don", "label": "Ngày hóa đơn", "type": "date", "required": False},
            {"key": "ma_san_pham", "label": "Mã sản phẩm", "type": "text", "required": False},
            {"key": "muc_dich", "label": "Mục đích trích lục", "type": "textarea", "required": True},
        ],
        "title_template": "ĐỀ XUẤT TRÍCH LỤC HÓA ĐƠN",
        "content_template": (
            "Cửa hàng kính đề xuất hỗ trợ trích lục hóa đơn số {{so_hoa_don}} ngày "
            "{{ngay_hoa_don}} của khách hàng {{ten_kh}} (mã KH {{ma_kh}}), liên quan "
            "sản phẩm {{ma_san_pham}}. Mục đích: {{muc_dich}}."
        ),
        "reason_template": "Khách hàng không còn hóa đơn gốc và cần bản trích lục để hoàn thiện hồ sơ.",
    },
    {
        "slug": "de-xuat-chung",
        "name": "Đề xuất chung",
        "group_name": "Khác",
        "description": "Khung chuẩn cho các trường hợp chưa có mẫu chuyên biệt.",
        "icon": "bi-pencil-square",
        "sort_order": 60,
        "fields": [
            {"key": "doi_tuong", "label": "Đối tượng liên quan", "type": "text", "required": False,
             "placeholder": "Khách hàng, nhân viên, nhà cung cấp..."},
            {"key": "su_viec", "label": "Sự việc", "type": "textarea", "required": True,
             "placeholder": "Mô tả ngắn gọn, theo trình tự thời gian"},
            {"key": "phuong_an", "label": "Phương án đề xuất", "type": "textarea", "required": True},
            {"key": "amount", "label": "Số tiền (nếu có)", "type": "money", "required": False},
        ],
        "title_template": "ĐỀ XUẤT {{doi_tuong}}",
        "content_template": (
            "Sự việc: {{su_viec}}\n\nCửa hàng kính đề xuất phương án xử lý: {{phuong_an}}"
            "\n\nGiá trị liên quan (nếu có): {{amount}} đồng."
        ),
        "reason_template": "Nhằm xử lý sự việc đúng quy trình và đảm bảo hoạt động tại cửa hàng.",
    },
]
