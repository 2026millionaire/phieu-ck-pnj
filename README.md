# Phiếu Xác Nhận Chuyển Khoản — PNJ 1305

Web app Flask tạo phiếu xác nhận thông tin thanh toán chuyển khoản cho cửa hàng PNJ.

## Tính năng

- **Tạo phiếu CK** — nhập thông tin KH, thanh toán, dán dữ liệu SAP ZFIE0029
- **OCR Bảng kê** — chụp ảnh BK từ SAP → quét tự động (Google Lens) → điền Mã KH, Tên, SĐT, CCCD, Số BK
- **Kiểm tra TK** — tra cứu tên chủ tài khoản liên ngân hàng qua MBBank API (miễn phí)
- **In phiếu A5** — đúng mẫu chuẩn PNJ, QR VietQR, chữ ký
- **Tải template eOffice QT82** — file Excel điền sẵn dữ liệu chứng từ
- **eOffice QT82** — copy nhanh từng trường (Mã KH, Nội dung, Tên TK, Số TK, Mã NH, CCCD, Tên file, Số tiền)
- **Lịch sử** — danh sách phiếu, tìm kiếm, Đã Trình, in lại
- **Cài đặt** — CHT, Kế toán, thời gian CK, MBBank credentials
- **Dropdown searchable** — 47 ngân hàng + TVV từ file Excel

## Cài đặt

```bash
pip install flask pandas openpyxl mbbank-lib chrome-lens-py
```

## Chạy

```bash
cd phieu-ck-app
python app.py
```

Hoặc double-click `start.bat` → tự mở http://localhost:5050

## Cấu trúc

```
phieu-ck-app/
├── app.py                  # Flask backend
├── start.bat               # Launcher Windows
├── phieu-ck-data.xlsx      # Data ngân hàng + TVV
├── static/
│   ├── logo_pnj.webp       # Logo PNJ
│   └── template_tt.xlsx    # Template eOffice QT82
└── templates/
    ├── base.html            # Layout chung
    ├── index.html           # Trang tạo phiếu
    ├── print.html           # Trang in A5
    ├── history.html         # Lịch sử
    ├── eoffice.html         # eOffice QT82
    └── settings.html        # Cài đặt
```

## Tech Stack

- Python Flask + SQLite
- Bootstrap 5 + Bootstrap Icons
- MBBank API (tra cứu TK liên ngân hàng)
- Google Lens OCR (chrome-lens-py)
- VietQR (QR code)
