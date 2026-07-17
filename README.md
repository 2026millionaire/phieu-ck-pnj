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

## Tra cứu khách hàng local

- CSDL dẫn xuất được lưu ngoài repository tại `%LOCALAPPDATA%\PNJCustomerLookup\customer_lookup.db`.
- Khóa chính local được Windows DPAPI bảo vệ trong `master-key.dpapi`; không sao chép khóa hoặc CSDL vào Git/OneDrive.
- Mã KH dùng HMAC làm chỉ mục; payload `Customer`, `SearchTerm`, `Name 1`, `DelF` được mã hóa AES-256-GCM.
- Dữ liệu Tên KH, SĐT và CCCD do TVV nhập chỉ được ghi nhận khi phiếu ở trạng thái `printed`. Giá trị khác dữ liệu chính thức được mã hóa và lưu song song ở trạng thái chờ ADMIN duyệt; không tự ghi đè dữ liệu SAP.
- Báo cáo `/customer-updates` chỉ cho ADMIN xem và duyệt/từ chối từng trường hợp, tối đa 50 dòng mỗi trang và không có chức năng xuất hàng loạt.
- ADMIN có thể tải file SAP tên bất kỳ tại trang Cài đặt. Hệ thống kiểm tra nội dung, chống nhập trùng bằng SHA-256, tự sao lưu, nhập trong một giao dịch và xóa file tạm sau khi hoàn tất.
- Công cụ nhập không in tên, SĐT hoặc mã KH ra terminal. Mỗi lần nhập là một giao dịch: nếu có lỗi, toàn bộ lần nhập được rollback.
- `start.bat` dùng khóa Turnstile thử nghiệm chính thức và chỉ dành cho local. Production bắt buộc cung cấp site key/secret thật qua biến môi trường.

Nhập hoặc cập nhật dữ liệu bằng lệnh:

```powershell
python scripts\import_customer_lookup.py "<file-1.txt>" "<file-2.txt>" --expected-min 100000000 --expected-max 100499999
```

Không đưa file SAP nguồn, `customer_lookup.db`, `master-key.dpapi`, log tra cứu hoặc khóa production vào repository.

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
    ├── customer_updates.html # Báo cáo duyệt dữ liệu KH
    └── settings.html        # Cài đặt
```

## Tech Stack

- Python Flask + SQLite
- Bootstrap 5 + Bootstrap Icons
- MBBank API (tra cứu TK liên ngân hàng)
- Google Lens OCR (chrome-lens-py)
- VietQR (QR code)
