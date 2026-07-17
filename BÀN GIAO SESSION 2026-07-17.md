# Bàn giao session 2026-07-17

## Trạng thái hiện tại

- Workspace: `G:\#ClaudeCode\SAP_auto\phieu-ck-app`
- Nhánh: `master`
- Website: `https://dangkhoa.io.vn/bk/`
- Commit đã deploy: `30a07d6 feat: tra cuu va cap nhat CCCD ma NV`
- Commit mới nhất chỉ ở local, **chưa deploy**: `b3a586c feat: hien thi thong ke ma NV va duyet cho`

## Phần đã deploy

- Mã KH tự đổi sang chữ hoa khi nhập mã NV dạng `E01...`.
- Gợi ý mã NV qua cùng cơ chế bảo vệ/captcha với mã khách hàng.
- Kho mã NV mã hóa độc lập đã có 18.322 bản ghi; mã mới nhất là `E0131538`.
- Upload bảng kê CCCD tách:
  - Mã `10...` vào kho CCCD khách hàng.
  - Mã `E01...` vào kho mã NV.
- Upload CCCD: tối đa 150 MB, 600.000 dòng; giới hạn giải nén XLSX 750 MB.
- Gunicorn production đã tăng timeout lên 600 giây để xử lý file bảng kê lớn.
- Không đưa file nguồn, CSDL rõ, khóa mã hóa hoặc dữ liệu cá nhân vào Git/log.

## Phần local chưa deploy

- Trong Cài đặt, khối cập nhật dữ liệu KH có thêm hàng “Mã NV” gồm:
  - Tổng bản ghi.
  - Mã NV mới nhất.
  - Cập nhật gần nhất.
- Khối “Dữ liệu khách hàng TVV đề xuất” hiển thị tổng số đang chờ duyệt.
- Menu Admin hiển thị `Cài đặt (n)` khi còn đề xuất chờ duyệt; không hiện khi bằng 0.
- Commit: `b3a586c`.
- Kiểm thử: `python -m unittest discover -s tests -v` — 41/41 đạt.

## Việc nên làm ngay ở session mới

1. Nếu người dùng đồng ý, deploy `b3a586c` lên web.
2. Sau deploy, kiểm tra đăng nhập Admin, menu Cài đặt và số liệu mã NV; không cần in dữ liệu cá nhân ra màn hình/log.
3. Khi người dùng upload bảng kê miền Trung, chỉ báo số lượng tổng hợp; không commit file XLSX hay CSDL.

## Lưu ý an toàn

- Hai file untracked của người dùng hiện có là `outputs/` và bản sao lưu XLSX; giữ nguyên, không xóa/commit.
- Trước mọi deploy, kiểm tra `git status`, test đầy đủ, sao lưu cấu hình/dữ liệu server nếu có thay đổi CSDL.
- Phân biệt rõ “đã deploy” và “chỉ local”; không tự deploy khi người dùng chưa xác nhận.
