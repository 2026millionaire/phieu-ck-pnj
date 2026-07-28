# Ghi chú bàn giao TTCK ngày 2026-07-28

## Phạm vi

Ứng dụng: `https://dangkhoa.io.vn/bk/`

Worktree đang dùng để deploy sạch:

`G:\#ClaudeCode\SAP_auto\phieu-ck-app-ttck-deploy`

Repo GitHub:

`2026millionaire/phieu-ck-pnj`, nhánh `master`

Service VPS:

`/opt/phieu-ck-app`, service systemd `phieuck`

## Trạng thái mới nhất

Tính năng `Luồng xanh` nằm trong block `3. Thông tin giao dịch`, bên dưới bảng chứng từ. Checkbox này thay thế việc người dùng tự chọn `T120 | T0`.

Quy tắc hiện tại:

- Tick `Luồng xanh`: giao diện hiển thị cho người dùng hiểu là `Thời gian thanh toán: T/T+1 ngày`.
- Tick `Luồng xanh`: dữ liệu lưu `green_flow = 1` và `payment_time_mode = T0`.
- Tick `Luồng xanh`: bản in/PDF phiếu xác nhận xử lý giống lựa chọn `T0` cũ, nghĩa là không in lộ trình thanh toán, không in ghi chú liên hệ, chỉ in tới câu `Giấy xác nhận thông tin thanh toán có hiệu lực cho đến khi khách nhận được tiền vào tài khoản.`
- Không tick `Luồng xanh`: giao diện hiển thị `Thời gian thanh toán: T+120 ngày`.
- Không tick `Luồng xanh`: dữ liệu lưu `green_flow = 0` và `payment_time_mode = T120`.
- Không tick `Luồng xanh`: bản in/PDF và bản xem trước phải in đầy đủ lộ trình thanh toán 5 giai đoạn:
  - `T/T+1: 10%`
  - `T+30: 20%`
  - `T+60: 25%`
  - `T+90: 25%`
  - `T+120: 20%`
- Không tick `Luồng xanh`: bản in/PDF và bản xem trước phải có dòng liên hệ `0234 3847 588`.

Điểm rất dễ nhầm: dòng hiển thị `T/T+1 ngày` trong giao diện chỉ để người dùng hiểu khi tick `Luồng xanh`. Không được in dòng này lên phiếu xác nhận.

## Các file chính đã chỉnh

- `templates/index.html`
  - Giao diện `Luồng xanh`.
  - Dòng hiển thị thời gian thanh toán cạnh checkbox.
  - Bản xem trước phiếu.
  - JS `selectedPaymentTimeMode()`, `greenFlowEnabled()`, `updatePaymentTimeModePreview()`.
- `templates/print.html`
  - In HTML phiếu xác nhận.
  - `p.show_payment_time` quyết định có in lộ trình 5 giai đoạn và ghi chú liên hệ hay không.
- `app.py`
  - Cột DB `green_flow`.
  - API `/api/save` tự ép `payment_time_mode = T0` nếu `green_flow`, ngược lại `T120`.
  - `prepare_phieu_for_output()` tạo `show_payment_time` theo `payment_time_mode == T120`.
  - PDF `make_phieu_pdf()` in lộ trình 5 giai đoạn khi `show_payment_time`.
  - Nội dung QT82 `Luồng xanh` dùng helper `build_qt82_noi_dung()`.
- `tests/test_eoffice_qt82.py`
  - Test UI tạo phiếu, bản in, nội dung QT82, preview lộ trình.

## Luồng xanh và nội dung QT82

Nếu `green_flow = 1` và danh sách chứng từ có dòng `Phải CK khác` (hiển thị là `Giấy Báo Có\n(KH CK thanh toán)`), nội dung QT82 đổi sang dạng:

`<plant>_LX <tên khách hàng> đã CK 100% - BK <Số BK 44...> ngày <yyyy-mm-dd> - <Số tiền CK> VNĐ`

Ví dụ:

`1305_LX TRẦN THỊ THANH THÚY đã CK 100% - BK 4403927546 ngày 2026-07-27 - 14.074.000 VNĐ`

Nếu không đủ điều kiện trên, nội dung QT82 giữ dạng cũ:

`<plant>_CK BK <Số BK> ngày <yyyy-mm-dd hoặc dd/mm/yyyy> cho <tên khách hàng> - <Số tiền> VND`

## Các thay đổi liên quan trước đó

- Không còn tự điền địa chỉ từ BP/PO vào ô `Địa chỉ`. App chỉ giữ địa chỉ nếu người dùng nhập hoặc mở lại phiếu cũ đã lưu địa chỉ.
- Dòng `Phải CK khác` trên phiếu in hiển thị ở cột `Loại chứng từ` là:
  - `Giấy Báo Có`
  - `(KH CK thanh toán)`
- Ô `Số chứng từ` của dòng này vẫn cho phép nhập hoặc để trống để ghi tay.
- `Thời gian thanh toán T120 | T0` không còn là lựa chọn radio trên UI.

## Kiểm thử nên chạy

Trong worktree:

```powershell
cd G:\#ClaudeCode\SAP_auto\phieu-ck-app-ttck-deploy
& 'C:\Users\ASUS\AppData\Local\Programs\Python\Python313\python.exe' -m unittest tests.test_eoffice_qt82
```

Kỳ vọng hiện tại:

`Ran 27 tests ... OK`

## Deploy

Sau khi commit và push:

```powershell
ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes -p 24700 root@103.72.98.135 "cd /opt/phieu-ck-app && git pull --ff-only origin master && systemctl restart phieuck && systemctl is-active phieuck && git rev-parse --short HEAD"
```

Kiểm tra nhanh:

```powershell
curl.exe -I https://dangkhoa.io.vn/bk/login
```

Kỳ vọng:

- Service trả `active`.
- `/bk/login` trả `HTTP/1.1 200 OK`.

## Lưu ý cho session sau

- Không sửa trực tiếp ở worktree chính `G:\#ClaudeCode\SAP_auto\phieu-ck-app` nếu đang bẩn. Ưu tiên worktree sạch `phieu-ck-app-ttck-deploy`.
- Worktree deploy có thể ở trạng thái detached HEAD. Khi push dùng:

```powershell
git -c safe.directory='G:/#ClaudeCode/SAP_auto/phieu-ck-app-ttck-deploy' push origin HEAD:master
```

- Nếu `origin/master` đi trước, fetch/rebase trước khi push.
- Không đưa mật khẩu, cookie, OTP, CCCD, SĐT, STK khách hàng vào file bàn giao hoặc log dài hạn.
- Mọi file `.md` ghi chú phải viết tiếng Việt có dấu đầy đủ.
