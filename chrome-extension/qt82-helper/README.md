# Tiện ích Chrome hỗ trợ bản nháp QT82

Tiện ích này nhận bản nháp từ trang eOffice QT82 của ứng dụng `dangkhoa.io.vn/bk/`, giữ dữ liệu trong bộ nhớ phiên Chrome tối đa 5 phút và điền vào form QT82 đang mở.

## Giới hạn an toàn

- Chỉ hoạt động trên `dangkhoa.io.vn/bk/eoffice*`, `localhost:5050/eoffice*` và `eoffice.pnj.com.vn/workflow/*`.
- Không đọc clipboard, mật khẩu, cookie hoặc OTP.
- Không tự bấm **Lưu**, **Gửi** hoặc đánh dấu **Đã trình**.
- Template TT được kiểm tra SHA-256, giữ trong bộ nhớ phiên tối đa 5 phút và nhập đúng một lần; không ghi xuống Desktop/Downloads.
- Sau khi điền đủ trường, dữ liệu tạm bị xóa khỏi bộ nhớ tiện ích.

## Cài đặt local

1. Mở Chrome và truy cập `chrome://extensions/`.
2. Bật **Developer mode**.
3. Chọn **Load unpacked**.
4. Chọn thư mục `chrome-extension/qt82-helper` trong project.
5. Mở lại trang chuẩn bị eOffice QT82 và kiểm tra trạng thái **Đã kết nối**.

## Quy trình thử nghiệm

1. Chỉ thử trên một phiếu đã kiểm tra số tiền và tài khoản.
2. Nhấn **Tạo bản nháp QT82 trên Chrome**.
3. Chờ hộp trạng thái của tiện ích hiển thị kết quả điền.
4. Chờ tiện ích gắn và nhập Template TT từ bộ nhớ đúng một lần.
5. Đối chiếu tên KH, mã KH, STK, ngân hàng, tổng tiền, CCCD và mã chứng từ SAP.
6. Nếu mã SAP đang là `1234`, thay bằng mã thật.
7. Không bấm **Gửi** trong lần thử đầu; chụp màn hình kết quả để đối chiếu selector và logic.

## Chế độ chẩn đoán an toàn

Nút **Sao chép cấu trúc trường** chỉ sao chép tên thẻ, `id`, `name`, class và vị trí tương đối của ô; báo cáo không chứa giá trị ô, tên khách hàng, CCCD, số tài khoản hoặc số tiền. Phiên bản `0.1.12` dùng selector chính xác lấy từ báo cáo này và ưu tiên control nằm ngay trong khối `.ItemRow` của từng trường.

Phiên bản `0.1.4` không ghi trực tiếp số tiền vào form QT82. Tổng thanh toán được tính từ các dòng chi tiết sau khi người dùng nhập Template TT. Các dropdown Kendo được mở qua wrapper bên ngoài để chọn đúng Loại tiền và Phương thức nhận tiền.

Phiên bản `0.1.5` chuẩn hóa dấu gạch ngang và tìm lựa chọn Kendo trong cả frame hiện tại lẫn frame cha, giúp chọn đúng `Bank transfer – Chuyển khoản`.

Phiên bản `0.1.6` dùng cầu nối trong trang để gọi Kendo widget cho hai dropdown Loại tiền và Phương thức nhận tiền. Cầu nối chỉ nhận giá trị lựa chọn cố định, không nhận dữ liệu khách hàng.

Phiên bản `0.1.7` chỉ sử dụng Kendo DropDownList/ComboBox thật, không dùng AutoComplete của ô lọc; thao tác chọn ngân hàng được chốt bằng chuỗi sự kiện chuột, change và blur/Tab.

Phiên bản `0.1.8` chọn Ngân hàng thụ hưởng trực tiếp qua Kendo ComboBox để eOffice cập nhật cả Mã chi nhánh.

Phiên bản `0.1.9` nhận Template TT đã kiểm tra từ trang ADMIN, xác minh lại SHA-256, gắn file bằng `DataTransfer`, nhấn **Nhập từ excel** đúng một lần và chỉ xóa bản nháp sau khi nhận diện được chứng từ cùng tổng tiền trên form.

Phiên bản `0.1.10` xử lý Loại tiền như AutoComplete, kiểm tra giá trị `VND` sau khi nhập Template TT và tự thử chốt lại đúng một lần nếu eOffice reset trường.

Phiên bản `0.1.11` thay delay cố định bằng cổng sẵn sàng: chờ document hoàn tất, Kendo hoạt động, không còn lớp loading và các control bắt buộc ổn định liên tục 1,5 giây trước khi điền.

Phiên bản `0.1.12` nhận URL form QT82 từ cấu hình ADMIN. Extension vẫn chỉ mở HTTPS đúng miền `eoffice.pnj.com.vn` và đường dẫn `/workflow/`, nên có thể cập nhật LID/wid mà không nới quyền sang miền khác.
