# Hướng Dẫn Chạy Code

## Các Bước Thực Hiện

1. **Dán tệp dữ liệu**
   - Đặt tệp dữ liệu đầu vào vào thư mục `Code`.

2. **Cấu hình đường dẫn tệp dữ liệu**
   - Mở file `Assignment1Tester.py`.
   - Thay đổi giá trị biến `INPUT_FILE_PATH` thành đường dẫn tới tệp dữ liệu vừa dán.

3. **Cập nhật số lượng bản ghi**
   - Trong file `Assignment1Tester.py`, thay đổi giá trị biến `ACTUAL_ROWS_IN_INPUT_FILE` thành số lượng bản ghi (dòng) có trong tệp dữ liệu.

4. **Cấu hình kết nối cơ sở dữ liệu**
   - Mở file `testHelper.py`.
   - Trong hàm `getopenconnection()`, thay đổi các tham số `user`, `password`, `dbname` thành thông tin tài khoản, mật khẩu và tên cơ sở dữ liệu do giảng viên cung cấp.

## Lưu Ý

- File `interface.py` chứa các hàm cần được cài đặt theo yêu cầu bài tập.
- Ngoài các hàm chính, có thể có thêm một số hàm phụ với tên giống tên hàm yêu cầu kèm theo một số ngẫu nhiên, được sử dụng để kiểm tra hiệu năng (nếu cần thiết).
- Xóa số sau tên hàm phụ và đổi tên hàm chính khi muốn test hiệu năng hàm phụ

