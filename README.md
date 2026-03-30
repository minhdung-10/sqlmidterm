# sqlmidterm

> Dự án giữa kỳ môn SQL của **Nhóm 4**, xây dựng một **Firm Data Hub** bằng **MySQL + Python** để lưu trữ, nhập liệu, versioning bằng snapshot, kiểm tra chất lượng dữ liệu và xuất bộ panel doanh nghiệp phục vụ phân tích.

---

## Giới thiệu

`sqlmidterm` là project xây dựng hệ thống quản lý dữ liệu doanh nghiệp niêm yết trong giai đoạn **2020–2024**.  
Hệ thống được thiết kế để:

- lưu trữ dữ liệu doanh nghiệp theo cấu trúc quan hệ
- nhập dữ liệu từ file Excel vào database
- tạo snapshot để theo dõi các lần cập nhật dữ liệu
- kiểm tra chất lượng dữ liệu bằng notebook QC
- xuất bộ dữ liệu panel mới nhất ra file CSV

Dự án sử dụng một file dữ liệu tổng hợp là **`DATA COLLECTION.xlsx`** và triển khai pipeline bằng Python kết nối với MySQL.

---

## Thành viên nhóm

- **Đào Minh Dũng - **
- **Vũ Đức Anh - 11245840**
- **Nguyễn Đức Quang - **
- **Đỗ Huy - **

---

## Công nghệ sử dụng

- **MySQL**: lưu trữ và quản lý cơ sở dữ liệu
- **Python**: ETL pipeline và export dữ liệu
- **Pandas / NumPy**: xử lý dữ liệu
- **SQLAlchemy + PyMySQL**: kết nối Python với MySQL
- **OpenPyXL**: đọc file Excel
- **Jupyter Notebook**: kiểm tra chất lượng dữ liệu

---

## Cấu trúc thư mục

```bash
sqlmidterm/
├── README.md              # mô tả dự án
├── database.sql           # script SQL tạo database và các bảng
├── main.py                # file chạy pipeline chính
├── import_firm.py         # nhập danh mục doanh nghiệp
├── import_panel.py        # nhập dữ liệu panel
├── export_panel.py        # xuất dữ liệu panel mới nhất
├── create_snapshot.py     # tạo snapshot dữ liệu
├── QC_Checks_Colab.ipynb  # notebook kiểm tra chất lượng dữ liệu
└── DATA COLLECTION.xlsx   # file dữ liệu đầu vào
```

---

## Mục tiêu dự án

Project hướng tới việc xây dựng một pipeline dữ liệu có thể:

1. khởi tạo database bằng SQL
2. import danh mục doanh nghiệp từ Excel
3. import dữ liệu panel theo năm
4. tạo snapshot để quản lý version dữ liệu
5. kiểm tra chất lượng dữ liệu
6. export bộ panel mới nhất phục vụ phân tích

---

## Thiết kế cơ sở dữ liệu

Database hiện tại được tổ chức theo hướng **dimension + fact + snapshot**.

### Dimension tables
- `dim_exchange`
- `dim_industry_l2`
- `dim_firm`
- `dim_data_source`

### Snapshot table
- `fact_data_snapshot`

### Fact tables
- `fact_ownership_year`
- `fact_financial_year`
- `fact_cashflow_year`
- `fact_market_year`
- `fact_innovation_year`
- `fact_firm_year_meta`

Cấu trúc này giúp dữ liệu được lưu theo từng **firm-year-snapshot**, thuận tiện cho việc theo dõi lịch sử cập nhật và xuất phiên bản mới nhất.

---

## Dữ liệu đầu vào

Project hiện sử dụng file:

```bash
DATA COLLECTION.xlsx
```

File này chứa dữ liệu doanh nghiệp và dữ liệu panel theo năm, bao gồm các nhóm biến như:

- ownership
- market
- financial statements
- cashflow
- innovation
- firm metadata

Một số cột dữ liệu chính trong file Excel gồm:

- `Company`
- `StockCode`
- `Year`
- `Managerial/Inside ownership`
- `State Ownership`
- `Institutional ownership`
- `Foreign ownership`
- `Total share outstanding`
- `R&D expenditure`
- `Product innovation`
- `Process innovation`
- `Firm Age`
- `Market value of equity`
- `Number of employees`
- `Selling expenses`
- `Total assets`
- `Total liabilities`
- `Net Income`
- `EPS`
- `Net cash from operating activities`
- `Growth Ratio`
- `Total sales revenue`

---

## Chức năng của từng file

### `database.sql`
Tạo database `midterm_proj` và toàn bộ các bảng dimension, fact, snapshot.

### `import_firm.py`
Đọc file Excel, làm sạch dữ liệu doanh nghiệp và import danh mục firm vào bảng `dim_firm`.

### `create_snapshot.py`
Tạo snapshot mới trong bảng `fact_data_snapshot` để ghi nhận một phiên bản dữ liệu tại thời điểm import.

### `import_panel.py`
Đọc file Excel và nạp dữ liệu panel vào các bảng fact:
- `fact_financial_year`
- `fact_cashflow_year`
- `fact_market_year`
- `fact_ownership_year`
- `fact_innovation_year`
- `fact_firm_year_meta`

### `export_panel.py`
Xuất bộ dữ liệu panel mới nhất ra file:

```bash
outputs/panel_latest.csv
```

Script chọn snapshot mới nhất cho mỗi cặp `firm_id - fiscal_year`.

### `QC_Checks_Colab.ipynb`
Notebook kiểm tra chất lượng dữ liệu sau khi import vào database.

### `main.py`
Chạy pipeline chính theo thứ tự:
1. nhập thông tin kết nối MySQL
2. import danh mục doanh nghiệp
3. tạo snapshot
4. import dữ liệu panel

---

## Cài đặt môi trường

Cài các thư viện cần thiết:

```bash
pip install pandas numpy sqlalchemy pymysql openpyxl pwinput jupyter notebook
```

---

## Cách chạy project

### Bước 1: Tạo database
Mở file `database.sql` trong MySQL Workbench hoặc VS Code extension hỗ trợ SQL, sau đó chạy toàn bộ script.

Database mặc định trong file là:

```sql
CREATE DATABASE midterm_proj;
USE midterm_proj;
```

### Bước 2: Chạy pipeline chính

```bash
python main.py
```

Khi chạy, chương trình sẽ yêu cầu nhập:

- host
- port
- user
- database name
- password

Ví dụ thường dùng trên máy local:

```text
Host: localhost
Port: 3306
User: root
Database name: midterm_proj
Password: ********
```

### Bước 3: Kiểm tra chất lượng dữ liệu
Mở notebook:

```bash
QC_Checks_Colab.ipynb
```

và chạy toàn bộ cell để kiểm tra dữ liệu đã import.

### Bước 4: Export dữ liệu panel mới nhất
Có thể chạy riêng:

```bash
python export_panel.py
```

Sau khi chạy thành công, file output sẽ nằm tại:

```bash
outputs/panel_latest.csv
```

---

## Luồng xử lý dữ liệu

Pipeline hiện tại của project hoạt động theo luồng:

```text
DATA COLLECTION.xlsx
        ↓
 import_firm.py
        ↓
 create_snapshot.py
        ↓
 import_panel.py
        ↓
 QC_Checks_Colab.ipynb
        ↓
 export_panel.py
        ↓
 outputs/panel_latest.csv
```

---

## Kiểm tra chất lượng dữ liệu

Notebook QC hiện kiểm tra một số nhóm lỗi chính như:

- thiếu dữ liệu cốt lõi (`net_sales`, `total_assets`, `total_equity`)
- giá trị âm bất thường
- sai lệch phương trình kế toán
- tăng trưởng doanh thu hoặc tài sản bất thường
- market capitalization không khớp logic
- outlier trong các chỉ số tài chính

Mục tiêu của bước này là tăng độ tin cậy của bộ dữ liệu trước khi export.

---

## Kết quả đầu ra

Project hiện hướng tới các kết quả chính:

- dữ liệu doanh nghiệp đã được nạp vào MySQL
- dữ liệu panel được lưu theo từng snapshot
- notebook QC để đánh giá chất lượng dữ liệu
- file xuất cuối cùng:

```bash
outputs/panel_latest.csv
```

---

## Điểm nổi bật của project

- tổ chức dữ liệu theo mô hình có cấu trúc
- hỗ trợ versioning bằng snapshot
- có bước QC sau khi import
- có thể export panel latest để phân tích
- pipeline đủ rõ để mở rộng thêm dữ liệu trong tương lai

---

## Hạn chế hiện tại

- README hiện mới mô tả ở mức hệ thống, chưa có ERD hoặc ảnh minh họa
- notebook QC chưa được đóng gói thành script tự động
- một số bước vẫn phụ thuộc vào file Excel nguồn
- output QC hiện chủ yếu được xem trong notebook

---

## Hướng phát triển

- bổ sung sơ đồ ERD cho database
- đóng gói phần QC thành script Python riêng
- thêm file `requirements.txt`
- tách cấu hình database sang `.env`
- hoàn thiện tài liệu input/output chi tiết hơn

---

## License

Dự án được thực hiện cho mục đích học tập.
