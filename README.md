# Firm Data Hub  
**Trung tâm lưu trữ và xử lý dữ liệu doanh nghiệp (2020–2024)**

Dự án xây dựng một hệ thống quản lý dữ liệu doanh nghiệp niêm yết tại Việt Nam bằng **MySQL + Python ETL**, hỗ trợ lưu trữ dữ liệu panel theo năm, kiểm tra chất lượng dữ liệu, quản lý snapshot/version và xuất bộ dữ liệu sạch phục vụ phân tích.

---

## Tổng quan

Firm Data Hub được thiết kế để giải quyết 4 bài toán chính:

- Lưu trữ tập trung thông tin doanh nghiệp và dữ liệu tài chính theo năm
- Quản lý dữ liệu theo phiên bản bằng cơ chế **snapshot**
- Kiểm tra chất lượng dữ liệu trước khi sử dụng
- Xuất bộ **panel latest** phục vụ phân tích hoặc nghiên cứu

Phạm vi dữ liệu của project gồm:

- **20 doanh nghiệp niêm yết**
- **Giai đoạn 2020–2024**
- **38 biến dữ liệu** thuộc các nhóm ownership, market, financials, cashflow, innovation và metadata

---

## Công nghệ sử dụng

- **MySQL**: thiết kế và quản lý cơ sở dữ liệu
- **Python**: ETL, kiểm tra dữ liệu và export output
- **CSV / Excel**: nguồn dữ liệu đầu vào
- **SQL View**: dựng panel cuối cùng để truy vấn nhanh

---

## Cấu trúc thư mục

```bash
FirmDataHub/
├── sql/
│   └── schema_and_seed.sql
├── etl/
│   ├── import_firms.py
│   ├── create_snapshot.py
│   ├── import_panel.py
│   ├── qc_checks.py
│   └── export_panel.py
├── data/
│   ├── team_tickers.csv
│   ├── firms.xlsx
│   └── panel_2020_2024.xlsx
├── outputs/
│   ├── qc_report.csv
│   └── panel_latest.csv
└── README.md
```

---

## Thiết kế hệ thống

Cơ sở dữ liệu được tổ chức theo mô hình **Dimension + Fact + Snapshot**.

### Dimension tables
- `dim_firm`
- `dim_exchange`
- `dim_industry_l2`
- `dim_data_source`

### Snapshot / Versioning
- `fact_data_snapshot`

### Fact tables
- `fact_ownership_year`
- `fact_financial_year`
- `fact_cashflow_year`
- `fact_market_year`
- `fact_innovation_year`
- `fact_firm_year_meta`

### Audit log
- `fact_value_override_log`

---

## Pipeline ETL

### 1. Import danh mục doanh nghiệp
Script `import_firms.py` dùng để nạp danh mục doanh nghiệp từ file đầu vào vào hệ thống.

### 2. Tạo snapshot
Script `create_snapshot.py` dùng để tạo phiên bản dữ liệu mới, phục vụ theo dõi lịch sử cập nhật.

### 3. Import dữ liệu panel
Script `import_panel.py` dùng để nạp dữ liệu panel 2020–2024 vào các bảng fact tương ứng.

### 4. Kiểm tra chất lượng dữ liệu
Script `qc_checks.py` thực hiện các rule kiểm tra dữ liệu và sinh file báo cáo lỗi.

### 5. Xuất panel cuối cùng
Script `export_panel.py` xuất bộ dữ liệu mới nhất ra file dùng cho phân tích.

---

## Các kiểm tra chất lượng dữ liệu

Hệ thống QC tập trung vào các rule quan trọng như:

- Ownership ratio phải nằm trong khoảng hợp lệ
- `shares_outstanding` phải lớn hơn 0
- `total_assets` và `current_liabilities` không được âm
- Growth ratio không vượt ngoài khoảng hợp lý
- `market_value_equity` phải khớp tương đối với:
  
```text
shares_outstanding × share_price
```

Kết quả QC được lưu tại:

```bash
outputs/qc_report.csv
```

---

## View chính của hệ thống

Project tạo view:

```sql
vw_firm_panel_latest
```

View này dùng để tổng hợp bộ dữ liệu panel cuối cùng, bao gồm:

- `ticker`
- `fiscal_year`
- các biến ownership
- các biến market
- các biến financial
- các biến cashflow
- các biến innovation
- các biến metadata

---

## Dữ liệu đầu vào

Các file đầu vào chính của project:

- `data/team_tickers.csv`: danh sách ticker của nhóm
- `data/firms.xlsx`: danh mục doanh nghiệp
- `data/panel_2020_2024.xlsx`: dữ liệu panel 5 năm

---

## Dữ liệu đầu ra

Sau khi chạy hoàn chỉnh pipeline, project sinh ra:

- `outputs/qc_report.csv`: báo cáo lỗi dữ liệu
- `outputs/panel_latest.csv`: bộ dữ liệu panel mới nhất

---

## Cách chạy project

### 1. Tạo database

```sql
CREATE DATABASE firm_data_hub;
USE firm_data_hub;
```

### 2. Tạo schema

```bash
mysql -u root -p firm_data_hub < sql/schema_and_seed.sql
```

### 3. Import danh mục doanh nghiệp

```bash
python etl/import_firms.py
```

### 4. Tạo snapshot

```bash
python etl/create_snapshot.py
```

### 5. Import dữ liệu panel

```bash
python etl/import_panel.py
```

### 6. Chạy kiểm tra chất lượng dữ liệu

```bash
python etl/qc_checks.py
```

### 7. Export bộ panel mới nhất

```bash
python etl/export_panel.py
```

---

## Tính tái lập

Project được tổ chức theo hướng có thể chạy lại toàn bộ từ đầu:

1. Tạo database
2. Chạy schema
3. Import firms
4. Tạo snapshot
5. Import panel
6. Chạy QC
7. Export panel latest

Cách tổ chức này giúp project dễ kiểm tra, dễ mở rộng và thuận tiện khi bàn giao.

---

## Kết quả đạt được

Dự án hoàn thiện các thành phần cốt lõi của một hệ thống dữ liệu doanh nghiệp:

- Thiết kế schema rõ ràng
- Tổ chức dữ liệu theo mô hình chuẩn
- Hỗ trợ versioning bằng snapshot
- Có kiểm tra chất lượng dữ liệu
- Xuất được bộ panel mới nhất để phân tích
- Có thể tái chạy toàn bộ pipeline

---

## Hạn chế

- Một số biến có thể thiếu dữ liệu từ nguồn công khai
- Một phần dữ liệu có thể cần nhập tay
- Chưa tự động hóa hoàn toàn quá trình thu thập dữ liệu

---

## Hướng phát triển

- Tự động hóa quá trình ingestion dữ liệu
- Mở rộng thêm doanh nghiệp và giai đoạn dữ liệu
- Bổ sung dashboard trực quan hóa
- Tăng cường rule QC và audit log

---

## Thành viên nhóm

- **Nhóm:** TEAM ...
- **Thành viên 1:** Đào Minh Dũng
- **Thành viên 2:** Vũ Đức Anh
- **Thành viên 3:** Đỗ Huy
- **Thành viên 4:** Nguyễn Đức Quang

---

## Danh sách ticker

```text
AAA
BBB
CCC
DDD
EEE
FFF
GGG
HHH
III
JJJ
KKK
LLL
MMM
NNN
OOO
PPP
QQQ
RRR
SSS
TTT
```

> Thay danh sách trên bằng đúng 20 ticker của nhóm bạn.

---

## License

Dự án được thực hiện cho mục đích học tập và nghiên cứu.
