# Install

## Environment Setup

```bash
# Tải và cài đặt Python
# https://www.python.org/downloads/windows/

# Tài và cài đặt ODBC driver 18 for SQL Server trên Windows
#https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver17
```

```bash
python --version
python -m venv venv
source venv/bin/activate
```

```bash
pip install pandas pyodbc dotenv sqlalchemy xlrd pyxlsb
pip install --upgrade pip
```

```bash
python -m app.etl
```

```bash
# Install Microsoft ODBC driver 18 for SQL Server on macOS
# https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-ver17
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18
```

## Cài đặt cơ sở dữ liệu

Tạo database và schema trước khi chạy code Python

```sql
CREATE DATABASE bim_report
GO

USE bim_report
GO

CREATE SCHEMA stg
GO
```

Tạo tài khoản có quyền owner để quản lý cơ sở dữ liệu

- database: bim_report
- user: etl
- password: BIM@2025

Tạo tài khoản có quyền đọc dữ liệu để cho phép hệ thống báo cáo Power BI kết nối tới cơ sở dữ liệu

- database: bim_report
- user: bi
- password: BIM@2025

```bash
# khởi tạo CSDL
python -m app.etl.init
```

```bash
python -m app.etl.init
python -m app.etl.fload
python -m app.etl.iload

python -m app.etl.etl_booking_pace_history -t init
python -m app.etl.etl_booking_pace_history -t restore_history --start_date "2025-08-18" --end_date "2025-09-09"

# thực hiện khởi tạo một khách sạn theo Template SMILE PQ
python -m app.etl.etl_smile_pq -t init_property -p "SCSRPQ"
python -m app.etl.etl_smile_pq -t fload_property -p "SCSRPQ"
python -m app.etl.etl_smile_pq -t iload_property -p "SCSRPQ"
python -m app.etl.etl_smile_pq -t fload_property_history -p "SCSRPQ"

python -m app.etl.etl_smile_pq -t init_property -p "SBHPQ"
python -m app.etl.etl_smile_pq -t fload_property -p "SBHPQ"
python -m app.etl.etl_smile_pq -t iload_property -p "SBHPQ"
python -m app.etl.etl_smile_pq -t fload_property_history -p "SBHPQ"

# thực hiện khởi tạo một khách sạn theo Template SMILE HL
python -m app.etl.etl_smile_hl -t init_property -p "SRC"
python -m app.etl.etl_smile_hl -t fload_property -p "SRC"
python -m app.etl.etl_smile_hl -t iload_property -p "SRC"
python -m app.etl.etl_smile_hl -t fload_property_history -p "SRC"

# thực hiện khởi tạo một khách sạn theo Template Opera
python -m app.etl.etl_opera -t init_property -p "CPV"
python -m app.etl.etl_opera -t fload_property -p "CPV"
python -m app.etl.etl_opera -t iload_property -p "CPV"
python -m app.etl.etl_opera -t fload_property_history -p "CPV"

python -m app.etl.etl_opera -t fload_property_history -p "CPV"

python -m app.etl.etl_opera -t fload_property_history -p "HIVT"
python -m app.etl.etl_opera -t fload_property_history -p "ICHL"
python -m app.etl.etl_opera -t fload_property_history -p "ICPQ"
python -m app.etl.etl_opera -t fload_property_history -p "REPQ"

# exchange_rate
python -m app.etl.etl_exchange_rate -t init
# lấy dữ liệu ngày hôm nay
python -m app.etl.etl_exchange_rate -t get_exchange_rate -p today

# lấy dữ liệu một ngày bất kỳ
python -m app.etl.etl_exchange_rate -t get_exchange_rate -p day -d "01082025"
python -m app.etl.etl_exchange_rate -t get_exchange_rate -p day -d "02082025"
python -m app.etl.etl_exchange_rate -t get_exchange_rate -p day -d "03082025"
python -m app.etl.etl_exchange_rate -t get_exchange_rate -p day -d "04082025"
python -m app.etl.etl_exchange_rate -t get_exchange_rate -p day -d "05082025"

# khởi tạo bảng
python -m app.etl.etl_booking_pace_detail -t init
python -m app.etl.etl_booking_pace_detail -t init_booking_pace_detail_table
python -m app.etl.etl_booking_pace_detail -t init_booking_pace_history_table
python -m app.etl.etl_booking_pace_detail -t init_sp_fload_booking_pace_detail
python -m app.etl.etl_booking_pace_detail -t init_sp_iload_booking_pace_detail
python -m app.etl.etl_booking_pace_detail -t fload
python -m app.etl.etl_booking_pace_detail -t iload

python -m app.etl.etl_booking_pace_report -t init
python -m app.etl.etl_booking_pace_report -t init_booking_pace_report_table
python -m app.etl.etl_booking_pace_report -t init_booking_pace_actual_table
python -m app.etl.etl_booking_pace_report -t init_sp_fload_booking_pace_report
python -m app.etl.etl_booking_pace_report -t init_sp_iload_booking_pace_report
python -m app.etl.etl_booking_pace_report -t fload
python -m app.etl.etl_booking_pace_report -t iload

```

```bash
# B1: thêm mới một khách sạn cần bổ sung vào trong file config
ALL_PROPERTIES = [
    {
        "code": "SRC",
        "name": "Syrena Cruises",
        "folder": "SRC",
        "template": "SMILE HL",
        "schema": "stg",
        "table": "booking_pace_src",
    },
]
# B2: Khởi tạo khách sạn theo template và thực hiện Full Load
python -m app.etl.etl_smile_hl -t init_property -p "SRC"
python -m app.etl.etl_smile_hl -t fload_property -p "SRC"
python -m app.etl.etl_smile_hl -t iload_property -p "SRC"

# B3: Chú ý cần cập nhật lại các store procedure
python -m app.etl.etl_booking_pace_detail -t init_sp_fload_booking_pace_detail
python -m app.etl.etl_booking_pace_detail -t init_sp_iload_booking_pace_detail

python -m app.etl.etl_booking_pace_report -t init_sp_fload_booking_pace_report
python -m app.etl.etl_booking_pace_report -t init_sp_iload_booking_pace_report

# B4: Chạy các store procedure lấy dữ liệu bổ sung
python -m app.etl.etl_booking_pace_detail -t iload
python -m app.etl.etl_booking_pace_report -t iload
```

```bash
# Copy file backup từ Docker Container về Local
docker cp daa1a69bb1e5079b34025f594d86849a0217b1a571e24f9ea10844c0a2de493f:/var/opt/mssql/data/bim_report-202579-4-28-33.bak "./data/Sample Data"
```

```bash
# job 1
cd /d D:\bim_etl && D:\bim_etl\venv\Scripts\python -m app.etl.etl_exchange_rate -t get_exchange_rate -p today
# job 2
cd /d D:\bim_etl && D:\bim_etl\venv\Scripts\python -m app.etl.iload
```

Chú ý cần phân quyền cho tài khoản NT Service\SQLSERVERAGENT có quyền đọc/ghi vào folder chứa dữ liệu
