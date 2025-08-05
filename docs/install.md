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

# thực hiện khởi tạo một khách sạn theo Template 01
python -m app.etl.etl_template01 -t init_property -p "Sailing Club Signature Resort Phu Quoc"
python -m app.etl.etl_template01 -t fload_property -p "Sailing Club Signature Resort Phu Quoc"
python -m app.etl.etl_template01 -t iload_property -p "Sailing Club Signature Resort Phu Quoc"

# thực hiện khởi tạo một khách sạn theo Template 02
python -m app.etl.etl_template02 -t init_property -p "Syrena Cruises"
python -m app.etl.etl_template02 -t fload_property -p "Syrena Cruises"
python -m app.etl.etl_template02 -t iload_property -p "Syrena Cruises"

# thực hiện khởi tạo một khách sạn theo Template 03
python -m app.etl.etl_template03 -t init_property -p "Crowne Plaza Vientaine"
python -m app.etl.etl_template03 -t fload_property -p "Crowne Plaza Vientaine"
python -m app.etl.etl_template03 -t iload_property -p "Crowne Plaza Vientaine"

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

# 
python -m app.etl.etl_booking_pace_detail -t init
python -m app.etl.etl_booking_pace_detail -t init_booking_pace_detail_table
python -m app.etl.etl_booking_pace_detail -t init_sp_fload_booking_pace_detail
python -m app.etl.etl_booking_pace_detail -t init_sp_iload_booking_pace_detail
python -m app.etl.etl_booking_pace_detail -t fload
python -m app.etl.etl_booking_pace_detail -t iload

python -m app.etl.etl_booking_pace_report -t init
python -m app.etl.etl_booking_pace_report -t init_booking_pace_report_table
python -m app.etl.etl_booking_pace_report -t init_sp_fload_booking_pace_report
python -m app.etl.etl_booking_pace_report -t init_sp_iload_booking_pace_report
python -m app.etl.etl_booking_pace_report -t fload
python -m app.etl.etl_booking_pace_report -t iload

```

```bash
# Copy file backup từ Docker Container về Local
docker cp daa1a69bb1e5079b34025f594d86849a0217b1a571e24f9ea10844c0a2de493f:/var/opt/mssql/data/bim_report-202579-4-28-33.bak "./data/Sample Data"
```

d:\bim_etl\venv\Scripts\python.exe -m app.etl.iload
