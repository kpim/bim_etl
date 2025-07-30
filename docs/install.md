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

```bash
python -m app.etl.init
python -m app.etl.fload

python -m app.etl.etl_template01
python -m app.etl.etl_template02
python -m app.etl.etl_template03
python -m app.etl.etl_booking_pace_detail
python -m app.etl.etl_booking_pace_report


```

```bash
# Copy file backup từ Docker Container về Local
docker cp daa1a69bb1e5079b34025f594d86849a0217b1a571e24f9ea10844c0a2de493f:/var/opt/mssql/data/bim_report-202579-4-28-33.bak "./data/Sample Data"
```

Tao database
bim_report

Tk ket noi toi
etl
BIM@2025

bi
BIM@2025

d:\bim_etl\venv\Scripts\python.exe -m app.etl.iload
