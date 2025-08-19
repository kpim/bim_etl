/* --------------------------------------------------------------------------------- *
Init
- Tạo database và schema trước khi chạy code Python
- Tạo tài khoản có quyền owner để quản lý cơ sở dữ liệu
  + database: bim_report
  + user: etl
  + password: BIM@2025
- Tạo tài khoản có quyền đọc dữ liệu để cho phép hệ thống báo cáo Power BI kết nối tới cơ sở dữ liệu
  + database: bim_report
  + user: bi
  + password: BIM@2025
*/

CREATE DATABASE bim_report
GO

USE bim_report
GO

CREATE SCHEMA stg
GO

/* --------------------------------------------------------------------------------- *
-- Test
*/
-- DROP TABLE dbo.booking_sample_data
CREATE TABLE dbo.booking_sample_data (
    REPORT_DATE DATE, 
    STAY_MONTH NVARCHAR(7),
    PROPERTY NVARCHAR(50),
    ARRIVAL DATE, 
    DEPARTURE DATE,
    STAYING DATE,
    CREATE_DATE DATE,
    MARKET NVARCHAR(20),
    RATE_CODE NVARCHAR(20),
    RATE_AMT FLOAT,
    TOTAL_TURN_OVER DECIMAL(18,2),
    ARR DECIMAL(18,2),
    ROOM_REV DECIMAL(18,2),
    FB_REV DECIMAL(18,2),
    OTHER_REV DECIMAL(18,2),
    STATUS NVARCHAR(20),
    R_TYPE NVARCHAR(20),
    R_CHARGE NVARCHAR(20),
    N_OF_ROOM INT,
    N_OF_ADT INT,
    N_OF_CHD INT,
    BK_SOURCE NVARCHAR(20),
    COUNTRY NVARCHAR(20),
    NATIONALITY NVARCHAR(20)
);

/* --------------------------------------------------------------------------------- *
-- Xóa CSDL cũ
*/

/*
DROP DATABASE bim_report
*/

/*
USE bim_report
GO

DROP TABLE stg.booking_pace_scsrpq
DROP TABLE stg.booking_pace_src
DROP TABLE stg.booking_pace_cpv

DROP TABLE dbo.booking_pace_detail
DROP TABLE dbo.booking_pace_history
DROP TABLE dbo.booking_pace_report
DROP TABLE dbo.booking_pace_actual

DROP TABLE stg.exchange_rate

DROP PROCEDURE dbo.sp_fload_booking_pace_detail
DROP PROCEDURE dbo.sp_iload_booking_pace_detail

DROP PROCEDURE dbo.sp_fload_booking_pace_report
DROP PROCEDURE dbo.sp_iload_booking_pace_report
*/

SELECT PROPERTY, REPORT_DATE, COUNT(*) AS NB_ROWS
FROM dbo.booking_pace_detail
GROUP BY PROPERTY, REPORT_DATE
ORDER BY PROPERTY, REPORT_DATE

SELECT PROPERTY, REPORT_DATE, COUNT(*) AS NB_ROWS
FROM dbo.booking_pace_report
GROUP BY PROPERTY, REPORT_DATE
ORDER BY PROPERTY, REPORT_DATE

SELECT PROPERTY, STAYING, COUNT(*) AS NB_ROWS
FROM dbo.booking_pace_history
GROUP BY PROPERTY, STAYING
ORDER BY PROPERTY, STAYING

SELECT PROPERTY, STAYING_DATE, COUNT(*) AS NB_ROWS
FROM dbo.booking_pace_actual
GROUP BY PROPERTY, STAYING_DATE
ORDER BY PROPERTY, STAYING_DATE

SELECT * FROM dbo.booking_pace_detail
SELECT * FROM dbo.booking_pace_report
SELECT * FROM dbo.booking_pace_history
SELECT * FROM dbo.booking_pace_actual

SELECT * FROM dbo.booking_pace_detail WHERE PROPERTY = 'CPV'
SELECT * FROM dbo.booking_pace_history WHERE PROPERTY = 'CPV'
SELECT * FROM dbo.booking_pace_actual WHERE PROPERTY = 'CPV'


[SQL: INSERT INTO dbo.booking_pace_history 
([STAY_MONTH], [PROPERTY], [ARRIVAL], [DEPARTURE], [STAYING], [CREATE_DATE], [MARKET], [RATE_CODE], [RATE_AMT], [TOTAL_TURN_OVER], [ARR], [ROOM_REV], [FB_REV], [OTHER_REV], [STATUS], [R_TYPE], [R_CHARGE], [N_OF_RO ... 6413 characters truncated ... ?, ?, ?, ?, ?, ?, ?), 
(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)]
[parameters: ('2024-10', 'SCSRPQ', datetime.datetime(2024, 10, 28, 0, 0), datetime.datetime(2024, 11, 1, 0, 0), datetime.datetime(2024, 10, 30, 0, 0), datetime.datetime(2024, 10, 22, 0, 0), 'CW', 'VOUCH', 6180000.0, 11072972.54679803, 5449735.0, 4194666.54679803, 1428571.0, 0.0, 'Definite', 'SV3A', 'SV3A', 1.0, 6.0, 2.0, 'LTA', 'KOR', 'KOR', 0, datetime.datetime(2025, 8, 18, 18, 52, 54, 328883), datetime.datetime(2025, 8, 18, 17, 0, 14), '2024-10', 'SCSRPQ', datetime.datetime(2024, 10, 27, 0, 0), datetime.datetime(2024, 10, 31, 0, 0), datetime.datetime(2024, 10, 30, 0, 0), datetime.datetime(2024, 10, 24, 0, 0), 'CW', 'VOUCH', 6180000.0, 11072972.54679803, 5449735.0, 4766094.54679803, 857143.0, 0.0, 'Definite', 'SV3A', 'SV3A', 1.0, 6.0, 0.0, 'LTA', 'KOR', 'KOR', 0 ... 1980 parameters truncated ... datetime.datetime(2024, 10, 16, 0, 0), datetime.datetime(2024, 10, 16, 0, 0), datetime.datetime(2024, 10, 16, 0, 0), datetime.datetime(2024, 11, 10, 0, 0), 'TI', 'IRB1', 4248240.0, None, 3746243.3862433867, 3746243.3862433867, None, 0.0, 'Definite', 'SV2E', 'CV2F', 1.0, 2.0, 0.0, 'ota', 'KOR', 'KOR', 1, datetime.datetime(2025, 8, 18, 18, 52, 54, 328883), datetime.datetime(2025, 8, 18, 17, 0, 14), '2024-11', 'SCSRPQ', 45329, 45658, datetime.datetime(2024, 11, 1, 0, 0), datetime.datetime(1970, 1, 1, 0, 0), 'PP', 'RB3', 2419354.0, 4409218.0, 2204609.0, 2204609.0, 0.0, 0.0, 'Definite', 'PV3B', 'CV2E', 1.0, 3.0, 0.0, 'BIM', 'AUS', 'AUS', 0, datetime.datetime(2025, 8, 18, 18, 52, 54, 328883), datetime.datetime(2025, 8, 18, 17, 0, 14))]
