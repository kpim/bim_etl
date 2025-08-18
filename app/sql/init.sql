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
