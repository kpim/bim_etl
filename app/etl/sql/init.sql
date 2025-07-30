CREATE DATABASE bim_report;

USE bim_report
GO

CREATE SCHEMA stg;
GO

/*
DROP TABLE stg.booking_pace_sailing_club_signature_resort_phu_quoc
DROP TABLE stg.booking_pace_syrena_cruises
DROP TABLE dbo.booking_pace_detail
DROP TABLE dbo.booking_pace_report 
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

SELECT * FROM dbo.booking_pace_detail;

SELECT * FROM dbo.booking_pace_sample_data;

SELECT * FROM dbo.booking_pace_report;

SELECT COUNT(*) FROM dbo.booking_pace_report WITH (NOLOCK);

