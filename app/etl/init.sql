CREATE DATABASE bim_report;

USE bim_report
GO

CREATE SCHEMA stg;
GO

-- DROP TABLE stg.booking_pace_p1;
CREATE TABLE stg.booking_pace_p1 (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    REPORT_DATE DATE,
    PROPERTY NVARCHAR(50),
    CONSIDERED_DATE DATE,
    ADULTS INT,
    CHILDREN INT,
    CREATED_DATE DATE,
    CREATED_USER NVARCHAR(50),
    COUNTRY NVARCHAR(10),
    NO_ROOMS INT,
    DEDUCT_YN CHAR(1),
    GROUP_YN CHAR(1),
    ROOM_REVENUE DECIMAL(18, 2),
    RATE_USD FLOAT,
    MARKET_CODE NVARCHAR(20),
    SOURCE_CODE NVARCHAR(20),
    CHANNEL NVARCHAR(20),
    RATE_CODE NVARCHAR(20),
    ROOM_CAT NVARCHAR(20),
    RTC NVARCHAR(20),
    ARR DATE,
    DEP DATE,
    RESV_NAME_ID INT,
    ROOM_REVENUE_1 DECIMAL(18, 2),
    FOOD_REVENUE DECIMAL(18, 2),
    PACKAGE_ROOM_REVENUE DECIMAL(18, 2),
    PACKAGE_FOOD_REVENUE DECIMAL(18, 2),
    TOTAL_PACKAGE_REVENUE DECIMAL(18, 2),
    OTHER_REVENUE DECIMAL(18, 2),
    PACKAGE_OTHER_REVENUE DECIMAL(18, 2),
    NON_REVENUE DECIMAL(18, 2),
    PACKAGE_NON_REVENUE DECIMAL(18, 2),
    TOTAL_NON_REVENUE_TAX DECIMAL(18, 2),
    PR_ROOM_REVENUE DECIMAL(18, 2),
    PR_FOOD_REVENUE DECIMAL(18, 2),
    PR_PACKAGE_ROOM_REVENUE DECIMAL(18, 2),
    PR_PACKAGE_FOOD_REVENUE DECIMAL(18, 2),
    PR_TOTAL_PACKAGE_REVENUE DECIMAL(18, 2),
    PR_TOTAL_REVENUE DECIMAL(18, 2),
    PR_OTHER_REVENUE DECIMAL(18, 2),
    PR_PACKAGE_OTHER_REVENUE DECIMAL(18, 2),
    PR_NON_REVENUE DECIMAL(18, 2),
    PR_PACKAGE_NON_REVENUE DECIMAL(18, 2),
    PR_TOTAL_NON_REVENUE_TAX DECIMAL(18, 2),
    CASH_ROOM_REVENUE DECIMAL(18, 2),
    COMP_ROOM_REVENUE DECIMAL(18, 2),

    CREATED_AT DATETIME,
    MODIFIED_AT DATETIME
);

-- DROP TABLE stg.booking_pace_p2;
CREATE TABLE stg.booking_pace_p2 (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    REPORT_DATE DATE,
    PROPERTY NVARCHAR(50),
    FOLIONUM INT,
    ARRIVAL DATE,
    DEPARTURE DATE,
    STAYING DATE,
    CREATE_TIME DATETIME,
    GROUP_CODE NVARCHAR(20),
    GROUP_NAME NVARCHAR(50),
    TA NVARCHAR(50),
    TA_ID INT,
    GUEST_NAME NVARCHAR(500),
    MARKET NVARCHAR(20),
    RATE_CODE NVARCHAR(20),
    RATE_AMT FLOAT,
    PACKAGE_CODE NVARCHAR(20),
    PACKAGE_AMOUNT DECIMAL(18,2),
    ARR DECIMAL(18,2),
    ROOM_REV DECIMAL(18,2),
    FB_REV DECIMAL(18,2),
    OTHER_REV DECIMAL(18,2),
    STATUS NVARCHAR(20),
    R_TYPE NVARCHAR(20),
    R_CHARGE NVARCHAR(20),
    R_SURCHARGE NVARCHAR(20),
    N_OF_ROOM INT,
    N_OF_ADT INT,
    N_OF_CHD INT,
    BK_SOURCE NVARCHAR(20),
    COUNTRY NVARCHAR(20),
    NATIONALITY NVARCHAR(20),

    CREATED_AT DATETIME,
    MODIFIED_AT DATETIME
);

-- DROP TABLE stg.booking_pace_syrena_cruises;
-- TRUNCATE TABLE stg.booking_pace_syrena_cruises;
CREATE TABLE stg.booking_pace_syrena_cruises (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    REPORT_DATE DATE,
    PROPERTY NVARCHAR(50),
    FOLIONUM INT,
    ARRIVAL DATE,
    DEPARTURE DATE,
    STAYING DATE,
    CREATE_TIME DATETIME,
    GROUP_CODE NVARCHAR(20),
    GROUP_NAME NVARCHAR(50),
    TA NVARCHAR(50),
    TA_ID INT,
    GUEST_NAME NVARCHAR(500),
    MARKET NVARCHAR(20),
    RATE_CODE NVARCHAR(20),
    RATE_AMT FLOAT,
    PACKAGE_CODE NVARCHAR(20),
    PACKAGE_AMOUNT DECIMAL(18,2),
    ARR DECIMAL(18,2),
    ROOM_REV DECIMAL(18,2),
    FB_REV DECIMAL(18,2),
    OTHER_REV DECIMAL(18,2),
    STATUS NVARCHAR(20),
    R_TYPE NVARCHAR(20),
    R_CHARGE NVARCHAR(20),
    R_SURCHARGE NVARCHAR(20),
    N_OF_ROOM INT,
    N_OF_ADT INT,
    N_OF_CHD INT,
    BK_SOURCE NVARCHAR(20),
    COUNTRY NVARCHAR(20),
    NATIONALITY NVARCHAR(20),

    CREATED_AT DATETIME,
    MODIFIED_AT DATETIME
);

-- DROP TABLE stg.booking_pace_p3;
CREATE TABLE stg.booking_pace_p3 (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    REPORT_DATE DATE,
    PROPERTY NVARCHAR(50),
    BOOKING_DATE INT,
    BOOKING_MONTH INT,
    BOOKING_YEAR INT,
    ARRIVAL_DATE INT,
    ARRIVAL_MONTH INT,
    ARRIVAL_YEAR INT,
    STAY_DATE INT,
    STAY_MONTH INT,
    STAY_YEAR INT,
    DOW NVARCHAR(20),
    ARRIVAL_ROOM INT,
    FOLIONUM INT,
    ARRIVAL DATE,
    DEPARTURE DATE,
    STAYING DATE,
    CREATE_TIME DATETIME,
    GROUP_CODE NVARCHAR(20),
    TA NVARCHAR(50),
    TA_ID INT,
    GUEST_NAME NVARCHAR(500),
    MARKET NVARCHAR(20),
    RATE_CODE NVARCHAR(20),
    RATE_AMT FLOAT,
    PACKAGE_CODE NVARCHAR(20),
    TOTAL_TURN_OVER DECIMAL(18,2),
    ARR DECIMAL(18,2),
    ROOM_REV DECIMAL(18,2),
    FB_REV DECIMAL(18,2),
    OTHER_REV DECIMAL(18,2),
    STATUS NVARCHAR(20),
    R_TYPE NVARCHAR(20),
    R_CHARGE NVARCHAR(20),
    R_SURCHARGE NVARCHAR(20),
    N_OF_ROOM INT,
    N_OF_ADT INT,
    N_OF_CHD INT,
    BK_SOURCE NVARCHAR(20),
    COUNTRY NVARCHAR(20),
    NATIONALITY NVARCHAR(20), 

    CREATED_AT DATETIME,
    MODIFIED_AT DATETIME
);

-- DROP TABLE dbo.booking_pace_detail
-- TRUNCATE TABLE dbo.booking_pace_detail;
CREATE TABLE dbo.booking_pace_detail (
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
    NATIONALITY NVARCHAR(20), 

    CREATED_AT DATETIME,
    MODIFIED_AT DATETIME
);

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

-- DROP TABLE dbo.booking_pace_report
-- TRUNCATE TABLE dbo.booking_pace_report;
CREATE TABLE dbo.booking_pace_report (
    REPORT_DATE DATE,
    STAYING_DATE DATE,
    PROPERTY NVARCHAR(50), 
    MARKET NVARCHAR(20),
    WINDOW NVARCHAR(20),
    WINDOW_SORT INT, 
    TOTAL_ROOM INT,
    ROOM_REV DECIMAL(18,2),
    ARR DECIMAL(18, 2)
)

SELECT * FROM stg.booking_pace_p1;

SELECT * FROM stg.booking_pace_p2;

SELECT * FROM stg.booking_pace_syrena_cruises;

SELECT * FROM stg.booking_pace_p3;

SELECT * FROM dbo.booking_pace_detail;

SELECT * FROM dbo.booking_pace_sample_data;


SELECT * FROM dbo.booking_pace_report;

SELECT COUNT(*) FROM dbo.booking_pace_report WITH (NOLOCK);

-- TRUNCATE TABLE dbo.booking_pace_detail;
-- TRUNCATE TABLE dbo.booking_pace_report;