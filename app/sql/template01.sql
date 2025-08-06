USE bim_report
GO

/* --------------------------------------------------------------------------------- *
Template: 01
Property: Sailing Club Signature Resort Phu Quoc
*/
-- DROP TABLE stg.booking_pace_scsrpq
-- TRUNCATE TABLE stg.booking_pace_scsrpq

DROP TABLE IF EXISTS stg.booking_pace_scsrpq
CREATE TABLE stg.booking_pace_scsrpq (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    REPORT_DATE DATE,
    PROPERTY NVARCHAR(50),
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
    MODIFIED_AT DATETIME,
    FILE_NAME NVARCHAR(500)
);

/*
Sailing Club Signature Resort Phu Quoc	2024-07-27	5708
Sailing Club Signature Resort Phu Quoc	2024-07-28	5689
Sailing Club Signature Resort Phu Quoc	2024-07-29	5712
Sailing Club Signature Resort Phu Quoc	2025-07-27	5093
Sailing Club Signature Resort Phu Quoc	2025-07-28	5072
Sailing Club Signature Resort Phu Quoc	2025-07-29	5092
*/
SELECT PROPERTY, REPORT_DATE, COUNT(*) AS NB_ROWS
FROM bim_report.stg.booking_pace_scsrpq
GROUP BY PROPERTY, REPORT_DATE
ORDER BY PROPERTY, REPORT_DATE

SELECT TOP(100) * FROM bim_report.stg.booking_pace_scsrpq