-- DROP TABLE stg.booking_pace_sailing_club_signature_resort_phu_quoc
-- TRUNCATE TABLE stg.booking_pace_sailing_club_signature_resort_phu_quoc

CREATE TABLE stg.booking_pace_sailing_club_signature_resort_phu_quoc (
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

SELECT PROPERTY, REPORT_DATE, COUNT(*) AS NB_ROWS
FROM bim_report.stg.booking_pace_sailing_club_signature_resort_phu_quoc
GROUP BY PROPERTY, REPORT_DATE

SELECT TOP(100) * FROM bim_report.stg.booking_pace_sailing_club_signature_resort_phu_quoc