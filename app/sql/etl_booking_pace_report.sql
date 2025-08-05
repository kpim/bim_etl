-- DROP TABLE dbo.booking_pace_report
-- TRUNCATE TABLE dbo.booking_pace_report;
CREATE TABLE dbo.booking_pace_report (
    REPORT_DATE DATE,
    STAYING_DATE DATE,
    PROPERTY NVARCHAR(50), 
    MARKET NVARCHAR(20),
    R_TYPE NVARCHAR(20),
    R_CHARGE NVARCHAR(20),
    WINDOW NVARCHAR(20),
    WINDOW_SORT INT, 
    TOTAL_ROOM INT,
    ROOM_REV DECIMAL(18,2),
    ARR DECIMAL(18, 2), 

    CREATED_AT DATETIME,
    MODIFIED_AT DATETIME
)

GO

/* ------------------------------------------------------------------------------------
-- Tổng hợp dữ liệu từ bảng raw gộp lại tổng hợp nhóm theo các chiều
*/
CREATE OR ALTER PROCEDURE dbo.sp_fload_booking_pace_report AS
BEGIN

SET NOCOUNT ON;
SET XACT_ABORT ON;

BEGIN TRAN;
BEGIN TRY;
    -- xóa toàn bộ dữ liệu của bảng
    TRUNCATE TABLE dbo.booking_pace_report;
    
    INSERT INTO dbo.booking_pace_report
    SELECT REPORT_DATE, STAYING AS STAYING_DATE, PROPERTY, MARKET, R_TYPE, R_CHARGE,WINDOW, WINDOW_SORT, 
        SUM(N_OF_ROOM) AS TOTAL_ROOM, SUM(ROOM_REV) AS ROOM_REV, SUM(ARR) AS ARR, MAX(CREATED_AT) AS CREATED_AT, MAX(MODIFIED_AT) AS MODIFIED_AT
    FROM
    (SELECT *, 
        DATEDIFF(DAY, CREATE_DATE, ARRIVAL) AS WINDOW_DAYS,
        CASE 
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL) <= 7 THEN '1 WEEK' 
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 14 THEN '2 WEEKS'
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 21 THEN '3 WEEKS'
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 30 THEN '4 WEEKS'
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 60 THEN '1-2 MONTHS'
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 90 THEN '2-3 MONTHS'
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 180 THEN '3-6 MONTHS'
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 365 THEN '6-12 MONTHS'
            ELSE '> 12 MONTHS'
        END AS [WINDOW], 
        CASE 
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 7 THEN 1
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 14 THEN 2
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 21 THEN 3
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 30 THEN 4
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 60 THEN 5
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 90 THEN 6
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 180 THEN 7
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 365 THEN 8
            ELSE 9
        END AS [WINDOW_SORT] 
        FROM dbo.booking_pace_detail
    ) r
    GROUP BY REPORT_DATE, STAYING, PROPERTY, MARKET, R_TYPE, R_CHARGE, WINDOW, WINDOW_SORT
    ORDER BY REPORT_DATE, STAYING, PROPERTY, MARKET, R_TYPE, R_CHARGE, WINDOW_SORT
    
    COMMIT
    RETURN 0 
END TRY
BEGIN CATCH 
    ROLLBACK 
    DECLARE @ERROR_MESSAGE NVARCHAR(2000)
    SELECT @ERROR_MESSAGE = 'ERROR:' + ERROR_MESSAGE()
    RAISERROR(@ERROR_MESSAGE, 16, 1)
END CATCH
END

-- EXEC dbo.sp_fload_booking_pace_report
GO

CREATE OR ALTER PROCEDURE dbo.sp_iload_booking_pace_report AS
BEGIN

SET NOCOUNT ON;
SET XACT_ABORT ON;

BEGIN TRAN;
BEGIN TRY;
    
    DECLARE @start_date DATE = DATEADD(DAY, -7, GETDATE())

    -- xóa dữ liệu trong 7 ngày gần nhất để chuẩn bị tính lại
    DELETE r 
    FROM dbo.booking_pace_report r
    WHERE REPORT_DATE >= @start_date
    
    INSERT INTO dbo.booking_pace_report
    SELECT REPORT_DATE, STAYING AS STAYING_DATE, PROPERTY, MARKET, R_TYPE, R_CHARGE, WINDOW, WINDOW_SORT, 
        SUM(N_OF_ROOM) AS TOTAL_ROOM, SUM(ROOM_REV) AS ROOM_REV, SUM(ARR) AS ARR, MAX(CREATED_AT) AS CREATED_AT, MAX(MODIFIED_AT) AS MODIFIED_AT
    FROM
    (SELECT *, 
        DATEDIFF(DAY, CREATE_DATE, ARRIVAL) AS WINDOW_DAYS,
        CASE 
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL) <= 7 THEN '1 WEEK' 
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 14 THEN '2 WEEKS'
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 21 THEN '3 WEEKS'
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 30 THEN '4 WEEKS'
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 60 THEN '1-2 MONTHS'
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 90 THEN '2-3 MONTHS'
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 180 THEN '3-6 MONTHS'
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 365 THEN '6-12 MONTHS'
            ELSE '> 12 MONTHS'
        END AS [WINDOW], 
        CASE 
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 7 THEN 1
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 14 THEN 2
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 21 THEN 3
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 30 THEN 4
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 60 THEN 5
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 90 THEN 6
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 180 THEN 7
            WHEN DATEDIFF(DAY, CREATE_DATE, ARRIVAL)  <= 365 THEN 8
            ELSE 9
        END AS [WINDOW_SORT] 
        FROM dbo.booking_pace_detail
        WHERE REPORT_DATE >= @start_date
    ) r
    GROUP BY REPORT_DATE, STAYING, PROPERTY, MARKET, R_TYPE, R_CHARGE, WINDOW, WINDOW_SORT
    ORDER BY REPORT_DATE, STAYING, PROPERTY, MARKET, R_TYPE, R_CHARGE, WINDOW_SORT
    
    COMMIT
    RETURN 0 
END TRY
BEGIN CATCH 
    ROLLBACK 
    DECLARE @ERROR_MESSAGE NVARCHAR(2000)
    SELECT @ERROR_MESSAGE = 'ERROR:' + ERROR_MESSAGE()
    RAISERROR(@ERROR_MESSAGE, 16, 1)
END CATCH
END

-- EXEC dbo.sp_iload_booking_pace_report
GO
/* ------------------------------------------------------------------------------------
-- Kiểm tra dữ liệu trong bảng
*/
SELECT PROPERTY, MIN(REPORT_DATE) AS MIN_DATE, MAX(REPORT_DATE) AS MAX_DATE, COUNT(*) AS NB_ROWS
FROM dbo.booking_pace_report
GROUP BY PROPERTY
ORDER BY PROPERTY

/*
Crowne Plaza Vientaine	2024-07-27	1480
Crowne Plaza Vientaine	2024-07-28	1467
Crowne Plaza Vientaine	2024-07-29	1472
Crowne Plaza Vientaine	2025-07-27	1476
Crowne Plaza Vientaine	2025-07-28	1463
Crowne Plaza Vientaine	2025-07-29	1429
Sailing Club Signature Resort Phu Quoc	2024-07-27	4454
Sailing Club Signature Resort Phu Quoc	2024-07-28	4448
Sailing Club Signature Resort Phu Quoc	2024-07-29	4452
Sailing Club Signature Resort Phu Quoc	2025-07-27	3864
Sailing Club Signature Resort Phu Quoc	2025-07-28	3856
Sailing Club Signature Resort Phu Quoc	2025-07-29	3860
Syrena Cruises	2025-07-23	947
Syrena Cruises	2025-07-24	952
*/
SELECT PROPERTY, REPORT_DATE, COUNT(*) AS NB_ROWS
FROM dbo.booking_pace_report
GROUP BY PROPERTY, REPORT_DATE
ORDER BY PROPERTY, REPORT_DATE

SELECT TOP(100) * FROM dbo.booking_pace_report
/* ------------------------------------------------------------------------------------
-- Test
*/

/* ------------------------------------------------------------------------------------
-- Test
*/
SELECT * FROM dbo.booking_pace_detail

---- giả lập dữ liệu quá khứ
SELECT MIN(STAYING_DATE), MAX(STAYING_DATE) -- 2023-01-01	2024-12-31
FROM dbo.booking_pace_report

SELECT * FROM dbo.booking_pace_report WHERE REPORT_DATE = '2022-01-01'

SELECT REPORT_DATE FROM dbo.booking_pace_report GROUP BY REPORT_DATE
-- UPDATE dbo.booking_pace_report SET REPORT_DATE = '2023-01-01'
-- DELETE dbo.booking_pace_report WHERE REPORT_DATE = '2023-05-02'
-- DELETE dbo.booking_pace_report WHERE REPORT_DATE = '2022-01-01'

DECLARE @report_date DATE = (SELECT MIN(STAYING_DATE) FROM dbo.booking_pace_report)
DECLARE @current_date DATE = DATEADD(YEAR, -1, @report_date)
DECLARE @date_diff INT, @rand FLOAT

-- SELECT @report_date, @current_date

-- WHILE @current_date < @report_date
WHILE @current_date < '2022-01-02'
BEGIN 
    SET @date_diff = DATEDIFF(DAY, @current_date, @report_date)
    
    INSERT INTO dbo.booking_pace_report
    SELECT REPORT_DATE, STAYING_DATE, PROPERTY, MARKET, WINDOW, WINDOW_SORT, FLOOR(TOTAL_ROOM * r) AS TOTAL_ROOM, ROOM_REV * r AS ROOM_REV
    FROM
    (SELECT @current_date AS REPORT_DATE, DATEADD(DAY, -@date_diff, @report_date) STAYING_DATE, 
        PROPERTY, MARKET, WINDOW, WINDOW_SORT, TOTAL_ROOM, ROOM_REV, RAND(CHECKSUM(NEWID())) + 1 AS r 
        FROM dbo.booking_pace_report
        WHERE REPORT_DATE = @report_date
    ) r

    SET @current_date = DATEADD(DAY, 1, @current_date)
END

SELECT RAND(CHECKSUM(NEWID())) * 2

--- 
SELECT *
FROM dbo.booking_pace_sample_data

DELETE dbo.booking_pace_sample_data WHERE REPORT_DATE = '2023-05-02'
UPDATE dbo.booking_pace_sample_data SET REPORT_DATE = '2023-01-01'