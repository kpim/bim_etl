-- DROP TABLE dbo.booking_pace_report
-- TRUNCATE TABLE dbo.booking_pace_report;
CREATE TABLE dbo.booking_pace_report (
    REPORT_DATE DATE,
    STAYING_DATE DATE,
    PROPERTY NVARCHAR(50), 
    MARKET NVARCHAR(20),
    R_TYPE NVARCHAR(20),
    R_CHARGE NVARCHAR(20),
    WINDOW_ID INT,
    TOTAL_ROOM INT,
    ROOM_REV DECIMAL(18,2),
    ARR DECIMAL(18, 2), 
    TOTAL_BOOKING INT,

    CREATED_AT DATETIME,
    MODIFIED_AT DATETIME
)

GO

/* ------------------------------------------------------------------------------------
-- dbo.sp_fload_booking_pace_report
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
    SELECT REPORT_DATE, STAYING AS STAYING_DATE, PROPERTY, MARKET, R_TYPE, R_CHARGE, w.ID AS WINDOW_ID, 
        SUM(N_OF_ROOM) AS TOTAL_ROOM, SUM(ROOM_REV) AS ROOM_REV, SUM(ARR) AS ARR, 
        SUM(BOOKING * N_OF_ROOM) AS TOTAL_BOOKING,
        MAX(CREATED_AT) AS CREATED_AT, MAX(MODIFIED_AT) AS MODIFIED_AT
    FROM
    (SELECT *, 
        DATEDIFF(DAY, CREATE_DATE, ARRIVAL) AS WINDOW_DAYS
        FROM dbo.booking_pace_detail
    ) d LEFT JOIN dbo.window w ON d.WINDOW_DAYS >= w.[FROM] AND d.WINDOW_DAYS <= w.[TO]
    GROUP BY REPORT_DATE, STAYING, PROPERTY, MARKET, R_TYPE, R_CHARGE, w.ID
    ORDER BY REPORT_DATE, STAYING, PROPERTY, MARKET, R_TYPE, R_CHARGE, WINDOW_ID

    INSERT INTO dbo.booking_pace_actual
    SELECT STAYING AS STAYING_DATE, PROPERTY, MARKET, R_TYPE, R_CHARGE, w.ID AS WINDOW_ID, 
        SUM(N_OF_ROOM) AS TOTAL_ROOM, SUM(ROOM_REV) AS ROOM_REV, SUM(ARR) AS ARR, 
        SUM(BOOKING * N_OF_ROOM) AS TOTAL_BOOKING,
        MAX(CREATED_AT) AS CREATED_AT, MAX(MODIFIED_AT) AS MODIFIED_AT
    FROM
    (SELECT *, 
        DATEDIFF(DAY, CREATE_DATE, ARRIVAL) AS WINDOW_DAYS
        FROM dbo.booking_pace_history
    ) d LEFT JOIN dbo.window w ON d.WINDOW_DAYS >= w.[FROM] AND d.WINDOW_DAYS <= w.[TO]
    GROUP BY STAYING, PROPERTY, MARKET, R_TYPE, R_CHARGE, w.ID
    ORDER BY STAYING, PROPERTY, MARKET, R_TYPE, R_CHARGE, WINDOW_ID

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

/* ------------------------------------------------------------------------------------
-- dbo.sp_iload_booking_pace_report
*/
CREATE OR ALTER PROCEDURE dbo.sp_iload_booking_pace_report AS
BEGIN

SET NOCOUNT ON;
SET XACT_ABORT ON;

BEGIN TRAN;
BEGIN TRY;
    DECLARE @today DATE = GETDATE();

    -- lấy thông tin dữ liệu sẽ bổ sung vào bảng đích
    DECLARE @iload_data TABLE (
        ID INT IDENTITY(1, 1),
        PROPERTY NVARCHAR(50),
        REPORT_DATE DATE, 
        MODIFIED_AT DATETIME
    )

    ;WITH s AS (
        SELECT PROPERTY, REPORT_DATE, MAX(MODIFIED_AT) AS MODIFIED_AT
        FROM dbo.booking_pace_detail
        GROUP BY PROPERTY, REPORT_DATE
    ), d AS (
        SELECT PROPERTY, REPORT_DATE, MAX(MODIFIED_AT) AS MODIFIED_AT
        FROM dbo.booking_pace_report
        GROUP BY PROPERTY, REPORT_DATE
    )
    INSERT @iload_data(PROPERTY, REPORT_DATE, MODIFIED_AT)
    SELECT s.PROPERTY, s.REPORT_DATE, s.MODIFIED_AT
    FROM s 
    LEFT JOIN d ON s.PROPERTY = d.PROPERTY AND s.REPORT_DATE = d.REPORT_DATE
    WHERE d.MODIFIED_AT IS NULL OR s.MODIFIED_AT > d.MODIFIED_AT

    -- xóa dữ liệu cũ trong bảng đích
    DELETE d
    FROM dbo.booking_pace_report d 
    JOIN @iload_data i ON d.PROPERTY = i.PROPERTY AND d.REPORT_DATE = i.REPORT_DATE

    -- đưa vào dữ liệu bổ sung vào bảng đích
    INSERT INTO dbo.booking_pace_report
    SELECT REPORT_DATE, STAYING_DATE, PROPERTY, MARKET, R_TYPE, R_CHARGE, w.ID AS WINDOW_ID, 
        SUM(N_OF_ROOM) AS TOTAL_ROOM, SUM(ROOM_REV) AS ROOM_REV, SUM(ARR) AS ARR, 
        SUM(BOOKING * N_OF_ROOM) AS TOTAL_BOOKING,
        MAX(CREATED_AT) AS CREATED_AT, MAX(MODIFIED_AT) AS MODIFIED_AT
    FROM
    (SELECT d.REPORT_DATE, STAYING AS STAYING_DATE, d.PROPERTY, MARKET, R_TYPE, R_CHARGE, 
        N_OF_ROOM, ROOM_REV, ARR, BOOKING,
        d.CREATED_AT, d.MODIFIED_AT, 
        DATEDIFF(DAY, CREATE_DATE, ARRIVAL) AS WINDOW_DAYS
        FROM dbo.booking_pace_detail d 
        JOIN @iload_data i ON d.PROPERTY = i.PROPERTY AND d.REPORT_DATE = i.REPORT_DATE
    ) d 
    LEFT JOIN dbo.window w ON d.WINDOW_DAYS >= w.[FROM] AND d.WINDOW_DAYS <= w.[TO]
    GROUP BY REPORT_DATE, STAYING_DATE, PROPERTY, MARKET, R_TYPE, R_CHARGE, w.ID
    ORDER BY REPORT_DATE, STAYING_DATE, PROPERTY, MARKET, R_TYPE, R_CHARGE, WINDOW_ID

    -- xóa dữ liệu cũ của ngày hôm qua trong bảng booking_pace_actual
    DELETE d 
    FROM dbo.booking_pace_actual d 
    WHERE STAYING_DATE = DATEADD(DAY, -1, @today)

    -- thêm dữ liệu actual của ngày hôm qua vào bảng booking_pace_actual
    INSERT INTO dbo.booking_pace_actual
    SELECT STAYING_DATE, PROPERTY, MARKET, R_TYPE, R_CHARGE, w.ID AS WINDOW_ID, 
        SUM(N_OF_ROOM) AS TOTAL_ROOM, SUM(ROOM_REV) AS ROOM_REV, SUM(ARR) AS ARR, 
        SUM(BOOKING * N_OF_ROOM) AS TOTAL_BOOKING,
        MAX(CREATED_AT) AS CREATED_AT, MAX(MODIFIED_AT) AS MODIFIED_AT
    FROM
    (SELECT STAYING AS STAYING_DATE, PROPERTY, MARKET, R_TYPE, R_CHARGE, 
        N_OF_ROOM, ROOM_REV, ARR, BOOKING,
        CREATED_AT, MODIFIED_AT, 
        DATEDIFF(DAY, CREATE_DATE, ARRIVAL) AS WINDOW_DAYS
        FROM dbo.booking_pace_history
        WHERE STAYING = DATEADD(DAY, -1, @today)
    ) d 
    LEFT JOIN dbo.window w ON d.WINDOW_DAYS >= w.[FROM] AND d.WINDOW_DAYS <= w.[TO]
    GROUP BY STAYING_DATE, PROPERTY, MARKET, R_TYPE, R_CHARGE, w.ID
    ORDER BY STAYING_DATE, PROPERTY, MARKET, R_TYPE, R_CHARGE, WINDOW_ID

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
Sailing Club Signature Resort Phu Quoc	2025-08-01	4361
Sailing Club Signature Resort Phu Quoc	2025-08-02	4366
Sailing Club Signature Resort Phu Quoc	2025-08-03	4370
Sailing Club Signature Resort Phu Quoc	2025-08-04	4297
Sailing Club Signature Resort Phu Quoc	2025-08-05	4307
Soul Boutique Hotel Phu Quoc	2025-08-01	1194
Soul Boutique Hotel Phu Quoc	2025-08-02	1217
Soul Boutique Hotel Phu Quoc	2025-08-03	1256
Soul Boutique Hotel Phu Quoc	2025-08-04	1278
Soul Boutique Hotel Phu Quoc	2025-08-05	1319
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

/*-- Old --*/
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
