import argparse
from app.lib.connect_db import get_engine, get_connection


def init():
    init_booking_pace_report_table()
    init_sp_fload_booking_pace_report()
    init_sp_iload_booking_pace_report()


def init_booking_pace_report_table():
    conn = get_connection()
    cursor = conn.cursor()
    # -- tạo bảng dbo.booking_pace_report -- #
    sql = """
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
    """
    cursor.execute(sql)
    conn.commit()
    conn.close()


def init_sp_fload_booking_pace_report():
    # -- tạo store procedure sp_fload_booking_pace_report -- #
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
    CREATE OR ALTER PROCEDURE dbo.sp_fload_booking_pace_report AS
    BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRAN;
    BEGIN TRY;
        -- xóa toàn bộ dữ liệu của bảng
        TRUNCATE TABLE dbo.booking_pace_report;
        
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
    """
    cursor.execute(sql)
    conn.commit()

    conn.close()


def init_sp_iload_booking_pace_report():
    # -- tạo store procedure sp_iload_booking_pace_report -- #
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
    CREATE OR ALTER PROCEDURE dbo.sp_iload_booking_pace_report AS
    BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRAN;
    BEGIN TRY;
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
        SELECT REPORT_DATE, STAYING_DATE, PROPERTY, MARKET, R_TYPE, R_CHARGE, WINDOW, WINDOW_SORT, 
            SUM(N_OF_ROOM) AS TOTAL_ROOM, SUM(ROOM_REV) AS ROOM_REV, SUM(ARR) AS ARR, MAX(CREATED_AT) AS CREATED_AT, MAX(MODIFIED_AT) AS MODIFIED_AT
        FROM
        (SELECT d.REPORT_DATE, STAYING AS STAYING_DATE, d.PROPERTY, MARKET, R_TYPE, R_CHARGE, 
            N_OF_ROOM, ROOM_REV, ARR, d.CREATED_AT, d.MODIFIED_AT,
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
            FROM dbo.booking_pace_detail d
            JOIN @iload_data i ON d.PROPERTY = i.PROPERTY AND d.REPORT_DATE = i.REPORT_DATE
        ) r
        GROUP BY REPORT_DATE, STAYING_DATE, PROPERTY, MARKET, R_TYPE, R_CHARGE, WINDOW, WINDOW_SORT
        ORDER BY REPORT_DATE, STAYING_DATE, PROPERTY, MARKET, R_TYPE, R_CHARGE, WINDOW_SORT

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
    """
    cursor.execute(sql)
    conn.commit()

    conn.close()


def fload():
    print(f"Full Load dữ liệu vào bảng booking_pace_report")
    conn = get_connection()
    cursor = conn.cursor()

    sql = "EXEC dbo.sp_fload_booking_pace_report"
    cursor.execute(sql)
    conn.commit()
    conn.close()


def iload():
    print(f"Incremental Load dữ liệu vào bảng booking_pace_report")
    conn = get_connection()
    cursor = conn.cursor()

    sql = "EXEC dbo.sp_iload_booking_pace_report"
    cursor.execute(sql)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", "-t", help="", default="init")

    args = parser.parse_args()
    task = args.task

    if task == "init":
        init()
    elif task == "init_booking_pace_report_table":
        init_booking_pace_report_table()
    elif task == "init_sp_fload_booking_pace_report":
        init_sp_fload_booking_pace_report()
    elif task == "init_sp_iload_booking_pace_report":
        init_sp_iload_booking_pace_report()
    elif task == "fload":
        fload()
    elif task == "iload":
        iload()
