import argparse
from app.lib.connect_db import get_engine, get_connection


def init():
    init_booking_pace_report_table()
    init_booking_pace_actual_table()
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
        WINDOW_ID INT, 
        TOTAL_ROOM INT,
        ROOM_REV DECIMAL(18,2),
        ARR DECIMAL(18, 2),
        TOTAL_BOOKING INT,

        CREATED_AT DATETIME,
        MODIFIED_AT DATETIME
    )
    """
    cursor.execute(sql)
    conn.commit()
    conn.close()


def init_booking_pace_actual_table():
    conn = get_connection()
    cursor = conn.cursor()
    # -- tạo bảng dbo.booking_pace_actual -- #
    sql = """
    CREATE TABLE dbo.booking_pace_actual (
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

        TRUNCATE TABLE dbo.booking_pace_actual;
        
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
    """
    cursor.execute(sql)
    conn.commit()

    conn.close()


def fload():
    print(f"Full load data into table booking_pace_report")
    conn = get_connection()
    cursor = conn.cursor()

    sql = "EXEC dbo.sp_fload_booking_pace_report"
    cursor.execute(sql)
    conn.commit()
    conn.close()


def iload():
    print(f"Incremental load data into table booking_pace_report")
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
    elif task == "init_booking_pace_actual_table":
        init_booking_pace_actual_table()
    elif task == "init_sp_fload_booking_pace_report":
        init_sp_fload_booking_pace_report()
    elif task == "init_sp_iload_booking_pace_report":
        init_sp_iload_booking_pace_report()
    elif task == "fload":
        fload()
    elif task == "iload":
        iload()
