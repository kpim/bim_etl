from app.lib.connect_db import get_engine, get_connection


def init():
    conn = get_connection()
    cursor = conn.cursor()
    # -- tạo bảng dbo.booking_pace_report -- #
    sql = """
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
    """
    cursor.execute(sql)
    conn.commit()

    # -- tạo store procedure sp_fload_booking_pace_report -- #
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
        SELECT REPORT_DATE, STAYING AS STAYING_DATE, PROPERTY, MARKET, WINDOW, WINDOW_SORT, 
            SUM(N_OF_ROOM) AS TOTAL_ROOM, SUM(ROOM_REV) AS ROOM_REV, SUM(ARR) AS ARR
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
        GROUP BY REPORT_DATE, STAYING, PROPERTY, MARKET, WINDOW, WINDOW_SORT
        ORDER BY REPORT_DATE, STAYING, PROPERTY, MARKET, WINDOW_SORT
        
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

    # -- tạo store procedure sp_iload_booking_pace_report -- #
    sql = """
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
        SELECT REPORT_DATE, STAYING AS STAYING_DATE, PROPERTY, MARKET, WINDOW, WINDOW_SORT, 
            SUM(N_OF_ROOM) AS TOTAL_ROOM, SUM(ROOM_REV) AS ROOM_REV, SUM(ARR) AS ARR
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
        GROUP BY REPORT_DATE, STAYING, PROPERTY, MARKET, WINDOW, WINDOW_SORT
        ORDER BY REPORT_DATE, STAYING, PROPERTY, MARKET, WINDOW_SORT
        
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
    )
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
    fload()
