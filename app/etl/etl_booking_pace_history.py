import argparse
from datetime import datetime

from app.lib.connect_db import get_engine, get_connection
import app.etl.etl_smile_pq as etl_smile_pq
import app.etl.etl_smile_hl as etl_smile_hl
import app.etl.etl_opera as etl_opera


def init():
    init_sp_restore_booking_pace_history()


def init_sp_restore_booking_pace_history():
    # -- tạo store procedure sp_restore_booking_pace_history -- #
    conn = get_connection()
    cursor = conn.cursor()
    sql = """
    CREATE OR ALTER PROCEDURE dbo.sp_restore_booking_pace_history 
        @start_date DATE,
        @end_date DATE
    AS
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
        
    """
    for property in etl_smile_pq.PROPERTIES + etl_smile_hl.PROPERTIES:
        sql += f"""
    ;WITH s AS (
        SELECT PROPERTY, REPORT_DATE, DATEADD(DAY, -1, REPORT_DATE) AS STAYING
        FROM {property["schema"]}.{property["table"]}
        WHERE REPORT_DATE >= @start_date AND REPORT_DATE <= @end_date
        GROUP BY PROPERTY, REPORT_DATE
    ), h AS (
        SELECT PROPERTY, STAYING
        FROM dbo.booking_pace_history
        WHERE PROPERTY= N'{property["code"]}'
        GROUP BY PROPERTY, STAYING
    )

    INSERT INTO @iload_data(PROPERTY, REPORT_DATE)
    SELECT s.PROPERTY, s.REPORT_DATE
    FROM s 
    LEFT JOIN h ON s.PROPERTY = h.PROPERTY AND s.STAYING = h.STAYING
    WHERE h.STAYING IS NULL
    ORDER BY s.PROPERTY, s.REPORT_DATE

    -- thêm vào bảng history dữ liệu acutal của ngày STAYING_DATE = REPORT_DATE - 1
    INSERT INTO dbo.booking_pace_history
    SELECT FORMAT(STAYING, 'yyyy-MM') AS STAY_MONTH, s.PROPERTY, 
    ARRIVAL, DEPARTURE, STAYING, CONVERT(DATE, CREATE_TIME) AS CREATE_DATE,
    MARKET, RATE_CODE, RATE_AMT, 
    ISNULL(ARR, 0) + ISNULL(ROOM_REV, 0) + ISNULL(FB_REV, 0) + ISNULL(OTHER_REV, 0) AS TOTAL_TURN_OVER, 
    ARR, ROOM_REV, FB_REV, OTHER_REV,
    [STATUS], R_TYPE, R_CHARGE,
    N_OF_ROOM, N_OF_ADT, N_OF_CHD, 
    BK_SOURCE, COUNTRY, NATIONALITY, 
    CASE WHEN STAYING = ARRIVAL THEN 1 ELSE 0 END AS BOOKING,
    s.CREATED_AT, s.MODIFIED_AT, FILE_NAME
    FROM {property["schema"]}.{property["table"]} s
    JOIN @iload_data i ON s.PROPERTY = i.PROPERTY AND s.REPORT_DATE = i.REPORT_DATE 
        AND STAYING = DATEADD(DAY, -1, s.REPORT_DATE)
    
    DELETE FROM @iload_data;
    """

    for property in etl_opera.PROPERTIES:
        sql += f"""
    ;WITH s AS (
        SELECT PROPERTY, REPORT_DATE, DATEADD(DAY, -1, REPORT_DATE) AS STAYING
        FROM {property["schema"]}.{property["table"]}
        WHERE REPORT_DATE >= @start_date AND REPORT_DATE <= @end_date
        GROUP BY PROPERTY, REPORT_DATE
    ), h AS (
        SELECT PROPERTY, STAYING
        FROM dbo.booking_pace_history
        WHERE PROPERTY= N'{property["code"]}'
        GROUP BY PROPERTY, STAYING
    )

    INSERT INTO @iload_data(PROPERTY, REPORT_DATE)
    SELECT s.PROPERTY, s.REPORT_DATE
    FROM s 
    LEFT JOIN h ON s.PROPERTY = h.PROPERTY AND s.STAYING = h.STAYING
    WHERE h.STAYING IS NULL
    ORDER BY s.PROPERTY, s.REPORT_DATE

    -- thêm vào bảng history dữ liệu acutal của ngày STAYING_DATE = REPORT_DATE - 1
    INSERT INTO dbo.booking_pace_history
    SELECT FORMAT(CONSIDERED_DATE, 'yyyy-MM') AS STAY_MONTH, s.PROPERTY, 
    ARR AS ARRIVAL, DEP AS DEPARTURE, CONSIDERED_DATE AS STAYING, CREATED_DATE AS CREATE_DATE,
    MARKET_CODE AS MARKET, RATE_CODE, NULL AS RATE_AMT, 
    ISNULL(ROOM_REVENUE, 0) + ISNULL(FOOD_REVENUE, 0) + ISNULL(OTHER_REVENUE, 0) AS TOTAL_TURN_OVER, 
    NULL AS ARR, ROOM_REVENUE AS ROOM_REV, FOOD_REVENUE AS FB_REV, OTHER_REVENUE AS OTHER_REV,
    NULL AS [STATUS], ROOM_CAT AS R_TYPE, RCT AS R_CHARGE,
    NO_ROOMS AS N_OF_ROOM, ADULTS AS N_OF_ADT, CHILDREN AS N_OF_CHD, 
    SOURCE_CODE AS BK_SOURCE, COUNTRY, COUNTRY AS NATIONALITY, 
    CASE WHEN CONSIDERED_DATE = ARR THEN 1 ELSE 0 END AS BOOKING,
    s.CREATED_AT, s.MODIFIED_AT, FILE_NAME
    FROM {property["schema"]}.{property["table"]} s
    JOIN @iload_data i ON s.PROPERTY = i.PROPERTY AND s.REPORT_DATE = i.REPORT_DATE
        AND CONSIDERED_DATE = DATEADD(DAY, -1, s.REPORT_DATE)

    DELETE FROM @iload_data;
    """
    sql += """
    ;WITH s AS (
        SELECT PROPERTY, STAYING
        FROM dbo.booking_pace_history
        WHERE DATEADD(DAY, 1, STAYING) >= @start_date AND DATEADD(DAY, 1, STAYING) <= @end_date
        GROUP BY PROPERTY, STAYING
    ), t AS (
        SELECT PROPERTY, STAYING_DATE
        FROM dbo.booking_pace_actual
        GROUP BY  PROPERTY, STAYING_DATE
    )
    INSERT INTO @iload_data(PROPERTY, REPORT_DATE)
    SELECT s.PROPERTY, s.STAYING
    FROM s 
    LEFT JOIN t ON s.PROPERTY = t.PROPERTY AND s.STAYING = t.STAYING_DATE
    WHERE t.STAYING_DATE IS NULL
    ORDER BY s.PROPERTY, s.STAYING

    -- SELECT * FROM @iload_data

    INSERT INTO dbo.booking_pace_actual
    SELECT STAYING AS STAYING_DATE, PROPERTY, MARKET, R_TYPE, R_CHARGE, w.ID AS WINDOW_ID, 
        SUM(N_OF_ROOM) AS TOTAL_ROOM, SUM(ROOM_REV) AS ROOM_REV, SUM(ARR) AS ARR, 
        SUM(BOOKING * N_OF_ROOM) AS TOTAL_BOOKING,
        MAX(CREATED_AT) AS CREATED_AT, MAX(MODIFIED_AT) AS MODIFIED_AT
    FROM
    (SELECT h.*, 
        DATEDIFF(DAY, CREATE_DATE, ARRIVAL) AS WINDOW_DAYS
        FROM dbo.booking_pace_history h
        JOIN @iload_data i ON h.PROPERTY = i.PROPERTY AND h.STAYING = i.REPORT_DATE
    ) d 
    LEFT JOIN dbo.window w ON d.WINDOW_DAYS >= w.[FROM] AND d.WINDOW_DAYS <= w.[TO]
    GROUP BY STAYING, PROPERTY, MARKET, R_TYPE, R_CHARGE, w.ID
    ORDER BY STAYING, PROPERTY, MARKET, R_TYPE, R_CHARGE, WINDOW_ID
    """
    sql += """
    
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
    -- EXEC dbo.sp_restore_booking_pace_history
    """

    # print(sql)
    cursor.execute(sql)
    conn.commit()

    conn.close()


def restore_history(start_date, end_date):
    print()
    print(f"Restore booking pace history: {start_date}, {end_date}")
    conn = get_connection()
    cursor = conn.cursor()

    sql = "EXEC dbo.sp_restore_booking_pace_history @start_date = ?, @end_date = ?"
    cursor.execute(sql, start_date, end_date)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", "-t", help="", default="restore_history")
    parser.add_argument("--start_date", help="")
    parser.add_argument("--end_date", help="")

    args = parser.parse_args()
    task = args.task

    if args.start_date is None or args.end_date is None:
        start_date = None
        end_date = None
    else:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()

    if task == "init":
        init()
    elif task == "restore_history":
        if start_date is None or end_date is None:
            print("start_date and end_date is not None")
        else:
            restore_history(start_date, end_date)
