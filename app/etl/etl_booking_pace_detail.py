import argparse
import pandas as pd
from pyxlsb import open_workbook

from app.lib.connect_db import get_engine, get_connection
import app.etl.etl_template01 as etl_template01
import app.etl.etl_template02 as etl_template02
import app.etl.etl_template03 as etl_template03


def init():
    init_booking_pace_detail_table()
    init_sp_fload_booking_pace_detail()
    init_sp_iload_booking_pace_detail()


def init_booking_pace_detail_table():
    conn = get_connection()
    cursor = conn.cursor()
    # -- tạo bảng dbo.booking_pace_detail -- #
    sql = """
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
        MODIFIED_AT DATETIME,
        FILE_NAME NVARCHAR(500)
    )
    """
    cursor.execute(sql)
    conn.commit()
    conn.close()


def init_sp_fload_booking_pace_detail():
    # -- tạo store procedure sp_fload_booking_pace_detail -- #
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
    CREATE OR ALTER PROCEDURE dbo.sp_fload_booking_pace_detail AS
    BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRAN;
    BEGIN TRY;
        -- xóa toàn bộ dữ liệu của bảng
        TRUNCATE TABLE dbo.booking_pace_detail;

    """
    for property in etl_template01.PROPERTIES + etl_template02.PROPERTIES:
        sql += f"""
        -- đưa vào dữ liệu của khách sạn {property["name"]}
        INSERT INTO dbo.booking_pace_detail
        SELECT REPORT_DATE, FORMAT(STAYING, 'yyyy-MM') AS STAY_MONTH, PROPERTY, 
        ARRIVAL, DEPARTURE, STAYING, CONVERT(DATE, CREATE_TIME) AS CREATE_DATE,
        MARKET, RATE_CODE, RATE_AMT, 
        ISNULL(ARR, 0) + ISNULL(ROOM_REV, 0) + ISNULL(FB_REV, 0) + ISNULL(OTHER_REV, 0) AS TOTAL_TURN_OVER, 
        ARR, ROOM_REV, FB_REV, OTHER_REV,
        [STATUS], R_TYPE, R_CHARGE,
        N_OF_ROOM, N_OF_ADT, N_OF_CHD, 
        BK_SOURCE, COUNTRY, NATIONALITY, CREATED_AT, MODIFIED_AT, FILE_NAME
        FROM {property["schema"]}.{property["table"]}

        """

    for property in etl_template03.PROPERTIES:
        sql += f"""
        -- đưa vào dữ liệu của khách sạn {property["name"]}
        INSERT INTO dbo.booking_pace_detail
        SELECT REPORT_DATE, FORMAT(CONSIDERED_DATE, 'yyyy-MM') AS STAY_MONTH, PROPERTY, 
        ARR AS ARRIVAL, DEP AS DEPARTURE, CONSIDERED_DATE AS STAYING, CREATED_DATE AS CREATE_DATE,
        MARKET_CODE AS MARKET, RATE_CODE, NULL AS RATE_AMT, 
        ISNULL(ROOM_REVENUE, 0) + ISNULL(FOOD_REVENUE, 0) + ISNULL(OTHER_REVENUE, 0) AS TOTAL_TURN_OVER, 
        NULL AS ARR, ROOM_REVENUE AS ROOM_REV, FOOD_REVENUE AS FB_REV, OTHER_REVENUE AS OTHER_REV,
        NULL AS [STATUS], NULL AS R_TYPE, NULL AS R_CHARGE,
        NO_ROOMS AS N_OF_ROOM, ADULTS AS N_OF_ADT, CHILDREN AS N_OF_CHD, 
        SOURCE_CODE AS BK_SOURCE, COUNTRY, COUNTRY AS NATIONALITY, CREATED_AT, MODIFIED_AT, FILE_NAME
        FROM {property["schema"]}.{property["table"]}

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

    -- EXEC dbo.sp_fload_booking_pace_detail
    """

    # print(sql)
    cursor.execute(sql)
    conn.commit()
    conn.close()


def init_sp_iload_booking_pace_detail():
    # -- tạo store procedure sp_iload_booking_pace_detail -- #
    conn = get_connection()
    cursor = conn.cursor()
    sql = """
    CREATE OR ALTER PROCEDURE dbo.sp_iload_booking_pace_detail AS
    BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRAN;
    BEGIN TRY;
        DECLARE @last_modified_at DATETIME;

        -- lấy thông tin dữ liệu sẽ bổ sung vào bảng đích
        DECLARE @iload_data TABLE (
            ID INT IDENTITY(1, 1),
            PROPERTY NVARCHAR(50),
            REPORT_DATE DATE, 
            MODIFIED_AT DATETIME
        )
        
    """
    for property in etl_template01.PROPERTIES + etl_template02.PROPERTIES:
        sql += f"""
        SET @last_modified_at = (SELECT MAX(MODIFIED_AT) FROM booking_pace_detail WHERE PROPERTY= N'{property["name"]}')

        INSERT @iload_data(PROPERTY, REPORT_DATE, MODIFIED_AT)
        SELECT DISTINCT PROPERTY, REPORT_DATE, MODIFIED_AT FROM {property["schema"]}.{property["table"]} WHERE MODIFIED_AT > @last_modified_at
        
        -- xóa dữ liệu cũ trong bảng đích
        DELETE d
        FROM booking_pace_detail d 
        JOIN @iload_data i ON d.PROPERTY = i.PROPERTY AND d.REPORT_DATE = i.REPORT_DATE

        -- đưa vào dữ liệu bổ sung vào bảng đích
        INSERT INTO dbo.booking_pace_detail
        SELECT s.REPORT_DATE, FORMAT(STAYING, 'yyyy-MM') AS STAY_MONTH, s.PROPERTY, 
        ARRIVAL, DEPARTURE, STAYING, CONVERT(DATE, CREATE_TIME) AS CREATE_DATE,
        MARKET, RATE_CODE, RATE_AMT, 
        ISNULL(ARR, 0) + ISNULL(ROOM_REV, 0) + ISNULL(FB_REV, 0) + ISNULL(OTHER_REV, 0) AS TOTAL_TURN_OVER, 
        ARR, ROOM_REV, FB_REV, OTHER_REV,
        [STATUS], R_TYPE, R_CHARGE,
        N_OF_ROOM, N_OF_ADT, N_OF_CHD, 
        BK_SOURCE, COUNTRY, NATIONALITY, s.CREATED_AT, s.MODIFIED_AT, s.FILE_NAME
        FROM {property["schema"]}.{property["table"]} s
        JOIN @iload_data i ON s.PROPERTY = i.PROPERTY AND s.REPORT_DATE = i.REPORT_DATE

        DELETE FROM @iload_data;

        """

    for property in etl_template03.PROPERTIES:
        sql += f"""
        SET @last_modified_at = (SELECT MAX(MODIFIED_AT) FROM booking_pace_detail WHERE PROPERTY= N'{property["name"]}')

        INSERT @iload_data(PROPERTY, REPORT_DATE, MODIFIED_AT)
        SELECT DISTINCT PROPERTY, REPORT_DATE, MODIFIED_AT FROM {property["schema"]}.{property["table"]} WHERE MODIFIED_AT > @last_modified_at
        
        -- xóa dữ liệu cũ trong bảng đích
        DELETE d
        FROM booking_pace_detail d 
        JOIN @iload_data i ON d.PROPERTY = i.PROPERTY AND d.REPORT_DATE = i.REPORT_DATE

        -- đưa vào dữ liệu bổ sung vào bảng đích
        INSERT INTO dbo.booking_pace_detail
        SELECT s.REPORT_DATE, FORMAT(CONSIDERED_DATE, 'yyyy-MM') AS STAY_MONTH, s.PROPERTY, 
        ARR AS ARRIVAL, DEP AS DEPARTURE, CONSIDERED_DATE AS STAYING, CREATED_DATE AS CREATE_DATE,
        MARKET_CODE AS MARKET, RATE_CODE, NULL AS RATE_AMT, 
        ISNULL(ROOM_REVENUE, 0) + ISNULL(FOOD_REVENUE, 0) + ISNULL(OTHER_REVENUE, 0) AS TOTAL_TURN_OVER, 
        NULL AS ARR, ROOM_REVENUE AS ROOM_REV, FOOD_REVENUE AS FB_REV, OTHER_REVENUE AS OTHER_REV,
        NULL AS [STATUS], NULL AS R_TYPE, NULL AS R_CHARGE,
        NO_ROOMS AS N_OF_ROOM, ADULTS AS N_OF_ADT, CHILDREN AS N_OF_CHD, 
        SOURCE_CODE AS BK_SOURCE, COUNTRY, COUNTRY AS NATIONALITY, s.CREATED_AT, s.MODIFIED_AT, s.FILE_NAME
        FROM {property["schema"]}.{property["table"]} s
        JOIN @iload_data i ON s.PROPERTY = i.PROPERTY AND s.REPORT_DATE = i.REPORT_DATE

        DELETE FROM @iload_data;

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
    -- EXEC dbo.sp_iload_booking_pace_detail
    """

    # print(sql)
    cursor.execute(sql)
    conn.commit()

    conn.close()


def fload():
    print(f"Full Load dữ liệu vào bảng booking_pace_detail")
    conn = get_connection()
    cursor = conn.cursor()

    sql = "EXEC dbo.sp_fload_booking_pace_detail"
    cursor.execute(sql)
    conn.commit()
    conn.close()


def iload():
    print(f"Incremental Load dữ liệu vào bảng booking_pace_detail")
    conn = get_connection()
    cursor = conn.cursor()

    sql = "EXEC dbo.sp_iload_booking_pace_detail"
    cursor.execute(sql)
    conn.commit()
    conn.close()


def fload_sample_data():
    try:
        file_path = "./data/Sample Data/OTB & Pace_v20250324.xlsb"
        rows_list = []

        with open_workbook(file_path) as wb:
            with wb.get_sheet("DATA") as sheet:
                print(f"Reading data from sheet: {sheet.name}")
                # print(sheet.cols)

                for row_index, row in enumerate(sheet.rows()):
                    row_data = [cell.v for cell in row]
                    rows_list.append(row_data)

                df = pd.DataFrame(data=rows_list[1:], index=None, columns=rows_list[0])
                # chọn các cột cần thiết
                columns = [
                    "Report Date",
                    "Stay Month",
                    "Property",
                    "Arrival",
                    "Deprature",
                    "Staying",
                    "Creation Date",
                    "Market",
                    "Rate code",
                    "Rate Amt",
                    "Total turn Over",
                    "ARR",
                    "Room REV",
                    "FB Rev",
                    "Other Rev",
                    "Status",
                    "R type",
                    "R T Charge",
                    "N of Room",
                    "N of Adt",
                    "N of Chd",
                    "Bk source",
                    "Country",
                    "Nationality",
                ]
                df = df[columns]

                # đổi tên các cột
                columns = {
                    "Report Date": "REPORT_DATE",
                    "Stay Month": "STAY_MONTH",
                    "Property": "PROPERTY",
                    "Arrival": "ARRIVAL",
                    "Deprature": "DEPARTURE",
                    "Staying": "STAYING",
                    "Creation Date": "CREATE_DATE",
                    "Market": "MARKET",
                    "Rate code": "RATE_CODE",
                    "Rate Amt": "RATE_AMT",
                    "Total turn Over": "TOTAL_TURN_OVER",
                    "ARR": "ARR",
                    "Room REV": "ROOM_REV",
                    "FB Rev": "FB_REV",
                    "Other Rev": "OTHER_REV",
                    "Status": "STATUS",
                    "R type": "R_TYPE",
                    "R T Charge": "R_CHARGE",
                    "N of Room": "N_OF_ROOM",
                    "N of Adt": "N_OF_ADT",
                    "N of Chd": "N_OF_CHD",
                    "Bk source": "BK_SOURCE",
                    "Country": "COUNTRY",
                    "Nationality": "NATIONALITY",
                }
                df.rename(columns=columns, inplace=True)

                df["REPORT_DATE"] = pd.to_datetime(
                    "2023-05-01", format="%Y-%m-%d", errors="coerce"
                )

                date_cols = [
                    "ARRIVAL",
                    "DEPARTURE",
                    "STAYING",
                    "CREATE_DATE",
                ]
                for col in date_cols:
                    df[col] = pd.to_datetime(df[col], unit="D", origin="1899-12-30")

                df["STAY_MONTH"] = df["STAYING"].dt.strftime("%Y-%m")
                print(df.head(5))

                engine = get_engine()
                df.to_sql(
                    "booking_pace_sample_data",
                    con=engine,
                    schema="dbo",
                    if_exists="append",
                    index=False,
                )
                print("Ghi dữ liệu thành công vào DB")

    except Exception as e:
        print(e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", "-t", help="", default="init")

    args = parser.parse_args()
    task = args.task

    if task == "init":
        init()
    elif task == "init_booking_pace_detail_table":
        init_booking_pace_detail_table()
    elif task == "init_sp_fload_booking_pace_detail":
        init_sp_fload_booking_pace_detail()
    elif task == "init_sp_iload_booking_pace_detail":
        init_sp_iload_booking_pace_detail()
    elif task == "fload":
        fload()
    elif task == "iload":
        iload()
