import os
import shutil
import re
import pathlib
from datetime import datetime

import pandas as pd

from app.lib.connect_db import get_engine, get_connection
from app.lib.file_helper import get_lastest_snapshot_df
from app.config import RAW_DATA_PATH, ARCHIVED_DATA_PATH

PROPERTIES = [
    {
        "name": "Syrena Cruises",
        "schema": "stg",
        "table": "booking_pace_syrena_cruises",
    }
]

RENAME_COLUMNS = {
    "Folionum": "FOLIONUM",
    "Arrival": "ARRIVAL",
    "Departure": "DEPARTURE",
    "Staying": "STAYING",
    "Create time": "CREATE_TIME",
    "Group code": "GROUP_CODE",
    "Group Name": "GROUP_NAME",
    "TA": "TA",
    "TA ID": "TA_ID",
    "Guest Name": "GUEST_NAME",
    "Market": "MARKET",
    "Rate code": "RATE_CODE",
    "Rate Amt": "RATE_AMT",
    "Package code": "PACKAGE_CODE",
    "Pkg Amount": "PACKAGE_AMOUNT",
    "ARR": "ARR",
    "Room Rev": "ROOM_REV",
    "FB Rev": "FB_REV",
    "Other Rev": "OTHER_REV",
    "Status": "STATUS",
    "R type": "R_TYPE",
    "R T Charge": "R_CHARGE",
    "R Surcharge": "R_SURCHARGE",
    "N of Room": "N_OF_ROOM",
    "N of Adt": "N_OF_ADT",
    "N of Chd": "N_OF_CHD",
    "Bk source": "BK_SOURCE",
    "Country": "COUNTRY",
    "Nationality": "NATIONALITY",
}

FINAL_COLUMNS = [
    "FOLIONUM",
    "ARRIVAL",
    "DEPARTURE",
    "STAYING",
    "CREATE_TIME",
    "GROUP_CODE",
    "GROUP_NAME",
    "TA",
    "TA_ID",
    "GUEST_NAME",
    "MARKET",
    "RATE_CODE",
    "RATE_AMT",
    "PACKAGE_CODE",
    "PACKAGE_AMOUNT",
    "ARR",
    "ROOM_REV",
    "FB_REV",
    "OTHER_REV",
    "STATUS",
    "R_TYPE",
    "R_CHARGE",
    "R_SURCHARGE",
    "N_OF_ROOM",
    "N_OF_ADT",
    "N_OF_CHD",
    "BK_SOURCE",
    "COUNTRY",
    "NATIONALITY",
]


def init():
    print(f"Khởi tạo cho các khách sạn sử dụng Template 02")

    for property in PROPERTIES:
        try:
            # tạo các folder lưu trữ file dữ liệu Booking Pace
            os.makedirs(
                os.path.join(RAW_DATA_PATH, "Booking Pace", property["name"]),
                exist_ok=True,
            )
            os.makedirs(
                os.path.join(ARCHIVED_DATA_PATH, "Booking Pace", property["name"]),
                exist_ok=True,
            )

            init_property(property)
        except Exception as e:
            print(f"Lỗi khách sạn: {property["name"]}")
            print(e)

    print("-- Hoàn thành --")


def fload():
    print(f"Full Load dữ liệu của các khách sạn sử dụng Template 02")

    for property in PROPERTIES:
        try:
            fload_property(property)
        except Exception as e:
            print(f"Lỗi khách sạn: {property["name"]}")
            print(e)


def iload():
    print(f"Icremental Load dữ liệu của các khách sạn sử dụng Template 02")

    for property in PROPERTIES:
        try:
            iload_property(property)
        except Exception as e:
            print(f"Lỗi khách sạn: {property["name"]}")
            print(e)


def init_property(property):
    print(f"Khởi tạo cho khách sạn: {property["name"]}")

    conn = get_connection()
    cursor = conn.cursor()
    sql = f"""
    CREATE TABLE {property["schema"]}. {property["table"]} (
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
    """
    cursor.execute(sql)
    conn.commit()
    conn.close()


def fload_property(property):
    print(f"Full Load dữ liệu của khách sạn: {property["name"]} vào CSDL")

    folder_path = os.path.join(RAW_DATA_PATH, "Booking Pace", property["name"])

    # lấy các thông tin metadata của files
    files = _get_files(folder_path)
    print(files)
    if len(files) == 0:
        return

    # lấy danh sách các snapshot mới nhất của từng ngày
    files_df = pd.DataFrame(files)
    lastest_snapshot_df = get_lastest_snapshot_df(files_df)
    print(lastest_snapshot_df)

    # đọc dữ liệu từ các files
    df_list = []
    for idx, f in lastest_snapshot_df.iterrows():
        try:
            file_path = os.path.join(folder_path, f["name"])

            df = pd.read_excel(file_path, engine="xlrd")
            # print(df.head())
            # đổi tên cột
            df.rename(columns=RENAME_COLUMNS, inplace=True)
            # lấy danh sách các cột cần thiết
            df = df[FINAL_COLUMNS]

            # thêm các cột ngày dữ liệu và khách sạn
            df["REPORT_DATE"] = f["report_date"]
            df["PROPERTY"] = property["name"]
            df["CREATED_AT"] = f["created_at"]
            df["MODIFIED_AT"] = f["modified_at"]

            df_list.append(df)
            lastest_snapshot_df.at[idx, "etl_check"] = True
            print(f"Xử lý thành công file: {file_path}")
        except Exception as e:
            print(e)
            print(f"Lỗi trong quá trình xử lý file: {file_path}")

    # ghi dữ liệu vào CSDL
    try:
        # gộp dữ liệu từ các file thành một bảng
        total_df = pd.concat(df_list, ignore_index=True)
        # print(total_df.head(5))
        # bổ sung dữ liệu vào trong bảng
        engine = get_engine()
        total_df.to_sql(
            property["table"],
            con=engine,
            schema=property["schema"],
            if_exists="append",
            index=False,
        )
        print("Ghi dữ liệu thành công vào DB")

        # thực hiện chuyển các file đã ETL thành công sang Archived Data
        files_df = pd.merge(
            files_df,
            lastest_snapshot_df,
            on="name",
            how="left",
            suffixes=("", "_right"),
        )

        for idx, f in files_df.iterrows():
            if files_df.at[idx, "etl_check_right"] == True:
                files_df.at[idx, "etl_check"] = files_df.at[idx, "etl_check_right"]

            if files_df.at[idx, "etl_check"] == True:
                file_path = os.path.join(folder_path, f["name"])
                print(f"Chuyển file sang Archived Data: {file_path}")

                archived_path = os.path.join(
                    ARCHIVED_DATA_PATH, "Booking Pace", property["name"], f["name"]
                )
                os.makedirs(os.path.dirname(archived_path), exist_ok=True)
                shutil.move(file_path, archived_path)
    except Exception as e:
        print(e)
        print(f"Lỗi khi ghi dữ liệu vào DB")


def iload_property(property):
    print(f"Incremental Load dữ liệu của khách sạn: {property["name"]} vào CSDL")

    folder_path = os.path.join(RAW_DATA_PATH, "Booking Pace", property["name"])
    # tạo kết nối tới CSDL
    conn = get_connection()
    cursor = conn.cursor()
    # lấy các thông tin metadata của files
    files = _get_files(folder_path)
    # print(files)
    if len(files) == 0:
        return

    # lấy danh sách các snapshot mới nhất của từng ngày
    files_df = pd.DataFrame(files)
    lastest_snapshot_df = get_lastest_snapshot_df(files_df)
    print(lastest_snapshot_df)

    # đọc dữ liệu từ các files
    for _, sf in lastest_snapshot_df.iterrows():
        try:
            snapshot_file_path = os.path.join(folder_path, sf["name"])

            df = pd.read_excel(snapshot_file_path, engine="xlrd")
            # print(df.head())
            # đổi tên cột
            df.rename(columns=RENAME_COLUMNS, inplace=True)
            # lấy danh sách các cột cần thiết
            df = df[FINAL_COLUMNS]
            # thêm các cột ngày dữ liệu và khách sạn
            df["REPORT_DATE"] = sf["report_date"]
            df["PROPERTY"] = property["name"]
            df["CREATED_AT"] = sf["created_at"]
            df["MODIFIED_AT"] = sf["modified_at"]

            # xóa dữ liệu snapshot cũ cho một ngày
            sql = f"""
            DELETE FROM {property["schema"]}.{property["table"]}
            WHERE PROPERTY = ? AND REPORT_DATE = ?
            """
            cursor.execute(sql, property["name"], sf["report_date"])
            conn.commit()
            # ghi dữ liệu snapshot mới cho một ngày
            engine = get_engine()
            df.to_sql(
                property["table"],
                con=engine,
                schema=property["schema"],
                if_exists="append",
                index=False,
                chunksize=10000,
            )
            print("Ghi dữ liệu thành công vào DB")
            # lastest_snapshot_df.at[idx, "etl_check"] = True
            print(f"Xử lý thành công file: {snapshot_file_path}")

            # chuyển các file trong cùng ngày sang folder đã xử lý
            related_df = files_df[files_df["report_date"] == sf["report_date"]]

            for _, f in related_df.iterrows():
                try:
                    file_path = os.path.join(folder_path, f["name"])
                    print(f"Chuyển file sang Archived Data: {file_path}")

                    archived_path = os.path.join(
                        ARCHIVED_DATA_PATH, "Booking Pace", property["name"], f["name"]
                    )
                    os.makedirs(os.path.dirname(archived_path), exist_ok=True)
                    shutil.move(file_path, archived_path)
                except Exception as e:
                    print(e)
                    print(f"Lỗi khi chuyển file sang Archived Data: {file_path}")
        except Exception as e:
            print(e)
            print(f"Lỗi trong quá trình xử lý file: {snapshot_file_path}")

    # đóng kết nối tới CSDL
    conn.close()


def _get_files(folder_path: str):
    files = []
    for f in pathlib.Path(folder_path).iterdir():
        if f.is_file():
            filename_re = re.compile(
                r".*(\d{2})(\d{2})(\d{4})_(\d{2})(\d{2})", re.IGNORECASE
            )
            match = filename_re.match(f.name)

            if match:
                dd, mm, yyyy, HH, MM = match.groups()
                report_date = datetime.strptime(f"{dd} {mm} {yyyy}", "%d %m %Y")
                report_at = datetime.strptime(
                    f"{dd} {mm} {yyyy} {HH} {MM}", "%d %m %Y %H %M"
                )

                files.append(
                    {
                        "name": f.name,
                        "report_date": report_date,
                        "report_at": report_at,
                        "modified_at": datetime.fromtimestamp(os.path.getmtime(f)),
                        "created_at": datetime.fromtimestamp(os.path.getctime(f)),
                        "etl_check": True,
                    }
                )
    return files


if __name__ == "__main__":
    # init()
    fload()
    # iload()
    pass
