import os
import argparse
import shutil
import re
import pathlib
from datetime import datetime

import pandas as pd

from app.lib.connect_db import get_engine, get_connection
from app.lib.file_helper import get_lastest_snapshot_df
from app.config import RAW_DATA_PATH, ARCHIVED_DATA_PATH, ALL_PROPERTIES

PROPERTIES = [p for p in ALL_PROPERTIES if p["template"] == "Template 01"]


RENAME_COLUMNS = {
    "FolioNum": "FOLIONUM",
    "Arrival": "ARRIVAL",
    "Departure": "DEPARTURE",
    "Staying": "STAYING",
    "Create Time": "CREATE_TIME",
    "Group Code": "GROUP_CODE",
    "TA": "TA",
    "TA ID": "TA_ID",
    "Guest Name": "GUEST_NAME",
    "Market": "MARKET",
    "Rate Code": "RATE_CODE",
    "Rate Amount": "RATE_AMT",
    "Package Code": "PACKAGE_CODE",
    "Total Turn Over": "TOTAL_TURN_OVER",
    "ARR": "ARR",
    "Room REV": "ROOM_REV",
    "FB Rev": "FB_REV",
    "OTher Rev": "OTHER_REV",
    "Status": "STATUS",
    "R Type": "R_TYPE",
    "R T Charge": "R_CHARGE",
    "R Surcharge": "R_SURCHARGE",
    "N of Room": "N_OF_ROOM",
    "N of Adt": "N_OF_ADT",
    "N of Child": "N_OF_CHD",
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
    "TA",
    "TA_ID",
    "GUEST_NAME",
    "MARKET",
    "RATE_CODE",
    "RATE_AMT",
    "PACKAGE_CODE",
    "TOTAL_TURN_OVER",
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
    print(f"Init properties use Template 01")

    for property in PROPERTIES:
        try:
            init_property(property)
        except Exception as e:
            print(f"Error property: {property["name"]}")
            print(e)


def fload():
    print(f"Full load data from properties use Template 01")

    for property in PROPERTIES:
        try:
            fload_property(property)
        except Exception as e:
            print(f"Error property: {property["name"]}")
            print(e)


def iload():
    print(f"Icremental load data from properties use Template 01")

    for property in PROPERTIES:
        try:
            iload_property(property)
        except Exception as e:
            print(f"Error property: {property["name"]}")
            print(e)


def init_property(property):
    print(f"Init property: {property["name"]}")

    # tạo các folder lưu trữ file dữ liệu Booking Pace
    os.makedirs(
        os.path.join(RAW_DATA_PATH, "Booking Pace", property["folder"]),
        exist_ok=True,
    )
    os.makedirs(
        os.path.join(ARCHIVED_DATA_PATH, "Booking Pace", property["folder"]),
        exist_ok=True,
    )

    conn = get_connection()
    cursor = conn.cursor()
    sql = f"""
    CREATE TABLE  {property["schema"]}. {property["table"]} (
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
    """
    cursor.execute(sql)
    conn.commit()
    conn.close()


def fload_property(property):
    print(f"Full load data from property: {property["name"]}")

    raw_folder_path = os.path.join(RAW_DATA_PATH, "Booking Pace", property["folder"])
    archived_folder_path = os.path.join(
        ARCHIVED_DATA_PATH, "Booking Pace", property["folder"]
    )

    # tạo kết nối tới CSDL
    conn = get_connection()
    cursor = conn.cursor()

    # xóa toàn bộ dữ liệu cũ trong bảng đồng bộ
    try:
        sql = f"""TRUNCATE TABLE {property["schema"]}.{property["table"]}"""
        cursor.execute(sql)
        conn.commit()
        print("Truncate destination table")
    except Exception as e:
        print(e)
        # print(f"ETL Error")
        return

    # lấy các thông tin metadata của files
    files = _get_files(raw_folder_path)
    print(files)
    if len(files) == 0:
        return

    # lấy danh sách các snapshot mới nhất của từng ngày
    files_df = pd.DataFrame(files)
    lastest_snapshot_df = get_lastest_snapshot_df(files_df)
    print(lastest_snapshot_df)

    # đọc dữ liệu từ các snapshot mới nhất của từng ngày
    for _, snapshot_file in lastest_snapshot_df.iterrows():
        try:
            snapshot_file_path = os.path.join(raw_folder_path, snapshot_file["name"])

            df = pd.read_excel(snapshot_file_path, engine="xlrd")
            # df = pd.read_excel(snapshot_file_path, sheet_name="Sheet1")
            print(df.head())
            # đổi tên cột
            df.rename(columns=RENAME_COLUMNS, inplace=True)
            # lấy danh sách các cột cần thiết
            df = df[FINAL_COLUMNS]
            # thêm các cột ngày dữ liệu và khách sạn
            df["REPORT_DATE"] = snapshot_file["report_date"]
            df["PROPERTY"] = property["name"]
            df["CREATED_AT"] = snapshot_file["created_at"]
            df["MODIFIED_AT"] = snapshot_file["modified_at"]
            df["FILE_NAME"] = snapshot_file["name"]

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

            # copy các file trong ngày sang folder đã xử lý thành công
            related_df = files_df[
                files_df["report_date"] == snapshot_file["report_date"]
            ]

            for _, f in related_df.iterrows():
                try:
                    file_path = os.path.join(raw_folder_path, f["name"])
                    archived_path = os.path.join(archived_folder_path, f["name"])

                    print(f"Save file into Archived Data: {file_path}")
                    # copy file giữ nguyên thông tin metadata
                    shutil.copy2(file_path, archived_path)
                except Exception as e:
                    print(e)
                    print(f"Error save file into Archived Data: {file_path}")

            print(f"Complete date: {snapshot_file["report_date"]}")
        except Exception as e:
            print(e)
            print(f"Error when processing file: {snapshot_file_path}")

    # đóng kết nối tới CSDL
    conn.close()


def iload_property(property):
    print(f"Incremental load data from property: {property["name"]}")

    raw_folder_path = os.path.join(RAW_DATA_PATH, "Booking Pace", property["folder"])
    archived_folder_path = os.path.join(
        ARCHIVED_DATA_PATH, "Booking Pace", property["folder"]
    )
    # tạo kết nối tới CSDL
    conn = get_connection()
    cursor = conn.cursor()

    # lấy các thông tin metadata của các files mới có trong folder
    raw_files = _get_files(raw_folder_path)
    archived_files = _get_files(archived_folder_path)

    files = _get_change_files(raw_files, archived_files)
    print(files)
    if len(files) == 0:
        print("No files")
        return

    # lấy danh sách các snapshot mới nhất của từng ngày
    files_df = pd.DataFrame(files)
    lastest_snapshot_df = get_lastest_snapshot_df(files_df)
    print(lastest_snapshot_df)

    # đọc dữ liệu từ các snapshot mới nhất của từng ngày
    for _, snapshot_file in lastest_snapshot_df.iterrows():
        try:
            snapshot_file_path = os.path.join(raw_folder_path, snapshot_file["name"])

            df = pd.read_excel(snapshot_file_path, engine="xlrd")
            # df = pd.read_excel(snapshot_file_path, sheet_name="Sheet1")
            # print(df.head())
            # đổi tên cột
            df.rename(columns=RENAME_COLUMNS, inplace=True)
            # lấy danh sách các cột cần thiết
            df = df[FINAL_COLUMNS]
            # thêm các cột ngày dữ liệu và khách sạn
            df["REPORT_DATE"] = snapshot_file["report_date"]
            df["PROPERTY"] = property["name"]
            df["CREATED_AT"] = snapshot_file["created_at"]
            df["MODIFIED_AT"] = snapshot_file["modified_at"]
            df["FILE_NAME"] = snapshot_file["name"]

            # xóa dữ liệu snapshot cũ cho một ngày
            sql = f"""
            DELETE FROM {property["schema"]}.{property["table"]}
            WHERE PROPERTY = ? AND REPORT_DATE = ?
            """
            cursor.execute(sql, property["name"], snapshot_file["report_date"])
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
            # print("Ghi dữ liệu thành công vào DB")

            # copy các file trong ngày sang folder đã xử lý thành công
            related_df = files_df[
                files_df["report_date"] == snapshot_file["report_date"]
            ]

            for _, f in related_df.iterrows():
                try:
                    file_path = os.path.join(raw_folder_path, f["name"])
                    archived_path = os.path.join(archived_folder_path, f["name"])

                    print(f"Save file into Archived Data: {file_path}")
                    # copy file giữ nguyên thông tin metadata
                    shutil.copy2(file_path, archived_path)
                except Exception as e:
                    print(e)
                    print(f"Error save file into Archived Data: {file_path}")

            print(f"Complete date: {snapshot_file["report_date"]}")
        except Exception as e:
            print(e)
            print(f"Error when processing file: {snapshot_file_path}")

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
                    }
                )
    return files


def _get_change_files(raw_files: list, archived_files: list):
    # print(raw_files)
    # print(archived_files)

    # các file đưa được xử lý thành công và được đưa vào Archived Data
    new_file_names = {f["name"] for f in raw_files} - {
        f["name"] for f in archived_files
    }

    result_files = []
    for file in raw_files:
        if file["name"] in new_file_names:
            result_files.append(file)

    return result_files


def _get_property(folder):
    for property in PROPERTIES:
        if property["folder"] == folder:
            return property
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", "-t", help="", default="init_property")
    parser.add_argument("--property", "-p", help="", default="")

    args = parser.parse_args()
    task = args.task
    folder = args.property

    if task == "init_property":
        property = _get_property(folder)
        if property is not None:
            init_property(property)
    elif task == "fload_property":
        property = _get_property(folder)
        if property is not None:
            fload_property(property)
    elif task == "iload_property":
        property = _get_property(folder)
        if property is not None:
            iload_property(property)
