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

PROPERTIES = [p for p in ALL_PROPERTIES if p["template"] == "Template 03"]

RENAME_COLUMNS = {}

FINAL_COLUMNS = []


def init():
    print(f"Init properties use Template 03")

    for property in PROPERTIES:
        try:
            init_property(property)
        except Exception as e:
            print(f"Error property: {property["name"]}")
            print(e)


def fload():
    print(f"Icremental load data from properties use Template 03")

    for property in PROPERTIES:
        try:
            fload_property(property)
        except Exception as e:
            print(f"Error property: {property["name"]}")
            print(e)


def iload():
    print(f"Icremental load data from properties use Template 03")

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
    CREATE TABLE {property["schema"]}. {property["table"]}  (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        REPORT_DATE DATE,
        PROPERTY NVARCHAR(50),
        CONSIDERED_DATE DATE,
        ADULTS INT,
        CHILDREN INT,
        CREATED_DATE DATE,
        COUNTRY NVARCHAR(10),
        NO_ROOMS INT,
        MARKET_CODE NVARCHAR(20),
        SOURCE_CODE NVARCHAR(20),
        CHANNEL NVARCHAR(20),
        RATE_CODE NVARCHAR(20),
        ROOM_CAT NVARCHAR(20),
        RTC NVARCHAR(20),
        ARR DATE,
        DEP DATE,
        RESV_NAME_ID INT,
        ROOM_REVENUE DECIMAL(18, 2),
        FOOD_REVENUE DECIMAL(18, 2),
        OTHER_REVENUE DECIMAL(18, 2),
        COMP_ROOM_REVENUE DECIMAL(18, 2),

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

            df = pd.read_excel(snapshot_file_path, sheet_name="Sheet1")
            # print(df.head())

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

            df = pd.read_excel(snapshot_file_path, sheet_name="Sheet1")
            # print(df.head())

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


def fload_property_old(property):
    print(f"Full Load dữ liệu của khách sạn: {property["name"]} vào CSDL")

    folder_path = os.path.join(RAW_DATA_PATH, "Booking Pace", property["folder"])

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
            file_path = os.path.join(folder_path, f["folder"])

            df = pd.read_csv(file_path, sep="\t", encoding="utf-16")
            print(df.head())
            # chuẩn hóa các cột date
            date_cols = ["CONSIDERED_DATE", "CREATED_DATE", "ARR", "DEP"]
            for col in date_cols:
                df[col] = pd.to_datetime("1899-12-30") + pd.to_timedelta(
                    df[col], unit="D"
                )

            # đổi tên cột
            # df.rename(columns=RENAME_COLUMNS, inplace=True)
            # lấy danh sách các cột cần thiết
            # df = df[FINAL_COLUMNS]

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
                    ARCHIVED_DATA_PATH, "Booking Pace", property["folder"], f["name"]
                )
                os.makedirs(os.path.dirname(archived_path), exist_ok=True)
                shutil.move(file_path, archived_path)
    except Exception as e:
        print(e)
        print(f"Lỗi khi ghi dữ liệu vào DB")


def iload_property_old(property):
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

            df = pd.read_csv(snapshot_file_path, sep="\t", encoding="utf-16")
            print(df.head())
            # chuẩn hóa các cột date
            date_cols = ["CONSIDERED_DATE", "CREATED_DATE", "ARR", "DEP"]
            for col in date_cols:
                df[col] = pd.to_datetime("1899-12-30") + pd.to_timedelta(
                    df[col], unit="D"
                )
            # đổi tên cột
            # df.rename(columns=RENAME_COLUMNS, inplace=True)
            # lấy danh sách các cột cần thiết
            # df = df[FINAL_COLUMNS]

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
        # print(f.name)
        if f.is_file():
            filename_re = re.compile(r".*_(\d{2})(\d{2})(\d{4})", re.IGNORECASE)
            match = filename_re.match(f.name)

            if match:
                dd, mm, yyyy = match.groups()
                report_date = datetime.strptime(f"{dd} {mm} {yyyy}", "%d %m %Y")
                report_at = datetime.strptime(f"{dd} {mm} {yyyy}", "%d %m %Y")

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
