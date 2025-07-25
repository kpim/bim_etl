import os
import pathlib
import shutil

import pandas as pd

from app.lib.connect_db import get_engine
from app.config import RAW_DATA_PATH, ARCHIVED_DATA_PATH

PROPERTY = "Property 01"
PROPERTY_SCHEMA = "stg"
PROPERTY_TABLE = "booking_pace_p1"


def fload_property01():
    print(f"Thực hiện Full Load dữ liệu của khách sạn: {PROPERTY}")

    folder_path = os.path.join(RAW_DATA_PATH, "Booking Pace", PROPERTY)
    files = [f.name for f in pathlib.Path(folder_path).iterdir() if f.is_file()]
    files = sorted(files)
    file_checks = [False for f in files]

    df_list = []

    for idx, file in enumerate(files):
        try:
            file_path = os.path.join(folder_path, file)
            # lấy thông tin về date từ tên file, chú ý date phải lưu ở cuối file
            date_str = os.path.splitext(file)[0].split("_")[-1]
            print(date_str)

            df = pd.read_csv(file_path, sep="\t", encoding="utf-16")
            # chuẩn hóa các cột date
            date_cols = ["CONSIDERED_DATE", "CREATED_DATE", "ARR", "DEP"]
            for col in date_cols:
                df[col] = pd.to_datetime("1899-12-30") + pd.to_timedelta(
                    df[col], unit="D"
                )
            # thêm các cột ngày dữ liệu và khách sạn
            df["REPORT_DATE"] = pd.to_datetime(
                date_str, format="%Y%m%d", errors="coerce"
            )
            df["PROPERTY"] = PROPERTY

            df_list.append(df)
            file_checks[idx] = True
            print(f"Xử lý thành công file: {file_path}")
        except Exception as e:
            print(e)
            print(f"Lỗi trong quá trình xử lý file: {file_path}")

    try:
        # gộp dữ liệu từ các file thành một bảng
        total_df = pd.concat(df_list, ignore_index=True)
        # print(total_df.head(5))
        # bổ sung dữ liệu vào trong bảng
        engine = get_engine()
        total_df.to_sql(
            PROPERTY_TABLE,
            con=engine,
            schema=PROPERTY_SCHEMA,
            if_exists="append",
            index=False,
        )
        print("Ghi dữ liệu thành công vào DB")

        # thực hiện chuyển các file đã ETL thành công sang Archived Data
        for idx, file in enumerate(files):
            file_path = os.path.join(folder_path, file)
            print(f"Chuyển file sang Archived Data: {file_path}")

            if file_checks[idx] == True:
                archived_path = os.path.join(
                    ARCHIVED_DATA_PATH, "Booking Pace", PROPERTY, file
                )
                os.makedirs(os.path.dirname(archived_path), exist_ok=True)
                shutil.move(file_path, archived_path)

    except Exception as e:
        print(e)
        print(f"Lỗi khi ghi dữ liệu vào DB")



def iload_property01():
    print(f"Thực hiện Incremental Load dữ liệu của khách sạn: {PROPERTY}")

    folder_path = os.path.join(RAW_DATA_PATH, "Booking Pace", PROPERTY)
    if not os.path.isdir(folder_path):
        print(f"Không tìm thấy thư mục dữ liệu: {folder_path}")
        return

    files = [f.name for f in pathlib.Path(folder_path).iterdir() if f.is_file()]
    files = sorted(files)

    if not files:
        print("Không có file nào trong thư mục RAW.")
        return

    # Lấy danh sách REPORT_DATE đã có trong DB cho Property này
    try:
        engine = get_engine()
        with engine.connect() as conn:
            existing_dates = pd.read_sql(
                f"""
                SELECT DISTINCT REPORT_DATE
                FROM {PROPERTY_SCHEMA}.{PROPERTY_TABLE}
                WHERE PROPERTY = %s
                """,
                conn,
                params=[PROPERTY],
            )["REPORT_DATE"]
        existing_dates = set(existing_dates.dt.normalize().tolist())
        print(f"Đã có {len(existing_dates)} REPORT_DATE trong DB.")
    except Exception as e:
        print(e)
        print("Lỗi khi đọc REPORT_DATE đã có trong DB")
        return

    df_list = []
    file_checks = []
    new_files = 0

    date_cols = ["CONSIDERED_DATE", "CREATED_DATE", "ARR", "DEP"]

    for file in files:
        file_path = os.path.join(folder_path, file)
        try:
            # Lấy date từ tên file
            date_str = os.path.splitext(file)[0].split("_")[-1]
            report_date = pd.to_datetime(date_str, format="%Y%m%d", errors="coerce")
            if pd.isna(report_date):
                print(f"Bỏ qua file (không đọc được REPORT_DATE): {file_path}")
                file_checks.append(False)
                continue

            if report_date.normalize().to_pydatetime() in existing_dates:
                print(f"Bỏ qua file (REPORT_DATE đã có trong DB): {file_path}")
                file_checks.append(False)
                continue

            # Đọc & chuẩn hóa
            df = pd.read_csv(file_path, sep="\t", encoding="utf-16")
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime("1899-12-30") + pd.to_timedelta(
                        df[col], unit="D"
                    )
            df["REPORT_DATE"] = report_date
            df["PROPERTY"] = PROPERTY

            df_list.append(df)
            file_checks.append(True)
            new_files += 1
            print(f"Xử lý thành công file mới: {file_path}")

        except Exception as e:
            print(e)
            print(f"Lỗi trong quá trình xử lý file: {file_path}")
            file_checks.append(False)

    if new_files == 0:
        print("Không có file mới để nạp.")
        return

    try:
        total_df = pd.concat(df_list, ignore_index=True)
        with engine.begin() as conn:  # transaction
            total_df.to_sql(
                PROPERTY_TABLE,
                con=conn,
                schema=PROPERTY_SCHEMA,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=10_000,
            )
        print("Ghi dữ liệu thành công vào DB")

        # Chuyển các file đã ETL thành công sang Archived Data
        for idx, file in enumerate(files):
            if idx >= len(file_checks):
                continue
            if file_checks[idx] is True:
                file_path = os.path.join(folder_path, file)
                print(f"Chuyển file sang Archived Data: {file_path}")
                try:
                    archived_path = os.path.join(
                        ARCHIVED_DATA_PATH, "Booking Pace", PROPERTY, file
                    )
                    os.makedirs(os.path.dirname(archived_path), exist_ok=True)
                    shutil.move(file_path, archived_path)
                except Exception as e:
                    print(e)
                    print(f"Lỗi khi chuyển file sang Archived Data: {file_path}")

    except Exception as e:
        print(e)
        print("Lỗi khi ghi dữ liệu vào DB")



if __name__ == "__main__":
    fload_property01()
