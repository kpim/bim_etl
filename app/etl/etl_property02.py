import os
import pathlib
import shutil

import pandas as pd

from app.lib.connect_db import get_engine
from app.config import RAW_DATA_PATH, ARCHIVED_DATA_PATH

PROPERTY = "Property 02"
PROPERTY_SCHEMA = "stg"
PROPERTY_TABLE = "booking_pace_p2"


def fload_property02():
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

            df = pd.read_excel(file_path, engine="xlrd")
            # chuẩn hóa các cột date
            date_cols = ["Arrival", "Deprature", "Staying", "Create time"]
            for col in date_cols:
                df[col] = pd.to_datetime("1899-12-30") + pd.to_timedelta(
                    df[col], unit="D"
                )
            # đổi tên các cột
            column_mapping = {
                "Folionum": "FOLIONUM",
                "Arrival": "ARRIVAL",
                "Deprature": "DEPARTURE",
                "Staying": "STAYING",
                "Create time": "CREATE_TIME",
                "Group code": "GROUP_CODE",
                "Grroup Name": "GROUP_NAME",
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
                "N of \nAdt": "N_OF_ADT",
                "N of Chd": "N_OF_CHD",
                "Bk source": "BK_SOURCE",
                "Country": "COUNTRY",
                "Nationality": "NATIONALITY",
            }
            # Đổi tên cột
            df.rename(columns=column_mapping, inplace=True)
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


def iload_property02():
    pass


if __name__ == "__main__":
    fload_property02()
