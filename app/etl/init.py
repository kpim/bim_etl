import os

from app.config import RAW_DATA_PATH, ARCHIVED_DATA_PATH
from app.lib.connect_db import get_connection

import app.etl.etl_template01 as etl_template01
import app.etl.etl_template02 as etl_template02
import app.etl.etl_booking_pace_detail as etl_booking_pace_detail
import app.etl.etl_booking_pace_report as etl_booking_pace_report


def init():
    init_folder_data()
    init_db()


def init_folder_data():
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    os.makedirs(ARCHIVED_DATA_PATH, exist_ok=True)


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # sql = "CREATE DATABASE bim_report;"
        # cursor.execute(sql)
        # conn.commit()

        # sql = "CREATE SCHEMA stg;"
        # cursor.execute(sql)
        # conn.commit()

        # conn.close()

        etl_template01.init()
        etl_template02.init()

        etl_booking_pace_detail.init()
        etl_booking_pace_report.init()
    except Exception as e:
        print("Lỗi khi khởi tạo CSDL")
        print(e)


if __name__ == "__main__":
    init()
