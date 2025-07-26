import os

from app.config import RAW_DATA_PATH, ARCHIVED_DATA_PATH, PROPERTY_LIST
from app.lib.connect_db import get_connection

import app.etl.etl_syrena_cruises as etl_syrena_cruises
import app.etl.etl_booking_pace_detail as etl_booking_pace_detail
import app.etl.etl_booking_pace_report as etl_booking_pace_report


def init():
    init_folder_data()
    init_db()


def init_folder_data():
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    os.makedirs(ARCHIVED_DATA_PATH, exist_ok=True)

    # tạo các folder lưu trữ file dữ liệu Booking Pace cho các khách sạn
    for p in PROPERTY_LIST:
        os.makedirs(os.path.join(RAW_DATA_PATH, "Booking Pace", p), exist_ok=True)
        os.makedirs(os.path.join(ARCHIVED_DATA_PATH, "Booking Pace", p), exist_ok=True)


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    sql = "CREATE DATABASE bim_report;"
    cursor.execute(sql)
    conn.commit()

    sql = "CREATE SCHEMA stg;"
    cursor.execute(sql)
    conn.commit()
    conn.close()

    etl_syrena_cruises.init()
    etl_booking_pace_detail.init()
    etl_booking_pace_report.init()


if __name__ == "__main__":
    init()
