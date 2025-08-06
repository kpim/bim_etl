import os
import argparse

from app.config import RAW_DATA_PATH, ARCHIVED_DATA_PATH
from app.lib.connect_db import get_connection

import app.etl.etl_template01 as etl_template01
import app.etl.etl_template02 as etl_template02
import app.etl.etl_template03 as etl_template03
import app.etl.etl_booking_pace_detail as etl_booking_pace_detail
import app.etl.etl_booking_pace_report as etl_booking_pace_report
import app.etl.etl_exchange_rate as etl_exchange_rate


def init():
    init_folder()
    init_db()


def init_folder():
    print(f"Init folder data")
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    os.makedirs(ARCHIVED_DATA_PATH, exist_ok=True)


def init_db():
    print(f"Init database")
    conn = get_connection()
    cursor = conn.cursor()

    try:
        etl_template01.init()
        etl_template02.init()
        etl_template03.init()
        etl_exchange_rate.init()

        etl_booking_pace_detail.init()
        etl_booking_pace_report.init()
    except Exception as e:
        print("Init database error")
        print(e)
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", "-t", help="", default="init")

    args = parser.parse_args()
    task = args.task

    if task == "init":
        init()
    elif task == "init_db":
        init_db()
    elif task == "init_folder":
        init_folder()
