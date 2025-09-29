import argparse

import app.etl.etl_smile_pq as etl_smile_pq
import app.etl.etl_smile_hl as etl_smile_hl
import app.etl.etl_opera as etl_opera
import app.etl.etl_booking_pace_detail as etl_booking_pace_detail
import app.etl.etl_booking_pace_report as etl_booking_pace_report


def iload():
    etl_smile_pq.iload()
    etl_smile_hl.iload()
    etl_opera.iload()

    etl_booking_pace_detail.iload()
    etl_booking_pace_report.iload()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", "-t", help="", default="iload")

    args = parser.parse_args()
    task = args.task

    if task == "iload":
        iload()
