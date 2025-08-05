import argparse

import app.etl.etl_template01 as etl_template01
import app.etl.etl_template02 as etl_template02
import app.etl.etl_template03 as etl_template03
import app.etl.etl_booking_pace_detail as etl_booking_pace_detail
import app.etl.etl_booking_pace_report as etl_booking_pace_report


def iload():
    etl_template01.iload()
    etl_template02.iload()
    etl_template03.iload()
    # etl_booking_pace_detail.iload()
    # etl_booking_pace_report.iload()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", "-t", help="", default="iload")

    args = parser.parse_args()
    task = args.task

    if task == "iload":
        iload()
