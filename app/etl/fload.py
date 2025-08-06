import argparse

import app.etl.etl_template01 as etl_template01
import app.etl.etl_template02 as etl_template02
import app.etl.etl_template03 as etl_template03
import app.etl.etl_booking_pace_detail as etl_booking_pace_detail
import app.etl.etl_booking_pace_report as etl_booking_pace_report


def fload():
    etl_template01.fload()
    etl_template02.fload()
    etl_template03.fload()

    etl_booking_pace_detail.fload()
    etl_booking_pace_report.fload()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", "-t", help="", default="fload")

    args = parser.parse_args()
    task = args.task

    if task == "fload":
        fload()
