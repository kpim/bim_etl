import argparse
from datetime import datetime

import app.etl.etl_smile_pq as etl_smile_pq
import app.etl.etl_smile_hl as etl_smile_hl
import app.etl.etl_opera as etl_opera
import app.etl.etl_booking_pace_detail as etl_booking_pace_detail
import app.etl.etl_booking_pace_report as etl_booking_pace_report


def fload():
    etl_smile_pq.fload()
    etl_smile_hl.fload()
    etl_opera.fload()

    etl_booking_pace_detail.fload()
    etl_booking_pace_report.fload()


def fload_history(history_date):
    etl_smile_hl.fload_history(history_date)
    etl_smile_pq.fload_history(history_date)
    etl_opera.fload_history(history_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", "-t", help="", default="fload")
    parser.add_argument("--history_date", help="", default="2025-08-16")

    args = parser.parse_args()
    task = args.task
    history_date = "2025-08-16" if args.history_date is None else args.history_date
    history_date = datetime.strptime(history_date, "%Y-%m-%d").date()

    if task == "fload":
        fload()
    elif task == "fload_history":
        fload_history(history_date)
