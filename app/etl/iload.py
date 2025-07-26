import app.etl.etl_syrena_cruises as etl_syrena_cruises
import app.etl.etl_booking_pace_detail as etl_booking_pace_detail
import app.etl.etl_booking_pace_report as etl_booking_pace_report


def iload():
    etl_syrena_cruises.iload()
    etl_booking_pace_detail.iload()
    etl_booking_pace_report.iload()


if __name__ == "__main__":
    iload()
