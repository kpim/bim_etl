import app.etl.etl_syrena_cruises as etl_syrena_cruises
import app.etl.etl_booking_pace_detail as etl_booking_pace_detail
import app.etl.etl_booking_pace_report as etl_booking_pace_report


def fload():
    etl_syrena_cruises.fload()
    etl_booking_pace_detail.fload()
    etl_booking_pace_report.fload()


if __name__ == "__main__":
    fload()
