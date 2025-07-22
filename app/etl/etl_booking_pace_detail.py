import pandas as pd
from pyxlsb import open_workbook

from app.lib.connect_db import get_engine


def fload_sample_data():
    try:
        file_path = "./data/Sample Data/OTB & Pace_v20250324.xlsb"
        rows_list = []

        with open_workbook(file_path) as wb:
            with wb.get_sheet("DATA") as sheet:
                print(f"Reading data from sheet: {sheet.name}")
                # print(sheet.cols)

                for row_index, row in enumerate(sheet.rows()):
                    row_data = [cell.v for cell in row]
                    rows_list.append(row_data)

                df = pd.DataFrame(data=rows_list[1:], index=None, columns=rows_list[0])
                # chọn các cột cần thiết
                columns = [
                    "Report Date",
                    "Stay Month",
                    "Property",
                    "Arrival",
                    "Deprature",
                    "Staying",
                    "Creation Date",
                    "Market",
                    "Rate code",
                    "Rate Amt",
                    "Total turn Over",
                    "ARR",
                    "Room REV",
                    "FB Rev",
                    "Other Rev",
                    "Status",
                    "R type",
                    "R T Charge",
                    "N of Room",
                    "N of Adt",
                    "N of Chd",
                    "Bk source",
                    "Country",
                    "Nationality",
                ]
                df = df[columns]

                # đổi tên các cột
                columns = {
                    "Report Date": "REPORT_DATE",
                    "Stay Month": "STAY_MONTH",
                    "Property": "PROPERTY",
                    "Arrival": "ARRIVAL",
                    "Deprature": "DEPARTURE",
                    "Staying": "STAYING",
                    "Creation Date": "CREATE_DATE",
                    "Market": "MARKET",
                    "Rate code": "RATE_CODE",
                    "Rate Amt": "RATE_AMT",
                    "Total turn Over": "TOTAL_TURN_OVER",
                    "ARR": "ARR",
                    "Room REV": "ROOM_REV",
                    "FB Rev": "FB_REV",
                    "Other Rev": "OTHER_REV",
                    "Status": "STATUS",
                    "R type": "R_TYPE",
                    "R T Charge": "R_CHARGE",
                    "N of Room": "N_OF_ROOM",
                    "N of Adt": "N_OF_ADT",
                    "N of Chd": "N_OF_CHD",
                    "Bk source": "BK_SOURCE",
                    "Country": "COUNTRY",
                    "Nationality": "NATIONALITY",
                }
                df.rename(columns=columns, inplace=True)

                df["REPORT_DATE"] = pd.to_datetime(
                    "2023-05-01", format="%Y-%m-%d", errors="coerce"
                )

                date_cols = [
                    "ARRIVAL",
                    "DEPARTURE",
                    "STAYING",
                    "CREATE_DATE",
                ]
                for col in date_cols:
                    df[col] = pd.to_datetime(df[col], unit="D", origin="1899-12-30")

                df["STAY_MONTH"] = df["STAYING"].dt.strftime("%Y-%m")
                print(df.head(5))

                engine = get_engine()
                df.to_sql(
                    "booking_pace_sample_data",
                    con=engine,
                    schema="dbo",
                    if_exists="append",
                    index=False,
                )
                print("Ghi dữ liệu thành công vào DB")

    except Exception as e:
        print(e)


if __name__ == "__main__":
    fload_sample_data()
