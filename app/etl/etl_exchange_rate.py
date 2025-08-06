import argparse
import requests
import pandas as pd
from io import StringIO

from datetime import datetime, date

from app.lib.connect_db import get_engine, get_connection


def init():
    print(f"Init table stg.exchange_rate")

    conn = get_connection()
    cursor = conn.cursor()

    sql = f"""
    CREATE TABLE stg.exchange_rate (
        REPORT_DATE DATE,
        FROM_CURRENCY_CODE NVARCHAR(10),
        TO_CURRENCY_CODE NVARCHAR(10),

        BUY_CASH DECIMAL(18, 4),
        BUY_TRANSFER DECIMAL(18, 4),
        SELL_CASH DECIMAL(18, 4),
        SELL_TRANSFER DECIMAL(18, 4),

        CREATED_AT DATETIME,
        MODIFIED_AT DATETIME
    )
    """
    cursor.execute(sql)
    conn.commit()
    conn.close()


def get_exchange_rate_usd(report_date: date):
    # print(report_date)
    current_time = datetime.today()

    url = f"https://vietcombank.com.vn/api/exchangerates?date={report_date.strftime("%Y-%m-%d")}"
    try:
        response = requests.get(url)
        result = response.json()
        # print(result)
        df = pd.DataFrame(result["Data"])
        # print(df.head())
        df.rename(
            columns={
                "currencyCode": "FROM_CURRENCY_CODE",
                "cash": "BUY_CASH",
                "transfer": "BUY_TRANSFER",
                "sell": "SELL_CASH",
            },
            inplace=True,
        )

        df["REPORT_DATE"] = report_date
        df["TO_CURRENCY_CODE"] = "VND"
        df["SELL_TRANSFER"] = df["SELL_CASH"]
        df["CREATED_AT"] = current_time
        df["MODIFIED_AT"] = current_time

        columns = [
            "REPORT_DATE",
            "FROM_CURRENCY_CODE",
            "TO_CURRENCY_CODE",
            "BUY_CASH",
            "BUY_TRANSFER",
            "SELL_CASH",
            "SELL_TRANSFER",
            "CREATED_AT",
            "MODIFIED_AT",
        ]

        df = df[columns]
        df = df[df["FROM_CURRENCY_CODE"] == "USD"]

        # xóa dữ liệu cũ trong ngày
        conn = get_connection()
        cursor = conn.cursor()
        sql = f"""
        DELETE FROM stg.exchange_rate WHERE REPORT_DATE = ? AND FROM_CURRENCY_CODE='USD' AND TO_CURRENCY_CODE='VND'
        """
        cursor.execute(sql, report_date)
        cursor.commit()

        # ghi dữ liệu vào CSDL
        engine = get_engine()
        df.to_sql(
            "exchange_rate",
            con=engine,
            schema="stg",
            if_exists="append",
            index=False,
        )
        print("Ghi dữ liệu thành công vào DB")

    except Exception as e:
        print(e)
    finally:
        conn.close()


def get_exchange_rate_lak(report_date: date):
    current_time = datetime.today()
    url = f"https://www.laovietbank.com.la/en_US/exchange/exchange-rate.html"

    try:
        response = requests.get(url)
        result = response.text

        dfs = pd.read_html(StringIO(result))

        if len(dfs) > 0:
            df = dfs[0]
            df = df[df["Currency"].isin(["USD/LAK", "VND/LAK"])]
            df[["FROM_CURRENCY_CODE", "TO_CURRENCY_CODE"]] = df["Currency"].str.split(
                "/", expand=True
            )

            df.rename(
                columns={
                    "Buying Rate": "BUY_CASH",
                    "Buying Rate.1": "BUY_TRANSFER",
                    "Selling Rate": "SELL_CASH",
                    "Selling Rate.1": "SELL_TRANSFER",
                },
                inplace=True,
            )
            df["BUY_CASH"] = df["BUY_CASH"].str.split(" ", n=1).str[0]

            for column in ["BUY_CASH", "BUY_TRANSFER", "SELL_CASH", "SELL_TRANSFER"]:
                df[column] = df[column].str.replace(".", "")
                df[column].loc[df[column].str.startswith("0")] = (
                    "0." + df[column].str[1:]
                )
                df[column] = df[column].astype(float)

            df["REPORT_DATE"] = report_date
            df["CREATED_AT"] = current_time
            df["MODIFIED_AT"] = current_time

            columns = [
                "REPORT_DATE",
                "FROM_CURRENCY_CODE",
                "TO_CURRENCY_CODE",
                "BUY_CASH",
                "BUY_TRANSFER",
                "SELL_CASH",
                "SELL_TRANSFER",
                "CREATED_AT",
                "MODIFIED_AT",
            ]

            df = df[columns]
            print(df)

            # xóa dữ liệu cũ trong ngày
            conn = get_connection()
            cursor = conn.cursor()
            sql = f"""
            DELETE FROM stg.exchange_rate 
            WHERE REPORT_DATE = ? AND FROM_CURRENCY_CODE IN('USD','VND') AND TO_CURRENCY_CODE='LAK'
            """
            cursor.execute(sql, report_date)
            cursor.commit()

            # ghi dữ liệu vào CSDL
            engine = get_engine()
            df.to_sql(
                "exchange_rate",
                con=engine,
                schema="stg",
                if_exists="append",
                index=False,
            )
            print("Ghi dữ liệu thành công vào DB")

    except Exception as e:
        print(e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", "-t", help="", default="get_exchange_rate")
    parser.add_argument("--period", "-p", help="", default="today")
    parser.add_argument("--day", "-d", help="%d%m%Y", default="26072025")

    args = parser.parse_args()
    task = args.task
    period = args.period
    day = args.day

    if task == "init":
        init()
    elif task == "get_exchange_rate":
        if period == "today":
            today = date.today()
            get_exchange_rate_usd(today)
            get_exchange_rate_lak(today)
        elif period == "day":
            day = datetime.strptime(day, "%d%m%Y")
            get_exchange_rate_usd(day)
