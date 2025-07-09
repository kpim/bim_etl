import os
import urllib

import pyodbc
from sqlalchemy import create_engine, URL
from dotenv import load_dotenv

load_dotenv()


def get_engine():
    try:
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        server = os.getenv("DB_SERVER")
        database = os.getenv("DB_DATABASE")

        connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            "TrustServerCertificate=yes"
        )
        connection_url = URL.create(
            "mssql+pyodbc", query={"odbc_connect": connection_string}
        )
        engine = create_engine(connection_url)

        with engine.connect() as conn:
            print("Kết nối thành công")
            return engine

    except Exception as e:
        print(f"Kết nối thất bại: {str(e)}")
        raise


def get_connection():
    try:
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        server = os.getenv("DB_SERVER")
        database = os.getenv("DB_DATABASE")

        connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            "TrustServerCertificate=yes"
        )
        connection = pyodbc.connect(connection_string)
        print("Kết nối thành công")
        return connection

    except Exception as e:
        print(f"Kết nối thất bại: {str(e)}")
        raise


if __name__ == "__main__":
    engine = get_engine()
    connection = get_connection()
