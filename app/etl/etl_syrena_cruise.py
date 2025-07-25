import os
import pathlib
import shutil
import re
from datetime import datetime

import numpy as np
import pandas as pd
from sqlalchemy import text

from app.lib.connect_db import get_engine
from app.config import RAW_DATA_PATH, ARCHIVED_DATA_PATH

PROPERTY = "Property 01"
PROPERTY_SCHEMA = "stg"
PROPERTY_TABLE = "booking_pace_p1"
TIMEZONE = "Asia/Ho_Chi_Minh"

RAW_SUBFOLDER = os.path.join("Booking Pace", PROPERTY)

FILENAME_RE = re.compile(r".*Date(\d{2})(\d{2})(\d{4})_(\d{2})(\d{2})", re.IGNORECASE)

# Final columns we keep 
LEGACY_RENAME_MAP = {
    "Report Date": "REPORT_DATE",  
    "Stay Month": "STAY_MONTH",
    "Property": "PROPERTY",
    "Arrival": "ARRIVAL",
    "Deprature": "DEPARTURE",      
    "Departure": "DEPARTURE",     
    "Staying": "STAYING",
    "Creation Date": "CREATE_DATE",
    "Create time": "CREATE_DATE", 
    "Market": "MARKET",
    "Rate code": "RATE_CODE",
    "Rate Amt": "RATE_AMT",
    "Total turn Over": "TOTAL_TURN_OVER",
    "ARR": "ARR",
    "Room REV": "ROOM_REV",
    "Room Rev": "ROOM_REV",       
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

FINAL_COLUMNS = [
    "REPORT_DATE",
    "STAY_MONTH",
    "PROPERTY",
    "ARRIVAL",
    "DEPARTURE",
    "STAYING",
    "CREATE_DATE",
    "MARKET",
    "RATE_CODE",
    "RATE_AMT",
    "TOTAL_TURN_OVER",
    "ARR",
    "ROOM_REV",
    "FB_REV",
    "OTHER_REV",
    "STATUS",
    "R_TYPE",
    "R_CHARGE",
    "N_OF_ROOM",
    "N_OF_ADT",
    "N_OF_CHD",
    "BK_SOURCE",
    "COUNTRY",
    "NATIONALITY",
    # timestamp columns
    "REPORT_TS",
    "LOADED_AT",
    "FILE_NAME",
]

DATE_COLS_MAYBE_EXCEL_SERIAL = [
    "ARRIVAL",
    "DEPARTURE",
    "CREATE_DATE",
]

def excel_serial_to_datetime(series: pd.Series) -> pd.Series:
    """
    Convert a pandas Series that may contain Excel serials (floats/ints)
    to datetime; leave datetime-like or strings as-is (best effort).
    """
    mask_num = pd.to_numeric(series, errors="coerce").notna()
    out = pd.to_datetime(series, errors="coerce")
    if mask_num.any():
        numeric_vals = pd.to_numeric(series[mask_num], errors="coerce")
        out.loc[mask_num] = (
            pd.to_datetime("1899-12-30") + pd.to_timedelta(numeric_vals, unit="D")
        )
    return out

def extract_snapshot_dt(filename):
    """
    Return (report_date (date-normalized Timestamp), report_ts (Timestamp with time))
    """
    m = FILENAME_RE.match(filename)
    if not m:
        return None, None
    dd, mm, yyyy, HH, MM = m.groups()
    ts = pd.Timestamp(f"{yyyy}-{mm}-{dd} {HH}:{MM}:00", tz=TIMEZONE)
    return ts.normalize(), ts

def choose_latest_per_day(files):
    """
    From a list of filenames, pick the latest snapshot by day.
    Returns: {filename: (report_date, report_ts)}
    """
    winners = {}
    meta = {}  
    for f in files:
        rd, rts = extract_snapshot_dt(f)
        if rd is None:
            continue
        if rd not in meta or rts > meta[rd][1]:
            meta[rd] = (f, rts)
    for rd, (f, rts) in meta.items():
        winners[f] = (rd, rts)
    return winners

def archive_file(src):
    dst = os.path.join(ARCHIVED_DATA_PATH, RAW_SUBFOLDER, os.path.basename(src))
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.move(src, dst)

def _coerce_and_fill_legacy(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Rename to legacy uppercase names using LEGACY_RENAME_MAP.
    - Keep only FINAL_COLUMNS (missing columns are created as NaN).
    - Coerce date columns if needed.
    - Create STAY_MONTH if missing (derive from ARRIVAL).
    """
    # rename
    # only rename columns that exist in df
    rename_subset = {k: v for k, v in LEGACY_RENAME_MAP.items() if k in df.columns}
    df = df.rename(columns=rename_subset)

    # Make sure all final columns exist
    for col in FINAL_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    # Coerce potential excel-serial date columns
    for c in DATE_COLS_MAYBE_EXCEL_SERIAL:
        if c in df.columns:
            df[c] = excel_serial_to_datetime(df[c])

    # STAY_MONTH logic if missing
    if df["STAY_MONTH"].isna().all() and "ARRIVAL" in df.columns:
        # YYYY-MM from ARRIVAL
        try:
            df["STAY_MONTH"] = pd.to_datetime(df["ARRIVAL"]).dt.strftime("%Y-%m")
        except Exception:
            pass

    # Keep only final columns and order them
    df = df[FINAL_COLUMNS]
    return df

def read_and_prepare(file_path, report_date, report_ts):
    df = pd.read_excel(file_path)  # .xls provided
    df = _coerce_and_fill_legacy(df)

    # Overwrite the technical columns
    # REPORT_DATE: store as date (no tz)
    df["REPORT_DATE"] = report_date.tz_localize(None).date()
    df["REPORT_TS"] = report_ts.tz_convert(TIMEZONE).tz_localize(None)
    df["LOADED_AT"] = pd.Timestamp.now(tz=TIMEZONE).tz_localize(None)
    df["PROPERTY"] = PROPERTY
    df["FILE_NAME"] = os.path.basename(file_path)

    return df

def _get_raw_folder():
    return os.path.join(RAW_DATA_PATH, RAW_SUBFOLDER)


def fload_property01():
    """
    Full load: For each calendar day, keep only the latest snapshot and append.
    """
    print(f"[Thuc hien Full Load {PROPERTY}")
    folder_path = _get_raw_folder()
    if not os.path.isdir(folder_path):
        print(f"Raw folder not found: {folder_path}")
        return

    files = sorted([f.name for f in pathlib.Path(folder_path).iterdir() if f.is_file()])
    winners = choose_latest_per_day(files)
    if not winners:
        print("No valid files to load.")
        return

    df_list = []
    for file in winners:
        file_path = os.path.join(folder_path, file)
        rd, rts = winners[file]
        try:
            df_list.append(read_and_prepare(file_path, rd, rts))
            print(f"Prepared (latest of day): {file}")
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    if not df_list:
        print("Nothing to insert.")
        return

    total_df = pd.concat(df_list, ignore_index=True)
    engine = get_engine()
    with engine.begin() as conn:
        total_df.to_sql(
            PROPERTY_TABLE,
            con=conn,
            schema=PROPERTY_SCHEMA,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10_000,
        )
    print("Inserted FLOAD snapshots.")

    # archive only the latest-per-day files
    for file in winners:
        archive_file(os.path.join(folder_path, file))


def iload_property01():
    """
    Incremental load:
    - For each day that appears in RAW, pick the latest snapshot.
    - DELETE old rows for (PROPERTY, REPORT_DATE) and INSERT the new snapshot.
    - Archive the loaded files.
    """
    print(f"[ILOAD] {PROPERTY}")

    folder_path = _get_raw_folder()
    if not os.path.isdir(folder_path):
        print(f"Raw folder not found: {folder_path}")
        return

    files = sorted([f.name for f in pathlib.Path(folder_path).iterdir() if f.is_file()])
    winners = choose_latest_per_day(files)
    if not winners:
        print("No valid files to load.")
        return

    engine = get_engine()
    for file, (rd, rts) in winners.items():
        file_path = os.path.join(folder_path, file)
        try:
            df = read_and_prepare(file_path, rd, rts)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            continue

        report_date_value = df["REPORT_DATE"].iloc[0]
        with engine.begin() as conn:
            # delete old snapshot for that day
            conn.execute(
                text(f"""
                    DELETE FROM {PROPERTY_SCHEMA}.{PROPERTY_TABLE}
                    WHERE PROPERTY = :prop AND REPORT_DATE = :rdate
                """),
                {"prop": PROPERTY, "rdate": report_date_value},
            )
            # insert new
            df.to_sql(
                PROPERTY_TABLE,
                con=conn,
                schema=PROPERTY_SCHEMA,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=10_000,
            )
        print(
            f"Replaced data for (PROPERTY={PROPERTY}, REPORT_DATE={report_date_value}) "
            f"with snapshot {rts} from file {file}"
        )
        archive_file(file_path)


if __name__ == "__main__":
    # fload_property01()
    iload_property01()
