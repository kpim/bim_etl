import os
import re
import pathlib
from datetime import datetime


def get_files(
    folder_path: str, pattern: str = r".*Date(\d{2})(\d{2})(\d{4})_(\d{2})(\d{2})"
):
    files = []
    for f in pathlib.Path(folder_path).iterdir():
        if f.is_file():
            filename_re = re.compile(pattern, re.IGNORECASE)
            match = filename_re.match(f.name)

            if match:
                dd, mm, yyyy, HH, MM = match.groups()
                report_date = datetime.strptime(f"{dd} {mm} {yyyy}", "%d %m %Y")
                report_at = datetime.strptime(
                    f"{dd} {mm} {yyyy} {HH} {MM}", "%d %m %Y %H %M"
                )

                files.append(
                    {
                        "name": f.name,
                        "report_date": report_date,
                        "report_at": report_at,
                        "modified_at": datetime.fromtimestamp(os.path.getmtime(f)),
                        "created_at": datetime.fromtimestamp(os.path.getctime(f)),
                    }
                )
    return files


def get_lastest_snapshot_df(files_df):
    lastest_snapshot_df = files_df.loc[
        files_df.groupby("report_date")["report_at"].idxmax()
    ]
    lastest_snapshot_df.sort_values("report_date", inplace=True)

    return lastest_snapshot_df


def get_history_file(folder_path: str):
    for f in pathlib.Path(folder_path).iterdir():
        if f.is_file():
            filename_re = re.compile(r".*_history", re.IGNORECASE)

            match = filename_re.match(f.name)
            if match:
                return {
                    "file": f,
                    "name": f.name,
                    "modified_at": datetime.fromtimestamp(os.path.getmtime(f)),
                    "created_at": datetime.fromtimestamp(os.path.getctime(f)),
                }
