import os
from dotenv import load_dotenv

load_dotenv()

RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", default="./data/Raw Data")
ARCHIVED_DATA_PATH = os.getenv("ARCHIVED_DATA_PATH", default="./data/Archived Data")

ALL_PROPERTIES = [
    {
        "code": "HLP",
        "name": "Ha Long Plaza Hotel",
        "folder": "HLP",
        "template": "SMILE HL",
        "schema": "stg",
        "table": "booking_pace_hlp",
    },
    {
        "code": "SRC",
        "name": "Syrena Cruises",
        "folder": "SRC",
        "template": "SMILE HL",
        "schema": "stg",
        "table": "booking_pace_src",
    },
    {
        "code": "FSH",
        "name": "Fraser Suites Hanoi",
        "folder": "FSH",
        "template": "",
        "schema": "stg",
        "table": "booking_pace_fsh",
    },
    {
        "code": "PHPQ",
        "name": "Park Hyatt Phu Quoc Residences",
        "folder": "PHPQ",
        "template": "Opera",
        "schema": "stg",
        "table": "booking_pace_phpq",
    },
    {
        "code": "ICHL",
        "name": "InterContinental Ha Long Bay Resort",
        "folder": "ICHL",
        "template": "Opera",
        "schema": "stg",
        "table": "booking_pace_ichl",
    },
    {
        "code": "ICPQ",
        "name": "InterContinental Phu Quoc Long Beach Resort",
        "folder": "ICPQ",
        "template": "Opera",
        "schema": "stg",
        "table": "booking_pace_icpq",
    },
    {
        "code": "REPQ",
        "name": "Regent Phu Quoc",
        "folder": "REPQ",
        "template": "Opera",
        "schema": "stg",
        "table": "booking_pace_repq",
    },
    {
        "code": "CPV",
        "name": "Crowne Plaza Vientiane",
        "folder": "CPV",
        "template": "Opera",
        "schema": "stg",
        "table": "booking_pace_cpv",
    },
    {
        "code": "HIVT",
        "name": "Holiday Inn & Suites Vientiane",
        "folder": "HIVT",
        "template": "Opera",
        "schema": "stg",
        "table": "booking_pace_hivt",
    },
    {
        "code": "SCSRHL",
        "name": "Sailing Club Signature Resort Ha Long Bay",
        "folder": "SCSRHL",
        "template": "SMILE HL",
        "schema": "stg",
        "table": "booking_pace_scsrhl",
    },
    {
        "code": "SCSRPQ",
        "name": "Sailing Club Signature Resort Phu Quoc",
        "folder": "SCSRPQ",
        "template": "SMILE PQ",
        "schema": "stg",
        "table": "booking_pace_scsrpq",
    },
    {
        "code": "SBHPQ",
        "name": "Soul Boutique Hotel Phu Quoc",
        "folder": "SBHPQ",
        "template": "SMILE PQ",
        "schema": "stg",
        "table": "booking_pace_sbhpq",
    },
    {
        "code": "CMH",
        "name": "Citadines Marina Ha Long",
        "folder": "CMH",
        "template": "",
        "schema": "stg",
        "table": "booking_pace_cmh",
    },
]
