import os
from dotenv import load_dotenv

load_dotenv()

RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", default="./data/Raw Data")
ARCHIVED_DATA_PATH = os.getenv("ARCHIVED_DATA_PATH", default="./data/Archived Data")

ALL_PROPERTIES = [
    # Template 01
    {
        "name": "Sailing Club Signature Resort Phu Quoc",
        "folder": "SCSRPQ",
        "template": "Template 01",
        "schema": "stg",
        "table": "booking_pace_scsrpq",
    },
    {
        "name": "Soul Boutique Hotel Phu Quoc",
        "folder": "SCSBHPQ",
        "template": "Template 01",
        "schema": "stg",
        "table": "booking_pace_scsbhpq",
    },
    # Template 02
    {
        "name": "Syrena Cruises",
        "folder": "Syrena Cruises",
        "template": "Template 02",
        "schema": "stg",
        "table": "booking_pace_syrena_cruises",
    },
    # Template 03
    {
        "name": "Crowne Plaza Vientaine",
        "folder": "Crowne Plaza Vientaine",
        "template": "Template 03",
        "schema": "stg",
        "table": "booking_pace_cpv",
    },
]
