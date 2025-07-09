import os
from dotenv import load_dotenv

load_dotenv()

RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", default="./data/Raw Data")
ARCHIVED_DATA_PATH = os.getenv("ARCHIVED_DATA_PATH", default="./data/Archived Data")

PROPERTY_LIST = ["Property 01", "Property 02", "Property 03"]
