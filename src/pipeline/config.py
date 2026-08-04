from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent

DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = DATA_DIR / "output"

ZONES_DATA_PATH = DATA_DIR / "taxi_zone_lookup.csv"

SPARK_APP_NAME = "NYCTaxiPipeline"