# src/config.py
import os

API_URL = "https://tcgbusfs.blob.core.windows.net/dotapp/youbike/v2/youbike_immediate.json"

DB_URL = os.getenv("DB_URL", "sqlite:///data/ubike.db")

RISK_THRESHOLD_EMPTY = 1
RISK_THRESHOLD_FULL = 1
