import os

API_URL = "https://tcgbusfs.blob.core.windows.net/dotapp/youbike/v2/youbike_immediate.json"
DB_PATH = os.path.join("data", "ubike.db")
DB_URL = f"sqlite:///{DB_PATH}"

# Analysis Thresholds
RISK_THRESHOLD_EMPTY = 1
RISK_THRESHOLD_FULL = 1




