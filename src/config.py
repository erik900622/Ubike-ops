import os

# YouBike 即時資料 API
API_URL = "https://tcgbusfs.blob.core.windows.net/dotapp/youbike/v2/youbike_immediate.json"

# ---- Database 連線設定 ----
# 優先使用環境變數 DB_URL（雲端 / GitHub Actions / Streamlit 用）
# 若未設定，則退回本地 SQLite（你本機開發、debug 用）
DB_URL = os.getenv("DB_URL", "sqlite:///data/ubike.db")

# ---- 分析閾值設定 ----
RISK_THRESHOLD_EMPTY = 1   # 可借車數 <= 1 視為空車風險
RISK_THRESHOLD_FULL = 1    # 可還車位 <= 1 視為滿站風險
