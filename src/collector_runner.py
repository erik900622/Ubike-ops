# src/collector_runner.py
import time
from src.collector import job

INTERVAL_SECONDS = 60  # 每 60 秒抓一次，你之後可以自己調

if __name__ == "__main__":
    while True:
        job()
        time.sleep(INTERVAL_SECONDS)