import sys
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT)

import pandas as pd
from sqlalchemy import text
from src.database import get_engine
from src.prediction import calculate_trend, predict_demand

def sample_station():
    engine = get_engine()
    sno = pd.read_sql("SELECT sno FROM stations_realtime LIMIT 1;", engine).iloc[0,0]
    return sno

def debug_station(sno):
    engine = get_engine()

    print("\n=== 最近 50 筆資料（照時間排序）===")
    df = pd.read_sql(
        text(f"""
            SELECT collection_time, rent, return_count
            FROM stations_realtime
            WHERE sno = '{sno}'
            ORDER BY collection_time DESC
            LIMIT 50;
        """),
        engine
    )
    print(df)

    print("\n=== 你現在模型計算的 trend ===")
    slope, curr, cap = calculate_trend(sno)
    print(f"slope = {slope:.4f} bikes/min")
    print(f"current_bikes = {curr}, capacity = {cap}")

    pred_30 = predict_demand(sno, 30)
    print(f"\n=== 預測 30 分鐘後車輛 ===")
    print(pred_30)

if __name__ == "__main__":
    sno = sample_station()
    print(f"Testing station: {sno}")
    debug_station(sno)