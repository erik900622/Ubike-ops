import sys, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.append(ROOT)
import pandas as pd
from sqlalchemy import text
from src.database import get_engine


def _get_recent_points(station_id: str, max_points: int = 30) -> pd.DataFrame:
    """
    抓出某站最近 max_points 筆資料，由新到舊，再轉成舊→新。
    """
    engine = get_engine()

    query = text("""
        SELECT collection_time, rent, return_count
        FROM stations_realtime
        WHERE sno = :sno
        ORDER BY collection_time DESC
        LIMIT :limit
    """)

    df = pd.read_sql(query, engine, params={"sno": station_id, "limit": max_points})

    if df.empty:
        return df

    df["collection_time"] = pd.to_datetime(df["collection_time"], errors="coerce")
    df = df.sort_values("collection_time")

    return df.dropna(subset=["collection_time"])


def calculate_trend(station_id: str, max_points: int = 30):
    """
    v2：使用「局部變化 slope 平均值」作為 trend，比單一斜率更穩定。
    回傳：(slope, current_bikes, capacity, points_used)
    """
    df = _get_recent_points(station_id, max_points=max_points)

    if df.empty or len(df) < 3:
        # 資料太少，直接抓最新一筆
        engine = get_engine()
        latest = pd.read_sql(
            text("""
                SELECT rent, return_count
                FROM stations_realtime
                WHERE sno = :sno
                ORDER BY collection_time DESC
                LIMIT 1
            """),
            engine,
            params={"sno": station_id}
        )

        if latest.empty:
            return 0.0, 0, 0, 0

        curr = int(latest.iloc[0]["rent"])
        cap = curr + int(latest.iloc[0]["return_count"])
        return 0.0, curr, cap, len(df)

    # 計算局部 slope（前後兩筆）
    slopes = []
    for i in range(1, len(df)):
        t1 = df.iloc[i - 1]["collection_time"]
        t2 = df.iloc[i]["collection_time"]
        dt_min = (t2 - t1).total_seconds() / 60

        if dt_min <= 0:
            continue

        r1 = df.iloc[i - 1]["rent"]
        r2 = df.iloc[i]["rent"]
        slopes.append((r2 - r1) / dt_min)

    # 平均 slope（更穩定）
    slope = sum(slopes) / len(slopes) if slopes else 0.0

    current_bikes = int(df.iloc[-1]["rent"])
    capacity = current_bikes + int(df.iloc[-1]["return_count"])

    return slope, current_bikes, capacity, len(df)


def predict_demand(station_id: str, minutes_ahead: int = 30, max_points: int = 30):
    """
    v2：使用平滑 slope 預測未來。
    回傳：(預測值, 模型資訊 dict)
    """
    slope, current_bikes, capacity, points_used = calculate_trend(
        station_id,
        max_points=max_points
    )

    predicted = current_bikes + slope * minutes_ahead

    predicted = max(0, min(capacity, predicted))

    return int(round(predicted)), {
        "slope": slope,
        "current": current_bikes,
        "capacity": capacity,
        "points_used": points_used,
    }


if __name__ == "__main__":
    engine = get_engine()
    sno = pd.read_sql("SELECT sno FROM stations_realtime LIMIT 1;", engine).iloc[0, 0]

    pred, info = predict_demand(sno, 30)

    print(f"\n=== 測試站 {sno} ===")
    print("Points used:", info["points_used"])
    print("Current:", info["current"])
    print("Capacity:", info["capacity"])
    print(f"Slope: {info['slope']:.4f} bikes/min")
    print("Prediction (30 min):", pred)