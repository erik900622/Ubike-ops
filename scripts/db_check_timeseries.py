import sys
import os

# 讓 Python 找到專案根目錄（重要）
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.append(ROOT)
from src.database import get_engine
from sqlalchemy import text
import pandas as pd

def main():
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT sno, COUNT(*) AS cnt
                FROM stations_realtime
                GROUP BY sno
                ORDER BY cnt DESC
                LIMIT 10
            """),
            conn
        )

    print("=== 累積最多資料的 10 個站 ===")
    print(df)

    # 找筆數最多的站
    top_sno = df.iloc[0]["sno"]
    print(f"\n最多資料的站：{top_sno}")

    # 讀取該站的時間序列
    df_ts = pd.read_sql(
        text("""
            SELECT collection_time, rent, return_count
            FROM stations_realtime
            WHERE sno = :sno
            ORDER BY collection_time DESC
            LIMIT 20
        """),
        get_engine(),
        params={"sno": top_sno}
    )

    print(f"\n=== {top_sno} 時間序列（最新） ===")
    print(df_ts)

if __name__ == "__main__":
    main()