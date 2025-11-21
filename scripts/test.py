import os
import sqlite3
import pandas as pd

# 這裡改成你的 SQLite 檔案路徑
# 例如：/Users/erikk/Desktop/Coding/Ubike/data/youbike.db
DB_PATH = os.path.expanduser("/Users/erikk/Desktop/Coding/Ubike/data/ubike.db")

def main():
    print("連線到 DB:", DB_PATH)
    conn = sqlite3.connect(DB_PATH)

    # 1. 看前 5 筆資料
    print("\n=== stations_realtime 前 5 筆 ===")
    df_head = pd.read_sql("SELECT * FROM stations_realtime LIMIT 5;", conn)
    print(df_head)

    # 2. 基本統計：資料量 & 時間範圍 & 站點數
    print("\n=== 基本統計 ===")
    df_stats = pd.read_sql(
        """
        SELECT 
            COUNT(*) AS total_rows,
            COUNT(DISTINCT sno) AS station_count,
            MIN(collection_time) AS first_time,
            MAX(collection_time) AS last_time
        FROM stations_realtime;
        """,
        conn,
    )
    print(df_stats)

    # 3. rent / return_count 是否有負數、極端值
    print("\n=== rent / return_count 範圍 ===")
    df_minmax = pd.read_sql(
        """
        SELECT 
            MIN(rent) AS min_rent,
            MAX(rent) AS max_rent,
            MIN(return_count) AS min_return,
            MAX(return_count) AS max_return
        FROM stations_realtime;
        """,
        conn,
    )
    print(df_minmax)

    print("\n=== 有沒有負數資料 ===")
    df_neg = pd.read_sql(
        """
        SELECT 
            SUM(CASE WHEN rent < 0 THEN 1 ELSE 0 END) AS neg_rent_rows,
            SUM(CASE WHEN return_count < 0 THEN 1 ELSE 0 END) AS neg_return_rows
        FROM stations_realtime;
        """,
        conn,
    )
    print(df_neg)

    # 4. 每個站容量是否大致固定（用 rent+return_count 的最大值當 capacity）
    print("\n=== 每站推算容量統計（前 10 站） ===")
    df_cap = pd.read_sql(
        """
        SELECT 
            sno,
            sna,
            sarea,
            MAX(rent + return_count) AS approx_capacity,
            COUNT(*) AS obs
        FROM stations_realtime
        GROUP BY sno, sna, sarea
        ORDER BY obs DESC
        LIMIT 10;
        """,
        conn,
    )
    print(df_cap)

    conn.close()


if __name__ == "__main__":
    main()