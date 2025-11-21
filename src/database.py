# src/database.py
import pandas as pd
from sqlalchemy import create_engine, text
from src.config import DB_URL

# 全域 Engine（本地 SQLite / 未來可換成 PostgreSQL）
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)


def get_engine():
    return engine


# ============================================================
# 1. 建表 + 索引
# ============================================================
def init_db():
    """建立 stations_realtime 表與索引（若不存在）。"""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stations_realtime (
                id INTEGER PRIMARY KEY,
                collection_time TEXT,
                sno TEXT,
                sna TEXT,
                sarea TEXT,
                lat REAL,
                lng REAL,
                rent INTEGER,
                return_count INTEGER,
                update_time TEXT
            );
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_sno_time
            ON stations_realtime (sno, collection_time);
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_collection_time
            ON stations_realtime (collection_time);
        """))

    print("[database] Initialized.")


# ============================================================
# 2. 寫入資料（API → DataFrame → DB）
# ============================================================
def save_data(df: pd.DataFrame):
    """清洗欄位型別後寫入 stations_realtime。"""
    if df.empty:
        print("[database] No data to save.")
        return

    df = df.rename(columns={
        "time": "collection_time",
        "return": "return_count",
    }).copy()

    # 時間欄位 → ISO 字串
    for col in ["collection_time", "update_time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").astype(str)

    # 數值欄位轉型
    for col in ["rent", "return_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in ["lat", "lng"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)

    df.to_sql("stations_realtime", engine, if_exists="append", index=False)
    print(f"[database] Saved {len(df)} records.")


# ============================================================
# 3. 刪舊資料（之後資料量大再用）
# ============================================================
def cleanup_old_data(days: int = 5):
    """
    刪除早於 now - days 的資料。
    注意：collection_time 是 TEXT，但因為是 ISO 格式，可以直接用。
    """
    with engine.begin() as conn:
        result = conn.execute(text(f"""
            DELETE FROM stations_realtime
            WHERE collection_time < datetime('now', '-{days} days')
        """))
    print(f"[database] Cleanup removed {result.rowcount} rows (< now-{days}d).")


# ============================================================
# 4. 查 DB 最新一筆 collection_time（給 dashboard sidebar 用）
# ============================================================
def get_latest_collection_time():
    """回傳 DB 中最大 collection_time（字串，ISO 格式）。"""
    with engine.connect() as conn:
        latest = conn.execute(text("""
            SELECT MAX(collection_time) AS latest_ct
            FROM stations_realtime
        """)).scalar()
    return latest


# ============================================================
# 5. 取得單站時間序列（給預測 / 檢查用）
# ============================================================
def get_station_timeseries(sno: str, limit: int = 200, order: str = "desc") -> pd.DataFrame:
    """
    取得某站最近 limit 筆資料。
    order: "asc"（舊→新）或 "desc"（新→舊）。
    """
    order_sql = "ASC" if order == "asc" else "DESC"

    with engine.connect() as conn:
        df = pd.read_sql(
            text(f"""
                SELECT collection_time, rent, return_count
                FROM stations_realtime
                WHERE sno = :sno
                ORDER BY collection_time {order_sql}
                LIMIT :limit
            """),
            conn,
            params={"sno": sno, "limit": limit},
        )

    if df.empty:
        return df

    df["collection_time"] = pd.to_datetime(df["collection_time"], errors="coerce")
    return df


# ============================================================
# 6. 取得單站「每日 summary」（給日流量趨勢圖用）
# ============================================================
def get_daily_summary(sno: str) -> pd.DataFrame:
    """
    依日期彙總單站資料：min/max/avg rent & return_count。
    之後畫「日趨勢圖」會用到。
    """
    with engine.connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT 
                    DATE(collection_time) AS day,
                    MIN(rent)          AS min_rent,
                    MAX(rent)          AS max_rent,
                    AVG(rent)          AS avg_rent,
                    MIN(return_count)  AS min_return,
                    MAX(return_count)  AS max_return,
                    COUNT(*)           AS samples
                FROM stations_realtime
                WHERE sno = :sno
                GROUP BY DATE(collection_time)
                ORDER BY day ASC
            """),
            conn,
            params={"sno": sno},
        )
    return df