import pandas as pd
from sqlalchemy import text
from src.database import get_engine
from src.config import RISK_THRESHOLD_EMPTY, RISK_THRESHOLD_FULL

def load_recent_data(minutes=30):
    """Load data from the last N minutes."""
    engine = get_engine()
    query = text(f"""
        SELECT * FROM stations_realtime 
        WHERE collection_time >= datetime('now', '-{minutes} minutes', 'localtime')
    """)
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return pd.DataFrame()

def get_current_status():
    """Get the latest snapshot of all stations (most recent record for each station)."""
    engine = get_engine()
    # We want the latest record for each sno.
    # Since we are using SQLite, we can use a window function or a group by max time.
    query = text("""
        SELECT t1.*
        FROM stations_realtime t1
        JOIN (
            SELECT sno, MAX(collection_time) as max_time
            FROM stations_realtime
            GROUP BY sno
        ) t2 ON t1.sno = t2.sno AND t1.collection_time = t2.max_time
    """)
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print(f"Error getting current status: {e}")
        return pd.DataFrame()

def find_high_risk_stations(df):
    """Identify stations with low bikes (empty risk) or low return spots (full risk)."""
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    # Empty Risk: rent <= threshold
    empty_risk = df[df['rent'] <= RISK_THRESHOLD_EMPTY].copy()
    
    # Full Risk: return_count <= threshold
    full_risk = df[df['return_count'] <= RISK_THRESHOLD_FULL].copy()
    
    return empty_risk, full_risk

def generate_status_report():
    """Generate a text report of high-risk stations."""
    df = get_current_status()
    if df.empty:
        return "No data available."
    
    empty_risk, full_risk = find_high_risk_stations(df)
    
    report = []
    report.append(f"=== Ubike Operation Status Report ===")
    report.append(f"Total Stations Monitored: {len(df)}")
    report.append(f"Time: {pd.Timestamp.now()}")
    report.append("-" * 30)
    
    report.append(f"🔴 EMPTY RISK (<= {RISK_THRESHOLD_EMPTY} bikes): {len(empty_risk)} stations")
    if not empty_risk.empty:
        for _, row in empty_risk.head(10).iterrows():
            report.append(f"  - [{row['sno']}] {row['sna']}: {row['rent']} bikes")
        if len(empty_risk) > 10:
            report.append(f"  ... and {len(empty_risk) - 10} more.")
            
    report.append("-" * 30)
    
    report.append(f"🔵 FULL RISK (<= {RISK_THRESHOLD_FULL} spots): {len(full_risk)} stations")
    if not full_risk.empty:
        for _, row in full_risk.head(10).iterrows():
            report.append(f"  - [{row['sno']}] {row['sna']}: {row['return_count']} spots")
        if len(full_risk) > 10:
            report.append(f"  ... and {len(full_risk) - 10} more.")
            
    return "\n".join(report)

if __name__ == "__main__":
    print(generate_status_report())
    
    
    
    
# --- Flow / Heatmap 用：最近 N 小時的流量統計 ---

from src.database import get_engine
from sqlalchemy import text


def get_hourly_flow_stats(hours_lookback: int = 24):
    """
    統計最近 N 小時的：
    - 全體平均 RPI（依小時）
    - 各區 x 小時 的平均 RPI（heatmap 用）

    RPI = (0.5 - utilization)
    utilization = rent / (rent + return_count)

    RPI > 0  → 需要補車
    RPI < 0  → 需要移車
    """
    engine = get_engine()

    query = text(f"""
        SELECT collection_time, sarea, rent, return_count
        FROM stations_realtime
        WHERE collection_time >= datetime('now', '-{hours_lookback} hours', 'localtime')
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        return (
            pd.DataFrame(columns=["hour", "avg_rpi"]),
            pd.DataFrame()
        )

    df["collection_time"] = pd.to_datetime(df["collection_time"], errors="coerce")
    df = df.dropna(subset=["collection_time"])
    df["hour"] = df["collection_time"].dt.hour

    df["capacity"] = (df["rent"] + df["return_count"]).clip(lower=1)
    df["util"] = df["rent"] / df["capacity"]
    df["rpi"] = 0.5 - df["util"]

    # 1) 全體平均 RPI（越大越需要補車）
    line_df = (
        df.groupby("hour", as_index=False)["rpi"]
        .mean()
        .rename(columns={"rpi": "avg_rpi"})
        .sort_values("hour")
    )

    # 2) 區域 x 小時 RPI heatmap
    heat_df = (
        df.groupby(["sarea", "hour"], as_index=False)["rpi"]
        .mean()
    )

    heat_piv = heat_df.pivot_table(
        index="sarea",
        columns="hour",
        values="rpi",
        aggfunc="mean",
    ).sort_index()

    heat_piv.columns = [int(c) for c in heat_piv.columns]

    return line_df, heat_piv
