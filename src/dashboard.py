import os
import sys
import datetime

import pandas as pd
import pydeck as pdk
import plotly.express as px
import streamlit as st
from sqlalchemy import text  # ✅ 新增

# 讓 Python 找到 src package
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.analysis import get_current_status, find_high_risk_stations
from src.config import RISK_THRESHOLD_EMPTY, RISK_THRESHOLD_FULL
from src.database import get_latest_collection_time, get_engine


# -----------------------------
# Cache：最新 snapshot
# -----------------------------
@st.cache_data(ttl=60)
def load_current_status():
    return get_current_status()


# -----------------------------
# Flow / Heatmap：最近 1 天 06–22
# -----------------------------
@st.cache_data(ttl=120)
def load_hourly_flow(start_hour: int = 6, end_hour: int = 22) -> pd.DataFrame:
    engine = get_engine()
    dialect = engine.dialect.name  # 'sqlite' or 'postgresql'

    if dialect == "postgresql":
        sql = """
            SELECT
                sarea,
                sno,
                sna,
                EXTRACT(HOUR FROM collection_time::timestamp)::integer AS hour,
                AVG(rent) AS avg_rent,
                AVG(return_count) AS avg_return
            FROM stations_realtime
            WHERE 
                collection_time::timestamp >= NOW() - INTERVAL '1 day'
                AND EXTRACT(HOUR FROM collection_time::timestamp)::integer 
                    BETWEEN %(sh)s AND %(eh)s
            GROUP BY sarea, sno, sna, hour
            ORDER BY sarea, sno, hour
        """
        df = pd.read_sql(sql, engine, params={"sh": start_hour, "eh": end_hour})

    else:
        sql = """
            SELECT
                sarea,
                sno,
                sna,
                CAST(strftime('%H', collection_time) AS INTEGER) AS hour,
                AVG(rent) AS avg_rent,
                AVG(return_count) AS avg_return
            FROM stations_realtime
            WHERE 
                collection_time >= datetime('now', '-1 day', 'localtime')
                AND CAST(strftime('%H', collection_time) AS INTEGER) 
                    BETWEEN :sh AND :eh
            GROUP BY sarea, sno, sna, hour
        """
        df = pd.read_sql(sql, engine, params={"sh": start_hour, "eh": end_hour})

    if df.empty:
        return df

    # ---- capacity & RPI ----
    capacity = df["avg_rent"] + df["avg_return"]
    df["capacity"] = capacity

    df["rpi"] = 0.0
    df["need_bikes"] = 0

    mask = capacity > 0
    df.loc[mask, "rpi"] = (
        (capacity[mask] * 0.5) - df.loc[mask, "avg_rent"]
    ) / capacity[mask]

    df.loc[mask, "need_bikes"] = (
        df.loc[mask, "rpi"] * capacity[mask]
    ).round().astype(int)

    return df


# -----------------------------
# Streamlit Layout 設定
# -----------------------------
st.set_page_config(page_title="Ubike Operation Dashboard", layout="wide")
st.title("🚲 Ubike Operation Optimization System")

# ===== Sidebar：導航 + 狀態 =====
st.sidebar.header("Configuration")

# Snapshot refresh（只清 cache，不動 page）
if st.sidebar.button("🔄 Refresh Snapshot"):
    load_current_status.clear()

# 頁面導航（Style B：用 sidebar radio，狀態放 session_state）
PAGES = [
    "🗺️ Map View",
    "⚠️ High Risk Stations",
    "🔮 Prediction",
    "🏷 Station Types",
    "📈 Flow / Heatmap",
]

default_page = st.session_state.get("active_page", PAGES[0])
page = st.sidebar.radio("頁面", PAGES, index=PAGES.index(default_page))
st.session_state["active_page"] = page

# DB 最新 collection_time
latest_ct = get_latest_collection_time()
if latest_ct:
    latest_dt = pd.to_datetime(latest_ct, errors="coerce")
    latest_str = (
        latest_dt.strftime("%Y-%m-%d %H:%M:%S")
        if not pd.isna(latest_dt)
        else str(latest_ct)
    )
else:
    latest_str = "N/A"

st.sidebar.markdown("### 📦 DB 最新資料時間")
st.sidebar.write(f"**{latest_str}**")

st.sidebar.markdown("### ⏱ 現在時間")
st.sidebar.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

st.sidebar.markdown("---")
st.sidebar.caption(
    "Snapshot 來自 load_current_status()；Flow/Heatmap 使用最近 24 小時資料。"
)

# ===== 主資料（snapshot） =====
df = load_current_status()
# 移除測試區域，避免 Test Area 影響所有頁面
df = df[df["sarea"] != "Test Area"].copy()

if df.empty:
    st.error("No data available. Please ensure the collector is running.")
    st.stop()

# Summary 指標
total_stations = len(df)
empty_risk, full_risk = find_high_risk_stations(df)

c1, c2, c3 = st.columns(3)
c1.metric("Total Stations", total_stations)
c2.metric("🔴 Empty Risk Stations", len(empty_risk))
c3.metric("🔵 Full Risk Stations", len(full_risk))

st.markdown("---")

# ======================================================
# PAGE 1 — Map View
# ======================================================
if page == "🗺️ Map View":
    st.subheader("Station Map (Risk View)")

    sarea_options = ["全部區域"] + sorted(df["sarea"].dropna().unique().tolist())
    selected_sarea = st.selectbox("區域篩選", sarea_options, index=0)

    risk_view = st.radio(
        "顯示類型", ["全部站", "空車風險", "滿站風險"], horizontal=True
    )

    if selected_sarea != "全部區域":
        map_df = df[df["sarea"] == selected_sarea].copy()
    else:
        map_df = df.copy()

    if map_df.empty:
        st.warning("這個區域目前沒有資料。")
    else:
        map_df = map_df[
            ["lat", "lng", "sna", "rent", "return_count", "sno"]
        ].copy()
        map_df = map_df.rename(columns={"lng": "lon"})

        capacity_map = (map_df["rent"] + map_df["return_count"]).clip(lower=1)
        map_df["empty_ratio"] = 1 - (map_df["rent"] / capacity_map)
        map_df["full_ratio"] = 1 - (map_df["return_count"] / capacity_map)

        if risk_view == "空車風險":
            map_df = map_df[map_df["rent"] <= RISK_THRESHOLD_EMPTY]
        elif risk_view == "滿站風險":
            map_df = map_df[map_df["return_count"] <= RISK_THRESHOLD_FULL]

        if map_df.empty:
            st.warning("目前符合條件的站點為 0。")
        else:
            color_expr_col = (
                "empty_ratio" if risk_view != "滿站風險" else "full_ratio"
            )

            center_lat = map_df["lat"].mean()
            center_lon = map_df["lon"].mean()

            layer = pdk.Layer(
                "ScatterplotLayer",
                data=map_df,
                get_position="[lon, lat]",
                get_radius=60,
                pickable=True,
                get_fill_color=f"[255 * {color_expr_col}, 100, 150]",
                get_line_color=[0, 0, 0],
            )

            view_state = pdk.ViewState(
                latitude=center_lat,
                longitude=center_lon,
                zoom=12,
                pitch=0,
            )

            deck = pdk.Deck(
                layers=[layer],
                initial_view_state=view_state,
                tooltip={
                    "text": "{sna}\n可借: {rent}  可還: {return_count}"
                },
            )

            st.pydeck_chart(deck)

# ======================================================
# PAGE 2 — High Risk Stations
# ======================================================
elif page == "⚠️ High Risk Stations":
    col_empty, col_full = st.columns(2)

    with col_empty:
        st.subheader("🔴 Empty Risk (Low Bikes)")
        if not empty_risk.empty:
            st.dataframe(
                empty_risk[
                    ["sno", "sna", "rent", "update_time", "collection_time"]
                ]
            )
        else:
            st.success("No empty risk stations.")

    with col_full:
        st.subheader("🔵 Full Risk (Low Spots)")
        if not full_risk.empty:
            st.dataframe(
                full_risk[
                    [
                        "sno",
                        "sna",
                        "return_count",
                        "update_time",
                        "collection_time",
                    ]
                ]
            )
        else:
            st.success("No full risk stations.")

# ======================================================
# PAGE 3 — Prediction
# ======================================================
elif page == "🔮 Prediction":
    st.subheader("Demand Prediction (Trend-Based)")
    st.write("使用最近時間序列的線性趨勢，預測未來 0–60 分鐘可借車數。")

    station_options = df.apply(
        lambda x: f"{x['sno']} - {x['sna']}", axis=1
    ).tolist()
    selected_station_str = st.selectbox("選擇站點", station_options)

    if selected_station_str:
        sno = selected_station_str.split(" - ")[0]
        from src.prediction import calculate_trend, predict_demand

        slope, current_bikes, capacity_pred, points_used = calculate_trend(
            sno, max_points=30
        )

        col_curr, col_trend, col_cap, col_pts = st.columns(4)
        col_curr.metric("Current Bikes", current_bikes)
        col_trend.metric("Trend (bikes/min)", f"{slope:.2f}")
        col_cap.metric("Capacity", capacity_pred)
        col_pts.metric("Points Used", points_used)

        if points_used < 3:
            st.warning("資料點少於 3 筆，預測準度較低。")

        future_times = []
        predictions = []
        now = datetime.datetime.now()

        for m in range(0, 61, 5):
            future_time = now + datetime.timedelta(minutes=m)
            pred, _info = predict_demand(
                sno, minutes_ahead=m, max_points=30
            )
            future_times.append(future_time.strftime("%H:%M"))
            predictions.append(pred)

        pred_df = pd.DataFrame(
            {"Time": future_times, "Predicted Bikes": predictions}
        )

        fig_pred = px.line(
            pred_df,
            x="Time",
            y="Predicted Bikes",
            markers=True,
        )
        st.plotly_chart(fig_pred, width="stretch")
        st.dataframe(pred_df.set_index("Time"))

# ======================================================
# PAGE 4 — Station Types (Clusters)
# ======================================================
elif page == "🏷 Station Types":
    st.subheader("Station Clusters (Usage Pattern)")

    try:
        cluster_df = pd.read_csv("data/station_clusters.csv")
        df["sno"] = df["sno"].fillna("").astype(str)
        cluster_df["sno"] = cluster_df["sno"].fillna("").astype(str)
    except FileNotFoundError:
        st.warning(
            "尚未產生 station_clusters.csv，請先在終端機執行：\n"
            "`python -m src.clustering`"
        )
    else:
        merged = df.merge(
            cluster_df[["sno", "cluster"]],
            on="sno",
            how="left",
        )

        cluster_ids = sorted(
            merged["cluster"].dropna().unique().tolist()
        )

        selected_cluster = st.selectbox(
            "選擇要查看的 cluster", options=cluster_ids
        )

        sub = merged[merged["cluster"] == selected_cluster].copy()
        st.write(f"Cluster {selected_cluster} — 站點數：{len(sub)}")

        st.write("目前 snapshot 狀態：")
        st.dataframe(
            sub[
                ["sarea", "sno", "sna", "rent", "return_count"]
            ].sort_values(["sarea", "sno"])
        )

        if not sub.empty:
            map_df = sub[
                ["lat", "lng", "sna", "rent", "return_count"]
            ].copy()
            map_df = map_df.rename(columns={"lng": "lon"})

            layer = pdk.Layer(
                "ScatterplotLayer",
                data=map_df,
                get_position="[lon, lat]",
                get_radius=60,
                pickable=True,
                get_fill_color="[50, 150, 255]",
                get_line_color=[0, 0, 0],
            )

            view_state = pdk.ViewState(
                latitude=map_df["lat"].mean(),
                longitude=map_df["lon"].mean(),
                zoom=12,
                pitch=0,
            )

            deck = pdk.Deck(
                layers=[layer],
                initial_view_state=view_state,
                tooltip={
                    "text": "{sna}\n可借: {rent}  可還: {return_count}"
                },
            )

            st.pydeck_chart(deck)

# ======================================================
# PAGE 5 — Flow / Heatmap
# ======================================================
elif page == "📈 Flow / Heatmap":
    st.subheader("📈 日流量趨勢 & 熱點圖（依小時 / 區域）")

    START_HOUR = 6
    END_HOUR = 22

    flow_df = load_hourly_flow(START_HOUR, END_HOUR)
    if flow_df.empty:
        st.warning("最近 24 小時內沒有足夠資料可供 Flow / Heatmap 分析。")
    else:
        # 模式切換在主畫面，不在 sidebar（避免頁面跳回去）
        mode = st.radio(
            "檢視模式",
            ["Flow（指定站）", "Heatmap（全部區域）"],
            horizontal=True,
        )

        # -------- Flow：指定站 --------
        if mode == "Flow（指定站）":
            st.markdown("### 手動選站 Flow（平均可借車數）")

            station_options = df.apply(
                lambda x: f"{x['sno']} - {x['sna']}", axis=1
            ).tolist()
            selected_stations = st.multiselect(
                "選擇要看的站點（建議 1–5 個）",
                options=station_options,
            )

            if selected_stations:
                selected_snos = [s.split(" - ")[0] for s in selected_stations]
                sub = flow_df[flow_df["sno"].isin(selected_snos)].copy()

                if sub.empty:
                    st.warning("這些站在最近 24 小時內沒有資料。")
                else:
                    sub["station_label"] = sub["sna"].fillna(sub["sno"])

                    fig_flow = px.line(
                        sub,
                        x="hour",
                        y="avg_rent",
                        color="station_label",
                        markers=True,
                        labels={
                            "hour": "Hour of Day",
                            "avg_rent": "Avg. Rent Bikes",
                            "station_label": "Station",
                        },
                    )
                    fig_flow.update_xaxes(dtick=1)
                    st.plotly_chart(fig_flow, width="stretch")
            else:
                st.info("請至少選擇一個站點來看 Flow。")

        # -------- Heatmap：全部區域（RPI） --------
        else:
            st.markdown("### 區域 x 小時 熱點圖（補車壓力指數 RPI）")

            # 先去掉測試用區域（保險：Flow 資料如果還留 Test Area 也一起過濾）
            area_df = (
                flow_df[flow_df["sarea"] != "Test Area"]
                .groupby(["sarea", "hour"], as_index=False)["rpi"]
                .mean()
            )

            if area_df.empty:
                st.warning("無法產生熱點圖，資料不足。")
            else:
                pivot = area_df.pivot(
                    index="sarea", columns="hour", values="rpi"
                ).fillna(0.0)

                cols = [
                    h
                    for h in range(START_HOUR, END_HOUR + 1)
                    if h in pivot.columns
                ]
                pivot = pivot[cols]

                fig_hm = px.imshow(
                    pivot,
                    aspect="auto",
                    labels=dict(
                        x="Hour of Day",
                        y="Area (sarea)",
                        color="RPI",
                    ),
                    origin="lower",
                    color_continuous_scale="RdBu_r",
                    zmin=-0.6,
                    zmax=0.6,
                )
                st.plotly_chart(fig_hm, width="stretch")