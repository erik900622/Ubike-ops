import os
import sys
from typing import Optional

import datetime
import pandas as pd
import pydeck as pdk
import streamlit as st

# 讓 Python 找到 src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.analysis import get_current_status, find_high_risk_stations
from src.config import RISK_THRESHOLD_EMPTY, RISK_THRESHOLD_FULL
from src.database import get_latest_collection_time
from src.rebalancing import compute_rebalance
from src.prediction import calculate_trend, predict_demand


# ========= Helpers =========
@st.cache_data(ttl=60)
def load_current_status() -> pd.DataFrame:
    """包一層 cache，避免每次互動都重抓 DB。"""
    return get_current_status()


def make_arrow_friendly(df: pd.DataFrame) -> pd.DataFrame:
    """
    避免 pyarrow 轉換問題：
    只要 object 欄位裡面混有非字串，就全部轉成字串。
    """
    if df.empty:
        return df

    out = df.copy()
    for col in out.columns:
        if out[col].dtype == "object":
            if not out[col].map(lambda x: isinstance(x, str) or pd.isna(x)).all():
                out[col] = out[col].astype(str)
    return out


# ========= Page Config =========
st.set_page_config(page_title="Ubike Operation Dashboard", layout="wide")
st.title("🚲 Ubike Operation Optimization System")

# ========= Sidebar =========
st.sidebar.header("Configuration")

# nav 在側邊欄（避免調整參數時跳回第一個 tab）
page = st.sidebar.radio(
    "頁面切換",
    [
        "🗺️ Map View",
        "⚠️ High Risk Stations",
        "🔮 Prediction",
        "🏷 Station Types",
        "🚚 Rebalance",
    ],
)

refresh = st.sidebar.button("Refresh Data")

# 讀取資料（有 cache）
if refresh:
    load_current_status.clear()

df = load_current_status()

# 最新 collection_time 顯示
latest_ct: Optional[str] = get_latest_collection_time()
if latest_ct:
    latest_dt = pd.to_datetime(latest_ct, errors="coerce")
    latest_str = (
        latest_dt.strftime("%Y-%m-%d %H:%M:%S")
        if not pd.isna(latest_dt)
        else str(latest_ct)
    )
else:
    latest_str = "N/A"

st.sidebar.markdown("---")
st.sidebar.info(f"📦 資料庫最新 collection_time：\n**{latest_str}**")
st.sidebar.markdown("---")
st.sidebar.info(
    f"Dashboard render time：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
)

# ========= Main Body =========
if df.empty:
    st.error("No data available. Please ensure the collector is running.")
    st.stop()

# Summary 指標（所有頁面共用）
total_stations = len(df)
empty_risk, full_risk = find_high_risk_stations(df)

col1, col2, col3 = st.columns(3)
col1.metric("Total Stations", total_stations)
col2.metric("🔴 Empty Risk Stations", len(empty_risk))
col3.metric("🔵 Full Risk Stations", len(full_risk))

st.markdown("---")

# ======================================================
# Page 1 — Map View
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

        capacity = (map_df["rent"] + map_df["return_count"]).clip(lower=1)
        map_df["empty_ratio"] = 1 - (map_df["rent"] / capacity)
        map_df["full_ratio"] = 1 - (map_df["return_count"] / capacity)

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
                latitude=center_lat, longitude=center_lon, zoom=12, pitch=0
            )

            deck = pdk.Deck(
                layers=[layer],
                initial_view_state=view_state,
                tooltip={"text": "{sna}\n可借: {rent}  可還: {return_count}"},
            )

            st.pydeck_chart(deck)

# ======================================================
# Page 2 — High Risk Stations
# ======================================================
elif page == "⚠️ High Risk Stations":
    st.subheader("High Risk Stations")

    col_empty, col_full = st.columns(2)

    with col_empty:
        st.markdown("### 🔴 Empty Risk (Low Bikes)")
        if not empty_risk.empty:
            st.dataframe(
                make_arrow_friendly(
                    empty_risk[
                        ["sno", "sna", "sarea", "rent", "update_time", "collection_time"]
                    ]
                )
            )
        else:
            st.success("No empty risk stations.")

    with col_full:
        st.markdown("### 🔵 Full Risk (Low Spots)")
        if not full_risk.empty:
            st.dataframe(
                make_arrow_friendly(
                    full_risk[
                        [
                            "sno",
                            "sna",
                            "sarea",
                            "return_count",
                            "update_time",
                            "collection_time",
                        ]
                    ]
                )
            )
        else:
            st.success("No full risk stations.")

# ======================================================
# Page 3 — Prediction
# ======================================================
elif page == "🔮 Prediction":
    st.subheader("Demand Prediction (Trend-Based)")
    st.write("利用最近時間序列趨勢預測未來短時間的可借車數。")

    station_options = df.apply(
        lambda x: f"{x['sno']} - {x['sna']}", axis=1
    ).tolist()
    selected_station_str = st.selectbox("選擇站點", station_options)

    if selected_station_str:
        sno = selected_station_str.split(" - ")[0]

        slope, current_bikes, capacity, points_used = calculate_trend(
            sno, max_points=30
        )

        st.markdown(f"### 預測站點：`{selected_station_str}`")

        col_curr, col_trend, col_cap, col_pts = st.columns(4)
        col_curr.metric("Current Bikes", current_bikes)
        col_trend.metric("Trend (bikes/min)", f"{slope:.2f}")
        col_cap.metric("Capacity", capacity)
        col_pts.metric("Points Used", points_used)

        if points_used < 3:
            st.warning("資料點少於 3 筆，預測準度較低。")

        # 0–60 分鐘，每 5 分鐘預測一次
        future_times = []
        predictions = []
        now = datetime.datetime.now()

        for m in range(0, 61, 5):
            future_time = now + datetime.timedelta(minutes=m)
            pred, info = predict_demand(
                sno, minutes_ahead=m, max_points=30
            )
            future_times.append(future_time.strftime("%H:%M"))
            predictions.append(pred)

        pred_df = pd.DataFrame(
            {"Time": future_times, "Predicted Bikes": predictions}
        )

        import plotly.express as px

        fig = px.line(pred_df, x="Time", y="Predicted Bikes", markers=True)
        fig.update_layout(
            xaxis_title="Time (next 60 min)",
            yaxis_title="Predicted Bikes",
            margin=dict(l=10, r=10, t=30, b=10),
        )
        st.plotly_chart(fig, width="stretch")

        st.dataframe(make_arrow_friendly(pred_df))

# ======================================================
# Page 4 — Station Types (Clusters)
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
            cluster_df[["sno", "cluster"]], on="sno", how="left"
        )

        cluster_ids = sorted(merged["cluster"].dropna().unique().tolist())
        if not cluster_ids:
            st.info("目前沒有 cluster 標籤，請確認 clustering 是否成功。")
        else:
            selected_cluster = st.selectbox(
                "選擇要查看的 cluster", options=cluster_ids
            )

            sub = merged[merged["cluster"] == selected_cluster].copy()
            st.write(f"Cluster {selected_cluster} — 站點數：{len(sub)}")

            st.write("目前 snapshot 狀態：")
            st.dataframe(
                make_arrow_friendly(
                    sub[
                        ["sarea", "sno", "sna", "rent", "return_count"]
                    ].sort_values(["sarea", "sno"])
                )
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
                    get_fill_color="[50, 150, 255]",  # 同一顏色代表同一群
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
# Page 5 — Rebalance (補車建議)
# ======================================================
elif page == "🚚 Rebalance":
    st.subheader("Rebalance Recommendation (補車 / 移車建議)")

    st.write(
        "根據目前庫存與最近時間序列趨勢，預測未來短時間內的空站 / 滿站風險，"
        "並給出補車 / 移車建議（站點級別，不考慮實際車隊路線）。"
    )

    col_hor, col_top = st.columns(2)
    minutes_ahead = col_hor.slider(
        "預測時間（分鐘）", min_value=15, max_value=120, value=30, step=15
    )
    top_k = col_top.slider(
        "每類建議最多顯示幾筆", min_value=5, max_value=50, value=20, step=5
    )

    supply_df, remove_df = compute_rebalance(
        df,
        minutes_ahead=minutes_ahead,
        max_points=30,
        empty_threshold=RISK_THRESHOLD_EMPTY,
        full_threshold=RISK_THRESHOLD_FULL,
        target_low_ratio=0.40,
        target_high_ratio=0.60,
        top_k=top_k,
    )

    col_supply, col_remove = st.columns(2)

    with col_supply:
        st.markdown("### 🔴 需補車站點（預測將變空站）")
        if supply_df.empty:
            st.success("目前找不到需要補車的站點（依你設定的條件）。")
        else:
            st.dataframe(
                make_arrow_friendly(
                    supply_df[
                        [
                            "sarea",
                            "sno",
                            "sna",
                            "current_bikes",
                            "predicted_bikes",
                            "capacity",
                            "need_add",
                            "slope",
                            "priority",
                        ]
                    ]
                )
            )

    with col_remove:
        st.markdown("### 🔵 需移車站點（預測將變滿站）")
        if remove_df.empty:
            st.success("目前找不到需要移車的站點（依你設定的條件）。")
        else:
            st.dataframe(
                make_arrow_friendly(
                    remove_df[
                        [
                            "sarea",
                            "sno",
                            "sna",
                            "current_bikes",
                            "predicted_bikes",
                            "capacity",
                            "need_remove",
                            "slope",
                            "priority",
                        ]
                    ]
                )
            )