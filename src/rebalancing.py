import logging
from typing import Tuple

import pandas as pd

from src.prediction import predict_demand

logger = logging.getLogger(__name__)


def compute_rebalance(
    snapshot_df: pd.DataFrame,
    minutes_ahead: int = 30,
    max_points: int = 30,
    empty_threshold: int = 1,
    full_threshold: int = 1,
    target_low_ratio: float = 0.40,
    target_high_ratio: float = 0.60,
    top_k: int = 20,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    給一個「當前站點 snapshot df」（例如 get_current_status() 的輸出），
    計算未來 minutes_ahead 內可能變成空站 / 滿站的站點，
    並根據缺口與趨勢給出補車 / 移車建議。

    回傳：
        need_supply_df: 需要補車的站列表
        need_remove_df: 需要移車的站列表
    """

    if snapshot_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # 確保必要欄位存在
    required_cols = {"sno", "sna", "sarea", "rent", "return_count"}
    missing = required_cols - set(snapshot_df.columns)
    if missing:
        logger.warning("[rebalancing] snapshot_df 缺少欄位: %s", missing)
        return pd.DataFrame(), pd.DataFrame()

    df = snapshot_df.copy()

    # 容量（可借 + 可還）
    df["capacity"] = (pd.to_numeric(df["rent"], errors="coerce").fillna(0) +
                      pd.to_numeric(df["return_count"], errors="coerce").fillna(0))
    df = df[df["capacity"] > 0].reset_index(drop=True)
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    supply_rows = []
    remove_rows = []

    for row in df.itertuples(index=False):
        sno = str(row.sno)
        sna = row.sna
        sarea = row.sarea
        current_bikes = int(row.rent)
        capacity = int(row.capacity)

        # 呼叫你的預測模型
        try:
            pred_bikes, info = predict_demand(
                sno, minutes_ahead=minutes_ahead, max_points=max_points
            )
        except Exception as e:
            logger.error("[rebalancing] predict_demand error for %s: %s", sno, e)
            continue

        slope = float(info.get("slope", 0.0))
        points_used = int(info.get("points_used", 0))

        if points_used < 2:
            # 資料太少，先不給建議
            continue

        # 基本保護
        if capacity <= 0:
            continue

        predicted_bikes = max(0, min(capacity, int(pred_bikes)))
        predicted_empty_slots = capacity - predicted_bikes

        # 空站風險（需要補車）
        if predicted_bikes <= empty_threshold:
            target = max(2, int(capacity * target_low_ratio))
            need_add = max(0, target - predicted_bikes)

            # 越接近 0 越嚴重
            severity = 1.0 - (predicted_bikes / capacity)
            priority = 0.4 * abs(slope) + 0.6 * severity

            if need_add > 0:
                supply_rows.append(
                    {
                        "sno": sno,
                        "sna": sna,
                        "sarea": sarea,
                        "current_bikes": current_bikes,
                        "predicted_bikes": predicted_bikes,
                        "capacity": capacity,
                        "need_add": need_add,
                        "slope": slope,
                        "severity": severity,
                        "priority": priority,
                        "forecast_horizon_min": minutes_ahead,
                        "points_used": points_used,
                    }
                )

        # 滿站風險（需要移車）
        if predicted_empty_slots <= full_threshold:
            target = min(capacity - 2, int(capacity * target_high_ratio))
            target = max(0, target)
            need_remove = max(0, predicted_bikes - target)

            # 越接近滿越嚴重
            severity = 1.0 - (predicted_empty_slots / capacity)
            priority = 0.4 * abs(slope) + 0.6 * severity

            if need_remove > 0:
                remove_rows.append(
                    {
                        "sno": sno,
                        "sna": sna,
                        "sarea": sarea,
                        "current_bikes": current_bikes,
                        "predicted_bikes": predicted_bikes,
                        "capacity": capacity,
                        "need_remove": need_remove,
                        "slope": slope,
                        "severity": severity,
                        "priority": priority,
                        "forecast_horizon_min": minutes_ahead,
                        "points_used": points_used,
                    }
                )

    need_supply_df = pd.DataFrame(supply_rows)
    need_remove_df = pd.DataFrame(remove_rows)

    if not need_supply_df.empty:
        need_supply_df = need_supply_df.sort_values(
            ["priority", "need_add"], ascending=[False, False]
        ).head(top_k)

    if not need_remove_df.empty:
        need_remove_df = need_remove_df.sort_values(
            ["priority", "need_remove"], ascending=[False, False]
        ).head(top_k)

    return need_supply_df, need_remove_df