import pandas as pd
from sqlalchemy import text
from sklearn.cluster import KMeans

from src.database import get_engine


def build_station_features(days: int = 3) -> pd.DataFrame:
    """
    從資料庫抽取最近 days 天資料，為每個站點建立特徵：
    - 平均可借車數 mean_rent
    - 平均可還車位 mean_return
    - 空站比例 empty_ratio (rent == 0 的比例)
    - 滿站比例 full_ratio (return_count == 0 的比例)
    - 早上 7–10 平均 rent (morning_rent)
    - 晚上 17–20 平均 rent (evening_rent)
    之後可以再加更多特徵。
    """
    engine = get_engine()

    query = text(f"""
        SELECT
            sno,
            sna,
            sarea,
            collection_time,
            rent,
            return_count
        FROM stations_realtime
        WHERE collection_time >= datetime('now', '-{days} days', 'localtime')
    """)

    df = pd.read_sql(query, engine)
    if df.empty:
        raise ValueError("最近幾天資料為空，無法做分群。")

    # 轉時間
    df["collection_time"] = pd.to_datetime(df["collection_time"])
    df["hour"] = df["collection_time"].dt.hour

    # 基本特徵
    grp = df.groupby("sno")

    mean_rent = grp["rent"].mean().rename("mean_rent")
    mean_return = grp["return_count"].mean().rename("mean_return")

    empty_ratio = (grp["rent"].apply(lambda x: (x == 0).mean())
                   .rename("empty_ratio"))
    full_ratio = (grp["return_count"].apply(lambda x: (x == 0).mean())
                  .rename("full_ratio"))

    # 早上 / 晚上特徵
    morning = df[(df["hour"] >= 7) & (df["hour"] < 10)]
    evening = df[(df["hour"] >= 17) & (df["hour"] < 20)]

    morning_rent = (morning.groupby("sno")["rent"].mean()
                    .rename("morning_rent"))
    evening_rent = (evening.groupby("sno")["rent"].mean()
                    .rename("evening_rent"))

    # 組成一個特徵表
    feat = pd.concat(
        [mean_rent, mean_return, empty_ratio, full_ratio,
         morning_rent, evening_rent],
        axis=1
    ).reset_index()

    # 把 sna, sarea 補回來（拿最新一筆）
    latest_meta = (df.sort_values("collection_time")
                     .groupby("sno")
                     .tail(1)[["sno", "sna", "sarea"]])

    feat = feat.merge(latest_meta, on="sno", how="left")

    # 缺值處理（沒有早上/晚上資料 → 用 0 補）
    for col in ["morning_rent", "evening_rent"]:
        if col in feat.columns:
            feat[col] = feat[col].fillna(0)

    return feat


def cluster_stations(k: int = 4, days: int = 3) -> pd.DataFrame:
    """
    對站點做 KMeans 分群，回傳帶有 cluster_label 的 DataFrame，並存成 CSV。
    """
    feat = build_station_features(days=days)

    # 只用數值欄位進行分群
    num_cols = [
        "mean_rent",
        "mean_return",
        "empty_ratio",
        "full_ratio",
        "morning_rent",
        "evening_rent",
    ]
    X = feat[num_cols].fillna(0).values

    model = KMeans(n_clusters=k, random_state=42, n_init=10)
    labels = model.fit_predict(X)

    feat["cluster"] = labels

    # 調整欄位順序
    feat = feat[["sno", "sna", "sarea", "cluster"] + num_cols]

    # 存一份到 data 資料夾
    feat.to_csv("data/station_clusters.csv", index=False, encoding="utf-8-sig")
    print(f"[clustering] Saved cluster result to data/station_clusters.csv")

    return feat


if __name__ == "__main__":
    # 本機測試用：直接跑一次分群
    res = cluster_stations(k=4, days=3)
    print(res.head())