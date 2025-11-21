# src/collector.py
import logging
from datetime import datetime
from typing import Optional

import requests
import pandas as pd
from requests.adapters import HTTPAdapter, Retry

from src.config import API_URL
from src.database import save_data

logger = logging.getLogger(__name__)


def _build_session() -> requests.Session:
    """
    建立帶重試機制的 Session。
    雲端環境網路偶發錯誤時，不會整次 job 掛掉。
    """
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,  # 0.5s, 1s, 2s...
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def parse_update_time(raw_time: Optional[str]) -> Optional[datetime]:
    """
    把 mday / updateTime 轉成 datetime：
    - 'YYYY-MM-DD HH:MM:SS'
    - 'YYYYMMDDHHMMSS'
    解析失敗回傳 None。
    """
    if not raw_time:
        return None

    raw_time = str(raw_time).strip()

    if " " in raw_time:
        try:
            return datetime.strptime(raw_time, "%Y-%m-%d %H:%M:%S")
        except Exception:
            pass

    if len(raw_time) == 14 and raw_time.isdigit():
        try:
            return datetime.strptime(raw_time, "%Y%m%d%H%M%S")
        except Exception:
            pass

    return None


def fetch_data(session: Optional[requests.Session] = None) -> Optional[pd.DataFrame]:
    """
    從 YouBike API 抓資料並清洗成 DataFrame。
    失敗時回傳 None，不丟 exception，方便排程穩定跑。
    """
    if session is None:
        session = _build_session()

    now = datetime.now()
    logger.info("[collector] Fetching data at %s ...", now)

    try:
        resp = session.get(API_URL, timeout=10)
    except Exception as e:
        logger.error("[collector] Request error: %s", e)
        return None

    if resp.status_code != 200:
        logger.error("[collector] Bad status code: %s", resp.status_code)
        return None

    try:
        data = resp.json()
    except Exception as e:
        logger.error("[collector] JSON parse error: %s", e)
        return None

    logger.info("[collector] Got %d records", len(data))

    records = []
    for s in data:
        # 數值：可借 / 可還
        try:
            rent = int(s.get("available_rent_bikes", 0))
        except Exception:
            rent = 0

        try:
            ret = int(s.get("available_return_bikes", 0))
        except Exception:
            ret = 0

        # 座標
        try:
            lat = float(s.get("latitude", 0) or 0)
            lng = float(s.get("longitude", 0) or 0)
        except Exception:
            lat, lng = 0.0, 0.0

        # update_time
        update_raw = s.get("mday") or s.get("updateTime")
        update_dt = parse_update_time(update_raw)

        records.append({
            "time": now,          # collection_time 來源
            "sno": s.get("sno"),
            "sna": s.get("sna"),
            "sarea": s.get("sarea"),
            "lat": lat,
            "lng": lng,
            "rent": rent,
            "return": ret,        # 交給 save_data 轉成 return_count
            "update_time": update_dt,
        })

    df = pd.DataFrame(records)
    if df.empty:
        logger.warning("[collector] DataFrame empty after parsing.")
        return None

    return df


def job() -> None:
    """
    單次 job：抓一次 API → 寫 DB。
    適合本地排程、或雲端 scheduler 觸發。
    """
    session = _build_session()
    df = fetch_data(session=session)

    if df is None or df.empty:
        logger.warning("[collector] Empty dataframe, skip saving.")
        return

    save_data(df)
    logger.info("[collector] Job finished, saved %d rows.", len(df))


if __name__ == "__main__":
    # 本機測試用：直接跑一次 job()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    job()