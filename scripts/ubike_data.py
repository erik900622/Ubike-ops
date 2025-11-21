
import requests
import pandas as pd
from datetime import datetime
import os
# 1. API URL
url = "https://tcgbusfs.blob.core.windows.net/dotapp/youbike/v2/youbike_immediate.json"

# 2. 抓資料
r = requests.get(url)
print("Status code:", r.status_code)

data = r.json()  # 這裡會是一個 list
print("資料筆數:", len(data))

timestamp = datetime.now()

# 3. 轉成 DataFrame
records = []
for s in data:
    records.append({
        "time": timestamp,
        "sno": s.get("sno"),
        "sna": s.get("sna"),
        "sarea": s.get("sarea"),
        "lat": s.get("latitude"),
        "lng": s.get("longitude"),
        "rent": s.get("available_rent_bikes"),
        "return": s.get("available_return_bikes"),
        "capacity": s.get("available_rent_bikes") + s.get("available_return_bikes"),
        "update_time": s.get("updateTime")
    })

df = pd.DataFrame(records)
print(df.head())
print("df shape:", df.shape)

# 4. 存到桌面
output_path = os.path.expanduser("/Users/erikk/Desktop/Coding/youbike_test2.csv")
df.to_csv(output_path, index=False)
print("已存到:", output_path)