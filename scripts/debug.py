####  time check
from src.database import get_engine
import pandas as pd

engine = get_engine()

sql = """
SELECT 
    MIN(collection_time) AS min_ct,
    MAX(collection_time) AS max_ct,
    MIN(update_time) AS min_ut,
    MAX(update_time) AS max_ut,
    COUNT(*) AS rows
FROM stations_realtime;
"""

df = pd.read_sql(sql, engine)
print(df)