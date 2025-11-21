import os
import pandas as pd
from src.collector import fetch_data
from src.database import init_db, save_data, get_engine
from sqlalchemy import text

def test_pipeline():
    print("Testing Pipeline...")
    
    # 1. Test Fetch
    print("1. Testing fetch_data()...")
    df = fetch_data()
    assert df is not None, "fetch_data returned None"
    assert not df.empty, "fetch_data returned empty DataFrame"
    print(f"Fetched {len(df)} records.")
    print(df.head(3))
    
    # 2. Test Database Init
    print("\n2. Testing init_db()...")
    # Use a test DB to avoid messing with real data if needed, but for now we use the dev DB
    init_db()
    assert os.path.exists("data/ubike.db"), "Database file not created"
    
    # 3. Test Save
    print("\n3. Testing save_data()...")
    save_data(df)
    
    # 4. Verify Data in DB
    print("\n4. Verifying data in DB...")
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM stations_realtime"))
        count = result.scalar()
        print(f"Total records in DB: {count}")
        assert count >= len(df), "Data not saved correctly"
        
    print("\nPipeline Test Passed!")

if __name__ == "__main__":
    test_pipeline()
