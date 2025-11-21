import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from src.database import init_db, save_data, cleanup_old_data, get_engine

def test_retention_policy():
    """Test that data older than 5 days is deleted."""
    init_db()
    engine = get_engine()
    
    # 1. Insert old data (6 days ago)
    old_time = datetime.now() - timedelta(days=6)
    df_old = pd.DataFrame([{
        "time": old_time,
        "sno": "1001",
        "sna": "Test Station Old",
        "sarea": "Test Area",
        "lat": 25.0,
        "lng": 121.0,
        "rent": 10,
        "return": 5,
        "update_time": old_time.strftime("%Y-%m-%d %H:%M:%S")
    }])
    save_data(df_old)
    
    # 2. Insert new data (today)
    new_time = datetime.now()
    df_new = pd.DataFrame([{
        "time": new_time,
        "sno": "1002",
        "sna": "Test Station New",
        "sarea": "Test Area",
        "lat": 25.0,
        "lng": 121.0,
        "rent": 10,
        "return": 5,
        "update_time": new_time.strftime("%Y-%m-%d %H:%M:%S")
    }])
    save_data(df_new)
    
    # Verify both exist initially
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM stations_realtime")).scalar()
        # Note: The database might have existing data, so we check if count increased by at least 2
        # But for a clean test, we'd ideally use a test DB. 
        # Given the environment, let's just check existence of our specific records by sno
        
        old_exists = conn.execute(text("SELECT COUNT(*) FROM stations_realtime WHERE sno='1001'")).scalar()
        new_exists = conn.execute(text("SELECT COUNT(*) FROM stations_realtime WHERE sno='1002'")).scalar()
        
        assert old_exists >= 1
        assert new_exists >= 1

    # 3. Run cleanup
    cleanup_old_data(days=5)
    
    # 4. Verify old data is gone, new data remains
    with engine.connect() as conn:
        old_exists_after = conn.execute(text("SELECT COUNT(*) FROM stations_realtime WHERE sno='1001'")).scalar()
        new_exists_after = conn.execute(text("SELECT COUNT(*) FROM stations_realtime WHERE sno='1002'")).scalar()
        
        assert old_exists_after == 0, "Old data should be deleted"
        assert new_exists_after >= 1, "New data should remain"

if __name__ == "__main__":
    test_retention_policy()
    print("Test passed!")
