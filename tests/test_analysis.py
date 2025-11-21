import pandas as pd
from src.analysis import find_high_risk_stations, generate_status_report
from src.config import RISK_THRESHOLD_EMPTY, RISK_THRESHOLD_FULL

def test_analysis():
    print("Testing Analysis Module...")
    
    # 1. Mock Data
    print("1. Creating mock data...")
    data = [
        {"sno": "1", "sna": "Station A", "rent": 0, "return_count": 10}, # Empty Risk
        {"sno": "2", "sna": "Station B", "rent": 10, "return_count": 0}, # Full Risk
        {"sno": "3", "sna": "Station C", "rent": 5, "return_count": 5},  # Normal
        {"sno": "4", "sna": "Station D", "rent": 1, "return_count": 20}, # Empty Risk (Threshold 1)
        {"sno": "5", "sna": "Station E", "rent": 20, "return_count": 1}, # Full Risk (Threshold 1)
    ]
    df = pd.DataFrame(data)
    
    # 2. Test find_high_risk_stations
    print("2. Testing find_high_risk_stations()...")
    empty_risk, full_risk = find_high_risk_stations(df)
    
    print(f"Empty Risk Count: {len(empty_risk)}")
    print(f"Full Risk Count: {len(full_risk)}")
    
    assert len(empty_risk) == 2, f"Expected 2 empty risk stations, got {len(empty_risk)}"
    assert "1" in empty_risk['sno'].values
    assert "4" in empty_risk['sno'].values
    
    assert len(full_risk) == 2, f"Expected 2 full risk stations, got {len(full_risk)}"
    assert "2" in full_risk['sno'].values
    assert "5" in full_risk['sno'].values
    
    print("Risk detection logic passed.")
    
    # 3. Test generate_status_report (Integration test with DB)
    print("\n3. Testing generate_status_report() (Integration)...")
    # This will run against the real DB if it exists
    report = generate_status_report()
    print("Report generated successfully:")
    print(report[:200] + "...") # Print first 200 chars
    
    print("\nAnalysis Test Passed!")

if __name__ == "__main__":
    test_analysis()
