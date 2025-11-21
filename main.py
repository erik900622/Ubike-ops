import schedule
import time
from src.database import init_db, cleanup_old_data
from src.collector import job
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

def main():
    print("Starting Ubike Operation Optimization System...")
    
    # Initialize Database
    init_db()
    
    # Run once immediately
    job()
    cleanup_old_data()
    
    # Schedule job every 1 minute
    schedule.every(1).minutes.do(job)
    
    # Schedule cleanup every hour
    schedule.every(1).hours.do(cleanup_old_data)
    
    print("Scheduler started. Press Ctrl+C to exit.")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping system...")

if __name__ == "__main__":
    main()
