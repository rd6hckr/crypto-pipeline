import schedule
import time
from ingest import run

# Run immediately on startup
run()

# Then every 5 minutes
schedule.every(5).minutes.do(run)

print("Scheduler running... fetching every 5 minutes.")

while True:
    schedule.run_pending()
    time.sleep(1)