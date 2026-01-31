# core/scheduler.py
import time
import json
from datetime import datetime
from core.broker.redis import RedisBroker

# 1. Configuration: What runs when?
# Format: "task_name": interval_in_seconds
SCHEDULE = {
    "add": 10,        # Run 'add' every 10 seconds
    "cleanup": 15     # Run 'cleanup' every 30 seconds
}

def run_scheduler():
    broker = RedisBroker("redis://redis:6379/0")
    print("[Scheduler] Started. Ticking...")
    
    # We track when we last ran each task
    last_run = {name: 0 for name in SCHEDULE}

    while True:
        now = time.time()
        
        for task_name, interval in SCHEDULE.items():
            # Check if enough time has passed since last run
            if now - last_run[task_name] >= interval:
                
                print(f"[Scheduler] Triggering scheduled task: {task_name}")
                
                # Payload can be dynamic, but let's keep it simple for now
                payload = {"a": 1, "b": 1} if task_name == "add" else {}
                
                # Push to Redis
                broker.enqueue(task_name, payload)
                
                # Update timestamp
                last_run[task_name] = now
        
        # Sleep short enough to be accurate, but long enough to save CPU
        time.sleep(1)

if __name__ == "__main__":
    run_scheduler()