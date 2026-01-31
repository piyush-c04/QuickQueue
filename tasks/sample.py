from tasks.registry import task
import time

@task(name="add")
def add(a:int,b:int)->int:
    return a + b

@task(name="unstable_task")
def unstable_task():
    # Simulate a 100% fail rate for testing
    raise ValueError("Something went wrong with the database!")

@task(name="slow_task")
def slow_task(seconds: int):
    print(f"Starting slow task for {seconds}s...")
    try: 
        time.sleep(seconds)
        return "Done sleeping"
    except Exception as e:
        print(f"slow_task interrupted: {e}")
        raise

# === MAKE SURE THIS IS HERE ===
@task(name="cleanup")
def cleanup():
    print("Running database cleanup...")
    time.sleep(1)
    return "Cleanup Done"