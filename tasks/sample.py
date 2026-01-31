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

@task(name="cleanup")
def cleanup():
    print("Running database cleanup...")
    time.sleep(1)
    return "Cleanup Done"

@task(name="step1_generate")
def generate_data(initial_value: int):
    print(f"--- Step 1: Generating data from {initial_value} ---")
    # Returns data to be passed to step 2
    return {"data": initial_value * 2} 

@task(name="step2_process")
def process_data(data: int):
    print(f"--- Step 2: Processing {data} ---")
    # Returns data to be passed to step 3
    return {"final_result": data + 100}

@task(name="step3_save")
def save_data(final_result: int):
    print(f"--- Step 3: Saving {final_result} to DB ---")
    return "Workflow Complete"