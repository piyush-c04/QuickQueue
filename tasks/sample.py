from tasks.registry import task
import time

@task(name="add")
def add(a:int,b:int)->int:
    return a + b
@task(name="unstable_task")
def unstable_task():
    # Simulate a 100% fail rate for testing
    raise ValueError("Something went wrong with the database!")