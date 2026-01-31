from core.broker.redis import RedisBroker

broker = RedisBroker("redis://localhost:6379/0")

# Define the Pipeline
# Step 1 is "enqueue", Step 2 and 3 go into the "chain" list
chain = [
    {
        "task_name": "step2_process", 
        "payload": {} # It waits for "data" from Step 1
    },
    {
        "task_name": "step3_save", 
        "payload": {} # It waits for "final_result" from Step 2
    }
]

print("Launching Chain...")
broker.enqueue(
    task_name="step1_generate",
    payload={"initial_value": 10},
    chain=chain
)
print("Chain submitted! Watch the worker logs.")