from fastapi import FastAPI
from core.broker.redis import RedisBroker


app = FastAPI()
broker = RedisBroker("redis://redis:6379/0")


@app.post("/tasks/submit")
def submit_task(task_name: str, payload: dict):
    task_id = broker.enqueue(task_name, payload)
    return {"task_id": task_id, "status": "PENDING"}

@app.get("/tasks/{task_id}")
def get_task_status(task_id: str):
    return broker.get_result(task_id)