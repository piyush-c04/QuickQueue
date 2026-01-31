from fastapi import FastAPI
from core.broker.redis import RedisBroker
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins (for development)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
broker = RedisBroker("redis://redis:6379/0")


@app.post("/tasks/submit")
def submit_task(task_name: str, payload: dict):
    task_id = broker.enqueue(task_name, payload)
    return {"task_id": task_id, "status": "PENDING"}

@app.get("/tasks/{task_id}")
def get_task_status(task_id: str):
    return broker.get_result(task_id)

@app.get("/stats")
def get_stats():
    # Use the raw redis connection from the broker
    r = broker.redis 
    return {
        "queue_depth": r.llen("fastqueue"),
        "total_processed": int(r.get("stats:total") or 0),
        "success": int(r.get("stats:success") or 0),
        "failures": int(r.get("stats:failure") or 0)
    }