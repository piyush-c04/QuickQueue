# core/executor/worker1.py
import traceback
import time
from concurrent.futures import ThreadPoolExecutor
from tasks.registry import get_task
from core.broker.redis import RedisBroker

class Worker:
    def __init__(self, broker:RedisBroker, max_retries=3, max_workers=5):
        self.broker = broker
        self.max_retries = max_retries
        # Create a pool of 5 threads to run tasks in parallel
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def start(self):
        print(f"[Worker] Started (Max Workers: {self.executor._max_workers})")
        while True:
            # 1. Fetch message (Blocks here if queue is empty)
            message = self.broker.dequeue()
            if not message:
                continue

            # 2. Submit the work to the thread pool immediately
            # The loop goes back to 'dequeue' instantly, not waiting for the task to finish
            self.executor.submit(self._process_task, message)

    def _process_task(self, message):
        """This method runs inside a background thread."""
        task_id = message["task_id"]
        task_name = message["task_name"]
        payload = message["payload"]
        current_retries = message.get("retry_count", 0)

        try:
            print(f"[Worker] Processing {task_id} (Attempt {current_retries + 1})")
            
            task_fn = get_task(task_name)
            result = task_fn(**payload)
            
            self.broker.save_result(task_id, result, status="SUCCESS")
            print(f"[Worker] Task {task_id} SUCCESS")

        except Exception as e:
            print(f"[Worker] Task {task_id} FAILED: {e}")
            
            if current_retries < self.max_retries:
                new_retry_count = current_retries + 1
                print(f"[Worker] Retrying {task_id} ({new_retry_count}/{self.max_retries})...")
                
                # Note: We re-queue to Redis so any available thread can pick it up later
                self.broker.re_enqueue(task_id, task_name, payload, new_retry_count)
                self.broker.save_result(task_id, None, status=f"RETRYING ({new_retry_count})")
            else:
                self.broker.save_result(task_id, str(e), status="FAILURE")