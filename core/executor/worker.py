from email import message
import time
from unittest import result
from core.broker.redis import RedisBroker
from tasks.registry import get_task, task
import traceback


class Worker:
    def __init__(self, broker: RedisBroker,max_retries:int=3):
        self.broker = broker
        self.max_retries = max_retries 
    def start(self):
        print("Worker started, waiting for tasks...",flush=True)
        
        
        while True:
            message = self.broker.dequeue()
            if not message:
                continue
            task_id = message["task_id"]
            task_name = message["task_name"]
            payload = message["payload"]
            # Default to 0 if key doesn't exist
            current_retries = message.get("retry_count", 0)
            
            try:
                print(f"[Worker] Processing {task_id} (Attempt {current_retries + 1})")
                task_fn = get_task(task_name)
                result = task_fn(**payload)
                # Success
                self.broker.save_result(task_id, result, status="SUCCESS")
                print(f"[Worker] Task {task_id} SUCCESS") 
                
                
            except Exception as e:
                print(f"[Worker] Task {task_id} FAILED: {e}")
                
                if current_retries < self.max_retries:
                    # === RETRY LOGIC ===
                    new_retry_count = current_retries + 1
                    print(f"[Worker] Retrying task {task_id} ({new_retry_count}/{self.max_retries})...")
                    
                    # Optional: Add a small delay before re-queuing (Backoff)
                    # Ideally, you'd use a separate "delayed" queue, but for now sleep is okay
                    time.sleep(2) 
                    
                    self.broker.re_enqueue(task_id, task_name, payload, new_retry_count)
                    
                    # Update status so user knows it's retrying
                    self.broker.save_result(task_id, None, status=f"RETRYING ({new_retry_count})")
                else:
                    # === GIVE UP ===
                    print(f"[Worker] Max retries reached for {task_id}. Giving up.")
                    self.broker.save_result(task_id, str(e), status="FAILURE")
                # traceback.print_exc()
        