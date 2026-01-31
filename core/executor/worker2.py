import traceback
import time
from concurrent.futures import ThreadPoolExecutor
from core.broker.redis import RedisBroker
from tasks.registry import get_task

class Worker:
    def __init__(self, broker:RedisBroker, max_retries=3, max_workers=5):
        self.broker = broker
        self.max_retries = max_retries
        # Initialize the pool of threads
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def start(self):
        print(f"[Worker] Started (Max Workers: {self.executor._max_workers})")
        while True:
            # 1. Fetch message (Blocks here if queue is empty)
            message = self.broker.dequeue()
            if not message:
                continue

            # 2. Submit the work to the thread pool immediately
            self.executor.submit(self._process_task, message)

    def _process_task(self, message):
        """This method runs inside a background thread."""
        task_id = message["task_id"]
        task_name = message["task_name"]
        payload = message["payload"]
        chain = message.get("chain", [])  # <--- Get the pipeline
        current_retries = message.get("retry_count", 0)

        try:
            print(f"[Worker] Processing {task_id} (Attempt {current_retries + 1})")
            
            # Run the task
            task_fn = get_task(task_name)
            result = task_fn(**payload)
            
            # Save Success
            self.broker.save_result(task_id, result, status="SUCCESS")
            print(f"[Worker] Task {task_id} SUCCESS")

            # === CHAINING LOGIC ===
            if chain:
                # 1. Get the next task in line
                next_task_info = chain.pop(0) 
                next_task_name = next_task_info["task_name"]
                
                # 2. Prepare the payload
                # Start with any hardcoded arguments defined in the chain
                next_payload = next_task_info.get("payload", {})
                
                # 3. MAGIC: Pass the result of the previous task to the next one
                # We assume 'result' is a dictionary, e.g., {"user_id": 5}
                if isinstance(result, dict):
                    next_payload.update(result)
                
                print(f"[Worker] 🔗 Chaining: Triggering '{next_task_name}'")
                
                # 4. Enqueue the next task (passing the remaining chain along!)
                self.broker.enqueue(next_task_name, next_payload, chain=chain)

        except Exception as e:
            print(f"[Worker] Task {task_id} FAILED: {e}")
            
            if current_retries < self.max_retries:
                new_retry_count = current_retries + 1
                print(f"[Worker] Retrying {task_id} ({new_retry_count}/{self.max_retries})...")
                
                # === CRITICAL UPDATE FOR REDIS.PY ===
                # We pass 'chain' here so the pipeline isn't lost during a retry!
                # Note: You must update re_enqueue in redis.py to accept 'chain' or **kwargs
                try:
                    self.broker.re_enqueue(task_id, task_name, payload, new_retry_count, chain=chain)
                except TypeError:
                    # Fallback if you haven't updated re_enqueue yet
                    self.broker.re_enqueue(task_id, task_name, payload, new_retry_count)
                
                self.broker.save_result(task_id, None, status=f"RETRYING ({new_retry_count})")
            else:
                print(f"[Worker] Max retries reached for {task_id}. Giving up.")
                self.broker.save_result(task_id, str(e), status="FAILURE")
                traceback.print_exc()