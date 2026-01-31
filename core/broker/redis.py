import redis
from typing import Dict, Any 
import uuid
import json
class RedisBroker:
    def __init__(self,url:str,queue_name:str="fastqueue"):
        self.redis = redis.Redis.from_url(url,decode_responses=True)
        self.queue_name = queue_name
        
# core/broker/redis.py

#     # Update the enqueue method signature
#     def enqueue(self, task_name: str, payload: Dict[str, Any], retry_count: int = 0) -> str:
#         task_id = str(uuid.uuid4())
#         message = {
#             "task_id": task_id,
#             "task_name": task_name,
#             "payload": payload,
#             "retry_count": retry_count  
#         }
#         self.redis.lpush(self.queue_name, json.dumps(message))
#         return task_id
    def enqueue(self, task_name: str, payload: Dict[str, Any], retry_count: int = 0, chain: list = None) -> str:
        if chain is None:
            chain = []
            
        task_id = str(uuid.uuid4())
        message = {
            "task_id": task_id,
            "task_name": task_name,
            "payload": payload,
            "retry_count": retry_count,
            "chain": chain  
        }
        self.redis.lpush(self.queue_name, json.dumps(message))
        return task_id
    
    # Add a specific method for re-queuing (keeps the SAME task_id)
    # def re_enqueue(self, task_id: str, task_name: str, payload: Dict[str, Any], retry_count: int):
    #     message = {
    #         "task_id": task_id,      
    #         "task_name": task_name,
    #         "payload": payload,
    #         "retry_count": retry_count
    #     }
    #     self.redis.lpush(self.queue_name, json.dumps(message))
# In core/broker/redis.py

    def re_enqueue(self, task_id, task_name, payload, retry_count, chain=None):
        if chain is None:
            chain = []
        message = {
            "task_id": task_id,
            "task_name": task_name,
            "payload": payload,
            "retry_count": retry_count,
            "chain": chain 
        }
        self.redis.lpush(self.queue_name, json.dumps(message))
        
    def dequeue(self)->Dict[str,Any]|None:
        item = self.redis.brpop(self.queue_name, timeout=5)
        if not item:
            return None
        _, data = item
        return json.loads(data)
    
    def save_result(self, task_id: str, result: Any, status: str = "SUCCESS"):
        """Save the task output to Redis with an expiration time."""
        data = {
            "status": status,
            "result": result
        }
        # Save to Redis key "task_result:{task_id}"
        # ex=3600 means it expires (deletes itself) after 1 hour to save space
        self.redis.set(f"task_result:{task_id}", json.dumps(data), ex=20)

    def get_result(self, task_id: str) -> Dict[str, Any]:
        """Fetch the result from Redis."""
        data = self.redis.get(f"task_result:{task_id}")
        if not data:
            return {"status": "PENDING", "result": None}
        return json.loads(data)