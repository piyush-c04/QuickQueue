from core.broker.redis import RedisBroker
from core.executor.worker2 import Worker
# from core.executor.worker1 import Worker
import tasks.sample # noqa: ensures task registration
#use worker1 code for testing concurrent tasks

if __name__ == "__main__":
    broker = RedisBroker("redis://redis:6379/0")
    worker = Worker(broker)
    worker.start()