from typing import Callable,Dict,Any

TASK_REGISTRY : Dict[str,Callable] = {}

def task(name:str):
    def decorator(func:Callable)->Callable:
        TASK_REGISTRY[name] = func
        return func
    return decorator

def get_task(task_name:str)->Callable|None:
    if task_name not in TASK_REGISTRY:
        return ValueError(f"Task '{task_name}' not registered")
    return TASK_REGISTRY.get(task_name)
