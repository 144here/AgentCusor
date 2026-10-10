# utils/retry.py
import time
import functools
import random
from typing import Callable, Any, Iterable

def retry(
    exceptions: Iterable[type] = (Exception,),
    tries: int = 3,
    delay: float = 0.5,
    backoff: int = 2,
    jitter: float = 0.1
) -> Callable:
    """
    简单的重试装饰器（指数退避 + jitter)
    使用：
    @retry(tries=5, delay=0.2)
    def call_io(): ...
    """
    def deco(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            _tries, _delay = tries, delay
            last_exc = None
            for i in range(_tries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    sleep = _delay + random.uniform(0, jitter)
                    time.sleep(sleep)
                    _delay *= backoff
            # 所有尝试失败，抛出最后一个错误
            raise last_exc
        return wrapper
    return deco