from typing import Callable, Union, Optional
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
import time


def log_exception(logger):
    def decorator(func):
        @wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(args, kwargs)
            except Exception as e:
                logger.error(e)

        return inner

    return decorator


class PeriodicTask:
    def __init__(self, func: Callable[[datetime], None],
                 next_sched: Union[datetime, Callable[[], datetime], None],
                 time_delta: timedelta,
                 thread_pool: Optional[ThreadPoolExecutor] = None) -> None:
        assert callable(func)
        assert callable(next_sched) or isinstance(next_sched, datetime) or next_sched is None
        assert isinstance(thread_pool, ThreadPoolExecutor) or thread_pool is None
        assert isinstance(time_delta, timedelta)
        self.func = func
        self.next_sched = next_sched
        self.thread_pool = thread_pool if thread_pool else ThreadPoolExecutor(10)
        self.time_delta = time_delta

    def start(self):
        self.thread_pool.submit(self.thread_start)

    def thread_start(self):
        now = datetime.now()
        if callable(self.next_sched):
            next_sched = self.next_sched()
        elif self.next_sched is None:
            next_sched = now
        else:
            next_sched = self.next_sched
        if next_sched < now:
            # self.thread_pool.submit(self.func, next_sched)
            next_sched = now
        while True:
            delta = next_sched - now
            time.sleep(delta.total_seconds())
            self.thread_pool.submit(self.func, next_sched)
            next_sched += self.time_delta
            now = datetime.now()
