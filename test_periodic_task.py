from datetime import datetime
from periodic_task import PeriodicTask, log_exception, timedelta
import time


def print_sched_time_and_now(sched_time):
    print(sched_time)
    print(datetime.now())
    time.sleep(0.1)
    print('finish.')
    pass


task = PeriodicTask(print_sched_time_and_now, datetime.now()-timedelta(seconds=1), timedelta(seconds=1))
task.start()

input()