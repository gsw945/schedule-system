import sys
import os
import time
from datetime import datetime, timedelta

sys.path.insert(0, '..')
from core import schedule_start, SchedulerService


def demo_job():
    print('job start', datetime.now())
    time.sleep(2)
    print('job stop', datetime.now())
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

class CustomScheduler(SchedulerService):
    '''CustomScheduler-自定义SchedulerService'''
    def exposed_api_add_demo_job(self):
        return self.scheduler.add_job(
            demo_job,
            trigger='interval',
            # minutes=1,
            seconds=5,
            next_run_time=datetime.now() + timedelta(seconds=2),
            args=[],
            replace_existing=True,
            id='add_demo_job'
        )

    def exposed_api_demo_hello(self, args=None):
        print(args)
        date_time = datetime.now() + timedelta(seconds=5)
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        print(now_str)
        return self.scheduler.add_job(
            print,
            trigger='date',
            next_run_time=date_time,
            args=[now_str.center(50, '~')]
        )

server_params = dict(
    listen_config={
        'port': 54345,
        'hostname': '0.0.0.0'
    },
    store_path = r'jobstore.sqlite',
    log_file = r'logger.log',
    SchedulerServiceClass=CustomScheduler
)

if __name__ == '__main__':
    schedule_start(**server_params)
