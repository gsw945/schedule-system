import sys
import os
import json
import time
from datetime import datetime, timedelta

import requests
from apscheduler.job import Job
sys.path.insert(0, '..')
from core import schedule_start, SchedulerService


def demo_job():
    print('job start', datetime.now())
    time.sleep(2)
    print('job stop', datetime.now())
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

def get_weather():
    url01 = 'http://weather.hao.360.cn/sed_api_weather_info.php?app=clockWeather&_jsonp=callback'
    url02 = 'http://tq.360.cn/api/weatherquery/querys?app=tq360&code={code}'
    idx = len('callback(')
    resp01 = requests.get(url01)
    data01 = json.loads(resp01.text[idx + 1:-2:])
    # TODO ...
    return 

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

    def exposed_get_jobs_json(self, jobstore=None):
         result = []
         for job_item in self.scheduler.get_jobs(jobstore):
            item_data = {
                'id': job_item.id,
                'name': job_item.name,
                'kwargs': job_item.kwargs,
                'next_run_time': job_item.next_run_time.strftime('%Y-%m-%d %H:%M:%S'),
                'pending': job_item.pending
            }
            result.append(item_data)
         return json.dumps(result)

    def exposed_add_job_json(self, job_params, jobstore=None):
        params = json.loads(job_params)
        new_job = self.scheduler.add_job(
            get_weather,
            trigger='date',
            next_run_time=datetime.now() + timedelta(seconds=1),
            **params
        )
        if isinstance(new_job, Job):
            return new_job.id

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
