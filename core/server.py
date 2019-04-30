'''
调度服务器
'''
import logging
import time
from datetime import datetime, timedelta

from rpyc.utils.server import ThreadedServer
from rpyc.utils.helpers import classpartial
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import (
    ThreadPoolExecutor,
    ProcessPoolExecutor
)
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.events import (
    EVENT_JOB_EXECUTED,
    EVENT_JOB_ERROR,
    EVENT_JOB_ADDED,
    EVENT_JOB_SUBMITTED,
    EVENT_JOB_REMOVED
)

from .service import SchedulerService


def event_listener(event, scheduler):
    code = getattr(event, 'code')
    job_id = getattr(event, 'job_id')
    jobstore = getattr(event, 'jobstore')
    event_codes = {
        EVENT_JOB_EXECUTED: 'JOB_EXECUTED',
        EVENT_JOB_ERROR: 'JOB_ERROR',
        EVENT_JOB_ADDED: 'JOB_ADDED',
        EVENT_JOB_SUBMITTED: 'JOB_SUBMITTED',
        EVENT_JOB_REMOVED: 'JOB_REMOVED'
    }
    event_name = None
    job = None
    need_record = False
    if code in event_codes:
        need_record = True
        event_name = event_codes[code]
        job = scheduler.get_job(job_id)
        if code == EVENT_JOB_SUBMITTED:
            time.sleep(0.05)
        elif code in [EVENT_JOB_EXECUTED, EVENT_JOB_ERROR]:
            # 执行完成和出错不可能同时出现，故延时一样
            time.sleep(0.1)
    print('*' * 80)
    print(job_id)
    print(event_name, datetime.now())
        # print(job)
    '''
    if hasattr(event, 'exception') and event.exception:
        error, *_ = event.exception.args
        tmp = re.sub(r'\([\w.]+\)\s*ORA-\d+:\s*', '', error)
        if bool(tmp):
            error = tmp
        # TODO: log error
    '''

def modify_logger(logger, log_file):
    # refer: https://docs.python.org/3.5/library/logging.html#logrecord-attributes
    formatter = logging.Formatter(
        fmt='\n'.join([
            '[%(name)s] %(asctime)s.%(msecs)d',
            '\t%(pathname)s [line: %(lineno)d]',
            '\t%(processName)s[%(process)d] => %(threadName)s[%(thread)d] => %(module)s.%(filename)s:%(funcName)s()',
            '\t%(levelname)s: %(message)s\n'
        ]),
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # stream_handler = logging.StreamHandler()
    # stream_handler.setFormatter(formatter)
    # logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.setLevel(logging.DEBUG)

    return logger

def get_scheduler(store_path=None, log_file=None):
    if store_path is None:
        store_path = r'jobstore.sqlite'
    if log_file is None:
        log_file = r'logger.log'
    scheduler = BackgroundScheduler({'apscheduler.timezone': 'Asia/Shanghai'})
    jobstores = {
        'default': SQLAlchemyJobStore(url='sqlite:///{0}'.format(store_path))
    }
    executors = {
        'default': ThreadPoolExecutor(20),
        'processpool': ProcessPoolExecutor(5)
    }
    job_defaults = {
        'coalesce': False,
        'max_instances': 1
    }
    scheduler.configure(jobstores=jobstores, executors=executors)
    # 事件记录
    scheduler.add_listener(
        lambda event: event_listener(event, scheduler),
        EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_ADDED | EVENT_JOB_SUBMITTED | EVENT_JOB_REMOVED
    )
    # 日志定制
    scheduler._logger = modify_logger(scheduler._logger, log_file=log_file)
    return scheduler

def get_server(listen_config, scheduler, SchedulerServiceClass=None):
    # 额外构造函数参数
    ser_args = ['test args']
    ser_kwargs = {}
    # 传递Service构造函数参数
    SSC = SchedulerServiceClass if SchedulerServiceClass is not None else SchedulerService
    service = classpartial(SSC, scheduler, *ser_args, **ser_kwargs)
    # 允许属性访问
    protocol_config = {'allow_public_attrs': True}
    # 实例化RPYC服务器
    server = ThreadedServer(service,  protocol_config=protocol_config, **listen_config)
    return server

def schedule_start(listen_config=None, store_path=None, log_file=None, SchedulerServiceClass=None):
    # 实例化调度器
    scheduler = get_scheduler(store_path=store_path, log_file=log_file)
    # 启动调度
    scheduler.start()
    # 监听配置
    if listen_config is None:
        listen_config = {
            'port': 12345,
            'hostname': '0.0.0.0'
        }
    # 实例化服务器
    server = get_server(listen_config, scheduler, SchedulerServiceClass=SchedulerServiceClass)
    print('rpyc server running at [{hostname}:{port}]'.format(**listen_config))
    try:
        # 启动RPYC服务器
        server.start()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        # 停止调度
        scheduler.shutdown()
        # 停止RPYC服务器
        server.close()

if __name__ == '__main__':
    schedule_start()
