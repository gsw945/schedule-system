import time

import rpyc
from apscheduler.job import Job
from apscheduler.jobstores.base import JobLookupError

if __name__ == '__main__':
    conn = rpyc.connect('localhost', 54345)
    # 添加job
    job01 = conn.root.api_demo_hello(args=['Hello, World'])
    job02 = conn.root.api_add_demo_job()
    # 显示所有job
    print(conn.root.get_jobs())
    # 显示job01的id
    print(job01.id)
    # 移除jib01
    try:
        conn.root.remove_job(job01.id)
    except JobLookupError:
        pass
    time.sleep(5)
    print(job02.id)
    # 查询job
    job03 = conn.root.get_job(job02.id)
    if bool(job03):
        print(job03, job03.id)
        # 删除job03
        job03.remove()
    # 移除所有job
    conn.root.remove_all_jobs()
