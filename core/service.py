'''
对外接口服务
'''
import sys

import rpyc


class SchedulerService(rpyc.Service):
    def __init__(self, scheduler, args=None, kwargs=None):
        self.scheduler = scheduler
        print(args, kwargs)

    def on_connect(self, conn):
        print('rpyc connect')
        pass

    def on_disconnect(self, conn):
        print('rpyc disconnect')
        pass

    def exposed_add_job(self, func, *args, **kwargs):
        return self.scheduler.add_job(func, *args, **kwargs)

    def exposed_modify_job(self, job_id, jobstore=None, **changes):
        return self.scheduler.modify_job(job_id, jobstore, **changes)

    def exposed_reschedule_job(self, job_id, jobstore=None, trigger=None, **trigger_args):
        return self.scheduler.reschedule_job(job_id, jobstore, trigger, **trigger_args)

    def exposed_pause_job(self, job_id, jobstore=None):
        return self.scheduler.pause_job(job_id, jobstore)

    def exposed_resume_job(self, job_id, jobstore=None):
        return self.scheduler.resume_job(job_id, jobstore)

    def exposed_remove_job(self, job_id, jobstore=None):
        self.scheduler.remove_job(job_id, jobstore)

    def exposed_remove_all_jobs(self, jobstore=None):
        return self.scheduler.remove_all_jobs(jobstore)

    def exposed_get_job(self, job_id, jobstore=None):
        return self.scheduler.get_job(job_id, jobstore)

    def exposed_get_jobs(self, jobstore=None):
        return self.scheduler.get_jobs(jobstore)

    def exposed_print_jobs(self, jobstore=None):
        return self.scheduler.print_jobs(jobstore, out=sys.stdout)
