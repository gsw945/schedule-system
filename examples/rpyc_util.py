from functools import wraps
import json

import rpyc
from rpyc.core.protocol import Connection


class RpycUtil(object):
    """RpycUtil-Rpyc客户端工具"""
    def __init__(self, host, port):
        super(RpycUtil, self).__init__()
        self.host = host
        self.port = port
        self.conn = None

    def connect(self):
        self.disconnect()
        client_config={
            "allow_all_attrs": True
        }
        self.conn = rpyc.connect(self.host, self.port, config=client_config)

    def disconnect(self):
        if isinstance(self.conn, Connection):
            if not self.conn.closed:
                self.conn.close()
        self.conn = None

    def ensure_connected(func):
        @wraps(func)
        def func_wrapper(*args):
            self = args[0]
            if not isinstance(self.conn, Connection) or self.conn.closed:
                self.connect()
            print('running {0}()'.format(func.__name__))
            # 如果和rpyc服务端断开了，则自动重新连接
            try:
                return func(*args)
            except Exception as e:
                if isinstance(e, EOFError):
                    self.connect()
                    return func(*args)
                else:
                    raise e
        return func_wrapper

    @ensure_connected
    def add_job_json(self, params={}):
        # 将dict转化成对应的json格式的str, 避免rpyc传递后变成netref对象，导致直接使用时不能被序列化
        job_params = json.dumps(params, ensure_ascii=False)
        return self.conn.root.add_job_json(job_params)

    @ensure_connected
    def get_jobs_json(self):
        ret = self.conn.root.get_jobs_json()
        return ret

    @ensure_connected
    def remove_job(self, job_id):
        ret = self.conn.root.remove_job(job_id)
        return ret