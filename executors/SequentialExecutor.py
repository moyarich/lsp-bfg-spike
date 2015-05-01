import os
import sys,time
from datetime import datetime
from multiprocessing import Process, Queue, Value , Array

LSP_HOME = os.getenv('LSP_HOME')

try:
    from Executor import Executor
except ImportError:
    sys.stderr.write('SequentialExecutor needs Executor in executors/Executor.py\n')
    sys.exit(2)

try:
    from workloads.Workload import Workload
except ImportError:
    sys.stderr.write('SequentialExecutor needs workloads/Workload.py\n')
    sys.exit(2)


class SequentialExecutor(Executor):
    def __init__(self, schedule_parser, report_directory, schedule_name, report_sql_file, cs_id, tr_id, rq_param):
        Executor.__init__(self, schedule_parser, report_directory, schedule_name, report_sql_file, cs_id, tr_id, rq_param)

    def handle_finished_workload(self, pid):
        '''routine to handle the situation when workload is finished'''
        pass

    def handle_ongoing_workload(self, pid):
        '''routine to handle the situation when workload is ongoing'''
        pass

    def cleanup(self):
        '''routine clean up environment after all workloads are finished'''
        pass

    def execute(self):
        # instantiate and prepare workloads, prepare report directory
        # execute workloads sequentially,such as Tpch,Xmarq
        self.setup()
        for wi in self.workloads_instance:
            p = Process(target=wi.execute)
            p.start()
            while True:
                if p.is_alive():
                    self.handle_ongoing_workload(p)
                    time.sleep(5)
                else:
                    break
        # clean up environment after all workload are finished
        self.cleanup()
