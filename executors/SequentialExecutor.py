import os
from datetime import datetime
from multiprocessing import Process, Queue, Value , Array

try:
    from Executor import Executor
except ImportError:
    sys.stderr.write('LSP needs Executor in executors/Executor.py\n')
    sys.exit(2)

LSP_HOME = os.getenv('LSP_HOME')

class SequentialExecutor(Executor):
    def __init__(self, workloads_list, workloads_specification, report_directory, schedule_name, report_sql_path):
        Executor.__init__(self, workloads_list, workloads_specification, report_directory, schedule_name, report_sql_path)
        self.AllProcess = []

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
        self.setup()
        # execute workloads sequentially
        for wi in self.workloads_instance:
            p = Process(target=wi.execute)
            p.start()
            while True:
                if p.is_alive():
                    self.handle_ongoing_workload(p)
                else:
                    break
        # clean up environment after all workload are finished
        self.cleanup()
