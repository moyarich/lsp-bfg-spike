import os
import time
from datetime import datetime
from multiprocessing import Process, Queue, Value , Array

try:
    from Executor import Executor
except ImportError:
    sys.stderr.write('LSP needs Executor in executors/Executor.py\n')
    sys.exit(2)

LSP_HOME = os.getenv('LSP_HOME')

class ConcurrentExecutor(Executor):
    def __init__(self, schedule_parser, report_directory, schedule_name, report_sql_file, cs_id, tr_id, rq_param):
        Executor.__init__(self, schedule_parser, report_directory, schedule_name, report_sql_file, cs_id, tr_id, rq_param)
        self.AllProcess = []
        self.should_stop = False

    def cleanup(self):
        ''' cleanup function , will be called after execution'''
        pass

    def handle_workload_done(self, process):
        ''' function that called every time when current workload has done'''
        pass

    def handle_workload_not_done(self, process):
        ''' function that called evert time when current workload not done'''
        pass

    def execute(self):
        # init workload and setup directories before execution
        result = self.setup()
        # routine of workload running
        for wi in self.workloads_instance:
            p = Process(target=wi.execute)
            self.AllProcess.append(p)
            p.start() 
     
        self.should_stop = False
        while True and not self.should_stop:
            for process in self.AllProcess[:]:
                process.join(timeout = 1)
                if process.is_alive():
                   self.handle_workload_not_done(process)
                   continue
                else:
                   self.handle_workload_done(process)
                   self.AllProcess.remove(process)
                    
            if len(self.AllProcess) == 0:
                self.should_stop = True
            else:
                time.sleep(5)

        # clean up after execution 
        self.cleanup()
