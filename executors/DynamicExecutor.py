import os
from datetime import datetime
from multiprocessing import Process, Queue, Value , Array

try:
    from Executor import Executor
except ImportError:
    sys.stderr.write('LSP needs Executor in executors/Executor.py\n')
    sys.exit(2)

LSP_HOME = os.getenv('LSP_HOME')

class DynamicExecutor(Executor):
    def __init__(self, workloads_dict):
        Executor.__init__(self, workloads_dict)
        self.AllProcess = []
        self.TARGET_CPU_USAGE = 30.0

    def cleanup(self):
        ''' cleanup function , will be called after execution'''
        pass

    def handle_workload_done(self, process):
        ''' function that called every time when current workload has done'''
        pass

    def handle_workload_not_done(self, process):
        ''' function that called evert time when current workload not done'''
        pass

    def getCpuUsage(self):
        pass

    def execute(self):
        # init workload and setup directories before execution
        self.setup()

        # routine of workload running 
        for workload in self.workloads:
            p = Process(target=workload.start)
            self.AllProcess.append(p)
            p.start()
 
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



        # setup executor
        setup()
        # run workloads
        while True:
            if getCpuStatic() < 90: 
                print "new process"
                p = Process(target=f, args=(cpus,))
                AllProcess.append(p)
                p.start()
            for process in AllProcess:
                process.join(timeout = 1)
                if process.is_alive():
                    print "process %d is still alive , continue join other process"%process.pid
                else:
                    print "process %d quit"%process.pid
                    AllProcess.remove(process)

        # clean up after execution 
        self.cleanup()
