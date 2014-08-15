import os
import datetime

try:
    from workloads.TPCH.Tpch import TpchWorkload
except ImportError:
    sys.stderr.write('LSP needs TpchWorkload in src/workloads/TPCH/Tpch.py.\n')
    sys.exit(2)

LSP_HOME = os.getenv('LSP_HOME')

class Executor(object):
    def __init__(self, workloads_list, workloads_content):
        self.workloads_list = workloads_list
        self.workloads_content = workloads_content
        self.workloads_inst = []
        self.should_stop = False

    def setup(self):
        # create report directory for test
        self.test_report_dir = LSP_HOME + '/report/' + datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        os.system('mkdir -p %s'%(self.test_report_dir))

        # instantiate and prepare workloads
        for wl in self.workloads_list:
            # check if the detailed definition of current workload exist
            workload_exist = False
            workload_def = None
            for wc in self.workloads_content:
                if wc['workload_name'] == wl:
                    workload_exist = True
                    workload_def = wc
            
            if not workload_exist:
                print 'Detaled definition of workload %s no found in schedule file' % (wl)

            # Find appropreciate workload type for current workload
            wt = wl.split('_')[0].upper()
            if wt == 'TPCH':
                self.workloads_inst.append(TpchWorkload(workload_def, self.test_report_dir))
            else:
                print 'No appropreciate workload type found for workload %s' % (wl)

    def cleanup(self):
        pass
