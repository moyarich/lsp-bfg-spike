import os
import datetime

try:
    from workloads.TPCH.Tpch import TpchWorkload
except ImportError:
    sys.stderr.write('LSP needs TpchWorkload in src/workloads/TPCH/Tpch.py.\n')
    sys.exit(2)

LSP_HOME = os.getenv('LSP_HOME')

class Executor(object):
    def __init__(self, workloads_list):
        self.workloads_list = workloads_list
        self.workloads_inst = []
        self.should_stop = False

    def setup(self):
        # create report directory for test
        self.test_report_dir = LSP_HOME + '/report/' + datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        os.system('mkdir -p %s'%(self.test_report_dir))

        # instantiate and prepare workloads
        for wl in self.workloads_list:
            wn = wl['workload_name'].strip()
            wc = wn.split('_')[0].upper()
            if wc == 'TPCH':
                self.workloads_inst.append(TpchWorkload(wl, self.test_report_dir))
            else:
                print 'Invalid workload name %s in schedule file.' % (wn)

    def cleanup(self):
        pass
