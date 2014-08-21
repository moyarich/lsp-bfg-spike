import os
import datetime

try:
    from workloads.TPCH.Tpch import TpchWorkload
except ImportError:
    sys.stderr.write('LSP needs TpchWorkload in workloads/TPCH/Tpch.py\n')
    sys.exit(2)

LSP_HOME = os.getenv('LSP_HOME')

class Executor(object):
    def __init__(self, workloads_list, workloads_specification):
        self.workloads_list = workloads_list
        self.workloads_specification = workloads_specification
        self.workloads_instance = []
        self.should_stop = False

    def setup(self):
        # create report directory for test
        self.report_directory = LSP_HOME + os.sep + 'report' + os.sep + datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        os.system('mkdir -p %s' % (self.report_directory))

        # instantiate and prepare workloads based on workloads sepecification
        for wl in self.workloads_list:
            # check if the detailed definition of current workload exist
            wl_exist = False
            wl_specs = None
            for ws in self.workloads_specification:
                if ws['workload_name'] == wl:
                    wl_exist = True
                    wd_specs = ws
            
            if not wl_exist:
                print 'Detaled definition of workload %s no found in schedule file' % (wl)

            # Find appropreciate workload type for current workload
            workload_category = wl.split('_')[0].upper()
            workload_directory = LSP_HOME + os.sep + 'workloads' + os.sep + workload_category
            os.system('mkdir -p %s' (workload_directory))

            if wt == 'TPCH':
                self.workloads_instance.append(Tpch(workload_specification, workload_directory, self.report_directory))
            else:
                print 'No appropreciate workload type found for workload %s' % (wl)

    def cleanup(self):
        pass
