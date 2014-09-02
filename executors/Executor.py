import os
import sys
import datetime

try:
    from workloads.TPCH.Tpch import Tpch
except ImportError:
    sys.stderr.write('LSP needs TpchWorkload in workloads/TPCH/Tpch.py\n')
    sys.exit(2)

LSP_HOME = os.getenv('LSP_HOME')

class Executor(object):
    def __init__(self, workloads_list, workloads_specification, report_directory, schedule_name, report_sql_path):
        self.workloads_list = workloads_list
        self.workloads_specification = workloads_specification
        self.report_directory = report_directory
        self.report_sql_path = report_sql_path
        self.schedule_name = schedule_name
        self.workloads_instance = []
        self.should_stop = False

    def setup(self):
        # create report directory for test
        self.report_directory = self.report_directory + os.sep + self.schedule_name
        os.system('mkdir -p %s' % (self.report_directory))

        # instantiate and prepare workloads based on workloads sepecification
        for wl in self.workloads_list:
            # check if the detailed definition of current workload exist
            wl_exist = False
            wl_specs = None
            for ws in self.workloads_specification:
                if ws['workload_name'] == wl:
                    wl_exist = True
                    wl_specs = ws
            
            if not wl_exist:
                print 'Detaled definition of workload %s no found in schedule file' % (wl)

            # Find appropreciate workload type for current workload
            workload_category = wl.split('_')[0].upper()
            workload_directory = LSP_HOME + os.sep + 'workloads' + os.sep + workload_category

            if workload_category == 'TPCH':
                self.workloads_instance.append(Tpch(wl_specs, workload_directory, self.report_directory, self.report_sql_path))
            else:
                print 'No appropreciate workload type found for workload %s' % (wl)

    def cleanup(self):
        pass
