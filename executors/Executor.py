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
    def __init__(self, workloads_list, workloads_content, report_directory, schedule_name, report_sql_file):
        self.workloads_list = workloads_list
        self.workloads_content = workloads_content
        self.report_directory = report_directory
        self.schedule_name = schedule_name
        self.report_sql_file = report_sql_file
        self.workloads_instance = []
        self.should_stop = False

    def setup(self):
        # create report directory for test
        self.report_directory = self.report_directory + os.sep + self.schedule_name
        os.system('mkdir -p %s' % (self.report_directory))

        # instantiate and prepare workloads based on workloads content
        for workload_name in self.workloads_list:
            # check if the detailed definition of current workload exist
            workload_name_exist = False
            workload_specification = None
            for workload_specs in self.workloads_content:
                if workload_specs['workload_name'] == workload_name:
                    workload_name_exist = True
                    workload_specification = workload_specs
            
            if not workload_name_exist:
                print 'Detaled definition of workload %s no found in schedule file' % (workload_name)

            # Find appropreciate workload type for current workload
            workload_category = workload_name.split('_')[0].upper()
            workload_directory = LSP_HOME + os.sep + 'workloads' + os.sep + workload_category

            if workload_category == 'TPCH':
                self.workloads_instance.append(Tpch(workload_specification, workload_directory, self.report_directory, self.report_sql_file))
            else:
                print 'No appropreciate workload type found for workload %s' % (workload_name)

    def cleanup(self):
        pass
