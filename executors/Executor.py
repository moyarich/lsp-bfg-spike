import os
import sys
import datetime

try:
    from workloads.TPCH.Tpch import Tpch
except ImportError:
    sys.stderr.write('Executor needs Tpch Workload in workloads/TPCH/Tpch.py\n')
    sys.exit(2)

try:
    from workloads.XMARQ.Xmarq import Xmarq
except ImportError:
    sys.stderr.write('Executor needs Xmarq Workload in workloads/XMARQ/Xmarq.py\n')
    sys.exit(2)

try:
    from workloads.TPCDS.Tpcds import Tpcds
except ImportError:
    sys.stderr.write('Executor needs Tpcds Workload in workloads/TPCDS/Tpcds.py\n')
    sys.exit(2)

try:
    from workloads.COPY.Copy import Copy
except ImportError:
    sys.stderr.write('Executor needs Copy Workload in workloads/COPY/Copy.py\n')
    sys.exit(2)

try:
    from workloads.SRI.Sri import Sri
except ImportError:
    sys.stderr.write('Executor needs Sri Workload in workloads/SRI/Sri.py\n')
    sys.exit(2)

try:
    from workloads.GPFDIST.Gpfdist import Gpfdist
except ImportError:
    sys.stderr.write('Executor needs Gpfdist Workload in workloads/GPFDIST/Gpfdist.py\n')
    sys.exit(2)

try:
    from workloads.RETAIL.Retail import Retail
except ImportError:
    sys.stderr.write('Executor needs Retail Workload in workloads/RETAIL/Retail.py\n')
    sys.exit(2)

LSP_HOME = os.getenv('LSP_HOME')

class Executor(object):
    def __init__(self, workloads_list, workloads_content, report_directory, schedule_name, report_sql_file, cs_id, validation):
        self.workloads_list = workloads_list
        self.workloads_content = workloads_content
        self.report_directory = report_directory + os.sep + schedule_name
        self.report_sql_file = report_sql_file
        self.cs_id = cs_id
        self.validation = validation
        self.workloads_instance = []
        self.should_stop = False

    def setup(self):
        # create report directory for schedule
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
            if not os.path.exists(workload_directory):
                print 'Not find workload_directory about %s' % (workload_category)
                continue

            # add one workload into the workloads_instance list
            if workload_category not in ('TPCH', 'XMARQ', 'TPCDS', 'COPY', 'SRI', 'GPFDIST', 'RETAIL'):
                print 'No appropreciate workload type found for workload %s' % (workload_name)
            else:
                wl_instance = workload_category.lower().capitalize() + \
                '(workload_specification, workload_directory, self.report_directory, self.report_sql_file, self.cs_id, self.validation)'
                self.workloads_instance.append(eval(wl_instance))
                

    def cleanup(self):
        pass
