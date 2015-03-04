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
    from workloads.RETAILDW.Retaildw import Retaildw
except ImportError:
    sys.stderr.write('Executor needs Retail Workload in workloads/RETAIL/Retail.py\n')
    sys.exit(2)

try:
    from workloads.RQTPCH.Rqtpch import Rqtpch
except ImportError:
    sys.stderr.write('Executor needs Rqtpch Workload in workloads/Rqtpch/Rqtpch.py\n')
    sys.exit(2)

try:
    from workloads.STREAMTPCH.Streamtpch import Streamtpch
except ImportError:
    sys.stderr.write('Executor needs Streamtpch Workload in workloads/Streamtpch/Streamtpch.py\n')
    sys.exit(2)

try:
    from generateRQ.RQ import RQ
except ImportError:
    sys.stderr.write('Executor needs generateRQ/RQ.py.\n')
    sys.exit(2)

LSP_HOME = os.getenv('LSP_HOME')

class Executor(object):
    def __init__(self, schedule_parser, report_directory, schedule_name, report_sql_file, cs_id):
        self.workloads_list = [wl.strip() for wl in schedule_parser['workloads_list'].split(',')]
        self.workloads_content = schedule_parser['workloads_content']
        
        # create report directory for schedule
        self.report_directory = report_directory + os.sep + schedule_name
        os.system('mkdir -p %s' % (self.report_directory))

        self.rq_instance = None
        self.rq_path_num = 1
        self.rq_path_count = 0
        self.adjust_factor_count = 1
        try:
            if schedule_parser['rq_path_list'] is not None:
                self.rq_instance = []
                for rq_path in schedule_parser['rq_path_list'].split(','):
                    rq_path = os.getcwd() + '/generateRQ/' + rq_path.strip()
                    os.system('mkdir -p %s' % (self.report_directory + os.sep + 'rqfile_%d' % (self.rq_path_count)))
                    rq_instance = RQ(path = rq_path, report_directory = self.report_directory + os.sep + 'rqfile_%d/' % (self.rq_path_count) )
                    rq_instance.generateRq()
                    self.rq_instance.append(rq_instance)
                    self.rq_path_count += 1
                self.rq_path_num = len(self.rq_instance)
                self.rq_path_count = 0
        except Exception, e:
            pass
        
        self.report_sql_file = report_sql_file
        self.cs_id = cs_id

        self.workloads_instance = []


    def setup(self):
        self.workloads_instance = []
        user_list = None
        if self.rq_path_count == self.rq_path_num:
            return 'stop'

        if self.rq_instance is None:
            user_list = None
            self.rq_path_count += 1
            report_directory = self.report_directory
        else:
            report_directory = self.report_directory + os.sep + 'rqfile_%d/factor_%d' % (self.rq_path_count, self.adjust_factor_count)
            user_list = self.rq_instance[self.rq_path_count].runRq()
            if len(user_list) == 0:
                self.rq_path_count += 1
                self.adjust_factor_count = 1
                return 'next'
            else:
                self.adjust_factor_count += 1

        # instantiate and prepare workloads based on workloads content
        for workload_name in self.workloads_list:
            # check if the detailed definition of current workload exist
            workload_name_exist = False
            workload_specification = None
            for workload_specs in self.workloads_content:
                if workload_specs['workload_name'] == workload_name:
                    workload_name_exist = True
                    workload_specification = workload_specs
                    if user_list is None:
                        user_list = [ user.strip() for user in workload_specification['user'].strip().split(',') ]
            
            if not workload_name_exist:
                print 'Detaled definition of workload %s no found in schedule file' % (workload_name)
                continue

            # Find appropreciate workload type for current workload
            workload_category = workload_name.split('_')[0].upper()
            workload_directory = LSP_HOME + os.sep + 'workloads' + os.sep + workload_category
            if not os.path.exists(workload_directory):
                print 'Not find workload_directory about %s' % (workload_category)
                continue

            # add one workload into the workloads_instance list
            if workload_category not in ('TPCH', 'XMARQ', 'TPCDS', 'COPY', 'SRI', 'GPFDIST', 'RETAILDW', 'RQTPCH', 'STREAMTPCH'):
                print 'No appropreciate workload type found for workload %s' % (workload_name)
            else:
                for user in user_list:
                    user = user.strip()
                    wl_instance = workload_category.lower().capitalize() + \
                    '(workload_specification, workload_directory, report_directory, self.report_sql_file, self.cs_id, user)'
                    self.workloads_instance.append(eval(wl_instance))

        return 'start'
        
                
    def cleanup(self):
        pass
