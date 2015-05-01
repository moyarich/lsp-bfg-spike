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
    from generateRQ.RQ import RQ
except ImportError:
    sys.stderr.write('Executor needs generateRQ/RQ.py.\n')
    sys.exit(2)

LSP_HOME = os.getenv('LSP_HOME')

class Executor(object):
    def __init__(self, schedule_parser, report_directory, schedule_name, report_sql_file, cs_id, tr_id, rq_param):
        self.workloads_list = [wl.strip() for wl in schedule_parser['workloads_list'].split(',')]
        self.workloads_content = schedule_parser['workloads_content']
        if 'workloads_user_map' in schedule_parser.keys():
            self.map_mode = schedule_parser['workloads_user_map'].strip()
        else:
            self.map_mode = 'loop'

        if self.map_mode not in ['loop', 'scan']:
            print "workloads and users map mode must in ['loop', 'scan']"
            sys.exit(2)


        # create report directory for schedule
        self.report_directory = report_directory + os.sep + schedule_name
        os.system('mkdir -p %s' % (self.report_directory))

        self.rq_instance = None
        if rq_param == '':
            p_name = ''
            p_value = ''
        else:
            p_name = rq_param.split(':')[0].strip()
            p_value = rq_param.split(':')[1].strip()

        if 'rq_path_list' in schedule_parser.keys():
            rq_path = os.getcwd() + '/generateRQ/' + schedule_parser['rq_path_list'].strip()
            self.rq_instance = RQ(rq_path, self.report_directory, p_name, p_value)
            # generate resource queue in two modes, inhert from pg_default or other
            self.rq_instance.generateRq()
        
        self.report_sql_file = report_sql_file
        self.cs_id = cs_id
        self.tr_id = tr_id

        self.workloads_instance = []


    def map_user_workload(self, user_list, report_directory, mode = 'loop'):
        # not have a resource queue yaml file
        if user_list is None:
            for workload_name in self.workloads_list:
                # check if the detailed definition of current workload exist
                workload_name_exist = False
                workload_specification = None
                for workload_specs in self.workloads_content:
                    if workload_specs['workload_name'] == workload_name:
                        workload_name_exist = True
                        workload_specification = workload_specs
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
                if workload_category not in ('TPCH', 'XMARQ', 'TPCDS', 'COPY', 'SRI', 'GPFDIST', 'RETAILDW', 'RQTPCH'):
                    print 'No appropreciate workload type found for workload %s' % (workload_name)
                else:
                    user_count = 0
                    for user in user_list:
                        if user_count > 0 and 'db_reuse' in workload_specification.keys() and workload_specification['db_reuse']:
                            workload_specification['load_data_flag'] = False
                        #print workload_specification
                        user_count += 1
                        wl_instance = workload_category.lower().capitalize() + \
                        '(workload_specification, workload_directory, report_directory, self.report_sql_file, self.cs_id, self.tr_id, user)'
                        self.workloads_instance.append(eval(wl_instance))
        # the user_list is from resource queue yaml file
        else:
            scan_user_count = 0
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
                    continue

                # Find appropreciate workload type for current workload
                workload_category = workload_name.split('_')[0].upper()
                workload_directory = LSP_HOME + os.sep + 'workloads' + os.sep + workload_category
                if not os.path.exists(workload_directory):
                    print 'Not find workload_directory about %s' % (workload_category)
                    continue

                # add one workload into the workloads_instance list
                if workload_category not in ('TPCH', 'XMARQ', 'TPCDS', 'COPY', 'SRI', 'GPFDIST', 'RETAILDW', 'RQTPCH'):
                    print 'No appropreciate workload type found for workload %s' % (workload_name)
                else:
                    user_count = 0
                    user_num = len(user_list)
                    if mode == 'loop':
                        for user in user_list:
                            if user_count > 0 and 'db_reuse' in workload_specification.keys() and workload_specification['db_reuse']:
                                workload_specification['load_data_flag'] = False
                            #print workload_specification
                            user = user.strip()
                            wl_instance = workload_category.lower().capitalize() + \
                            '(workload_specification, workload_directory, report_directory, self.report_sql_file, self.cs_id, self.tr_id, user)'
                            self.workloads_instance.append(eval(wl_instance))
                            user_count += 1
                            #print workload_name, user
                    elif mode == 'scan':
                        user = user_list[scan_user_count].strip()
                        wl_instance = workload_category.lower().capitalize() + \
                        '(workload_specification, workload_directory, report_directory, self.report_sql_file, self.cs_id, self.tr_id, user)'
                        self.workloads_instance.append(eval(wl_instance))
                        #print workload_name, user
                        scan_user_count += 1
                        if scan_user_count == user_num:
                            scan_user_count = 0


    def setup(self):
        self.workloads_instance = []
        user_list = None
        report_directory = self.report_directory
        if self.rq_instance is None:
            user_list = None
        else:
            user_list = self.rq_instance.runRq()

        # instantiate and prepare workloads based on workloads content
        self.map_user_workload(user_list = user_list, report_directory = report_directory, mode = self.map_mode)
                
    def cleanup(self):
        pass
