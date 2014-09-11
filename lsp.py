import os
import sys
from datetime import datetime

try:
    import yaml
except ImportError:
    sys.stderr.write('LSP needs pyyaml. You can get it from http://pyyaml.org.\n') 
    sys.exit(2)

try:
    from optparse import OptionParser
except ImportError:
    sys.stderr.write('LSP needs optparse.\n') 
    sys.exit(2)

LSP_HOME = os.path.abspath(os.path.dirname(__file__))
os.environ['LSP_HOME'] = LSP_HOME

EXECUTOR_DIR = LSP_HOME + os.sep + 'executors'
if EXECUTOR_DIR not in sys.path:
    sys.path.append(EXECUTOR_DIR)

WORKLOAD_DIR = LSP_HOME + os.sep + 'workloads'
if WORKLOAD_DIR not in sys.path:
    sys.path.append(WORKLOAD_DIR)

try:
    from executors.SequentialExecutor import SequentialExecutor
except ImportError:
    sys.stderr.write('LSP needs SequentialExecutor in executors/SequentialExecutor.py.\n')
    sys.exit(2)

try:
    from executors.ConcurrentExecutor import ConcurrentExecutor
except ImportError:
    sys.stderr.write('LSP needs ConcurrentExecutor in executors/ConcurrentExecutor.py.\n')
    sys.exit(2)

try:
    from executors.DynamicExecutor import DynamicExecutor
except ImportError:
    sys.stderr.write('LSP needs DynamicExecutor in executors/DynamicExecutor.py.\n')
    sys.exit(2)

try:
    from lib.PSQL import psql
except ImportError:
    sys.stderr.write('LSP needs psql in lib/PSQL.py in Workload.py\n')
    sys.exit(2)

try:
    from lib.utils.Check import check
except ImportError:
    sys.stderr.write('LSP needs check in lib/utils/Check.py in lsp.py\n')
    sys.exit(2)

    
###########################################################################
#  Try to run if user launches this script directly
if __name__ == '__main__':
    # parse user options
    parser = OptionParser()
    parser.add_option('-c', '--cluster', dest='cluster', action='store', help='Cluster for test execution')
    parser.add_option('-s', '--schedule', dest='schedule', action='store', help='Schedule for test execution')
    (options, args) = parser.parse_args()
    cluster_name = options.cluster
    schedules = options.schedule
    if cluster_name is None or schedules is None:
        print 'Usage: python -u lsp.py -c cluster_name -s schedule_file1[,schedule_file2]\nPlease use python -u lsp.py -h for more info'
        sys.exit(2)
    
    # check if cluster exist    
    cs_id = check.check_id(result_id = 'cs_id', table_name = 'hst.cluster_settings', search_condition = "cs_name = '%s'" % (cluster_name))
    if cs_id is None:
        sys.stderr.write('The cluster_name is wrong!\n')
        sys.exit(2)

    # prepare report directory with times and the report.sql file
    report_directory = LSP_HOME + os.sep + 'report' + os.sep + datetime.now().strftime('%Y%m%d-%H%M%S')
    os.system('mkdir -p %s' % (report_directory))
    report_sql_file = os.path.join(report_directory, 'report.sql')
    
    schedule_list = schedules.split(',')
    beg_time = datetime.now()
    check.insert_new_record(table_name = 'hst.test_run', col_list = '(start_time)', values = "'%s'" % (str(beg_time).split('.')[0]))

    # parse schedule file
    for schedule_name in schedule_list:
        schedule_file = LSP_HOME + os.sep + 'schedules' + os.sep + schedule_name + '.yml'
        with open(schedule_file, 'r') as fschedule:
            schedule_parser = yaml.load(fschedule)

        # parse list of the workloads for execution
        workloads_list = schedule_parser['workloads_list']
        workloads_list = [wl.strip(' ') for wl in workloads_list.split(',')]
        if len(workloads_list) == 0:
            print 'No workload is specified in schedule file : %s' %(schedule_name + '.yml')
            exit(-1)

        # parse detailed definition of the workloads
        workloads_content = schedule_parser['workloads_content']

        # select appropriate executor to run workloads
        workloads_executor = None 
        try:
            workloads_mode = schedule_parser['workloads_mode'].upper()
            if workloads_mode == 'SEQUENTIAL':
                workloads_executor = SequentialExecutor(workloads_list, workloads_content, report_directory, schedule_name, report_sql_file, cs_id)
            elif workloads_mode == 'CONCURRENT':
                workloads_executor = ConcurrentExecutor(workloads_list, workloads_content, report_directory, schedule_name)
            elif workloads_mode == 'DYNAMIC':
                workloads_executor = DynamicExecutor(workloads_list, workloads_content)
            else:
                print 'Invalid workloads mode ' + workloads_mode + ' specified in schedule file.'
                exit(-1)
        except Exception as e:
            print 'Error while selecting appropreciate executor for workloads: ' + str(e)
            exit(-1)
        workloads_executor.execute()
    
    end_time = datetime.now()
    duration = end_time - beg_time
    duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds /1000
    check.update_record(table_name = 'hst.test_run', set_content = "end_time = '%s', duration = %d" % (str(end_time).split('.')[0], duration),
        search_condition = "start_time = '%s'" % (str(beg_time).split('.')[0]))
    
 #   psql.runfile(ifile = report_sql_file, dbname = 'hawq_cov', username = 'hawq_cov', password = None,
  #           host = 'gpdb63.qa.dh.greenplum.com', port = 5430)