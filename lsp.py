import os
import sys
import commands
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

if LSP_HOME not in sys.path:
    sys.path.append(LSP_HOME)

EXECUTOR_DIR = LSP_HOME + os.sep + 'executors'
if EXECUTOR_DIR not in sys.path:
    sys.path.append(EXECUTOR_DIR)

WORKLOAD_DIR = LSP_HOME + os.sep + 'workloads'
if WORKLOAD_DIR not in sys.path:
    sys.path.append(WORKLOAD_DIR)

LIB_DIR = LSP_HOME + os.sep + 'lib'
if LIB_DIR not in sys.path:
    sys.path.append(LIB_DIR)

#try:
#    import pexpect
#except ImportError:
#    PEXPECT_DIR = LIB_DIR + os.sep + 'pexpect.tar.gz'
#    os.system('cd %s && tar -zxvf %s' % (LIB_DIR, PEXPECT_DIR))
#    os.system('cd %s/pexpect && python ./setup.py install' % (LIB_DIR))

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
    from lib.QueryFile import QueryFile
except ImportError:
    sys.stderr.write('LSP needs QueryFile in lib/QueryFile.py\n')
    sys.exit(2)

try:
    from lib.utils.Check import check
except ImportError:
    sys.stderr.write('LSP needs check in lib/utils/Check.py\n')
    sys.exit(2)

try:
    from lib.utils.Report import Report
except ImportError:
    sys.stderr.write('LSP needs Report in lib/utils/Report.py\n')
    sys.exit(2)

try:
    from lib.RemoteCommand import remotecmd
except ImportError:
    sys.stderr.write('LSP needs remotecmd in lib/RemoteCommand.py \n')
    sys.exit(2)

import gl

###########################################################################
#  Try to run if user launches this script directly
if __name__ == '__main__':
    # parse user options
    parser = OptionParser()
    parser.add_option('-a', '--standalone', dest='mode', action='store_true', default=False, help='Standalone mode')
    parser.add_option('-s', '--schedule', dest='schedule', action='store', help='Schedule for test execution')
    parser.add_option('-v', '--validation', dest='validation', action='store_true', default=False, help='Validation')
    parser.add_option('-f', '--suffix', dest='suffix', action='store_true', default=False, help='Suffix')
    (options, args) = parser.parse_args()
    standalone_mode = options.mode
    schedules = options.schedule
    gl.validation = options.validation
    gl.suffix = options.suffix

    cs_id = 0
    if schedules is None:
        sys.stderr.write('Usage: python -u lsp.py -a -s schedule_file1[,schedule_file2]\npython -u lsp.py -s schedule_file1[,schedule_file2]\nPlease use python -u lsp.py -h for more info')
        sys.exit(2)

    schedule_list = schedules.split(',')
    beg_time = datetime.now()

    start_flag = False
    # parse schedule file
    for schedule_name in schedule_list:
        schedule_file = LSP_HOME + os.sep + 'schedules' + os.sep + schedule_name + '.yml'
        with open(schedule_file, 'r') as fschedule:
            schedule_parser = yaml.load(fschedule)

        # check cluster information if lsp not run in standalone mode
        if standalone_mode is False:
            cluster_name = schedule_parser['cluster_name']
            # check if specified cluster exists 
            cs_id = check.check_id(result_id = 'cs_id', table_name = 'hst.cluster_settings', search_condition = "cs_name = '%s'" % (cluster_name))
            if cs_id is None:
                sys.stderr.write('Invalid cluster name %s!\n' % (cluster_name))
                continue

        if not start_flag:
            # add test run information in backend database if lsp not run in standalone mode
            start_flag = True
            if standalone_mode is False:
                output = commands.getoutput('cat ~/qa.sh')
                try:
                    wd = output[output.index('wd='):].split('"')[1]
                    output = commands.getoutput('%s; cat build_info_file.txt' % (wd))
                    build_id = output[output.index('PULSE_ID_INFO'):].split('\n')[0].split('=')[1]
                    build_url = output[output.index('PULSE_PROJECT_INFO'):].split('\n')[0].split('=')[1]
                except Exception, e:
                    print('read build_info_file error. ')
                    build_id = -1
                    build_url = 'Local'

                (status, output) = commands.getstatusoutput('rpm -qa | grep hadoop | grep hdfs | grep -v node')
                hdfs_version = output
                if status != 0 or hdfs_version == '':
                    hdfs_version = 'Local HDFS Deployment'

                (status, output) = commands.getstatusoutput('rpm -qa | grep hawq')
                hawq_version = output
                if status != 0 or hawq_version == '':
                    hawq_version = 'Local HAWQ Deployment'

                check.insert_new_record(table_name = 'hst.test_run', 
                    col_list = 'pulse_build_id, pulse_build_url, hdfs_version, hawq_version, start_time', 
                    values = "'%s', '%s', '%s', '%s', '%s'" % (build_id, build_url, hdfs_version, hawq_version, str(beg_time).split('.')[0]))
            
            # prepare report directory with times and the report.sql file
            report_directory = LSP_HOME + os.sep + 'report' + os.sep + datetime.now().strftime('%Y%m%d-%H%M%S')
            os.system('mkdir -p %s' % (report_directory))
            os.system('mkdir -p %s' % (report_directory + os.sep + 'tmp'))
            report_sql_file = os.path.join(report_directory, 'report.sql')

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
                workloads_executor = ConcurrentExecutor(workloads_list, workloads_content, report_directory, schedule_name, report_sql_file, cs_id)
            elif workloads_mode == 'DYNAMIC':
                workloads_executor = DynamicExecutor(workloads_list, workloads_content, report_directory, schedule_name, report_sql_file, cs_id)
            else:
                print 'Invalid workloads mode ' + workloads_mode + ' specified in schedule file.'
                exit(-1)
        except Exception as e:
            print 'Error while selecting appropreciate executor for workloads: ' + str(e)
            exit(-1)
        workloads_executor.execute()
    
    end_time = datetime.now()
    duration = end_time - beg_time
    duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds/1000

    # update backend database to log execution time
    if standalone_mode is False and start_flag:
        check.update_record(table_name = 'hst.test_run', set_content = "end_time = '%s', duration = %d" % (str(end_time).split('.')[0], duration), search_condition = "start_time = '%s'" % (str(beg_time).split('.')[0]))

        # add detailed execution information of test cases into backend database
        remotecmd.scp_command(from_user = '', from_host = '', from_file = report_sql_file,
            to_user = 'gpadmin@', to_host = 'gpdb63.qa.dh.greenplum.com', to_file = ':/tmp/', password = 'changeme')
        cmd = 'source psql.sh && psql -d hawq_cov -t -q -f /tmp/report.sql'
        remotecmd.ssh_command(user = 'gpadmin', host = 'gpdb63.qa.dh.greenplum.com', password = 'changeme', command = cmd)

        # retrieve test report from backend database for pulse report purpose`
        result_file = os.path.join(report_directory, 'result.txt')
        tr_id = check.get_max_id(result_id = 'tr_id', table_name = 'hst.test_run')
        sql = "select 'Test Suite Name|'|| wl_name || '|Test Case Name|' || action_type ||'.' || action_target \
        || '|Test Detail|' \
        || 'Actural Run time is: ' || CASE WHEN actual_execution_time is NOT NULL THEN actual_execution_time::int::text ELSE 'N.A.' END || ' ms, ' \
        || 'Baseline time is: ' || CASE WHEN baseline_execution_time IS NOT NULL THEN baseline_execution_time::int::text ELSE 'N.A.' END || ' ms, ' \
        || 'Comparision is: ' || CASE WHEN deviation is NOT NULL THEN deviation::decimal(5,2)::text ELSE 'N.A.' END \
        || ' ('|| CASE WHEN actual_execution_time is NOT NULL THEN actual_execution_time::int::text ELSE '0' END || ' ms)' \
        || '|Test Status|' || test_result \
        from \
            hst.f_generate_test_report_detail(%s, 'PHD 2.1', 'HAWQ 1.2.1.0 build 10335');" % (tr_id)

        result = check.get_result_by_sql(sql = sql)
        
        result = str(result).strip().split('\r\n')
        for one_tuple in result:
            msg = str(one_tuple).strip()
            Report(result_file , msg)



