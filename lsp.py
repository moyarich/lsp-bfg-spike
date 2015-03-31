import os
import sys
import commands
import time
from datetime import datetime
from multiprocessing import Process

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

MONI_DIR = LSP_HOME + os.sep + 'monitor'
if MONI_DIR not in sys.path:
    sys.path.append(MONI_DIR)

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
    sys.stderr.write('LSP needs psql in lib/PSQL.py in Workload.py.\n')
    sys.exit(2)

try:
    from lib.utils.Check import check
except ImportError:
    sys.stderr.write('LSP needs check in lib/utils/Check.py.\n')
    sys.exit(2)

try:
    from lib.utils.Report import Report
except ImportError:
    sys.stderr.write('LSP needs Report in lib/utils/Report.py.\n')
    sys.exit(2)

try:
    from lib.RemoteCommand import remotecmd
except ImportError:
    sys.stderr.write('LSP needs remotecmd in lib/RemoteCommand.py.\n')
    sys.exit(2)

import gl

from MonitorControl import Monitor_control

###########################################################################
#  Try to run if user launches this script directly
if __name__ == '__main__':
    # parse user options
    parser = OptionParser()
    parser.add_option('-s', '--schedule', dest='schedule', action='store', help='Schedule for test execution')
    parser.add_option('-a', '--add', dest='add_option', action='store_true', default=False, help='Add result in backend database')
    parser.add_option('-c', '--check', dest='check', action='store_true', default=False, help='Check query result')
    parser.add_option('-f', '--suffix', dest='suffix', action='store_true', default=False, help='Add table suffix')
    parser.add_option('-m', '--monitor', dest='monitor', action='store', default=0, help='Monitor interval')
    parser.add_option('-r', '--report', dest='report', action='store', default=0, help='Generate monitor report num')
    parser.add_option('-p', '--parameter', dest='param', action='store', help='Assign resource queue parameter name and value')
    parser.add_option('-d', '--delete', dest='del_flag', action='store_true', default=False, help='Delete table parameters')
    (options, args) = parser.parse_args()
    schedules = options.schedule
    add_database = options.add_option
    gl.check_result = options.check
    gl.suffix = options.suffix
    monitor_interval = options.monitor
    report_num = options.report
    
    if not str(report_num).isdigit():
        print '-r option must follow a digit, such as -r 1'
        sys.exit(2)

    if options.param is None:
        rq_param = ''
    elif options.param.find(':') != -1 and options.param[-1] != ':':
        rq_param = options.param
    else:
        print 'formart error: For example, -p RESOURCE_UPPER_FACTOR:2, -p MEMORY_LIMIT_CLUSTER:20'
        sys.exit(2)

    cs_id = 0
    if schedules is None:
        sys.stderr.write('Usage: python -u lsp.py -s schedule_file1[,schedule_file2] [-a] [-c] [-f] [-p] [-r]\nPlease use python -u lsp.py -h for more info\n')
        sys.exit(2)

    schedule_list = schedules.split(',')
    beg_time = datetime.now()

    start_flag = False
    # parse schedule file
    for schedule_name in schedule_list:
        schedule_file = LSP_HOME + os.sep + 'schedules' + os.sep + schedule_name + '.yml'
        with open(schedule_file, 'r') as fschedule:
            schedule_parser = yaml.load(fschedule)

        # parse list of the workloads for execution
        if 'workloads_list' not in schedule_parser.keys() or schedule_parser['workloads_list'] is None :
            print 'No workload is specified in schedule file : %s' %(schedule_name + '.yml')
            continue

        # check cluster information if lsp not run in standalone mode
        if add_database:
            cluster_name = schedule_parser['cluster_name']
            # check if specified cluster exists 
            cs_id = check.check_id(result_id = 'cs_id', table_name = 'hst.cluster_settings', search_condition = "cs_name = '%s'" % (cluster_name))
            if cs_id is None:
                sys.stderr.write('Invalid cluster name %s!\n' % (cluster_name))
                continue

        if not start_flag:
            start_flag = True
            # add test run information in backend database if lsp not run in standalone mode,such as build_id, build_url, hawq_version, hdfs_version
            tr_id = -1
            if add_database:
                output = commands.getoutput('cat ~/qa.sh')
                try:
                    wd = output[output.index('wd='):].split('"')[1]
                    output = commands.getoutput('%s; cat build_info_file.txt' % (wd))
                    build_id = output[output.index('PULSE_ID_INFO'):].split('\n')[0].split('=')[1]
                    build_url = output[output.index('PULSE_PROJECT_INFO'):].split('\n')[0].split('=')[1]
                except Exception, e:
                    print('read build_info_file error: ' + str(e))
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
                    values = "'%s', '%s', '%s', '%s', '%s'" % (build_id, build_url, hdfs_version, hawq_version, str(beg_time)))

                tr_id = check.check_id(result_id = 'tr_id', table_name = 'hst.test_run', search_condition = "start_time = '%s'" % ( str(beg_time) ))
            
            # prepare report directory with times and the report.sql file
            report_directory = LSP_HOME + os.sep + 'report' + os.sep + datetime.now().strftime('%Y%m%d-%H%M%S')
            os.system('mkdir -p %s' % (report_directory))
            #os.system('mkdir -p %s' % (report_directory + os.sep + 'tmp'))
            report_sql_file = os.path.join(report_directory, 'report.sql')

            if monitor_interval > 0:
                monitor_control = Monitor_control(mode = 'remote', interval = monitor_interval , run_id = tr_id)
                monitor_control.start(mode = 'sync')

        # select appropriate executor to run workloads
        workloads_executor = None 
        workloads_mode = schedule_parser['workloads_mode'].upper()
        if workloads_mode == 'SEQUENTIAL':
            workloads_executor = SequentialExecutor(schedule_parser, report_directory, schedule_name, report_sql_file, cs_id, tr_id, rq_param)
        elif workloads_mode == 'CONCURRENT':
            workloads_executor = ConcurrentExecutor(schedule_parser, report_directory, schedule_name, report_sql_file, cs_id, tr_id, rq_param)
        elif workloads_mode == 'DYNAMIC':
            workloads_executor = DynamicExecutor(schedule_parser, report_directory, schedule_name, report_sql_file, cs_id, tr_id, rq_param)
        else:
            print 'Invalid workloads mode ' + workloads_mode + ' specified in schedule file.'
            sys.exit(2)

        workloads_executor.execute()
    
    end_time = datetime.now()
    duration = end_time - beg_time
    duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds/1000

    if monitor_interval > 0 and start_flag:
        monitor_control.stop()

    # update backend database to log execution time
    if add_database and start_flag:
        check.update_record(table_name = 'hst.test_run', set_content = "end_time = '%s', duration = %d" % (str(end_time), duration), search_condition = "start_time = '%s'" % (str(beg_time)))

        # add detailed execution information of test cases into backend database
        remotecmd.scp_command(from_user = '', from_host = '', from_file = report_sql_file,
            to_user = 'gpadmin@', to_host = 'gpdb63.qa.dh.greenplum.com', to_file = ':/tmp/', password = 'changeme')
        cmd = 'source ~/psql.sh && psql -d hawq_cov -t -q -f /tmp/report.sql'
        remotecmd.ssh_command(user = 'gpadmin', host = 'gpdb63.qa.dh.greenplum.com', password = 'changeme', command = cmd)

        # retrieve test report from backend database for pulse report purpose
        result_file = os.path.join(report_directory, 'result.txt')
        tr_id = check.check_id(result_id = 'tr_id', table_name = 'hst.test_run', search_condition = "start_time = '%s'" % ( str(beg_time) ))
        sql = "select 'Test Suite Name|'|| wl_name || '|Test Case Name|' || action_type ||'.' || action_target \
        || '|Test Detail|' \
        || 'Actural Run time is: ' || CASE WHEN actual_execution_time is NOT NULL THEN actual_execution_time::int::text ELSE 'N.A.' END || ' ms, ' \
        || 'Baseline time is: ' || CASE WHEN baseline_execution_time IS NOT NULL THEN baseline_execution_time::int::text ELSE 'N.A.' END || ' ms, ' \
        || 'Comparision is: ' || CASE WHEN deviation is NOT NULL THEN deviation::decimal(5,2)::text ELSE 'N.A.' END \
        || ' ('|| CASE WHEN actual_execution_time is NOT NULL THEN actual_execution_time::int::text ELSE '0' END || ' ms)' \
        || '|Test Status|' || test_result \
        from \
            hst.f_generate_test_report_detail(%d, 'PHD 2.2 build 59', 'HAWQ 2.0.0.0 build 12988 ORCA OFF') where wl_name not like '%s';" % (tr_id, '%' + 'RWITHD' + '%')

        result = check.get_result_by_sql(sql = sql)
        result = str(result).strip().split('\r\n')
        for one_tuple in result:
            msg = str(one_tuple).strip()
            Report(result_file , msg)

        # generate summary report
        if report_num > 0:
            start_run_id = int(tr_id) - int(report_num) + 1
            sql = "select wl_name, action_type,overral_test_result,  improvenum, passnum, failurenum, skipnum, errornum, actual_total_execution_time,baseline_total_execution_time,deviation \
            from hst.f_generate_test_report_summary(%d, %d, 'PHD 2.2 build 59', 'HAWQ 2.0.0.0 build 12988 GVA ORCA OFF') where wl_name not like '%s' order by tr_id, s_id,action_type;" % (start_run_id, tr_id, '%' + 'RWITHD' + '%')

            result = check.get_result_by_sql(sql = sql)
            result = str(result).strip().split('\r\n')

            for one_tuple in result:
                msg = str(one_tuple).strip()
                Report('./report/summary_report.txt' , msg)

            sql = "select wl_name, action_type,overral_test_result,  improvenum, passnum, failurenum, skipnum, errornum, actual_total_execution_time,baseline_total_execution_time,deviation \
            from hst.f_generate_test_report_summary(%d, %d, 'PHD 2.2 build 59', 'HAWQ 1.2.1.2 build 11946 GVA ORCA OFF') where wl_name not like '%s' order by tr_id, s_id,action_type;" % (start_run_id, tr_id, '%' + 'RWITHD' + '%')

            result = check.get_result_by_sql(sql = sql)
            result = str(result).strip().split('\r\n')

            for one_tuple in result:
                msg = str(one_tuple).strip()
                Report('./report/summary_report_baseline1x.txt' , msg)

        # add resource parameter and run info into backend database
        if options.param is not None:
            if options.del_flag:
                sql = "DELETE FROM hst.parameters WHERE param_name = '%s';" % (options.param.split(':')[0].strip().upper())
                print check.get_result_by_sql(sql = sql)
            sql = "INSERT INTO hst.parameters (run_id, param_name, param_value) VALUES (%d, '%s', '%s');" % (tr_id, options.param.split(':')[0].strip().upper(), options.param.split(':')[1].strip())
            print check.get_result_by_sql(sql = sql)

        # generate monitor report
        if monitor_interval > 0 and report_num > 0:
            start_run_id = int(tr_id) - int(report_num) + 1
            sql = 'select hst.f_generate_monitor_report(%d, %d, false);' % (start_run_id, tr_id)
            print sql
            result = check.get_result_by_sql(sql = sql)
            print 'generate monitor report: ', result
