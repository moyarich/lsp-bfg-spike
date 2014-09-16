import os
import sys
from datetime import datetime

LSP_HOME = os.path.abspath(os.path.dirname(__file__))
os.environ['LSP_HOME'] = LSP_HOME

EXECUTOR_DIR = LSP_HOME + os.sep + 'executors'
if EXECUTOR_DIR not in sys.path:
    sys.path.append(EXECUTOR_DIR)

WORKLOAD_DIR = LSP_HOME + os.sep + 'workloads'
if WORKLOAD_DIR not in sys.path:
    sys.path.append(WORKLOAD_DIR)

LIB_DIR = LSP_HOME + os.sep + 'lib'
if LIB_DIR not in sys.path:
    sys.path.append(LIB_DIR)

try:
    from lib.RemoteCommand import remotecmd
except ImportError:
    sys.stderr.write('LSP needs rm in lib/PSQL.py in Workload.py\n')
    sys.exit(2)

import pexpect

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

def result():
    result_file = 'result.txt'
    col_list = 's_id, action_type, action_target, basetime, runtime, deviration, testresult'
    search_condition = "where action_type = 'Loading'"
    result = check.get_result(col_list = col_list, table_list = 'hst.test_result_perscenario_perquery', search_condition = search_condition)
    for one_tuple in result:
        if str(one_tuple).replace('\n','').strip():
            all_col = str(one_tuple).split('|')
            msg = 'Test Suite Name|' + all_col[0].strip() + \
            '|Test Case Name|' + all_col[1].strip() + ':' + all_col[2].strip() + \
            '|Test Detail|' + all_col[3].strip() + ':' + all_col[4].strip() + ':' + all_col[5].strip() + \
            '|Test Status|' + all_col[6].strip()
            Report(result_file , msg)
   
###########################################################################
#  Try to run if user launches this script directly
if __name__ == '__main__':
    child = remotecmd.scp_command (from_user = '', from_host = '', from_file = '/home/gpadmin/Dev/gpsql/private/liuq8/test/lsp/expect', 
        to_user = 'gpadmin@', to_host = 'gpdb63.qa.dh.greenplum.com', to_file = ':~', password = 'changeme')
    child.expect(pexpect.EOF)
    print child.before
    
    child = remotecmd.scp_command (from_user = 'gpadmin@', from_host = 'gpdb63.qa.dh.greenplum.com', from_file = ':~/expect/expect_scp.sh', 
        to_user = '', to_host = '', to_file = '/home/gpadmin/Dev/gpsql/private/liuq8/test/lsp', password = 'changeme')
    child.expect(pexpect.EOF)
    print child.before

    child = remotecmd.ssh_command(user = 'gpadmin', host = 'gpdb63.qa.dh.greenplum.com', password = 'changeme', command = "source psql.sh && psql -d hawq_cov -c 'select version();'")
    child.expect(pexpect.EOF)
    print child.before

   