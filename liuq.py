import os
import sys
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
    sys.stderr.write('LSP needs RemoteCommand in lib/utils/Report.py\n')
    sys.exit(2)

import pexpect

###########################################################################
#  Try to run if user launches this script directly
if __name__ == '__main__':
#    remotecmd.scp_command(from_user = '', from_host = '', from_file = '/home/gpadmin/Dev/gpsql/private/liuq8/test/lsp/liuq.py', 
#        to_user = 'gpadmin@', to_host = 'gpdb63.qa.dh.greenplum.com', to_file = ':~/huor/', password = 'changeme')

    result = remotecmd.ssh_command(user = 'gpadmin', host = 'gpdb63.qa.dh.greenplum.com', password = 'changeme', 
        command = "source psql.sh && psql -d 'hawq_cov' -c 'select * from hst.test_run;' -t -q")
    print str(result).strip().split('\r\n')[2]

