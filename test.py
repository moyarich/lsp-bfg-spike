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

LIB_DIR = LSP_HOME + os.sep + 'lib'
if LIB_DIR not in sys.path:
    sys.path.append(LIB_DIR)

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

###########################################################################
#  Try to run if user launches this script directly
if __name__ == '__main__':
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
            Report('result_out', msg)