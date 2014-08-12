import os
import sys

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

SRC_DIR = LSP_HOME + os.sep + 'src'
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)

try:
    from executors.SequentialExecutor import SequentialExecutor
except ImportError:
    sys.stderr.write('LSP needs SequentialExecutor in src/executors/SequentialExecutor.py.\n')
    sys.exit(2)

try:
    from executors.ConcurrentExecutor import ConcurrentExecutor
except ImportError:
    sys.stderr.write('LSP needs ConcurrentExecutor in src/executors/ConcurrentExecutor.py.\n')
    sys.exit(2)

try:
    from executors.DynamicExecutor import DynamicExecutor
except ImportError:
    sys.stderr.write('LSP needs DynamicExecutor in src/executors/DynamicExecutor.py.\n')
    sys.exit(2)


###########################################################################
#  Try to run if user launches this script directly
if __name__ == '__main__':
    # parse user options
    parser = OptionParser()
    parser.add_option('-s', '--schedule', dest='schedule', action='store', help='Schedule to be ran')
    (options, args) = parser.parse_args()
    schedule = options.schedule
    if schedule is None:
        print 'Usage: python -u lsp.py -s schedule_file\nPlease use python -u lsp.py -h for more info'
        sys.exit(0)

    # parse schedule file
    schedule_file = LSP_HOME + '/schedules/' + schedule + '.yml'
    with open(schedule_file, 'r') as fschedule:
        schedule_parser = yaml.load(fschedule)

    # parse all workloads in the list
    workloads_list = schedule_parser['workloads']
    print workloads_list
    if len(workloads_list) == 0:
        print 'No workload is specified in schedule file'
        sys.exit(-1)

    # select appropriate executor to run workloads
    workloads_executor = None 
    try:
        execution_mode = schedule_parser['run_workload_mode'].upper()
        print execution_mode
        if execution_mode == 'SEQUENTIAL':
            workloads_executor = SequentialExecutor(workloads_list)
        elif execution_mode == 'CONCURRENT':
            workloads_executor = ConcurrentExecutor(workloads_list)
        elif execution_mode == 'DYNAMIC':
            workloads_executor = DynamicExecutor(workloads_list)
        else:
            print 'Invalid execution mode ' + run_workload_mode + ' specified in schedule file.'
            sys.exit(-1)
    except Exception as e:
        print 'Error while selecting appropreciate executor for workloads: ' + str(e)
        exit(-1)

    # execute workloads
    workloads_executor.execute()
