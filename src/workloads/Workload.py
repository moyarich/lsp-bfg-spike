import os
import sys
import time
import random
from multiprocessing import Process, Queue, Value , Array

try:
    from pygresql import pg
except ImportError:
    sys.stderr.write('LSP needs pygresql.\n')
    sys.exit(2)

try:
    from QueryFile import QueryFile
except ImportError:
    sys.stderr.write('LSP needs QueryFile in lib/QueryFile.py.\n')
    sys.exit(2)

try:
    from utils.Log import Log
except ImportError:
    sys.stderr.write('LSP needs Log in lib/utils/Log.py.\n')
    sys.exit(2)

try:
    from utils.Report import Report
except ImportError:
    sys.stderr.write('LSP needs Report in lib/utils/Report.py.\n')
    sys.exit(2)

LSP_HOME = os.getenv('LSP_HOME')

'''
members of BaseWorkload:
    name :           workload name
    config:          raw config, type of map
    dbname:          database name
    need_load_data   load data, True/False
    need_run_query   run_query, True/False
    run_query_mode   run_query_mode, RANDOM/SEQUENTIAL
    datasize:        datasize need load for some workload
    niteration:      num of iteration 
    nconcurrency     concurrency of querys
'''

class Workload(object):
    def __init__(self, workload, test_report_dir=''):
        # initialize common propertities for workload
        self.workload_name = workload['workload_name'].strip()
        self.database_name = workload['database_name'].strip()
        self.load_data_flag = str(workload['load_data_flag']).strip().upper()
        self.data_volumn_type = str(workload['data_volumn_type']).strip().upper()
        self.data_volumn_size = int(str(workload['data_volumn_size']).strip())
        self.data_volumn = 0
        self.run_workload_flag = str(workload['run_workload_flag']).strip().upper()
        self.run_workload_mode = workload['run_workload_mode'].strip().upper()
        self.num_concurrency = int(str(workload['num_concurrency']).strip())
        self.num_iteration = int(str(workload['num_iteration']).strip())
     
        # prepare report directory for workload
        if test_report_dir != '':
            self.workload_report_dir = os.path.join(test_report_dir, self.workload_name)
        else:
            print 'Test report directory is not available before preparing report directory for workload %s' % (self.workload_name)
            exit(-1)
        os.system('mkdir %s'%self.workload_report_dir)

        # check flag for data loading
        if self.load_data_flag == 'TRUE':
            self.load_data_flag = True
        elif self.load_data_flag == 'FALSE':
            self.load_data_flag = False
        else:
            self.error('Invalid value for data loading flag in workload %s: %s. Must be TRUE/FALSE.' % (self.workload_name, self.load_data_flag))
            exit(-1)

        # check type for data volumn
        if self.data_volumn_type == 'TOTAL':
            self.data_volumn = self.data_volumn_size
        elif self.data_volumn_type == 'PER_NODE':
            self.data_volumn = self.data_volumn_size * 8
        elif self.data_volumn_type == 'PER_SEGMENT':
            self.data_volumn = self.data_volumn_size * 8 * 4
        else:
            self.error('Invalid value for type of data volumn in workload %s: %s. Mast be TOTAL/PER_NODE/PER_SEGMENT.' % (self.workload_name, self.data_volumn_type))
            exit(-1)

        # check flag for workload execution
        if self.run_workload_flag == 'TRUE':
            self.run_workload_flag = True
        elif self.run_workload_flag == 'FALSE':
            self.run_workload_flag = False
        else:
            self.error('Invalid value for data loading flag in workload %s: %s. Must be TRUE/FALSE.' % (self.workload_name, self.run_workload_flag))
            exit(-1)

        # check mode for workload execution
        if self.run_workload_mode == 'SEQUENTIAL':
            pass
        elif self.run_workload_mode == 'RANDOM':
            pass
        else:
            self.error('Invalid value for mode of workload execution in workload %s: %s. Mast be TOTAL/PER_NODE/PER_SEGMENT.' % (self.workload_name, self.run_workload_mode))
            exit(-1)

        # set workload source directory
        self.workload_category = self.workload_name.split('_')[0].upper()
        self.workload_src_dir =  LSP_HOME + os.sep + 'src' + os.sep + 'workloads' + os.sep + self.workload_category

        # set output log, error log, and report
        self.outputlog = os.path.join(self.workload_report_dir, 'output.csv')
        self.errorlog = os.path.join(self.workload_report_dir, 'error.csv')
        self.reportlog= os.path.join(self.workload_report_dir, 'report.csv')

        # should always run the workload by default
        self.should_stop = False

    def setup(self):
        '''Setup prerequisites for workload'''
        pass

    def load_data(self):
        '''Load data for workload'''
        pass

    def vacuum_analyze(self):
        '''VACUUM/ANALYZE against data for workload'''
        pass

    def run_query(self):
        '''
        Run queries in lsp/src/workloads/$workload_name/queries/*.sql one by one in user-specified order
        1) The queries would be run in one or more times as specified by niteration
        2) The queries would be run in one or more concurrent streams as specified by nconcurrency
        It needs to be overwritten in child class if user want run queries of the workload in customized way
        '''
        workload_query_directory = self.workload_src_dir + 'queries'
        if not os.path.exists(workload_query_directory):
            return

        query_files = [os.path.join(workload_query_directory) for file in os.listdir(workload_query_directory) if file.endswith('.sql')]
        if self.run_workload_mode == 'SEQUENTIAL':
            query_files = sorted(query_files)
        else:
            query_files = random.shuffle(query_files)

        cnx = pg.connect(dbname = self.database_name)
        # run all sql file under queries forder
        for file in query_files:
            qf = QueryFile(file)
            # run all query in sql file
            for query in qf:
                # run this query
                try:
                    cnx.query(query)
                except Exception, e:
                    self.error('Query Fail:')
                    self.error( 'sql:')
                    self.error( query)
                    self.error( 'ans:' )
                    self.error( str(e) )
 
        cnx.close()

    def cleanup(self):
        '''Cleanup environment after execution of workload'''
        pass


    def run_workload_query(self):
        if not self.need_run_query:
            self.output('Skip Query Running....')
            return
 
        niteration = self.niteration
        while niteration > 0:
            self.output('Start interation %s ....'%(self.niteration - niteration))
            AllWorkers = []
            nconcurrency = self.nconcurrency
            while nconcurrency > 0:
                self.output('Process - %s  Start..'%(self.nconcurrency - nconcurrency))
                p = Process(target = self.run_query)
                AllWorkers.append(p)
                nconcurrency = nconcurrency - 1
                p.start()

            self.should_stop = False
            while True and not self.should_stop:
                for p in AllWorkers[:]:
                    p.join(timeout = 0.3)
                    if p.is_alive():
                        pass
                    else:
                        AllWorkers.remove(p)

                if len(AllWorkers) == 0:
                    self.should_stop = True
                    continue

                if len(AllWorkers) != 0:
                    time.sleep(2)

            self.output('interation %s Finished'%(self.niteration - niteration))
            niteration = niteration -1

    def output(self, msg):
        Log(self.outputlog, msg)

    def error(self, msg):
        Log(self.errorlog, msg)

    def report(self, msg):
        Report(self.reportlog, msg)

    def start(self):
        '''
        this function is main entry of workload , user should not override this function. 
        '''
        # setup
        self.setup()

        # load data
        self.load_data()

        # vacuum_analyze
        self.vacuum_analyze()

        # run workload concurrently and loop by iteration
        self.run_workload_query()

        # clean up 
        self.clean_up()
        
        self.output('workload [%s]  finished'%self.name)
