import os
import sys
import time
import datetime
import random
from multiprocessing import Process, Queue, Value , Array

try:
    from pygresql import pg
except ImportError:
    sys.stderr.write('LSP needs pygresql\n')
    sys.exit(2)

try:
    from QueryFile import QueryFile
except ImportError:
    sys.stderr.write('LSP needs QueryFile in lib/QueryFile.py\n')
    sys.exit(2)

try:
    from utils.Log import Log
except ImportError:
    sys.stderr.write('LSP needs Log in lib/utils/Log.py\n')
    sys.exit(2)

try:
    from utils.Report import Report
except ImportError:
    sys.stderr.write('LSP needs Report in lib/utils/Report.py\n')
    sys.exit(2)


LSP_HOME = os.getenv('LSP_HOME')


class Workload(object):
    def __init__(self, workload_specification, workload_directory, report_directory):
        # initialize common propertities for workload
        self.workload_name = workload_specification['workload_name'].strip()
        self.database_name = workload_specification['database_name'].strip()
        self.user = workload_specification['user'].strip()
        self.table_setting = workload_specification['table_setting']
        self.load_data_flag = str(workload_specification['load_data_flag']).strip().upper()
        self.run_workload_flag = str(workload_specification['run_workload_flag']).strip().upper()
        self.run_workload_mode = workload_specification['run_workload_mode'].strip().upper()
        self.num_concurrency = int(str(workload_specification['num_concurrency']).strip())
        self.num_iteration = int(str(workload_specification['num_iteration']).strip())
        self.tbl_suffix = ''
     
        # prepare report directory for workload
        if report_directory != '':
            self.report_directory = os.path.join(report_directory, self.workload_name)
        else:
            print 'Test report directory is not available before preparing report directory for workload %s' % (self.workload_name)
            exit(-1)
        os.system('mkdir %s' % (self.report_directory))

        # check flag for data loading
        if self.load_data_flag == 'TRUE':
            self.load_data_flag = True
        elif self.load_data_flag == 'FALSE':
            self.load_data_flag = False
        else:
            self.error('Invalid value for data loading flag in workload %s: %s. Must be TRUE/FALSE.' % (self.workload_name, self.load_data_flag))
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
            self.error('Invalid value for mode of workload execution in workload %s: %s. Mast be SEQUENTIAL/RANDOM.' % (self.workload_name, self.run_workload_mode))
            exit(-1)

        # set workload source directory
        self.workload_directory = workload_directory

        # set output log, error log, and report
        self.output_file = os.path.join(self.report_directory, 'output.csv')
        self.error_file  = os.path.join(self.report_directory, 'error.csv')
        self.report_file = os.path.join(self.report_directory, 'report.csv')

        # should always run the workload by default
        self.should_stop = False

    def setup(self):
        '''Setup prerequisites for workload'''
        pass

    def load_data(self):
        '''Load data for workload'''
        pass

    def run_queries(self):
        '''
        Run queries in lsp/workloads/$workload_name/queries/*.sql one by one in user-specified order
        1) The queries would be run in one or more times as specified by niteration
        2) The queries would be run in one or more concurrent streams as specified by nconcurrency
        It needs to be overwritten in child class if user want run queries of the workload in customized way
        '''
        queries_directory = self.workload_directory + os.sep + 'queries'
        if not os.path.exists(queries_directory):
            return

        query_files = [file for file in os.listdir(queries_directory) if file.endswith('.sql')]
        if self.run_workload_mode == 'SEQUENTIAL':
            query_files = sorted(query_files)
        else:
            query_files = random.shuffle(query_files)

        try: 
            cnx = pg.connect(dbname = self.database_name)
        except Exception, e:
            self.error('Connect to Database %s is fail!' %(self.database_name))
            self.error( str(e) )
            exit(2)

        # run all sql files in queries directory
        self.output('-- Start running queries for %s:' % (self.workload_name))
        for qf_name in query_files:
            beg_time = datetime.datetime.now()
            qf_path = QueryFile(os.path.join(queries_directory, qf_name))
            # run all queries in each sql file
            for q in qf_path:
                # run current query
                try:
                    q = q.replace('TABLESUFFIX', self.tbl_suffix)
                    cnx.query(q)
                except Exception, e:
                    self.error('Failed to run query %s: %s' % (qf_name, str(e)))
            end_time = datetime.datetime.now()
            duration = end_time - beg_time
            duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds
            self.output('Workload=%s Query=%s: %d ms' % (self.workload_name, qf_name, duration))
            self.report('Workload=%s Query=%s: %d ms' % (self.workload_name, qf_name, duration))
        self.output('-- End running queries for %s:' % (self.workload_name))
 
        cnx.close()

    def run_workload(self):
        if not self.run_workload_flag:
            self.output('Skip Query Running....')
            return

        niteration = 1
        while niteration <= self.num_iteration:
            self.output('Start interation %s ....' % (niteration))
            AllWorkers = []
            nconcurrency = 1
            while nconcurrency <= self.num_concurrency:
                self.output('Process - %s  Start..' % (nconcurrency))
                p = Process(target = self.run_queries)
                AllWorkers.append(p)
                nconcurrency += 1
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

            self.output('interation %s Finished' % (niteration))
            niteration += 1

    def cleanup(self):
        '''Cleanup environment after execution of workload'''
        pass

    def output(self, msg):
        Log(self.output_file, msg)

    def error(self, msg):
        Log(self.error_file, msg)

    def report(self, msg):
        Report(self.report_file, msg)

    def execute(self):
        self.output('-- Start running workload %s' % (self.workload_name))
        self.report('-- Start running workload %s' % (self.workload_name))

        # setup
        self.setup()

        # load data
        self.load_data()

        # run workload concurrently and loop by iteration
        self.run_workload()

        # clean up 
        self.clean_up()
        
        self.output('-- End running workload %s' % (self.workload_name))
        self.report('-- End running workload %s' % (self.workload_name))
