import os
import sys
import commands
from datetime import datetime, date, timedelta

try:
    from workloads.Workload import *
except ImportError:
    sys.stderr.write('TPCH needs workloads/Workload.py\n')
    sys.exit(2)

try:
    from pygresql import pg
except ImportError:
    sys.stderr.write('TPCH needs pygresql\n')
    sys.exit(2)

try:
    from lib.PSQL import psql
except ImportError:
    sys.stderr.write('TPCH needs psql in lib/PSQL.py\n')
    sys.exit(2)

try:
    from lib.QueryFile import QueryFile
except ImportError:
    sys.stderr.write('TPCH needs QueryFile in lib/QueryFile.py\n')
    sys.exit(2)


class Copy(Workload):

    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id)
        self.fname = self.workload_directory + os.sep + 'copy.lineitem.tbl'
        self.dss = self.workload_directory + os.sep + 'dists.dss'

    def setup(self):
        pass

    def replace_sql(self, sql, table_name, num):
        sql = sql.replace('TABLESUFFIX', self.tbl_suffix)
        sql = sql.replace('SQLSUFFIX', self.sql_suffix)
        sql = sql.replace('NUMBER', str(num))
        sql = sql.replace('FNAME', self.fname)
        return sql

    def load_data(self):
        # check if the database exist
        try: 
            cnx = pg.connect(dbname = self.database_name)
        except Exception, e:
            cnx = pg.connect(dbname = 'postgres')
            cnx.query('CREATE DATABASE %s;' % (self.database_name))
        finally:
            cnx.close()

        self.output('-- Start loading data')
        self.output('-- generate data file: %s' % (self.fname))
        cmd = "dbgen -b %s -s 1 -T L > %s " % (self.dss, self.fname)
        (status, output) = commands.getstatusoutput(cmd)
        self.output(cmd)
        self.output(output)
        if status != 0:
            print("generate data file %s error. " % (self.fname))
            sys.exit(2)
        self.output('generate data file successed. ')

        # get the data dir
        data_directory = self.workload_directory + os.sep + 'data'
        if not os.path.exists(data_directory):
            self.output('ERROR: Cannot find DDL to create tables for TPC-H: %s does not exists' % (data_directory))
            return
        
        tables = ['lineitem_copy']
        niteration = 1
        while niteration <= self.num_iteration:
            self.output('-- Start iteration %d' % (niteration))
            if not self.load_data_flag:
                beg_time = str(datetime.now()).split('.')[0]
                for table_name in tables:
                    self.output('   Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, niteration, 1, 'SKIP', 0))
                    self.report('   Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, niteration, 1, 'SKIP', 0)) 
                    self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, 'Loading', '%s', %d, 1, 'SKIP', '%s', '%s', 0, NULL, NULL, NULL);" 
                        % (self.tr_id, self.s_id, niteration, table_name, beg_time, beg_time))
            else:
                for table_name in tables:
                    load_success_flag = True
                    qf_path = QueryFile(os.path.join(data_directory, table_name + '.sql'))
                    beg_time = datetime.now()
                    # run all sql in each loading data file
                    for cmd in qf_path:
                        cmd = self.replace_sql(sql = cmd, table_name = table_name, num = niteration)
                        self.output(cmd)
                        (ok, result) = psql.runcmd(cmd = cmd, dbname = self.database_name, flag = '')
                        self.output('RESULT: ' + str(result))
                        if not ok:
                            load_success_flag = False

                    end_time = datetime.now()
                    duration = end_time - beg_time
                    duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds /1000
          
                    if load_success_flag:    
                        self.output('   Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, niteration, 1, 'SUCCESS', duration))
                        self.report('   Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, niteration, 1, 'SUCCESS', duration))
                        self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, 'Loading', '%s', %d, 1, 'SUCCESS', '%s', '%s', %d, NULL, NULL, NULL);" 
                            % (self.tr_id, self.s_id, table_name, niteration, str(beg_time).split('.')[0], str(end_time).split('.')[0], duration))
                    else:
                        self.output('ERROR: Failed to load data for table %s' % (table_name))
                        self.report('   Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, niteration, 1, 'ERROR', 0)) 
                        self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, 'Loading', '%s', %d, 1, 'ERROR', '%s', '%s', %d, NULL, NULL, NULL);" 
                            % (self.tr_id, self.s_id, table_name, niteration, str(beg_time).split('.')[0], str(end_time).split('.')[0], duration))
            self.output('-- Complete iteration %d' % (niteration))
            niteration += 1

        self.output('-- Complete loading data')      
    
    def execute(self):
        self.output('-- Start running workload %s' % (self.workload_name))
        self.report('-- Start running workload %s' % (self.workload_name))

        # setup
        self.setup()

        # load data
        self.load_data()

        # clean up 
        self.clean_up()
        
        self.output('-- Complete running workload %s' % (self.workload_name))
        self.report('-- Complete running workload %s' % (self.workload_name))