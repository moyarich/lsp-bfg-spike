import os
import sys
import commands
from datetime import datetime, date, timedelta

try:
    from workloads.Workload import *
except ImportError:
    sys.stderr.write('COPY needs workloads/Workload.py\n')
    sys.exit(2)

try:
    from pygresql import pg
except ImportError:
    sys.stderr.write('COPY needs pygresql\n')
    sys.exit(2)

try:
    from lib.PSQL import psql
except ImportError:
    sys.stderr.write('COPY needs psql in lib/PSQL.py\n')
    sys.exit(2)


class Copy(Workload):

    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, validation): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, validation)
        self.fname = self.workload_directory + os.sep + 'copy.lineitem.tbl'
        self.dss = self.workload_directory + os.sep + 'dists.dss'

    def setup(self):
        # check if the database exist
        try: 
            cnx = pg.connect(dbname = self.database_name)
        except Exception, e:
            cnx = pg.connect(dbname = 'postgres')
            cnx.query('CREATE DATABASE %s;' % (self.database_name))
        finally:
            cnx.close()

    def replace_sql(self, sql, table_name, num):
        sql = sql.replace('TABLESUFFIX', self.tbl_suffix)
        sql = sql.replace('SQLSUFFIX', self.sql_suffix)
        sql = sql.replace('NUMBER', str(num))
        sql = sql.replace('FNAME', self.fname)
        return sql

    def load_data(self):
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
            for table_name in tables:
                if self.load_data_flag or self.run_workload_flag:
                    with open(data_directory + os.sep + table_name + '.sql', 'r') as f:
                        cmd = f.read()
                    cmd = self.replace_sql(sql = cmd, table_name = table_name, num = niteration)
                    with open('loading_data_tmp.sql', 'w') as f:
                        f.write(cmd)

                    self.output(cmd)    
                    beg_time = datetime.now()
                    (ok, result) = psql.runfile(ifile = 'loading_data_tmp.sql', dbname = self.database_name)
                    end_time = datetime.now()
                    self.output('RESULT: ' + str(result))

                    if ok: 
                        status = 'SUCCESS'    
                    else:
                        status = 'ERROR'
                
                else:
                    status = 'SKIP'
                    beg_time = datetime.now()
                    end_time = beg_time
                    
                duration = end_time - beg_time
                duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds /1000
                beg_time = str(beg_time).split('.')[0]
                end_time = str(end_time).split('.')[0]
                
                self.output('   Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, niteration, 1, status, duration))
                self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, 'Loading', '%s', %d, 1, '%s', '%s', '%s', %d, NULL, NULL, NULL);" 
                    % (self.tr_id, self.s_id, table_name, niteration, status, beg_time, end_time, duration))

            self.output('-- Complete iteration %d' % (niteration))
            niteration += 1

        self.output('-- Complete loading data')      
    
    def execute(self):
        self.output('-- Start running workload %s' % (self.workload_name))

        # setup
        self.setup()

        # load data
        self.load_data()

        # clean up 
        self.clean_up()
        
        self.output('-- Complete running workload %s' % (self.workload_name))