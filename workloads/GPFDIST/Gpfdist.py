import os
import sys
import commands, socket, shutil
from datetime import datetime, date, timedelta

try:
    from workloads.Workload import *
except ImportError:
    sys.stderr.write('GPFDIST needs workloads/Workload.py\n')
    sys.exit(2)

try:
    from pygresql import pg
except ImportError:
    sys.stderr.write('GPFDIST needs pygresql\n')
    sys.exit(2)

try:
    from lib.PSQL import psql
except ImportError:
    sys.stderr.write('GPFDIST needs psql in lib/PSQL.py\n')
    sys.exit(2)

try:
    from lib.Config import config
except ImportError:
    sys.stderr.write('GPFDIST needs config in lib/Config.py\n')
    sys.exit(2)

try:
    import gl
except ImportError:
    sys.stderr.write('GPFDIST needs gl.py in lib/\n')
    sys.exit(2)
    

class Gpfdist(Workload):
    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, user): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, user)
        self.fname = self.tmp_folder + os.sep + 'gpfdist.lineitem.tbl'
        self.dss = self.workload_directory + os.sep + 'dists.dss'
        self.host_name = config.getMasterHostName()

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
        if gl.suffix:
            sql = sql.replace('TABLESUFFIX', self.tbl_suffix)
        else:
            sql = sql.replace('_TABLESUFFIX', '')

        if self.sql_suffix != '':
            sql = sql.replace('SQLSUFFIX', self.sql_suffix)
        else:
            sql = sql.replace('WITH (SQLSUFFIX)', self.sql_suffix)
        
        sql = sql.replace('NUMBER', str(num))
        sql = sql.replace('HOSTNAME', self.host_name)
        sql = sql.replace('GPFDIST_PORT', str(self.gpfdist_port))
        return sql

    def load_data(self):
        self.output('-- Start loading data ')

        # get the data dir
        data_directory = self.workload_directory + os.sep + 'data'
        if not os.path.exists(data_directory):
            self.output('ERROR: Cannot find DDL to create tables for Copy: %s does not exists' % (data_directory))
            return
        
        self.output('-- start gpfdist service')
        self.gpfdist_port = self._getOpenPort()

        cmd = "gpssh -h %s -e 'gpfdist -d %s -p %d -l ./gpfdist.log &'" % (self.host_name, self.tmp_folder, self.gpfdist_port)
        (status, output) = commands.getstatusoutput(cmd)
        self.output(cmd)
        self.output(output)
        
        cmd = 'ps -ef | grep gpfdist'
        (status, output) = commands.getstatusoutput(cmd)
        self.output(cmd)
        self.output(output)
        
        self.output('-- generate data file: %s' % (self.fname))
        cmd = "dbgen -b %s -s %d -T L > %s " % (self.dss, self.scale_factor, self.fname)
        (status, output) = commands.getstatusoutput(cmd)
        self.output(cmd)
        self.output(output)
        if status != 0:
            print("generate data file %s error. " % (self.fname))
            sys.exit(2)
        self.output('generate data file successed. ')

        tables = ['lineitem_gpfdist']
        
        niteration = 1
        while niteration <= self.num_iteration:
            self.output('-- Start iteration %d' % (niteration))
            for table_name in tables:
                if self.load_data_flag or self.run_workload_flag:
                    with open(data_directory + os.sep + table_name + '.sql', 'r') as f:
                        cmd = f.read()
                    cmd = self.replace_sql(sql = cmd, table_name = table_name, num = niteration)
                    with open(self.tmp_folder + os.sep + 'gpfdist_loading_temp.sql', 'w') as f:
                        f.write(cmd)

                    self.output(cmd)    
                    beg_time = datetime.now()
                    (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + 'gpfdist_loading_temp.sql', dbname = self.database_name)
                    end_time = datetime.now()
                    self.output('\n'.join(result))

                    if ok and str(result).find('ERROR') == -1 and str(result).find('FATAL') == -1: 
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
    
    def _getOpenPort(self, port = 8050):
        defaultPort = port
        s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
        s.bind( ( "localhost", 0) ) 
        addr, defaultPort = s.getsockname()
        s.close()
        return defaultPort
    
    def clean_up(self):
        self.output('-- gpfdist clean up')
        cmd = "ps -ef|grep gpfdist|grep %s|grep -v grep|awk \'{print $2}\'|xargs kill -9" % (self.gpfdist_port)
        command = "gpssh -h %s -e \"%s\"" % (self.host_name , cmd)
        self.output(command)
        (status, output) = commands.getstatusoutput(command)
        self.output('kill gpfdist: ' + output)

        command = "rm -rf %s" % (self.fname)
        self.output(command)
        (status, output) = commands.getstatusoutput(command)
        if status != 0:
            print('remove %s error. ' % (self.fname))
        else:
            self.output('remove %s successed ' % (self.fname))

    def execute(self):
        self.output('-- Start running workload %s' % (self.workload_name))

        # setup
        self.setup()

        # load data
        self.load_data()

        # clean up 
        self.clean_up()
        
        self.output('-- Complete running workload %s' % (self.workload_name))

