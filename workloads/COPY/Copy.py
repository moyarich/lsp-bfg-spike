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

try:
    import gl
except ImportError:
    sys.stderr.write('COPY needs gl.py in lib/\n')
    sys.exit(2)


class Copy(Workload):

    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, tr_id, user): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, tr_id, user)
        self.fname = self.tmp_folder + os.sep + 'copy.lineitem.tbl'
        self.dss = self.workload_directory + os.sep + 'dists.dss'

    def setup(self):
        pass

    def get_partition_suffix(self, num_partitions = 128, table_name = ''):
        beg_date = date(1992, 01, 01)
        end_date = date(1998, 12, 31)
        duration_days = int(round(float((end_date - beg_date).days) / float(num_partitions)))

        part = '''PARTITION BY RANGE(l_shipdate)\n    (\n'''
                
        for i in range(1, num_partitions+1):
            beg_cur = beg_date + timedelta(days = (i-1)*duration_days)
            end_cur = beg_date + timedelta(days = i*duration_days)

            part += '''        PARTITION p1_%s START (\'%s\'::date) END (\'%s\'::date) EVERY (\'%s days\'::interval) WITH (tablename=\'%s_part_1_prt_p1_%s\', %s )''' % (i, beg_cur, end_cur, duration_days, table_name + '_' + self.tbl_suffix, i, self.sql_suffix)
            
            if i != num_partitions:
                part += ''',\n'''
            else:
                part += '''\n'''

        part += '''    )'''
                
        return part 
        

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
        sql = sql.replace('FNAME', self.fname)

        if self.distributed_randomly:
            import re
            old_string = re.search(r'DISTRIBUTED BY\(\S+\)', sql).group()
            sql = sql.replace(old_string, 'DISTRIBUTED RANDOMLY')

        if self.partitions == 0 or self.partitions is None:
            sql = sql.replace('PARTITIONS', '')
        else:
            part_suffix = self.get_partition_suffix(num_partitions = self.partitions, table_name = table_name + '_' + str(num))
            sql = sql.replace('PARTITIONS', part_suffix)

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

        if self.load_data_flag:
            cmd = 'drop database if exists %s;' % (self.database_name)
            (ok, output) = psql.runcmd(cmd = cmd)
            if not ok:
                print cmd
                print '\n'.join(output)
                sys.exit(2)
            self.output(cmd)
            self.output('\n'.join(output))

            count = 0
            while(True):
                cmd = 'create database %s;' % (self.database_name)
                (ok, output) = psql.runcmd(cmd = cmd)
                if not ok:
                    count = count + 1
                    time.sleep(1)
                else:
                    self.output(cmd)
                    self.output('\n'.join(output))
                    if self.user != 'gpadmin':
                        cmd1 = 'GRANT ALL ON DATABASE %s TO %s;' % (self.database_name, self.user)
                        (ok1, output1) = psql.runcmd(cmd = cmd1)
                        self.output(cmd1)
                        self.output('\n'.join(output1))
                    break
                if count == 10:
                    print cmd
                    print '\n'.join(output)
                    sys.exit(2)
        
        tables = ['lineitem_copy']
        niteration = 1
        while niteration <= self.num_iteration:
            self.output('-- Start iteration %d' % (niteration))
            for table_name in tables:
                con_id = -1
                if self.load_data_flag or self.run_workload_flag:
                    with open(data_directory + os.sep + table_name + '.sql', 'r') as f:
                        cmd = f.read()
                    cmd = self.replace_sql(sql = cmd, table_name = table_name, num = niteration)

                    # get con_id use this query
                    unique_string1 = '%s_%s_' % (self.workload_name, self.user) + table_name
                    unique_string2 = '%' + unique_string1 + '%'
                    get_con_id_sql = "select '***', '%s', sess_id from pg_stat_activity where current_query like '%s';" % (unique_string1, unique_string2)
                    
                    with open(self.tmp_folder + os.sep + 'copy_loading_temp.sql', 'w') as f:
                        f.write(cmd)
                        f.write(get_con_id_sql)

                    self.output(cmd)    
                    beg_time = datetime.now()
                    (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + 'copy_loading_temp.sql', dbname = self.database_name, flag = '-t -A') #, username = self.user)
                    end_time = datetime.now()
                    self.output(result[0].split('***')[0])

                    if ok and str(result).find('ERROR') == -1 and str(result).find('FATAL') == -1 and str(result).find('COPY ') != -1: 
                        status = 'SUCCESS'
                        con_id = int(result[0].split('***')[1].split('|')[2].strip()) 
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
                self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, %d, 'Loading', '%s', %d, 1, '%s', '%s', '%s', %d, NULL, NULL, NULL, %d);" 
                    % (self.tr_id, self.s_id, con_id, table_name, niteration, status, beg_time, end_time, duration, self.adj_s_id))

            self.output('-- Complete iteration %d' % (niteration))
            niteration += 1

        if self.user != 'gpadmin':
            cmd1 = 'REVOKE ALL ON DATABASE %s FROM %s;' % (self.database_name, self.user)
            (ok1, output1) = psql.runcmd(cmd = cmd1)
            self.output(cmd1)
            self.output('\n'.join(output1))

        self.output('-- Complete loading data')      
    
    def clean_up(self):
        command = "rm -rf %s" % (self.fname)
        self.output(command)
        #(status, output) = commands.getstatusoutput(command)
        #if status != 0:
        #    print('remove %s error. ' % (self.fname))
        #else:
        #    self.output('remove %s successed ' % (self.fname))


    def execute(self):
        self.output('-- Start running workload %s' % (self.workload_name))

        # setup
        self.setup()

        # load data
        self.load_data()

        # clean up 
        self.clean_up()
        
        self.output('-- Complete running workload %s' % (self.workload_name))