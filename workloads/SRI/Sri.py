import os
import sys
import commands,time
from random import shuffle, randint 
from datetime import datetime, date, timedelta

try:
    from workloads.Workload import *
except ImportError:
    sys.stderr.write('SRI needs workloads/Workload.py\n')
    sys.exit(2)

try:
    from pygresql import pg
except ImportError:
    sys.stderr.write('SRI needs pygresql\n')
    sys.exit(2)

try:
    from lib.PSQL import psql
except ImportError:
    sys.stderr.write('SRI needs psql in lib/PSQL.py\n')
    sys.exit(2)

try:
    import gl
except ImportError:
    sys.stderr.write('SRI needs gl.py in lib/\n')
    sys.exit(2)


class Sri(Workload):

    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, tr_id, user): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, tr_id, user)

    def setup(self):
        pass

    def load_data(self):
        if gl.suffix:
            table_name = 'sri_table_' + self.tbl_suffix
        else:
            table_name = 'sri_table'

        self.output('-- Start loading data')

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
                        self.output(output1)
                    break
                if count == 10:
                    print cmd
                    print '\n'.join(output)
                    sys.exit(2)
        # create table
        if self.distributed_randomly:
            cmd = 'drop table if exists %s;\n' % (table_name) + 'create table %s (tid int, bdate date, aid int, delta int, mtime timestamp) with (%s) distributed randomly' % (table_name, self.sql_suffix)
        else:
            cmd = 'drop table if exists %s;\n' % (table_name) + 'create table %s (tid int, bdate date, aid int, delta int, mtime timestamp) with (%s) distributed by (tid)' % (table_name, self.sql_suffix)
        
        if self.partitions == 0 or self.partitions is None:
            partition_query = ''
        else:
            with open(self.workload_directory + os.sep + 'partition.tpl', 'r') as p:
                partition_query = p.read()
            partition_query = partition_query.replace('table_name', table_name)
            partition_query = partition_query.replace('table_orientation', self.orientation)
            if self.compression_type is None:
                partition_query = partition_query.replace(', compresstype=table_compresstype', '')
            else:
                partition_query = partition_query.replace('table_compresstype', str(self.compression_type))

            if self.compression_level is None:
                partition_query = partition_query.replace(', compresslevel=table_compresslevel', '')
            else:
                partition_query = partition_query.replace('table_compresslevel', str(self.compression_level))

        cmd = cmd + partition_query + ';'

        with open(self.tmp_folder + os.sep + 'sri_loading_temp.sql', 'w') as f:
            f.write(cmd)
        
        self.output(cmd)    
        (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + 'sri_loading_temp.sql', dbname = self.database_name, flag = '-t -A', username = self.user)
        self.output('\n'.join(result))
        
        niteration = 1
        while niteration <= self.num_iteration:
            self.output('-- Start iteration %d' % (niteration))
            con_id = -1
            if self.load_data_flag or self.run_workload_flag:
                cmd = 'insert into %s' % (table_name) + \
                ' (tid, bdate, aid, delta, mtime) values ( %d, \'%d-%02d-%02d\', 1, 1, current_timestamp);' \
                % (niteration, randint(1992,1997), randint(01, 12),randint(01, 28))
                
                # get con_id use this query
                unique_string1 = '%s_%s_' % (self.workload_name, self.user) + table_name
                unique_string2 = '%' + unique_string1 + '%'
                get_con_id_sql = "select '***', '%s', sess_id from pg_stat_activity where current_query like '%s';" % (unique_string1, unique_string2)
                
                with open(self.tmp_folder + os.sep + 'sri_loading_temp.sql', 'w') as f:
                    f.write(cmd)
                    f.write(get_con_id_sql)

                self.output(cmd)    
                beg_time = datetime.now()
                (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + 'sri_loading_temp.sql', dbname = self.database_name, flag = '-t -A', username = self.user)
                end_time = datetime.now()
                self.output(result[0].split('***')[0])

                if ok and str(result).find('ERROR') == -1 and str(result).find('FATAL') == -1 and str(result).find('INSERT 0 1') != -1: 
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
            
            self.output('   Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % ('sri_table_' + self.tbl_suffix , niteration, 1, status, duration))
            self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, %d, 'Loading', '%s', %d, 1, '%s', '%s', '%s', %d, NULL, NULL, NULL, %d);" 
                % (self.tr_id, self.s_id, con_id, table_name, niteration, status, beg_time, end_time, duration, self.adj_s_id))

            self.output('-- Complete iteration %d' % (niteration))
            niteration += 1

        if self.user != 'gpadmin':
            cmd1 = 'REVOKE ALL ON DATABASE %s FROM %s;' % (self.database_name, self.user)
            (ok1, output1) = psql.runcmd(cmd = cmd1)
            self.output(cmd1)
            self.output(output1)

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