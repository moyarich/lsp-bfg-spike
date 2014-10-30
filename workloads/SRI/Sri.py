import os
import sys
import commands
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

    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id)

    def setup(self):
        # check if the database exist
        try: 
            cnx = pg.connect(dbname = self.database_name)
        except Exception, e:
            cnx = pg.connect(dbname = 'postgres')
            cnx.query('CREATE DATABASE %s;' % (self.database_name))
        finally:
            cnx.close()

    def load_data(self):
        if gl.suffix:
            table_name = 'sri_table_' + self.tbl_suffix
        else:
            table_name = 'sri_table'

        self.output('-- Start loading data')

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
        (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + 'sri_loading_temp.sql', dbname = self.database_name)
        self.output('\n'.join(result))
        
        niteration = 1
        while niteration <= self.num_iteration:
            self.output('-- Start iteration %d' % (niteration))
            if self.load_data_flag or self.run_workload_flag:
                cmd = 'insert into %s' % (table_name) + \
                ' (tid, bdate, aid, delta, mtime) values ( %d, \'%d-%02d-%02d\', 1, 1, current_timestamp);' \
                % (niteration, randint(1992,1997), randint(01, 12),randint(01, 28))
                
                with open(self.tmp_folder + os.sep + 'sri_loading_temp.sql', 'w') as f:
                    f.write(cmd)

                self.output(cmd)    
                beg_time = datetime.now()
                (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + 'sri_loading_temp.sql', dbname = self.database_name)
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
            
            self.output('   Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % ('sri_table_' + self.tbl_suffix , niteration, 1, status, duration))
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