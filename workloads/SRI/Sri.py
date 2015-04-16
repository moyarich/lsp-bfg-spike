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
        if gl.suffix:
            self.table_name = 'sri_table_' + self.tbl_suffix
        else:
            self.table_name = 'sri_table'
    def setup(self):
        pass

    def load_data(self):
        if self.load_data_flag == False:
            return
        self.output('-- Start loading data')
        super(Sri,self).load_data()

        # create table and sequence
        if self.distributed_randomly:
            cmd = 'drop sequence sri_seg;create sequence sri_seq;drop table if exists %s;\n' % (self.table_name) + 'create table %s (tid bigint, bdate date, aid int, delta int, mtime timestamp) with (%s) distributed randomly' % (self.table_name, self.sql_suffix)
        else:
            cmd = 'drop sequence sri_seg;create sequence sri_seq;drop table if exists %s;\n' % (self.table_name) + 'create table %s (tid bigint, bdate date, aid int, delta int, mtime timestamp) with (%s) distributed by (tid)' % (self.table_name, self.sql_suffix)
        
        if self.partitions == 0 or self.partitions is None:
            partition_query = ''
        else:
            with open(self.workload_directory + os.sep + 'partition.tpl', 'r') as p:
                partition_query = p.read()
            partition_query = partition_query.replace('table_name', self.table_name)
            partition_query = partition_query.replace('table_orientation', self.orientation)
            if self.compression_type is None:
                partition_query = partition_query.replace(', compresstype=table_compresstype', '')
            else:
                partition_query = partition_query.replace('table_compresstype', str(self.compression_type))
            if self.compression_level is None or self.compression_level < 0:
                partition_query = partition_query.replace(', compresslevel=table_compresslevel', '')
            else:
                partition_query = partition_query.replace('table_compresslevel', str(self.compression_level))

        cmd = cmd + partition_query + ';'

        with open(self.tmp_folder + os.sep + 'sri_create.sql', 'w') as f:
            f.write(cmd)
            if self.user != 'gpadmin':
                f.write('GRANT ALL ON TABLE %s TO %s;' % (self.table_name, self.user))
                f.write('GRANT ALL ON SEQUENCE %s TO %s;' % ( self.user))
        
        self.output(cmd)    
        (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + 'sri_create.sql', dbname = self.database_name, flag = '-t -A')
        self.output('\n'.join(result))
        
        self.output('-- Complete loading data')

    def grand_revoke_privileges(self, filename = ''):
        pass

    def vacuum_analyze(self):
        pass

    def run_one_query(self, iteration, stream, qf_name, query):
        newquery = "BEGIN;\n"
        for i in range(0,self.num_insert_pertran):
            cmd = 'INSERT INTO %s' % (self.table_name) + \
                ' (tid, bdate, aid, delta, mtime) VALUES (nextval(\'sri_seq\'), \'%d-%02d-%02d\', 1, 1, current_timestamp);\n' \
                % (randint(1992,1997), randint(01, 12),randint(01, 28))              
            newquery = newquery + cmd
        newquery = newquery + "COMMIT;\n"
        newquery = newquery + "SELECT COUNT(*) FROM %s;\n" %(self.table_name)
        super(Sri,self).run_one_query(iteration, stream, qf_name, newquery)
