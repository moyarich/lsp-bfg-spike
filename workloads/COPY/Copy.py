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
        

    def replace_sql(self, sql, table_name):
        if gl.suffix:
            sql = sql.replace('TABLESUFFIX', self.tbl_suffix)
        else:
            sql = sql.replace('_TABLESUFFIX', '')

        if self.sql_suffix != '':
            sql = sql.replace('SQLSUFFIX', self.sql_suffix)
        else:
            sql = sql.replace('WITH (SQLSUFFIX)', self.sql_suffix)
        sql = sql.replace('FNAME', self.fname)

        if self.distributed_randomly:
            import re
            old_string = re.search(r'DISTRIBUTED BY\(\S+\)', sql).group()
            sql = sql.replace(old_string, 'DISTRIBUTED RANDOMLY')

        if self.partitions == 0 or self.partitions is None:
            sql = sql.replace('PARTITIONS', '')
        else:
            part_suffix = self.get_partition_suffix(self.partitions, table_name)
            sql = sql.replace('PARTITIONS', part_suffix)

        return sql

    def load_data(self):
        self.output('-- generate data file: %s' % (self.fname))
        cmd = "dbgen -b %s -s 1 -T L > %s " % (self.dss, self.fname)
        (status, output) = commands.getstatusoutput(cmd)
        self.output(cmd)
        self.output(output)
        if status != 0:
            print("generate data file %s error. " % (self.fname))
            sys.exit(2)
        self.output('generate data file successed. ')
        
        if self.load_data_flag == False:
            return
        self.output('-- Start loading data')
        super(Copy,self).load_data()

        # get the data dir
        data_directory = self.workload_directory + os.sep + 'data'
        if not os.path.exists(data_directory):
            self.output('ERROR: Cannot find DDL to create tables for TPC-H: %s does not exists' % (data_directory))
            return
        
        table_name = 'lineitem_copy'
        with open(data_directory + os.sep + table_name + '.sql', 'r') as f:
               cmd = f.read()
               cmd = self.replace_sql(sql = cmd, table_name = table_name)

        with open(self.tmp_folder + os.sep + 'copy_create.sql', 'w') as f:
            f.write(cmd)
            if self.user != 'gpadmin':
                f.write('GRANT ALL ON TABLE %s TO %s;' % (self.table_name, self.user))
        self.output(cmd)
        (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + 'copy_create.sql', dbname = self.database_name, flag = '-t -A')
        self.output('\n'.join(result))
        self.output('-- Complete loading data')      
    
    def clean_up(self):
        command = "rm -rf %s" % (self.fname)
        self.output(command)

    def vacuum_analyze(self):
        pass

    def grand_revoke_privileges(self, filename = ''):
        pass

    def run_one_query(self, iteration, stream, qf_name, query):
        query = query.replace('FNAME', self.fname)           
        super(Copy,self).run_one_query(iteration, stream, qf_name, query)
