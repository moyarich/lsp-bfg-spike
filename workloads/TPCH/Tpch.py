import os
import sys
from datetime import datetime, date, timedelta

try:
    from workloads.Workload import *
except ImportError:
    sys.stderr.write('TPCH needs workloads/Workload.py\n')
    sys.exit(2)

try:
    from lib.PSQL import psql
except ImportError:
    sys.stderr.write('TPCH needs psql in lib/PSQL.py\n')
    sys.exit(2)

try:
    import gl
except ImportError:
    sys.stderr.write('TPCH needs gl.py in lsp_home\n')
    sys.exit(2)

class Tpch(Workload):

    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, user): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, user)

    def get_partition_suffix(self, num_partitions = 128, table_name = ''):
        beg_date = date(1992, 01, 01)
        end_date = date(1998, 12, 31)
        duration_days = int(round(float((end_date - beg_date).days) / float(num_partitions)))

        part = ''

        if table_name == 'lineitem':
            part = '''PARTITION BY RANGE(l_shipdate)\n    (\n'''
        elif table_name == 'orders':
            part = '''PARTITION BY RANGE(o_orderdate)\n    (\n'''

        if gl.suffix:
            table_name = table_name + '_' + self.tbl_suffix
                
        for i in range(1, num_partitions+1):
            beg_cur = beg_date + timedelta(days = (i-1)*duration_days)
            end_cur = beg_date + timedelta(days = i*duration_days)

            part += '''        PARTITION p1_%s START (\'%s\'::date) END (\'%s\'::date) EVERY (\'%s days\'::interval) WITH (tablename=\'%s_part_1_prt_p1_%s\', %s )''' % (i, beg_cur, end_cur, duration_days, table_name, i, self.sql_suffix)
            
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

        sql = sql.replace('SCALEFACTOR', str(self.scale_factor))
        sql = sql.replace('NUMSEGMENTS', str(self.nsegs))

        if self.distributed_randomly and table_name != 'revenue':
            import re
            old_string = re.search(r'DISTRIBUTED BY\(\S+\)', sql).group()
            sql = sql.replace(old_string, 'DISTRIBUTED RANDOMLY')

        if self.partitions == 0 or self.partitions is None:
            sql = sql.replace('PARTITIONS', '')
        else:
            part_suffix = self.get_partition_suffix(num_partitions = self.partitions, table_name = table_name)
            sql = sql.replace('PARTITIONS', part_suffix)
        
        return sql


    def load_data(self):
        self.output('-- Start loading data')

        # get the data dir
        data_directory = self.workload_directory + os.sep + 'data'
        if not os.path.exists(data_directory):
            self.output('ERROR: Cannot find DDL to create tables for TPC-H: %s does not exists' % (data_directory))
            sys.exit(2)

        if self.load_data_flag:
            cmd = 'drop database if exists %s;' % (self.database_name)
            (ok, output) = psql.runcmd(cmd = cmd, username = 'gpadmin')
            if not ok:
                print cmd
                print '\n'.join(output)
                sys.exit(2)
            self.output(cmd)
            self.output('\n'.join(output))

            cmd = 'create database %s;' % (self.database_name)
            (ok, output) = psql.runcmd(cmd = cmd, username = 'gpadmin')
            if not ok:
                print cmd
                print '\n'.join(output)
                sys.exit(2)
            self.output(cmd)
            self.output('\n'.join(output))

        tables = ['nation', 'region', 'part', 'supplier', 'partsupp', 'customer', 'orders','lineitem' ,'revenue']
        for table_name in tables:
            con_id = -1
            if self.continue_flag:
                if self.load_data_flag:
                    with open(data_directory + os.sep + table_name + '.sql', 'r') as f:
                        cmd = f.read()
                    cmd = self.replace_sql(sql = cmd, table_name = table_name)
                    
                    # get con_id use this query
                    unique_string1 = '%s_%s_' % (self.workload_name, self.user) + table_name
                    unique_string2 = '%' + unique_string1 + '%'
                    get_con_id_sql = "select '***', '%s', sess_id from pg_stat_activity where current_query like '%s';" % (unique_string1, unique_string2)

                    with open(self.tmp_folder + os.sep + table_name + '.sql', 'w') as f:
                        f.write(cmd)
                        f.write(get_con_id_sql)

                    self.output(cmd)
                    beg_time = datetime.now()
                    (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + table_name + '.sql', dbname = self.database_name, flag = '-t -A') #, username = self.user)
                    end_time = datetime.now()
                    self.output(result[0].split('***')[0])
                    #self.output('\n'.join(result))
                    
                    if ok and str(result).find('ERROR:') == -1 and str(result).find('FATAL:') == -1 and (str(result).find('INSERT 0') != -1 or str(result).find('CREATE VIEW') != -1):
                        status = 'SUCCESS'
                        con_id = int(result[0].split('***')[1].split('|')[2].strip())
                    else:
                        status = 'ERROR'
                        self.continue_flag = False
                else:
                    status = 'SKIP'
                    beg_time = datetime.now()
                    end_time = beg_time
            else:
                status = 'ERROR'
                beg_time = datetime.now()
                end_time = beg_time
                
            duration = end_time - beg_time
            duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds /1000
            beg_time = str(beg_time)
            end_time = str(end_time)        
            self.output('   Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, status, duration))
            self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, %d, 'Loading', '%s', 1, 1, '%s', '%s', '%s', %d, NULL, NULL, NULL, %d);" 
                % (self.tr_id, self.s_id, con_id, table_name, status, beg_time, end_time, duration, self.adj_s_id))
               
        self.output('-- Complete loading data')

