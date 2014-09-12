import os
import sys
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

try:
    from utils.Log import Log
except ImportError:
    sys.stderr.write('TPCH needs Log in lib/utils/Log.py\n')
    sys.exit(2)

try:
    from utils.Report import Report
except ImportError:
    sys.stderr.write('LSP needs Report in lib/utils/Report.py\n')
    sys.exit(2)

class Tpch(Workload):

    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id)

    def setup(self):
        pass

    def get_partition_suffix(self, num_partitions = 128, table_name = ''):
        beg_date = date(1992, 01, 01)
        end_date = date(1998, 12, 31)
        duration_days = int(round(float((end_date - beg_date).days) / float(num_partitions)))

        part = ''

        if table_name == 'lineitem':
            part = '''PARTITION BY RANGE(l_shipdate)\n    (\n'''
        elif table_name == 'orders':
            part = '''PARTITION BY RANGE(o_orderdate)\n    (\n'''
                
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
        sql = sql.replace('TABLESUFFIX', self.tbl_suffix)
        sql = sql.replace('SQLSUFFIX', self.sql_suffix)
        sql = sql.replace('SCALEFACTOR', str(self.scale_factor))
        sql = sql.replace('NUMSEGMENTS', str(self.nsegs))
        if self.partitions == 0 or self.partitions is None:
            sql = sql.replace('PARTITIONS', '')
        else:
            part_suffix = self.get_partition_suffix(num_partitions = self.partitions, table_name = table_name)
            sql = sql.replace('PARTITIONS', part_suffix)
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

        tables = ['nation', 'region', 'part', 'supplier', 'partsupp', 'customer', 'orders','lineitem' ,'revenue']
        if not self.load_data_flag:
            beg_time = str(datetime.now()).split('.')[0]
            for table_name in tables:
                self.output('Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, 'SKIP', 0))
                self.report('  Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, 'SKIP', 0)) 
                self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, 'Loading', '%s', 1, 1, 'SKIP', '%s', '%s', 0, NULL, NULL, NULL);" 
                    % (self.tr_id, self.s_id, table_name, beg_time, beg_time))
        else:
            # get the data dir
            data_directory = self.workload_directory + os.sep + 'data'
            if not os.path.exists(data_directory):
                self.output('ERROR: Cannot find DDL to create tables for TPC-H: %s does not exists' % (data_directory))
                return

            for table_name in tables:
                load_success_flag = True
                qf_path = QueryFile(os.path.join(data_directory, table_name + '.sql'))
                beg_time = datetime.now()
                # run all sql in each loading data file
                for cmd in qf_path:
                    cmd = self.replace_sql(sql = cmd, table_name = table_name)
                    self.output(cmd)
                    (ok, result) = psql.runcmd(cmd = cmd, dbname = self.database_name)
                    self.output('RESULT: ' + str(result))
                    if not ok:
                        load_success_flag = False

                end_time = datetime.now()
                duration = end_time - beg_time
                duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds /1000
      
                if load_success_flag:    
                    self.output('Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, 'SUCCESS', duration))
                    self.report('  Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, 'SUCCESS', duration))
                    self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, 'Loading', '%s', 1, 1, 'SUCCESS', '%s', '%s', %d, NULL, NULL, NULL);" 
                        % (self.tr_id, self.s_id, table_name, str(beg_time).split('.')[0], str(end_time).split('.')[0], duration))
                else:
                    self.output('ERROR: Failed to load data for table %s' % (table_name))
                    self.report('    Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, 'ERROR', 0)) 
                    self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, 'Loading', '%s', 1, 1, 'ERROR', '%s', '%s', %d, NULL, NULL, NULL);" 
                        % (self.tr_id, self.s_id, table_name, str(beg_time).split('.')[0], str(end_time).split('.')[0], duration))
        self.output('-- Complete loading data')
       
        # vacuum_analyze
        self.vacuum_analyze()

    def vacuum_analyze(self):
        self.output('-- Start vacuum analyze')     
        
        sql = 'VACUUM ANALYZE;'
        self.output(sql)
        beg_time = datetime.now()
        (ok, result) = psql.runcmd(cmd = sql, dbname = self.database_name)
        end_time = datetime.now()
        self.output('RESULT: ' + str(result))
        duration = end_time - beg_time
        duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds/1000
        beg_time = str(beg_time).split('.')[0]
        end_time = str(end_time).split('.')[0]
    
        if ok:   
            self.output('VACUUM ANALYZE   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (1, 1, 'SUCCESS', duration))
            self.report('  VACUUM ANALYZE   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (1, 1, 'SUCCESS', duration))
            self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, 'Vacuum_analyze', 'Vacuum_analyze', 1, 1, 'SUCCESS', '%s', '%s', %d, NULL, NULL, NULL);" 
                % (self.tr_id, self.s_id, beg_time, end_time, duration))
        else:
            self.output('ERROR: VACUUM ANALYZE failure')
            self.report('  VACUUM ANALYZE   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (1, 1, 'ERROR', 0))
            self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, 'Vacuum_analyze', 'Vacuum_analyze', 1, 1, 'ERROR', '%s', '%s', %d, NULL, NULL, NULL);" 
                % (self.tr_id, self.s_id, beg_time, end_time, duration))
        
        self.output('-- Complete vacuum analyze')

    def clean_up(self):
        pass
