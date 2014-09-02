#!/usr/bin/env python
import os
import sys
from datetime import datetime, date, timedelta

try:
    from pygresql import pg
except ImportError:
    sys.stderr.write('LSP needs pygresql\n')
    sys.exit(2)

try:
    from QueryFile import QueryFile
except ImportError:
    sys.stderr.write('LSP needs QueryFile in lib/QueryFile.py\n')
    sys.exit(2)

try:
    from utils.Log import Log
except ImportError:
    sys.stderr.write('LSP needs Log in lib/utils/Log.py\n')
    sys.exit(2)

try:
    from utils.Report import Report
except ImportError:
    sys.stderr.write('LSP needs Report in lib/utils/Report.py\n')
    sys.exit(2)


class TpchLoader(object):

    def __init__(self, database_name = 'gpadmin', user = 'gpadmin', \
        scale_factor = 1, nsegs = 1, append_only = True, orientation= 'ROW', page_size = 1048576, \
        row_group_size = 8388608, compression_type = None, compression_level = None, partitions = 0, \
        tables = ['nation', 'lineitem', 'orders','region','part','supplier','partsupp', 'customer'], \
        tbl_suffix = '', sql_suffix = '', tpch_load_log = '/tmp/tpch_load.log', \
        output_file = '/tmp/tpch_output', report_file = '/tmp/tpch_report', \
        workload_directory = '', report_sql_path = ''):

        self.database_name = None if database_name is None else database_name.lower()
        self.user = user.lower()
        self.scale_factor = scale_factor
        self.nsegs = nsegs
        self.append_only = True if append_only is None else append_only
        self.orientation = 'row' if orientation is None else orientation.lower()
        self.page_size = page_size
        self.row_group_size = row_group_size
        self.compression_type = None if compression_type is None else compression_type.lower()
        self.compression_level = None if compression_level is None else compression_level
        self.partitions = partitions
        self.tables = tables
        self.tbl_suffix = tbl_suffix
        self.sql_suffix = sql_suffix
        self.tpch_load_log = tpch_load_log
        self.output_file = output_file
        self.report_file = report_file
        self.workload_directory = workload_directory
        self.report_sql_path = report_sql_path

    def output(self, msg):
        Log(self.output_file, msg)

    def report_sql(self, msg):
        Log(self.report_sql_path, msg)

    def report(self, msg):
        Report(self.report_file, msg)

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
        if self.partitions == 0:
            sql = sql.replace('PARTITIONS', '')
        else:
            part_suffix = self.get_partition_suffix(num_partitions = self.partitions, table_name = table_name)
            sql = sql.replace('PARTITIONS', part_suffix)
        return sql

    def load_data(self):
        data_directory = self.workload_directory + os.sep + 'data'
        if not os.path.exists(data_directory):
            self.output('ERROR: Cannot find DDL to create tables for TPC-H: %s does not exists' % (data_directory))
            return

        for table_name in self.tables:
            qf_path = QueryFile(os.path.join(data_directory, table_name + '.sql'))
            beg_time = datetime.now()
            # run all sql in each loading data file
            try:
                for cmd in qf_path:
                    # run current query
                    cmd = self.replace_sql(sql = cmd, table_name = table_name)
                    self.output(cmd)
                    result = self.cnx.query(cmd)
                    self.output(str(result))
            except Exception, e:
                    self.output('ERROR: Failed to load data for table %s: %s' % (table_name, str(e)))
                    self.report('    Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, 'ERROR', 0)) 
                    self.report_sql("INSERT INTO table_name VALUES ('Loading', '%s', 1, 1, 'ERROR', 0);" % (table_name))
                    continue
            end_time = datetime.now()
            duration = end_time - beg_time
            duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds       
            self.output('    Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, 'SUCCESS', duration))
            self.report('    Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, 'SUCCESS', duration))
            self.report_sql("INSERT INTO table_name VALUES ('Loading', '%s', 1, 1, 'SUCCESS', %d);" % (table_name, duration))
            
    def load(self):
        try: 
            self.cnx = pg.connect(dbname = self.database_name)
        except Exception, e:
            cnx = pg.connect(dbname = 'postgres')
            cnx.query('CREATE DATABASE %s;' % (self.database_name))
            cnx.close()
        finally:
            self.cnx = pg.connect(dbname = self.database_name)    
        self.load_data()
        self.cnx.close()
