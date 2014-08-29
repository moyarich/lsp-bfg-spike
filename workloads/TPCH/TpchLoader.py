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
        output_file = '/tmp/tpch_output', error_file = '/tmp/tpch_error', report_file = '/tmp/tpch_report', \
        workload_directory = ''):

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
        self.error_file = error_file
        self.report_file = report_file
        self.workload_directory = workload_directory
        
        # connect to db
        try: 
            self.cnx = pg.connect(dbname = self.database_name)
        except Exception, e:
            cnx = pg.connect(dbname = 'postgres')
            cnx.query('CREATE DATABASE %s;' % (self.database_name))
            cnx.close()
        finally:
            self.cnx = pg.connect(dbname = self.database_name)

    def output(self, msg):
        Log(self.output_file, msg)

    def error(self, msg):
        Log(self.error_file, msg)

    def report(self, msg):
        Report(self.report_file, msg)

    def get_partition_suffix(self, num_partitions = 128, table_name = ''):
        beg_date = date(1992, 01, 01)
        end_date = date(1999, 01, 01)
        duration_days = (end_date - beg_date).days / num_partitions

        if table_name == 'lineitem':
            part = '''
                PARTITION BY RANGE(l_shipdate)
                (
                '''
        elif table_name == 'orders':
            part = '''
                PARTITION BY RANGE(o_orderdate)
                (
                '''
                
        for i in range(1, num_partitions+1):
            beg_cur = beg_date + timedelta(days = (i-1)*duration_days)
            end_cur = beg_date + timedelta(days = i*duration_days)
            if i != num_partitions:
                part += '''
                     PARTITION p1_%s START (\'%s\'::date) END (\'%s\'::date) EVERY (\'%s days\'::interval) WITH (tablename=\'%s_part_1_prt_p1_%s\', %s ),
                        '''%(i, beg_cur, end_cur, duration_days, table_name + self.tbl_suffix, i, self.sql_suffix)
            else:
                part += '''
                     PARTITION p1_%s START (\'%s\'::date) END (\'%s\'::date) EVERY (\'%s days\'::interval) WITH (tablename=\'%s_part_1_prt_p1_%s\', %s )
                        '''%(i, beg_cur, end_cur, duration_days, table_name + self.tbl_suffix, i, self.sql_suffix)
                
        part += '''
                     )
                '''
        return part
        
    def run_sql(self, sql):
        out = self.cnx.query(sql)
        if out == None:
            return ''
        return out

    # table_name without suffix
    def replace_sql(self, sql, table_name):
        sql = sql.replace('TABLESUFFIX', self.tbl_suffix)
        sql = sql.replace('SQLSUFFIX', self.sql_suffix)
        sql = sql.replace('SCALEFACTOR', str(self.scale_factor))
        sql = sql.replace('NUMSEGMENTS', str(self.nsegs))
        if self.partitions == 0:
            sql = sql.replace('PARTITIONS', '')
        else:
            part_suffix = get_partition_suffix(num_partitions = self.partitions, table_name = table_name)
            sql = sql.replace('PARTITIONS', part_suffix)
        return sql

    def load_data(self):
        data_directory = self.workload_directory + os.sep + 'data'
        if not os.path.exists(data_directory):
            self.error('Loading data is error, there is no directory: ' + data_directory)
            return
        
        data_files = [file for file in os.listdir(data_directory) if file.endswith('.sql')]
        for table_name in data_files:
            qf_path = QueryFile(os.path.join(data_directory, table_name))
            table_name = table_name.replace('.sql', '')
            if table_name == 'revenue':
                self.output('-- Start creating revenue view:')
                self.report('-- Start creating revenue view:')
            else:
                self.output('-- Start loading data for %s:' % (table_name))
                self.report('-- Start loading data for %s:' % (table_name))

            beg_time = datetime.now()

            # run all sql in each sql file
            for cmd in qf_path:
                # run current query
                try:
                    cmd = self.replace_sql(sql = cmd, table_name = table_name)
                    result = self.run_sql(cmd)
                    self.output(cmd)
                    self.output(result)
                except Exception, e:
                    self.error('Failed to loading table %s: %s' % (table_name, str(e)))                  

            end_time = datetime.now()
            duration = end_time - beg_time
            duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds
            if table_name == 'revenue':
                self.output('Creating VIEW for %s: %s ms' % ('lineitem_' + self.tbl_suffix, duration))
                self.report('Creating VIEW for %s: %s ms' % ('lineitem_' + self.tbl_suffix, duration))
            else:
                self.output('Data loading for %s: %s ms' % (table_name + self.tbl_suffix, duration))
                self.report('Data loading for %s: %s ms' % (table_name + self.tbl_suffix, duration))
        
        self.output('-- End loading data for %s' % (table_name))
        self.report('-- End loading data for %s' % (table_name))
    
    def vacuum_analyze(self):
        try:
            sql = 'VACUUM ANALYZE;'
            beg_time = datetime.now()
            self.run_sql(sql)
            end_time = datetime.now()
            duration = end_time - beg_time
            self.output('VACUUM ANALYZE: %s ms' % (duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))
            self.report('VACUUM ANALYZE: %s ms' % (duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))
        except Exception, e:
            self.error('VACUUM ANALYZE failure: %s' % (str(e)))
            return False
            
    def load(self):
        self.load_data()
        self.vacuum_analyze()
        self.cnx.close();
