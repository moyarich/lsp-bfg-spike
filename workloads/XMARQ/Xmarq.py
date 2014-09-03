import os
import sys
import time
from datetime import datetime

LSP_HOME = os.getenv('LSP_HOME')
lib = os.path.join(LSP_HOME, 'lib')
if lib not in sys.path:
    sys.path.append(lib)

try:
    from workloads.Workload import *
except ImportError:
    sys.stderr.write('LSP needs workloads/Workload.py\n')
    sys.exit(2)

try:
    from XmarqLoader import XmarqLoader
except ImportError:
    sys.stderr.write('LSP needs XmarqLoader in workloads/XMARQ/XmarqLoader.py\n')
    sys.exit(2)

try:
    from lib.PSQL import psql
except ImportError:
    sys.stderr.write('LSP needs psql in lib/PSQL.py in Xmarq.py\n')

try:
    from lib.Config import config
except ImportError:
    sys.stderr.write('LSP needs config in lib/Config.py\n')
    sys.exit(2)


class Xmarq(Workload):
    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file)

        # init Xmarq specific configuration such as Xmarq table_settings
        ts = workload_specification['table_setting']

        # Calculate scale factor for TPC-H workload
        self.data_volume_type = ts['data_volume_type'].upper()
        self.data_volume_size = ts['data_volume_size']
        
        # Need to make it univerally applicable instead of hard-code number of segments
        self.nsegs =  config.getNPrimarySegments()

        self.scale_factor = 1
        if self.data_volume_type == 'TOTAL':
            self.scale_factor = self.data_volume_size
        elif self.data_volume_type == 'PER_NODE':
            nnodes = len(config.getSegHostNames())
            self.scale_factor = self.data_volume_size * nnodes
        elif self.data_volume_type == 'PER_SEGMENT':
            self.scale_factor = self.data_volume_size * self.nsegs
        else:
            self.output('Error in calculating data volumn for workloads %s: data_volume_type=%s, data_volume_size=%s' % (self.workload_name, self.data_volume_type, self.data_volume_size))
            exit(-1)

        # Parse table setting
        ts_keys = ts.keys()

        self.append_only = None
        if 'append_only' in ts_keys:
            self.append_only = ts['append_only']
            assert self.append_only in [True, False]

        self.orientation = 'ROW'
        if 'orientation' in ts_keys:
            self.orientation = ts['orientation'].upper()
            assert self.orientation in ['PARQUET', 'ROW', 'COLUMN']

        self.row_group_size = None
        if 'row_group_size' in ts_keys:
            self.row_group_size = int(ts['row_group_size'])
            
        self.page_size = None
        if 'page_size' in ts_keys:
            self.page_size = int(ts['page_size'])
       
        self.compression_type = None
        if 'compression_type' in ts_keys:
            self.compression_type = ts['compression_type'].upper()
        
        self.compression_level = None
        if 'compression_level' in ts_keys:
                self.compression_level = int(ts['compression_level'])

        self.partitions = 0
        if 'partitions' in ts_keys:
            self.partitions = int(ts['partitions'])

        # prepare name with suffix for table and corresponding sql statement to create it
        tbl_suffix = ''
        sql_suffix = ''

        if self.append_only in [None, True]:
            tbl_suffix = tbl_suffix + 'ao'
            sql_suffix = sql_suffix + 'appendonly = true'
        else:
            tbl_suffix = tbl_suffix + 'heap'
            sql_suffix = sql_suffix + 'appendonly = false'

        tbl_suffix = tbl_suffix + '_' + self.orientation
        sql_suffix = sql_suffix + ', '+ 'orientation = ' + self.orientation

        if self.orientation == 'PARQUET':
            sql_suffix = sql_suffix + ', ' + 'pagesize = %s, rowgroupsize = %s' % (self.page_size, self.row_group_size)

        if self.compression_type is not None:
            if self.compression_level is not None:
                tbl_suffix = tbl_suffix + '_' + self.compression_type + str(self.compression_level)
                sql_suffix = sql_suffix + ', ' + 'compresstype = ' + self.compression_type 
                sql_suffix = sql_suffix + ', ' + 'compresslevel = ' + str(self.compression_level)
            else:
                tbl_suffix = tbl_suffix + '_' + self.compression_type
                sql_suffix = sql_suffix + ', ' + 'compresstype = ' + self.compression_type
        else:
            tbl_suffix = tbl_suffix + '_nocomp'

        if self.partitions > 0:
            tbl_suffix += '_part'
        else:
            tbl_suffix += '_nopart'

        self.tbl_suffix = tbl_suffix.lower()
        self.sql_suffix = sql_suffix
           
    def load_data(self):
        self.vacuum_analyze()
        tables = ['nation', 'region', 'part', 'supplier', 'partsupp', 'customer', 'orders','lineitem' ,'revenue']
        if not self.load_data_flag:
            for table_name in tables:
                self.output('    Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, 'SKIP', 0))
                self.report('    Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, 'SKIP', 0)) 
                self.report_sql("INSERT INTO table_name VALUES ('Loading', '%s', 1, 1, 'SKIP', 0);" % (table_name))
            return

        # load all 8 tables and 1 view
        loader = XmarqLoader(database_name = self.database_name, user = self.user, \
                            scale_factor = self.scale_factor, nsegs = self.nsegs, append_only = self.append_only, orientation = self.orientation, \
                            page_size = self.page_size, row_group_size = self.row_group_size, \
                            compression_type = self.compression_type, compression_level = self.compression_level, \
                            partitions = self.partitions, tables = tables, tbl_suffix = self.tbl_suffix, sql_suffix = self.sql_suffix, \
                            output_file = self.output_file, report_file = self.report_file, report_sql_file = self.report_sql_file, \
                            workload_directory = self.workload_directory)
        loader.load_data()

        # vacuum_analyze
        

    def vacuum_analyze(self):
        cmd = 'VACUUM ANALYZE;'
        self.output('--' + cmd)
        beg_time = datetime.now()
        print beg_time
        (ok, result) = psql.runcmd(cmd = cmd, dbname = self.database_name, flag = '-a')
        print '*****\n'
        end_time = datetime.now()
        print end_time
        duration = end_time - beg_time
        duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds
        if ok:
            self.output('VACUUM ANALYZE success: %s' % (str(result)))
            self.report('    VACUUM ANALYZE   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (1, 1, 'SUCCESS', 0))
            self.report_sql("INSERT INTO table_name VALUES ('Vacuum Analyze', 'Vacuum Analyze', 1, 1, 'SUCCESS', %d);" % (duration))
        else:
            self.output('VACUUM ANALYZE failure: %s' % (str(result)))
            self.report('    VACUUM ANALYZE   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (1, 1, 'ERROR', 0))
            self.report_sql("INSERT INTO table_name VALUES ('Vacuum Analyze', 'Vacuum Analyze', 1, 1, 'ERROR', 0);")
 


