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
    from TpchLoader import TpchLoader
except ImportError:
    sys.stderr.write('LSP needs TpchLoader in workloads/TPCH/TpchLoader.py\n')
    sys.exit(2)

try:
    from pygresql import pg
except ImportError:
    sys.stderr.write('LSP needs pygresql\n')
    sys.exit(2)

try:
    from PSQL import psql
except ImportError:
    sys.stderr.write('LSP needs psql in lib/PSQL.py\n')
    sys.exit(2)


class Tpch(Workload):
    def __init__(self, workload_specification, workload_directory, report_directory): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload_specification, workload_directory, report_directory)

        # init tpch specific configuration such as tpch table_settings
        ts = workload_specification['table_setting']

        # Calculate scale factor for TPC-H workload
        self.data_volume_type = ts['data_volume_type'].upper()
        self.data_volume_size = ts['data_volume_size']
        
        # Need to make it univerally applicable instead of hard-code number of segments
        # config.getNPrimarySegments()
        self.nsegs = 2

        self.scale_factor = 1
        if self.data_volume_type == 'TOTAL':
            self.scale_factor = self.data_volume_size
        elif self.data_volume_type == 'PER_NODE':
            nnodes = len(config.getSegHostNames())
            self.scale_factor = self.data_volume_size * nnodes
        elif self.data_volume_type == 'PER_SEGMENT':
            self.scale_factor = self.data_volume_size * self.nsegs
        else:
            self.error('Error in calculating data volumn for workloads %s: data_volume_type=%s, data_volume_size=%s' % (self.workload_name, self.data_volume_type, self.data_volume_size))
            exit(-1)

        # Parse table setting
        if ts['append_only']:
            self.append_only = ts['append_only']
        assert self.append_only in [True, False]
       
        if ts['orientation']:
            self.orientation = ts['orientation'].upper()
        assert self.orientation in ['PARQUET', 'ROW', 'COLUMN']
        
        if ts['row_group_size']:
            self.row_group_size = int(ts['row_group_size'])
            
        if ts['page_size']:
            self.page_size = int(ts['page_size'])
        
        if ts['compression_type']:
            self.compression_type = ts['compression_type'].upper()
        assert self.orientation in ['PARQUET'] and self.compression_type in ['SNAPPY', 'GZIP'] or \
               self.orientation in ['ROW', 'COLUMN'] and self.compression_type in ['QUICKLZ', 'ZLIB']
        
        if ts['compression_level']:
            self.compression_level = int(ts['compression_level'])
        assert self.compression_type in ['GZIP', 'QUICKLZ', 'ZLIB']
        
        if ts['partitions']:
            self.partitions = ts['partitions']
        else:
            self.partitions = False
        assert self.partitions in [True, False]

        # prepare name with suffix for table and corresponding sql statement to create it
        tbl_suffix = ''
        sql_suffix = ''

        if self.append_only:
            tbl_suffix = tbl_suffix + 'ao'
            sql_suffix = sql_suffix + 'appendonly = true'
        else:
            tbl_suffix = tbl_suffix + 'heap'
            sql_suffix = sql_suffix + 'appendonly = false'

        tbl_suffix = tbl_suffix + '_' + self.orientation
        sql_suffix = sql_suffix + ', '+ 'orientation = ' + self.orientation

        if self.orientation == 'parquet':
            sql_suffix = sql_suffix + ', ' + 'pagesize = %s, rowgroupsize = %s' % (self.page_size, self.row_group_size)

        if self.compression_type is not None:
            if self.compression_level is not None:
                tbl_suffix = tbl_suffix + '_' + self.compression_type + '_' + str(self.compression_level)
                sql_suffix = sql_suffix + ', ' + 'compresstype = ' + self.compression_type 
                sql_suffix = sql_suffix + ', ' + 'compresslevel = ' + str(self.compression_level)
            else:
                tbl_suffix = tbl_suffix + '_' + self.compression_type
                sql_suffix = sql_suffix + ', ' + 'compresstype = ' + self.compression_type
        else:
            tbl_suffix = tbl_suffix + '_nocomp'

        if self.partitions:
            tbl_suffix += '_part'
        else:
            tbl_suffix += '_nopart'

        self.tbl_suffix = tbl_suffix.lower()
        self.sql_suffix = sql_suffix

    def setup(self):
        pass
           
    def load_data(self):
        if not self.load_data_flag:
            self.output( 'Skipping data loading for %s' % (self.workload_name))
            return True

        # load all 8 tables 
        tables = ['nation', 'region', 'part', 'supplier', 'partsupp', 'customer', 'orders','lineitem']
        loader = TpchLoader(database_name = self.database_name, user = self.user, \
                            scale_factor = self.scale_factor, nsegs = self.nsegs, append_only = self.append_only, orientation = self.orientation, \
                            page_size = self.page_size, row_group_size = self.row_group_size, \
                            compression_type = self.compression_type, compression_level = self.compression_level, \
                            partitions = self.partitions, tables = tables, tbl_suffix = self.tbl_suffix, sql_suffix = self.sql_suffix, \
                            tpch_load_log = os.path.join(self.report_directory, 'tpch_load.log'), \
                            output_file = self.output_file, error_file = self.error_file, report_file = self.report_file)
        loader.load()

    def vacuum_analyze(self):
        pass

    def clean_up(self):
        pass
