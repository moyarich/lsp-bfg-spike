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
    from lib.Shell import Shell
except ImportError:
    sys.stderr.write('LSP needs shell in lib/Shell.py\n')
    sys.exit(2)

try:
    from lib.Config import config
except ImportError:
    sys.stderr.write('LSP needs config in lib/Config.py\n')
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
            self.error('Error in calculating data volumn for workloads %s: data_volume_type=%s, data_volume_size=%s' % (self.workload_name, self.data_volume_type, self.data_volume_size))
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

        if self.orientation == 'parquet':
            sql_suffix = sql_suffix + ', ' + 'pagesize = %s, rowgroupsize = %s' % (self.page_size, self.row_group_size)

        if self.compression_type is not None:
            if self.compression_level is not None:
                tbl_suffix = tbl_suffix + '_' + self.compression_type + '_' + 'level' + str(self.compression_level)
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

    def setup(self):
        pass
           
    def load_data(self):
        if not self.load_data_flag:
            self.output( 'Skip loading data for %s' % (self.workload_name))
            self.report( 'Skip loading data for %s' % (self.workload_name))
            return True

        # load all 8 tables and 1 view
        tables = ['nation', 'region', 'part', 'supplier', 'partsupp', 'customer', 'orders','lineitem' ,'revenue']
        loader = TpchLoader(database_name = self.database_name, user = self.user, \
                            scale_factor = self.scale_factor, nsegs = self.nsegs, append_only = self.append_only, orientation = self.orientation, \
                            page_size = self.page_size, row_group_size = self.row_group_size, \
                            compression_type = self.compression_type, compression_level = self.compression_level, \
                            partitions = self.partitions, tables = tables, tbl_suffix = self.tbl_suffix, sql_suffix = self.sql_suffix, \
                            tpch_load_log = os.path.join(self.report_directory, 'tpch_load.log'), \
                            output_file = self.output_file, error_file = self.error_file, report_file = self.report_file, \
                            workload_directory = self.workload_directory)
        loader.load()

        # vacuum_analyze
        self.vacuum_analyze()

    def vacuum_analyze(self):
        '''VACUUM/ANALYZE data for workload'''
        try: 
            cnx = pg.connect(dbname = self.database_name)
        except Exception, e:
            self.error('Failed to connect to database %s: %s' % (self.database_name), str(e))
            exit(2)
        try:
            sql = 'VACUUM ANALYZE;'
            beg_time = datetime.datetime.now()
            cnx.query(sql)
            end_time = datetime.datetime.now()
            duration = end_time - beg_time
            self.output('VACUUM ANALYZE: %s ms' % (duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))
            self.report('VACUUM ANALYZE: %s ms' % (duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))
        except Exception, e:
            self.error('VACUUM ANALYZE failure: %s' % (str(e)))
            exit(2)
        cnx.close()

    def clean_up(self):
        pass
