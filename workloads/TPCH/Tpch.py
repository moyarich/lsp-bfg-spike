import os
import sys
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
    from lib.PSQL import psql
except ImportError:
    sys.stderr.write('LSP needs psql in lib/PSQL.py when use in Tpch.py\n')
    sys.exit(2)


class Tpch(Workload):

    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file)

    def setup(self):
        pass
           
    def load_data(self):
        self.output('-- Start loading data')

        tables = ['nation', 'region', 'part', 'supplier', 'partsupp', 'customer', 'orders','lineitem' ,'revenue']

        if not self.load_data_flag:
            for table_name in tables:
                self.output('Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, 'SKIP', 0))
                self.report('  Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, 'SKIP', 0)) 
                self.report_sql("INSERT INTO table_name VALUES ('Loading', '%s', 1, 1, 'SKIP', 0);" % (table_name))
        else:
            # load all 8 tables and 1 view
            loader = TpchLoader(database_name = self.database_name, user = self.user, \
                                scale_factor = self.scale_factor, nsegs = self.nsegs, append_only = self.append_only, orientation = self.orientation, \
                                page_size = self.page_size, row_group_size = self.row_group_size, \
                                compression_type = self.compression_type, compression_level = self.compression_level, \
                                partitions = self.partitions, tables = tables, tbl_suffix = self.tbl_suffix, sql_suffix = self.sql_suffix, \
                                output_file = self.output_file, report_file = self.report_file, report_sql_file = self.report_sql_file, \
                                workload_directory = self.workload_directory)
            loader.load()

        self.output('-- Complete loading data')
        # vacuum_analyze
        self.vacuum_analyze()

    def vacuum_analyze(self):
        self.output('-- Start Vacuum Analyze')     
        sql = 'VACUUM ANALYZE;'
        beg_time = datetime.now()
        (ok, result) = psql.runcmd(cmd = sql, dbname = self.database_name)
        end_time = datetime.now()
        self.output('RESULT: ' + str(result))
        duration = end_time - beg_time
        duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds/1000

        if ok:
            self.output('VACUUM ANALYZE   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (1, 1, 'SUCCESS', duration))
            self.report('  VACUUM ANALYZE   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (1, 1, 'SUCCESS', duration))
            self.report_sql("INSERT INTO table_name VALUES ('Vacuum Analyze', 'Vacuum Analyze', 1, 1, 'SUCCESS', %d);" % (duration))
        else:
            self.output('ERROR: VACUUM ANALYZE failure')
            self.report('  VACUUM ANALYZE   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (1, 1, 'ERROR', 0))
            self.report_sql("INSERT INTO table_name VALUES ('Vacuum Analyze', 'Vacuum Analyze', 1, 1, 'ERROR', 0);")
        self.output('-- Complete Vacuum Analyze')

    def clean_up(self):
        pass
