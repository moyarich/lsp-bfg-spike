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


class Tpch(Workload):
    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file)

    def setup(self):
        pass
           
    def load_data(self):
        tables = ['nation', 'region', 'part', 'supplier', 'partsupp', 'customer', 'orders','lineitem' ,'revenue']
        if not self.load_data_flag:
            for table_name in tables:
                self.output('    Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, 'SKIP', 0))
                self.report('    Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, 'SKIP', 0)) 
                self.report_sql("INSERT INTO table_name VALUES ('Loading', '%s', 1, 1, 'SKIP', 0);" % (table_name))
            return

        # load all 8 tables and 1 view
        loader = TpchLoader(database_name = self.database_name, user = self.user, \
                            scale_factor = self.scale_factor, nsegs = self.nsegs, append_only = self.append_only, orientation = self.orientation, \
                            page_size = self.page_size, row_group_size = self.row_group_size, \
                            compression_type = self.compression_type, compression_level = self.compression_level, \
                            partitions = self.partitions, tables = tables, tbl_suffix = self.tbl_suffix, sql_suffix = self.sql_suffix, \
                            output_file = self.output_file, report_file = self.report_file, report_sql_file = self.report_sql_file, \
                            workload_directory = self.workload_directory)
        loader.load()

        # vacuum_analyze
        self.vacuum_analyze()

    def vacuum_analyze(self):
        try: 
            cnx = pg.connect(dbname = self.database_name)
        except Exception, e:
            self.output('Failed to connect to database %s: %s' % (self.database_name), str(e))
            exit(2)
        
        try:
            sql = 'VACUUM ANALYZE;'
            beg_time = datetime.datetime.now()
            cnx.query(sql)
            end_time = datetime.datetime.now()
            duration = end_time - beg_time
            duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds
            self.output('    VACUUM ANALYZE   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (1, 1, 'SUCCESS', duration))
            self.report('    VACUUM ANALYZE   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (1, 1, 'SUCCESS', duration))
            self.report_sql("INSERT INTO table_name VALUES ('Vacuum Analyze', 'Vacuum Analyze', 1, 1, 'SUCCESS', %d);" % (duration))
        except Exception, e:
            self.output('VACUUM ANALYZE failure: %s' % (str(e)))
            self.report('    VACUUM ANALYZE   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (1, 1, 'ERROR', 0))
            self.report_sql("INSERT INTO table_name VALUES ('Vacuum Analyze', 'Vacuum Analyze', 1, 1, 'ERROR', 0);")
            exit(2)
        cnx.close()

    def clean_up(self):
        pass
