import os
import sys
from datetime import datetime, date, timedelta

try:
    from workloads.TPCH.Tpch import Tpch
except ImportError:
    sys.stderr.write('XMARQ needs Tpch Workload in workloads/TPCH/Tpch.py\n')
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

class Streamtpch(Tpch):

    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, user): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Tpch.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, user)

            
    def execute(self):
        self.output('-- Start running workload %s' % (self.workload_name))

        # setup
        self.setup()

        # load data
        self.load_data()

        # vacuum_analyze
        self.vacuum_analyze()

        # run workload concurrently and loop by iteration
        self.run_workload_by_stream()

        # clean up 
        self.clean_up()
        
        self.output('-- Complete running workload %s' % (self.workload_name))
