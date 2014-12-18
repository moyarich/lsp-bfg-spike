import os
import sys
from datetime import datetime, date, timedelta

try:
    from workloads.TPCH.Tpch import Tpch
except ImportError:
    sys.stderr.write('XMARQ needs Tpch Workload in workloads/TPCH/Tpch.py\n')
    sys.exit(2)

try:
    from pygresql import pg
except ImportError:
    sys.stderr.write('XMARQ needs pygresql\n')
    sys.exit(2)

try:
    from lib.PSQL import psql
except ImportError:
    sys.stderr.write('XMARQ needs psql in lib/PSQL.py\n')
    sys.exit(2)

try:
    from lib.QueryFile import QueryFile
except ImportError:
    sys.stderr.write('XMARQ needs QueryFile in lib/QueryFile.py\n')
    sys.exit(2)

try:
    import gl
except ImportError:
    sys.stderr.write('XMARQ needs gl.py in lib/\n')
    sys.exit(2)


class Xmarq(Tpch):
    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, user): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Tpch.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, user)


