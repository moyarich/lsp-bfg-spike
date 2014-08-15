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
    sys.stderr.write('LSP needs src/workloads/Workload.py.\n')
    sys.exit(2)

try:
    from TpchLoader import TpchLoader
except ImportError:
    sys.stderr.write('LSP needs TpchLoader in src/workloads/TPCH/TpchLoader.py.\n')
    sys.exit(2)

try:
    from pygresql import pg
except ImportError:
    sys.stderr.write('LSP needs pygresql.\n')
    sys.exit(2)

try:
    from PSQL import psql
except ImportError:
    sys.stderr.write('LSP needs psql in lib/PSQL.py.\n')
    sys.exit(2)


class TpchWorkload(Workload):
    def __init__(self, workload, test_report_dir = ""):
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload, test_report_dir)

        # init tpch specific configuration such as tpch table_settings
        ts = workload['table_setting']
        try:
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
  
        except ValueError ,e:
            self.error('Error in table settings for workload %s: %s' % (workload['workload_name'], str(e)))
            exit(-1)

        self.need_load_data = workload['load_data_flag']
        self.workload_name = workload['workload_name']
        self.need_run_query = workload['run_workload_flag']
        # todo calculate data size to be created


        # join table suffix
        sep = "_"
        table_suffix = ""
        if self.append_only:
            table_suffix = table_suffix + sep + "ao"
        else:
            table_suffix = table_suffix + sep + "heap"

        table_suffix = table_suffix + sep + self.orientation + sep + self.compression_type + sep + str(self.compression_level)
        if self.partitions:
            table_suffix += "_part"

        self.table_suffix = table_suffix

        # init tpch load log
        self.tpch_loader_log = os.path.join(self.workload_report_dir, "tpch_load.csv")

    def setup(self):
        pass
           

    def load_data(self):
        if not self.need_load_data:
            self.output( '[INFO] %s skip data load... '% self.workload_name )
            return True
        # load all 8 tables 
        tables = ["nation", "lineitem", "orders","region","part","supplier","partsupp", "customer"]
        loader = TpchLoader(dbname = self.dbname, npsegs = 2, scale = 1, append_only = self.append_only, \
                            orientation = self.orientation, page_size = self.page_size, row_group_size = self.row_group_size, \
                            compress_type = self.compression_type, compression_level = self.compression_level, \
                            partitions = self.partitions, tables = tables, ofile = self.tpch_loader_log)

        loader.load()

        # create revenue  view 
        query  = 'DROP VIEW IF EXISTS revenue;'
        (result, out) = psql.runcmd(query, dbname = self.dbname)
        if not result:
            self.error( "%s \n failed %s\n"%(query, out) )
            return False

        query = '''CREATE VIEW revenue (supplier_no, total_revenue) AS
                         select
                         l_suppkey,
                         sum(l_extendedprice * (1 - l_discount))
                         from
                         %s
                         where
                         l_shipdate >= date '1997-04-01'
                         and l_shipdate < date '1997-04-01' + interval '90 days'
                         group by
                         l_suppkey;
                '''%("lineitem" + self.table_suffix)
        (result, out) = psql.runcmd(query, dbname = self.dbname)
        if not result:
            self.error( "%s \n failed %s\n"%(query, out) )
            return False

    def run_query(self):
        queries_dir = os.path.join(self.src_dir, "queries")
        if not os.path.exists(queries_dir):
            return
        
        query_files = [file for file in os.listdir(queries_dir) if file.endswith(".sql")]

        cnx = pg.connect(dbname = self.dbname)
        # run all sql file under queries forder
        for file in query_files:
            start = datetime.now()
            qf = QueryFile(os.path.join(queries_dir, file))
            # run all query in sql file
            for query in qf:
                # run this query
                query = query.replace("TABLESUFFIX",self.table_suffix)
                try:
                    cnx.query(query)
                except Exception, e:
                    self.error("Query Fail:")
                    self.error( "sql:")
                    self.error( query)
                    self.error( "ans:" )
                    self.error( str(e) )
            end = datetime.now()
            interval = end - start
            duration = interval.days*24*3600 + interval.seconds
            self.output("%s,%s"%(file, duration))
            self.report("%s,%s"%(file, duration))

        cnx.close()


    def clean_up(self):
        pass

    def vacuum_analyze(self):
        pass

'''
9. replace table_name in all 22 query files

'''
