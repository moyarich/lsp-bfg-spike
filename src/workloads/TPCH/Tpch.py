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
    def __init__(self, workload, report_dir = ""):
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload, report_dir)

        # init tpch specific configuration such as tpch table_settings
        config = workload["settings"]
        (append_only, orientation, pagesize, rowgroup_size, compression_type, compression_level,  partition ) = \
                                         [item.strip().lower() for item in  config["table_setting"].split(",")]

        assert append_only == "true" or append_only == "false"
        assert orientation == "column" or  orientation== "row" or orientation == "parquet"
        assert compression_type == "quicklz" or compression_type == "zlib" or compression_type == "snappy"\
                                                                              or compression_type == "gzip"
        assert partition == "true" or partition == "false" 

        try:
            if append_only == "true":
                self.append_only = True
            else:
                self.append_only = False

            self.orientation = orientation 
            self.pagesize = int(pagesize)
            self.rowgroup_size = int(rowgroup_size)
            self.compress_type = compression_type
            self.compress_level = compression_level
            self.partition = True if partition == "true" else False

        except ValueError ,e:
            self.error("%s table_settings configure error : page_size or rowgroupsize is nor a number"%workload["name"])
            self.error(str(e))
            exit(-1)

        # todo calculate data size to be created


        # join table suffix
        sep = "_"
        table_suffix = ""
        if self.append_only:
            table_suffix = table_suffix + sep + "ao"
        else:
            table_suffix = table_suffix + sep + "heap"

        table_suffix = table_suffix + sep + self.orientation + sep + self.compress_type + sep + self.compress_level
        if self.partition:
            table_suffix += "_part"

        self.table_suffix = table_suffix

        # init tpch load log
        self.tpch_loader_log = os.path.join(self.report_dir, "tpch_load.csv")

    def setup(self):
        pass
           

    def load_data(self):
        if not self.need_load_data:
            self.output( "[INFO] %s skip data load... "%self.name )
            return True
        # load all 8 tables 
        tables = ["nation", "lineitem", "orders","region","part","supplier","partsupp", "customer"]
        loader = TpchLoader(dbname = self.dbname, npsegs = 2, scale = 1, append_only = self.append_only,\
                            orientation = self.orientation, pagesize = self.pagesize, rowgroup_size = self.rowgroup_size,\
                            compress_type = self.compress_type, compress_level = self.compress_level,  \
                            partition = self.partition, tables = tables, ofile = self.tpch_loader_log)

        loader.load()

        # create revenue  view 
        query  = "drop view if exists revenue"
        (result, out) = psql.runcmd(query, dbname = self.dbname)
        if not result:
            self.error( "%s \n failed %s\n"%(query, out) )
            return False

        query = '''create view revenue (supplier_no, total_revenue) as
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
