#!/usr/bin/env python
import os
import sys
from datetime import datetime, date, timedelta

try:
    from pygresql import pg
except ImportError:
    sys.stderr.write('LSP needs pygresql.\n')
    sys.exit(2)

try:
    from utils.Log import Log
except ImportError:
    sys.stderr.write('LSP needs Log in lib/utils/Log.py.\n')
    sys.exit(2)

# Arguments to the program
class TpchLoader(object):
    def __init__(self, dbname = "gpadmin", port = 5432, npsegs = 0,\
            scale = 0, append_only = False, orientation= "row", pagesize = 0, \
            rowgroup_size = 0, compress_type = "quicklz1", compress_level = 1,\
            partition = False, \
            tables = ["nation", "lineitem", "orders","region","part","supplier","partsupp", "customer"],\
            ofile = "/tmp/tpchloader.log"):

        self.dbname = dbname
        self.port = port
        self.npsegs = npsegs
        self.scale = scale
        self.append_only = append_only
        self.orientation = orientation
        self.pagesize = pagesize
        self.rowgroup_size = rowgroup_size
        self.compress_type = compress_type
        self.compress_level = compress_level
        self.partition = partition
        self.tables = tables
        self.ofile = ofile

        # join table name suffix and sql suffix
        sep = "_"
        table_suffix = ""
        sql_suffix = " "
        if self.append_only:
            table_suffix = table_suffix + sep + "ao"
            sql_suffix = sql_suffix + "appendonly = true, "
        else:
            table_suffix = table_suffix + sep + "heap"

        table_suffix = table_suffix + sep + self.orientation + sep + self.compress_type + sep + self.compress_level
        sql_suffix = sql_suffix + "orientation = %s, compresstype = %s, compresslevel= %s"%(self.orientation, self.compress_type, self.compress_level)

        if self.orientation == "parquet":
            sql_suffix = sql_suffix + "pagesize = %s, rowgroupsize = %s"%(self.pagesize, self.rowgroup_size) 

        if self.partition:
            table_suffix += "_part"

        self.table_suffix = table_suffix
        self.sql_suffix = sql_suffix


        # connect to db
        try: 
            self.cnx = pg.connect(dbname = self.dbname)
        except Exception, e:
            cnx = pg.connect(dbname = "postgres")
            cnx.query("create database %s"%self.dbname)
            cnx.close()
        finally:
            self.cnx = pg.connect(dbname = self.dbname)

    def __del__(self):
        ''' close connection to database'''
        self.cnx.close()

    def log(self, msg):
        Log(self.ofile, msg)

    def get_partition_suffix(self, size = 128, table_name = ""):
        start_date = date(1992, 01, 01)
        end_date = date(1999, 01, 01)
        interval_days = (end_date - start_date).days/size

        suffix = '''
                 PARTITION BY RANGE(o_orderdate)
                 (
                 '''
        for idx in range(1, size + 1):
            start = start_date + timedelta(days = (idx-1)*interval_days)
            end = start_date + timedelta(days = idx*interval_days)
            if idx != size:
                suffix = suffix + '''
                     PARTITION p1_%s START (\'%s\'::date) END (\'%s\'::date) EVERY (\'%s days\'::interval) WITH (tablename=\'%s_part_1_prt_p1_%s\', %s ),
                      '''%(idx, start, end, interval_days, table_name, idx, self.sql_suffix)
            else:
                suffix = suffix + '''
                     PARTITION p1_%s START (\'%s\'::date) END (\'%s\'::date) EVERY (\'%s days\'::interval) WITH (tablename=\'%s_part_1_prt_p1_%s\', %s )
                      '''%(idx, start, end, interval_days, table_name, idx, self.sql_suffix)
                
        suffix = suffix + '''
                     );
                     '''
        return suffix
        

    def run_query(self, cmd):
        result = self.cnx.query(cmd)
        if result == None:
            return ""
        return result

    def drop_table(self, name):
        ''' drop table if exist '''
        cmd = "drop table if exists %s cascade;"%name
        self.log(cmd)
        self.run_query(cmd)

    def drop_external_table(self, name):
        ''' drop external table if exist '''
        cmd = "drop external web table if exists %s;"%name
        self.log(cmd)
        self.run_query(cmd)

    def create_load_nation_table(self):
        # drop table if exist
        self.log("[INFO]: start load nation table")
        try:
            table_name = "nation" + self.table_suffix
            e_table_name = "e_" + "nation" + self.table_suffix
            self.drop_table(table_name)
            self.drop_external_table(e_table_name)

            # create nation table
            cmd = '''CREATE TABLE %s  ( N_NATIONKEY  INTEGER NOT NULL,
                                N_NAME       CHAR(25) NOT NULL,
                                N_REGIONKEY  INTEGER NOT NULL,
                                N_COMMENT    VARCHAR(152)) WITH (%s);'''%(table_name, self.sql_suffix)
            self.log(cmd)
            result = self.run_query(cmd)
            self.log("CREATE TABLE")


            # create nation external table
            cmd = '''create external web table %s (N_NATIONKEY  INTEGER ,
                                N_NAME       CHAR(25) ,
                                N_REGIONKEY  INTEGER ,
                                N_COMMENT    VARCHAR(152)) 
                            execute 'bash -c \"$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T n -s %s\"' on 1 format 'text' (delimiter '|');'''%(e_table_name, self.scale)
            self.log(cmd)
            result = self.run_query(cmd)
            self.log("CREATE TABLE")

            # insert data to nation table from e_nation table

            cmd = "insert into %s select * from %s;"%(table_name, e_table_name)
            self.log(cmd)
            start = datetime.now()
            result = self.run_query(cmd)
            end = datetime.now()
            interval = end - start
            self.log("INSERT  " + result)
            self.log("%s ms"%(interval.days*24*3600*1000 + interval.seconds*1000 + interval.microseconds/1000))
 
        except Exception, e :
            self.log("[Fail]: Load nation table failed")
            self.log(str(e))
            return False

    def create_load_region_table(self):
        # drop table if exist
        self.log("[INFO]: start load region table")
        try:
            table_name = "region" + self.table_suffix
            e_table_name = "e_" + "region" + self.table_suffix
            self.drop_table(table_name)
            self.drop_external_table(e_table_name)

            # create region table
            cmd = '''CREATE TABLE %s  ( R_REGIONKEY  INTEGER NOT NULL,
                            R_NAME       CHAR(25) NOT NULL,
                            R_COMMENT    VARCHAR(152)) WITH (%s);'''%(table_name, self.sql_suffix)
            self.log(cmd)
            result = self.run_query(cmd)
            self.log("CREATE TABLE")


            # create region external table
            cmd = '''CREATE external web TABLE %s  ( R_REGIONKEY  INTEGER ,
                            R_NAME       CHAR(25) ,
                            R_COMMENT    VARCHAR(152)) 
                        execute 'bash -c \"$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T r -s %s\"'
                        on 1 format 'text' (delimiter '|');'''%(e_table_name, self.scale)


            self.log(cmd)
            result = self.run_query(cmd)
            self.log("CREATE TABLE")

            # insert data to region table from e_region table

            cmd = "insert into %s select * from %s;"%(table_name, e_table_name)
            self.log(cmd)
            start = datetime.now()
            result = self.run_query(cmd)
            end = datetime.now()
            interval = end - start
            self.log("INSERT  " + result)
            self.log("%s ms"%(interval.days*24*3600*1000 + interval.seconds*1000 + interval.microseconds/1000))
 
        except Exception, e :
            self.log("[Fail]: Load region table failed")
            self.log(str(e))
            return False


    def create_load_part_table(self):
        # drop table if exist
        self.log("[INFO]: start load part table")
        try:
            table_name = "part" + self.table_suffix
            e_table_name = "e_" + "part" + self.table_suffix
            self.drop_table(table_name)
            self.drop_external_table(e_table_name)

            # create part table
            cmd = '''CREATE TABLE %s  ( P_PARTKEY     INTEGER NOT NULL,
                          P_NAME        VARCHAR(55) NOT NULL,
                          P_MFGR        CHAR(25) NOT NULL,
                          P_BRAND       CHAR(10) NOT NULL,
                          P_TYPE        VARCHAR(25) NOT NULL,
                          P_SIZE        INTEGER NOT NULL,
                          P_CONTAINER   CHAR(10) NOT NULL,
                          P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                          P_COMMENT     VARCHAR(23) NOT NULL ) WITH (%s);'''%(table_name, self.sql_suffix)

            self.log(cmd)
            result = self.run_query(cmd)
            self.log("CREATE TABLE")


            # create part external table
            cmd = '''CREATE external web TABLE %s  ( P_PARTKEY     INTEGER ,
                          P_NAME        VARCHAR(55) ,
                          P_MFGR        CHAR(25) ,
                          P_BRAND       CHAR(10) ,
                          P_TYPE        VARCHAR(25) ,
                          P_SIZE        INTEGER ,
                          P_CONTAINER   CHAR(10) ,
                          P_RETAILPRICE DECIMAL(15,2) ,
                          P_COMMENT     VARCHAR(23) ) 
                        execute 'bash -c \"$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T P -s %s -N %s -n $((GP_SEGMENT_ID + 1))\"'
                        on %s format 'text' (delimiter '|');'''%(e_table_name, self.scale, self.npsegs, self.npsegs)

            self.log(cmd)
            result = self.run_query(cmd)
            self.log("CREATE TABLE")

            # insert data to part table from e_part table

            cmd = "insert into %s select * from %s;"%(table_name, e_table_name)
            self.log(cmd)
            start = datetime.now()
            result = self.run_query(cmd)
            end = datetime.now()
            interval = end - start
            self.log("INSERT  " + result)
            self.log("%s ms"%(interval.days*24*3600*1000 + interval.seconds*1000 + interval.microseconds/1000))

        except Exception, e :
            self.log("[Fail]: Load part table failed")
            self.log(str(e))
            return False


    def create_load_supplier_table(self):
        # drop table if exist
        self.log("[INFO]: start load supplier table")
        try:
            table_name = "supplier" + self.table_suffix
            e_table_name = "e_" + "supplier" + self.table_suffix
            self.drop_table(table_name)
            self.drop_external_table(e_table_name)

            # create supplier table
            cmd = '''CREATE TABLE %s ( S_SUPPKEY     INTEGER NOT NULL,
                             S_NAME        CHAR(25) NOT NULL,
                             S_ADDRESS     VARCHAR(40) NOT NULL,
                             S_NATIONKEY   INTEGER NOT NULL,
                             S_PHONE       CHAR(15) NOT NULL,
                             S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                             S_COMMENT     VARCHAR(101) NOT NULL) WITH (%s);'''%(table_name, self.sql_suffix)

            self.log(cmd)
            result = self.run_query(cmd)
            self.log("CREATE TABLE")


            # create supplier external table
            cmd = '''CREATE external web TABLE %s ( S_SUPPKEY     INTEGER ,
                             S_NAME        CHAR(25) ,
                             S_ADDRESS     VARCHAR(40) ,
                             S_NATIONKEY   INTEGER ,
                             S_PHONE       CHAR(15) ,
                             S_ACCTBAL     DECIMAL(15,2) ,
                             S_COMMENT     VARCHAR(101) ) 
                        execute 'bash -c \"$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T s -s %s -N %s -n $((GP_SEGMENT_ID + 1))\"'
                        on %s format 'text' (delimiter '|');'''%(e_table_name, self.scale, self.npsegs, self.npsegs)

            self.log(cmd)
            result = self.run_query(cmd)
            self.log("CREATE TABLE")

            # insert data to supplier table from e_supplier table

            cmd = "insert into %s select * from %s;"%(table_name, e_table_name)
            self.log(cmd)
            start = datetime.now()
            result = self.run_query(cmd)
            end = datetime.now()
            interval = end - start
            self.log("INSERT  " + result)
            self.log("%s ms"%(interval.days*24*3600*1000 + interval.seconds*1000 + interval.microseconds/1000))

        except Exception, e :
            self.log("[Fail]: Load supplier table failed")
            self.log(str(e))
            return False


    def create_load_partsupp_table(self):
        # drop table if exist
        self.log("[INFO]: start load partsupp table")
        try:
            table_name = "partsupp" + self.table_suffix
            e_table_name = "e_" + "partsupp" + self.table_suffix
            self.drop_table(table_name)
            self.drop_external_table(e_table_name)

            # create partsupp table
            cmd = '''CREATE TABLE %s ( PS_PARTKEY     INTEGER NOT NULL,
                             PS_SUPPKEY     INTEGER NOT NULL,
                             PS_AVAILQTY    INTEGER NOT NULL,
                             PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                             PS_COMMENT     VARCHAR(199) NOT NULL ) WITH (%s);'''%(table_name, self.sql_suffix)

            self.log(cmd)
            result = self.run_query(cmd)
            self.log("CREATE TABLE")

            # create partsupp external table
            cmd = '''CREATE external web TABLE %s ( PS_PARTKEY     INTEGER ,
                             PS_SUPPKEY     INTEGER ,
                             PS_AVAILQTY    INTEGER ,
                             PS_SUPPLYCOST  DECIMAL(15,2)  ,
                             PS_COMMENT     VARCHAR(199) ) 
                        execute 'bash -c \"$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T S -s %s -N %s -n $((GP_SEGMENT_ID + 1))\"'
                        on %s format 'text' (delimiter '|');'''%(e_table_name, self.scale, self.npsegs, self.npsegs)

            self.log(cmd)
            result = self.run_query(cmd)
            self.log("CREATE TABLE")

            # insert data to partsupp table from e_partsupp table

            cmd = "insert into %s select * from %s;"%(table_name, e_table_name)
            self.log(cmd)
            start = datetime.now()
            result = self.run_query(cmd)
            end = datetime.now()
            interval = end - start
            self.log("INSERT  " + result)
            self.log("%s ms"%(interval.days*24*3600*1000 + interval.seconds*1000 + interval.microseconds/1000))

        except Exception, e :
            self.log("[Fail]: Load partsupp table failed")
            self.log(str(e))
            return False


        pass

    def create_load_customer_table(self):
        # drop table if exist
        self.log("[INFO]: start load customer table")
        try:
            table_name = "customer" + self.table_suffix
            e_table_name = "e_" + "customer" + self.table_suffix
            self.drop_table(table_name)
            self.drop_external_table(e_table_name)

            # create customer table
            cmd = '''CREATE TABLE %s ( C_CUSTKEY     INTEGER NOT NULL,
                             C_NAME        VARCHAR(25) NOT NULL,
                             C_ADDRESS     VARCHAR(40) NOT NULL,
                             C_NATIONKEY   INTEGER NOT NULL,
                             C_PHONE       CHAR(15) NOT NULL,
                             C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                             C_MKTSEGMENT  CHAR(10) NOT NULL,
                             C_COMMENT     VARCHAR(117) NOT NULL) WITH (%s);'''%(table_name, self.sql_suffix)

            self.log(cmd)
            result = self.run_query(cmd)
            self.log("CREATE TABLE")


            # create customer external table
            cmd = '''create external web table %s (N_NATIONKEY  INTEGER ,
                                N_NAME       CHAR(25) ,
                                N_REGIONKEY  INTEGER ,
                                N_COMMENT    VARCHAR(152)) 
                            execute 'bash -c \"$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T n -s %s\"' on 1 format 'text' (delimiter '|');'''%(e_table_name, self.scale)

            cmd = '''CREATE external web TABLE %s ( C_CUSTKEY     INTEGER ,
                             C_NAME        VARCHAR(25) ,
                             C_ADDRESS     VARCHAR(40) ,
                             C_NATIONKEY   INTEGER ,
                             C_PHONE       CHAR(15) ,
                             C_ACCTBAL     DECIMAL(15,2) ,
                             C_MKTSEGMENT  CHAR(10) ,
                             C_COMMENT     VARCHAR(117) ) 
                        execute 'bash -c \"$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T c -s %s -N %s -n $((GP_SEGMENT_ID + 1))\"'
                        on %s format 'text' (delimiter '|');'''%(e_table_name, self.scale, self.npsegs, self.npsegs)

            self.log(cmd)
            result = self.run_query(cmd)
            self.log("CREATE TABLE")

            # insert data to customer table from e_customer table

            cmd = "insert into %s select * from %s;"%(table_name, e_table_name)
            self.log(cmd)
            start = datetime.now()
            result = self.run_query(cmd)
            end = datetime.now()
            interval = end - start
            self.log("INSERT  " + result)
            self.log("%s ms"%(interval.days*24*3600*1000 + interval.seconds*1000 + interval.microseconds/1000))

        except Exception, e :
            self.log("[Fail]: Load customer table failed")
            self.log(str(e))
            return False


        pass
     
    def create_load_orders_table(self):
        # drop table if exist
        self.log("[INFO]: start load orders table")
        try:
            table_name = "orders" + self.table_suffix
            e_table_name = "e_" + "orders" + self.table_suffix
            self.drop_table(table_name)
            self.drop_external_table(e_table_name)

            # create orders table
            cmd = '''CREATE TABLE %s  ( O_ORDERKEY       INT8 NOT NULL,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    CHAR(1) NOT NULL,
                           O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  CHAR(15) NOT NULL,
                           O_CLERK          CHAR(15) NOT NULL,
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        VARCHAR(79) NOT NULL) WITH (%s);'''%(table_name, self.sql_suffix)
            if self.partition:
                cmd = cmd + self.get_partition_suffix(128, table_name)

            self.log(cmd)
            result = self.run_query(cmd)
            self.log("CREATE TABLE")


            # create orders external table
            cmd = '''CREATE external web TABLE %s  ( O_ORDERKEY       INT8 ,
                           O_CUSTKEY        INTEGER ,
                           O_ORDERSTATUS    CHAR(1) ,
                           O_TOTALPRICE     DECIMAL(15,2) ,
                           O_ORDERDATE      DATE ,
                           O_ORDERPRIORITY  CHAR(15) ,
                           O_CLERK          CHAR(15) ,
                           O_SHIPPRIORITY   INTEGER ,
                           O_COMMENT        VARCHAR(79) ) 
                        execute 'bash -c \"$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T O -s %s -N %s -n $((GP_SEGMENT_ID + 1))\"'
                        on %s format 'text' (delimiter '|')
                        log errors into %s_errtbl segment reject limit 100 percent;'''%(e_table_name, self.scale, self.npsegs, self.npsegs, table_name)


            self.log(cmd)
            result = self.run_query(cmd)
            self.log("CREATE TABLE")

            # insert data to orders table from e_orders table

            cmd = "insert into %s select * from %s;"%(table_name, e_table_name)
            self.log(cmd)
            start = datetime.now()
            result = self.run_query(cmd)
            end = datetime.now()
            interval = end - start
            self.log("INSERT  " + result)
            self.log("%s ms"%(interval.days*24*3600*1000 + interval.seconds*1000 + interval.microseconds/1000))

        except Exception, e :
            self.log("[Fail]: Load orders table failed")
            self.log(str(e))
            return False


        pass

    def create_load_lineitem_table(self):
        # drop table if exist
        self.log("[INFO]: start load lineitem table")
        try:
            table_name = "lineitem" + self.table_suffix
            e_table_name = "e_" + "lineitem" + self.table_suffix
            self.drop_table(table_name)
            self.drop_external_table(e_table_name)

            # create lineitem table
            cmd = '''CREATE TABLE %s ( L_ORDERKEY    INT8 NOT NULL,
                              L_PARTKEY     INTEGER NOT NULL,
                              L_SUPPKEY     INTEGER NOT NULL,
                              L_LINENUMBER  INTEGER NOT NULL,
                              L_QUANTITY    DECIMAL(15,2) NOT NULL,
                              L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                              L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                              L_TAX         DECIMAL(15,2) NOT NULL,
                              L_RETURNFLAG  CHAR(1) NOT NULL,
                              L_LINESTATUS  CHAR(1) NOT NULL,
                              L_SHIPDATE    DATE NOT NULL,
                              L_COMMITDATE  DATE NOT NULL,
                              L_RECEIPTDATE DATE NOT NULL,
                              L_SHIPINSTRUCT CHAR(25) NOT NULL,
                              L_SHIPMODE     CHAR(10) NOT NULL,
                              L_COMMENT      VARCHAR(44) NOT NULL) WITH (%s);'''%(table_name, self.sql_suffix)
            if self.partition:
                cmd = cmd + self.get_partition_suffix(128, table_name)

            self.log(cmd)
            result = self.run_query(cmd)
            self.log("CREATE TABLE")

            # create lineitem external table
            cmd = '''CREATE EXTERNAL WEB TABLE %s ( L_ORDERKEY    INT8 ,
                              L_PARTKEY     INTEGER ,
                              L_SUPPKEY     INTEGER ,
                              L_LINENUMBER  INTEGER ,
                              L_QUANTITY    DECIMAL(15,2) ,
                              L_EXTENDEDPRICE  DECIMAL(15,2) ,
                              L_DISCOUNT    DECIMAL(15,2) ,
                              L_TAX         DECIMAL(15,2) ,
                              L_RETURNFLAG  CHAR(1) ,
                              L_LINESTATUS  CHAR(1) ,
                              L_SHIPDATE    DATE ,
                              L_COMMITDATE  DATE ,
                              L_RECEIPTDATE DATE ,
                              L_SHIPINSTRUCT CHAR(25) ,
                              L_SHIPMODE     CHAR(10) ,
                              L_COMMENT      VARCHAR(44) )
                              EXECUTE 'bash -c \"$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T L -s %s -N %s -n $((GP_SEGMENT_ID + 1))\"' 
                              on %s format 'text' (delimiter '|')
                              log errors into %s_errtbl segment reject limit 100 percent;'''%(e_table_name, self.scale, self.npsegs, self.npsegs, table_name)


            self.log(cmd)
            result = self.run_query(cmd)
            self.log("CREATE TABLE")

            # insert data to lineitem table from e_lineitem table
            cmd = "insert into %s select * from %s;"%(table_name, e_table_name)
            self.log(cmd)
            start = datetime.now()
            result = self.run_query(cmd)
            end = datetime.now()
            interval = end - start
            self.log("INSERT  " + result)
            self.log("%s ms"%(interval.days*24*3600*1000 + interval.seconds*1000 + interval.microseconds/1000))

        except Exception, e :
            self.log("[Fail]: Load lineitem table failed")
            self.log(str(e))
            return False
     
    def vacuum_analyze(self):
        try:
            cmd = "VACUUM ANALYZE;"
            start = datetime.now()
            self.run_query(cmd)
            end = datetime.now()
            interval = end - start
            self.log("vacuum analyze")
            self.log("%s ms"%(interval.days*24*3600*1000 + interval.seconds*1000 + interval.microseconds/1000))

        except Exception, e:
            self.log("VACUUM ANALYZE Failed.")
            self.log(str(e))
            return False
            
    def load(self):
        for table in self.tables:
            func = "self.create_load_" + table + "_table"
            eval(func)()
        self.vacuum_analyze()
    


