#!/usr/bin/env python
import os
import sys
from datetime import datetime, date, timedelta

try:
    from pygresql import pg
except ImportError:
    sys.stderr.write('LSP needs pygresql\n')
    sys.exit(2)

try:
    from utils.Log import Log
except ImportError:
    sys.stderr.write('LSP needs Log in lib/utils/Log.py\n')
    sys.exit(2)

try:
    from utils.Report import Report
except ImportError:
    sys.stderr.write('LSP needs Report in lib/utils/Report.py\n')
    sys.exit(2)


class TpchLoader(object):

    def __init__(self, database_name = 'gpadmin', user = 'gpadmin', \
        scale_factor = 1, nsegs = 1, append_only = True, orientation= 'ROW', page_size = 1048576, \
        row_group_size = 8388608, compression_type = None, compression_level = None, partitions = None, \
        tables = ['nation', 'lineitem', 'orders','region','part','supplier','partsupp', 'customer'], \
        tbl_suffix = '', sql_suffix = '', tpch_load_log = '/tmp/tpch_load.log', \
        output_file = '/tmp/tpch_output', error_file = '/tmp/tpch_error', report_file = '/tmp/tpch_report'):

        self.database_name = None if database_name is None else database_name.lower()
        self.user = user.lower()
        self.scale_factor = scale_factor
        self.nsegs = nsegs
        self.append_only = True if append_only is None else append_only
        self.orientation = 'row' if orientation is None else orientation.lower()
        self.page_size = page_size
        self.row_group_size = row_group_size
        self.compression_type = None if compression_type is None else compression_type.lower()
        self.compression_level = None if compression_level is None else compression_level
        self.partitions = 0 if partitions is None else partitions
        self.tables = tables
        self.tbl_suffix = tbl_suffix
        self.sql_suffix = sql_suffix
        self.tpch_load_log = tpch_load_log
        self.output_file = output_file
        self.error_file = error_file
        self.report_file = report_file
        
        # connect to db
        try: 
            self.cnx = pg.connect(dbname = self.database_name)
        except Exception, e:
            cnx = pg.connect(dbname = 'postgres')
            cnx.query('CREATE DATABASE %s;' % (self.database_name))
            cnx.close()
        finally:
            self.cnx = pg.connect(dbname = self.database_name)

    def output(self, msg):
        Log(self.output_file, msg)

    def error(self, msg):
        Log(self.error_file, msg)

    def report(self, msg):
        Report(self.report_file, msg)

    def get_partition_suffix(self, num_partitions = 128, table_name = ''):
        beg_date = date(1992, 01, 01)
        end_date = date(1999, 01, 01)
        duration_days = (end_date - beg_date).days / num_partitions

        part = '''
                 PARTITION BY RANGE(o_orderdate)
                 (
               '''
        for i in range(1, num_partitions+1):
            beg_cur = beg_date + timedelta(days = (i-1)*duration_days)
            end_cur = beg_date + timedelta(days = i*duration_days)
            if i != num_partitions:
                part += '''
                     PARTITION p1_%s START (\'%s\'::date) END (\'%s\'::date) EVERY (\'%s days\'::interval) WITH (tablename=\'%s_part_1_prt_p1_%s\', %s ),
                        '''%(i, beg_cur, end_cur, duration_days, table_name, i, self.sql_suffix)
            else:
                part += '''
                     PARTITION p1_%s START (\'%s\'::date) END (\'%s\'::date) EVERY (\'%s days\'::interval) WITH (tablename=\'%s_part_1_prt_p1_%s\', %s )
                        '''%(i, beg_cur, end_cur, duration_days, table_name, i, self.sql_suffix)
                
        part += '''
                     );
                '''
        return part
        
    def run_sql(self, sql):
        out = self.cnx.query(sql)
        if out == None:
            return ''
        return out

    def drop_table(self, table_name):
        sql = 'DROP TABLE IF EXISTS %s CASCADE;' % (table_name)
        self.output(sql)
        result = self.run_sql(sql)
        self.output(result)

    def drop_external_table(self, table_name):
        sql = 'DROP EXTERNAL WEB TABLE IF EXISTS %s;' % (table_name)
        self.output(sql)
        result = self.run_sql(sql)
        self.output(result)

    def create_load_nation_table(self):
        self.output('-- Start loading data for nation:')
        self.report('-- Start loading data for nation:')
        try:
            # drop table if exist
            table_name = 'nation_' + self.tbl_suffix
            e_table_name = 'e_' + 'nation_' + self.tbl_suffix
            self.drop_table(table_name)
            self.drop_external_table(e_table_name)

            # create nation table
            cmd = '''CREATE TABLE %s  ( N_NATIONKEY  INTEGER NOT NULL,
                                N_NAME       CHAR(25) NOT NULL,
                                N_REGIONKEY  INTEGER NOT NULL,
                                N_COMMENT    VARCHAR(152)) WITH (%s);'''%(table_name, self.sql_suffix)
            self.output(cmd)
            result = self.run_sql(cmd)
            self.output(result)

            # create nation external table
            cmd = '''CREATE EXTERNAL WEB TABLE %s (N_NATIONKEY  INTEGER ,
                                N_NAME       CHAR(25) ,
                                N_REGIONKEY  INTEGER ,
                                N_COMMENT    VARCHAR(152)) 
                            execute 'bash -c \"$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T n -s %s\"' 
                            on 1 format 'text' (delimiter '|');'''%(e_table_name, self.scale_factor)
            self.output(cmd)
            result = self.run_sql(cmd)
            self.output(result)

            # insert data to nation table from e_nation table
            cmd = '''INSERT INTO %s SELECT * FROM %s;'''%(table_name, e_table_name)
            self.output(cmd)
            beg_time = datetime.now()
            result = self.run_sql(cmd)
            end_time = datetime.now()
            self.output(result)
            duration = end_time - beg_time
            self.output('Data loading for %s: %s ms' % (table_name, duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))
            self.report('Data loading for %s: %s ms' % (table_name, duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))
 
        except Exception, e:
            self.error('Data loading for %s failed: %s' % (table_name, str(e)))
            return False

        self.output('-- End loading data for nation')
        self.report('-- End loading data for nation')

    def create_load_region_table(self):
        self.output('-- Start loading data for region:')
        self.report('-- Start loading data for region:')
        try:
            # drop table if exist
            table_name = 'region_' + self.tbl_suffix
            e_table_name = 'e_' + 'region_' + self.tbl_suffix
            self.drop_table(table_name)
            self.drop_external_table(e_table_name)

            # create region table
            cmd = '''CREATE TABLE %s  ( R_REGIONKEY  INTEGER NOT NULL,
                            R_NAME       CHAR(25) NOT NULL,
                            R_COMMENT    VARCHAR(152)) WITH (%s);'''%(table_name, self.sql_suffix)
            self.output(cmd)
            result = self.run_sql(cmd)
            self.output(result)

            # create region external table
            cmd = '''CREATE EXTERNAL WEB TABLE %s  ( R_REGIONKEY  INTEGER ,
                            R_NAME       CHAR(25) ,
                            R_COMMENT    VARCHAR(152)) 
                        execute 'bash -c \"$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T r -s %s\"'
                        on 1 format 'text' (delimiter '|');'''%(e_table_name, self.scale_factor)

            self.output(cmd)
            result = self.run_sql(cmd)
            self.output(result)

            # insert data to region table from e_region table
            cmd = '''INSERT INTO %s SELECT * FROM %s;'''%(table_name, e_table_name)
            self.output(cmd)
            beg_time = datetime.now()
            result = self.run_sql(cmd)
            end_time = datetime.now()
            self.output(result)
            duration = end_time - beg_time
            self.output('Data loading for %s: %s ms' % (table_name, duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))
            self.report('Data loading for %s: %s ms' % (table_name, duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))
 
        except Exception, e:
            self.error('Data loading for %s failed: %s' % (table_name, str(e)))
            return False

        self.output('-- End loading data for region')
        self.report('-- End loading data for region')

    def create_load_part_table(self):
        self.output('-- Start loading data for part:')
        self.report('-- Start loading data for part:')
        try:
            # drop table if exist
            table_name = 'part_' + self.tbl_suffix
            e_table_name = 'e_' + 'part_' + self.tbl_suffix
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

            self.output(cmd)
            result = self.run_sql(cmd)
            self.output(result)

            # create part external table
            cmd = '''CREATE EXTERNAL WEB TABLE %s  ( P_PARTKEY     INTEGER ,
                          P_NAME        VARCHAR(55) ,
                          P_MFGR        CHAR(25) ,
                          P_BRAND       CHAR(10) ,
                          P_TYPE        VARCHAR(25) ,
                          P_SIZE        INTEGER ,
                          P_CONTAINER   CHAR(10) ,
                          P_RETAILPRICE DECIMAL(15,2) ,
                          P_COMMENT     VARCHAR(23) ) 
                        execute 'bash -c \"$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T P -s %s -N %s -n $((GP_SEGMENT_ID + 1))\"'
                        on %s format 'text' (delimiter '|');'''%(e_table_name, self.scale_factor, self.nsegs, self.nsegs)

            self.output(cmd)
            result = self.run_sql(cmd)
            self.output(result)

            # insert data to part table from e_part table
            cmd = '''INSERT INTO %s SELECT * FROM %s;'''%(table_name, e_table_name)
            self.output(cmd)
            beg_time = datetime.now()
            result = self.run_sql(cmd)
            end_time = datetime.now()
            self.output(result)
            duration = end_time - beg_time
            self.output('Data loading for %s: %s ms' % (table_name, duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))
            self.report('Data loading for %s: %s ms' % (table_name, duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))

        except Exception, e:
            self.error('Data loading for %s failed: %s' % (table_name, str(e)))
            return False

        self.output('-- End loading data for part')
        self.report('-- End loading data for part')

    def create_load_supplier_table(self):
        self.output('-- Start loading data for supplier:')
        self.report('-- Start loading data for supplier:')
        try:
            # drop table if exist
            table_name = 'supplier_' + self.tbl_suffix
            e_table_name = 'e_' + 'supplier_' + self.tbl_suffix
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

            self.output(cmd)
            result = self.run_sql(cmd)
            self.output(result)

            # create supplier external table
            cmd = '''CREATE EXTERNAL WEB TABLE %s ( S_SUPPKEY     INTEGER ,
                             S_NAME        CHAR(25) ,
                             S_ADDRESS     VARCHAR(40) ,
                             S_NATIONKEY   INTEGER ,
                             S_PHONE       CHAR(15) ,
                             S_ACCTBAL     DECIMAL(15,2) ,
                             S_COMMENT     VARCHAR(101) ) 
                        execute 'bash -c \"$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T s -s %s -N %s -n $((GP_SEGMENT_ID + 1))\"'
                        on %s format 'text' (delimiter '|');'''%(e_table_name, self.scale_factor, self.nsegs, self.nsegs)

            self.output(cmd)
            result = self.run_sql(cmd)
            self.output(result)

            # insert data to supplier table from e_supplier table
            cmd = '''INSERT INTO %s SELECT * FROM %s;'''%(table_name, e_table_name)
            self.output(cmd)
            beg_time = datetime.now()
            result = self.run_sql(cmd)
            end_time = datetime.now()
            self.output(result)
            duration = end_time - beg_time
            self.output('Data loading for %s: %s ms' % (table_name, duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))
            self.report('Data loading for %s: %s ms' % (table_name, duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))

        except Exception, e:
            self.error('Data loading for %s failed: %s' % (table_name, str(e)))
            return False

        self.output('-- End loading data for supplier')
        self.report('-- End loading data for supplier')

    def create_load_partsupp_table(self):
        self.output('-- Start loading data for partsupp:')
        self.report('-- Report loading data for partsupp:')
        try:
            # drop table if exist
            table_name = 'partsupp_' + self.tbl_suffix
            e_table_name = 'e_' + 'partsupp_' + self.tbl_suffix
            self.drop_table(table_name)
            self.drop_external_table(e_table_name)

            # create partsupp table
            cmd = '''CREATE TABLE %s ( PS_PARTKEY     INTEGER NOT NULL,
                             PS_SUPPKEY     INTEGER NOT NULL,
                             PS_AVAILQTY    INTEGER NOT NULL,
                             PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                             PS_COMMENT     VARCHAR(199) NOT NULL ) WITH (%s);'''%(table_name, self.sql_suffix)

            self.output(cmd)
            result = self.run_sql(cmd)
            self.output(result)

            # create partsupp external table
            cmd = '''CREATE EXTERNAL WEB TABLE %s ( PS_PARTKEY     INTEGER ,
                             PS_SUPPKEY     INTEGER ,
                             PS_AVAILQTY    INTEGER ,
                             PS_SUPPLYCOST  DECIMAL(15,2)  ,
                             PS_COMMENT     VARCHAR(199) ) 
                        execute 'bash -c \"$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T S -s %s -N %s -n $((GP_SEGMENT_ID + 1))\"'
                        on %s format 'text' (delimiter '|');'''%(e_table_name, self.scale_factor, self.nsegs, self.nsegs)

            self.output(cmd)
            result = self.run_sql(cmd)
            self.output(result)

            # insert data to partsupp table from e_partsupp table
            cmd = '''INSERT INTO %s SELECT * FROM %s;'''%(table_name, e_table_name)
            self.output(cmd)
            beg_time = datetime.now()
            result = self.run_sql(cmd)
            end_time = datetime.now()
            self.output(result)
            duration = end_time - beg_time
            self.output('Data loading for %s: %s ms' % (table_name, duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))
            self.report('Data loading for %s: %s ms' % (table_name, duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))

        except Exception, e:
            self.error('Data loading for %s failed: %s' % (table_name, str(e)))
            return False

        self.output('-- End loading data for partsupp')
        self.output('-- End loading data for partsupp')

    def create_load_customer_table(self):
        self.output('-- Start loading data for customer:')
        self.report('-- Start loading data for customer:')
        try:
            # drop table if exist
            table_name = 'customer_' + self.tbl_suffix
            e_table_name = 'e_' + 'customer_' + self.tbl_suffix
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

            self.output(cmd)
            result = self.run_sql(cmd)
            self.output(result)

            # create customer external table
            cmd = '''CREATE EXTERNAL WEB TABLE %s ( C_CUSTKEY     INTEGER ,
                             C_NAME        VARCHAR(25) ,
                             C_ADDRESS     VARCHAR(40) ,
                             C_NATIONKEY   INTEGER ,
                             C_PHONE       CHAR(15) ,
                             C_ACCTBAL     DECIMAL(15,2) ,
                             C_MKTSEGMENT  CHAR(10) ,
                             C_COMMENT     VARCHAR(117) ) 
                        execute 'bash -c \"$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T c -s %s -N %s -n $((GP_SEGMENT_ID + 1))\"'
                        on %s format 'text' (delimiter '|');'''%(e_table_name, self.scale_factor, self.nsegs, self.nsegs)

            self.output(cmd)
            result = self.run_sql(cmd)
            self.output(result)

            # insert data to customer table from e_customer table
            cmd = '''INSERT INTO %s SELECT * FROM %s;'''%(table_name, e_table_name)
            self.output(cmd)
            beg_time = datetime.now()
            result = self.run_sql(cmd)
            end_time = datetime.now()
            self.output(result)
            duration = end_time - beg_time
            self.output('Data loading for %s: %s ms' % (table_name, duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))
            self.report('Data loading for %s: %s ms' % (table_name, duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))

        except Exception, e:
            self.error('Data loading for %s failed: %s' % (table_name, str(e)))
            return False
     
        self.output('-- End loading data for customer')
        self.report('-- End loading data for customer')

    def create_load_orders_table(self):
        self.output('-- Start loading data for orders:')
        self.report('-- Start loading data for orders:')
        try:
            # drop table if exist
            table_name = 'orders_' + self.tbl_suffix
            e_table_name = 'e_' + 'orders_' + self.tbl_suffix
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
            if self.partitions:
                cmd = cmd + self.get_partition_suffix(128, table_name)

            self.output(cmd)
            result = self.run_sql(cmd)
            self.output(result)

            # create orders external table
            cmd = '''CREATE EXTERNAL WEB TABLE %s  ( O_ORDERKEY       INT8 ,
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
                        log errors into %s_errtbl segment reject limit 100 percent;'''%(e_table_name, self.scale_factor, self.nsegs, self.nsegs, table_name)


            self.output(cmd)
            result = self.run_sql(cmd)
            self.output(result)

            # insert data to orders table from e_orders table
            cmd = '''INSERT INTO %s SELECT * FROM %s;'''%(table_name, e_table_name)
            self.output(cmd)
            beg_time = datetime.now()
            result = self.run_sql(cmd)
            end_time = datetime.now()
            self.output(result)
            duration = end_time - beg_time
            self.output('Data loading for %s: %s ms' % (table_name, duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))
            self.report('Data loading for %s: %s ms' % (table_name, duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))

        except Exception, e:
            self.error('Data loading for %s failed: %s' % (table_name, str(e)))
            return False

        self.output('-- End loading data for orders')
        self.report('-- End loading data for orders')

    def create_load_lineitem_table(self):
        self.output('-- Start loading data for lineitem:')
        self.report('-- Start loading data for lineitem:')
        try:
            # drop table if exist
            table_name = 'lineitem_' + self.tbl_suffix
            e_table_name = 'e_' + 'lineitem_' + self.tbl_suffix
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
            if self.partitions:
                cmd = cmd + self.get_partition_suffix(128, table_name)

            self.output(cmd)
            result = self.run_sql(cmd)
            self.output(result)

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
                              log errors into %s_errtbl segment reject limit 100 percent;'''%(e_table_name, self.scale_factor, self.nsegs, self.nsegs, table_name)


            self.output(cmd)
            result = self.run_sql(cmd)
            self.output(result)

            # insert data to lineitem table from e_lineitem table
            cmd = '''INSERT INTO %s SELECT * FROM %s;'''%(table_name, e_table_name)
            self.output(cmd)
            beg_time = datetime.now()
            result = self.run_sql(cmd)
            end_time = datetime.now()
            self.output(result)
            duration = end_time - beg_time
            self.output('Data loading for %s: %s ms' % (table_name, duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))
            self.report('Data loading for %s: %s ms' % (table_name, duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))

        except Exception, e :
            self.error('Data loading for %s failed: %s' % (table_name, str(e)))
            return False

        self.output('-- End loading data for lineitem')
        self.report('-- End loading data for lineitem')

    def create_view_revenue(self):
        self.output('-- Start creating revenue view:')
        self.report('-- Start creating revenue view:')
        try:
            # drop view if exist
            cmd  = 'DROP VIEW IF EXISTS revenue;'
            self.output(cmd)
            result = self.run_sql(cmd)
            self.output(result)

            # create revenue view 
            cmd = '''CREATE VIEW revenue (supplier_no, total_revenue) AS
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
                    '''%('lineitem_' + self.tbl_suffix)
            
            self.output(cmd)
            beg_time = datetime.now()
            result = self.run_sql(cmd)
            end_time = datetime.now()
            self.output(result)
            duration = end_time - beg_time
            self.output('Creating VIEW for %s: %s ms' % ('lineitem_' + self.tbl_suffix, duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))
            self.report('Creating VIEW for %s: %s ms' % ('lineitem_' + self.tbl_suffix, duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))

        except Exception, e :
            self.error('Create VIEW for %s failed: %s' % ('lineitem_' + self.tbl_suffix, str(e)))
            return False

        self.output('-- End creating revenue view')
        self.report('-- End creating revenue view')
     
    def vacuum_analyze(self):
        try:
            sql = 'VACUUM ANALYZE;'
            beg_time = datetime.now()
            self.run_sql(sql)
            end_time = datetime.now()
            duration = end_time - beg_time
            self.output('VACUUM ANALYZE: %s ms' % (duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))
            self.report('VACUUM ANALYZE: %s ms' % (duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))
        except Exception, e:
            self.error('VACUUM ANALYZE failure: %s' % (str(e)))
            return False
            
    def load(self):
        for table in self.tables:
            func = 'self.create_load_' + table + '_table'
            eval(func)()
        self.create_view_revenue()
        self.vacuum_analyze()
