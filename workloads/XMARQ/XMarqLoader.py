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


class TpchLoader(object):
    def __init__(self, database_name = 'gpadmin', user = 'gpadmin', \
            scale_factor = 1, append_only = True, orientation= 'ROW', page_size = 1048576, \
            row_group_size = 8388608, compression_type = None, compression_level = None, partitions = False, \
            tables = ['nation', 'lineitem', 'orders','region','part','supplier','partsupp', 'customer'], \
            tbl_suffix = '', sql_suffix = '', output_file = '/tmp/tpch_load.out'):

        self.database_name = None if database_name is None else database_name.lower()
        self.user = user.lower()
        self.scale_factor = scale_factor
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
        self.output_file = output_file

        # connect to db
        try: 
            self.cnx = pg.connect(dbname = self.database_name)
        except Exception, e:
            cnx = pg.connect(dbname = 'postgres')
            cnx.query('create database %s' % (self.database_name))
            cnx.close()
        finally:
            self.cnx = pg.connect(dbname = self.database_name)

    def __close_database_coneection__(self):
        ''' close connection to database'''
        self.cnx.close()

    def log(self, msg):
        Log(self.output_file, msg)

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
        self.log(sql)
        self.run_query(sql)

    def drop_external_table(self, name):
        cmd = 'DROP EXTERNAL WEB TABLE IF EXISTS %s;' % (table_name)
        self.log(sql)
        self.run_query(sql)

    def create_load_nation_table(self):
        # drop table if exist
        self.log('[INFO]: start load nation table')
        try:
            table_name = 'nation' + self.tbl_suffix
            e_table_name = 'e_' + 'nation' + self.tbl_suffix
            self.drop_table(table_name)
            self.drop_external_table(e_table_name)

            # create nation table
            cmd = '''CREATE TABLE %s  ( N_NATIONKEY  INTEGER NOT NULL,
                                N_NAME       CHAR(25) NOT NULL,
                                N_REGIONKEY  INTEGER NOT NULL,
                                N_COMMENT    VARCHAR(152)) WITH (%s);'''%(table_name, self.sql_suffix)
            self.log(cmd)
            result = self.run_query(cmd)
            self.log('CREATE TABLE')


            # create nation external table
            cmd = '''create external web table %s (N_NATIONKEY  INTEGER ,
                                N_NAME       CHAR(25) ,
                                N_REGIONKEY  INTEGER ,
                                N_COMMENT    VARCHAR(152)) 
                            execute 'bash -c \'$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T n -s %s\'' on 1 format 'text' (delimiter '|');'''%(e_table_name, self.scale)
            self.log(cmd)
            result = self.run_query(cmd)
            self.log('CREATE TABLE')

            # insert data to nation table from e_nation table

            cmd = 'insert into %s select * from %s;'%(table_name, e_table_name)
            self.log(cmd)
            beg_time = datetime.now()
            result = self.run_query(cmd)
            end_timne = datetime.now()
            duration = end_time - beg_time
            self.log('INSERT  ' + result)
            self.log('%s ms'%(duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))
 
        except Exception, e :
            self.log('[Fail]: Load nation table failed')
            self.log(str(e))
            return False

    def create_load_region_table(self):
        # drop table if exist
        self.log('[INFO]: start load region table')
        try:
            table_name = 'region' + self.tbl_suffix
            e_table_name = 'e_' + 'region' + self.tbl_suffix
            self.drop_table(table_name)
            self.drop_external_table(e_table_name)

            # create region table
            cmd = '''CREATE TABLE %s  ( R_REGIONKEY  INTEGER NOT NULL,
                            R_NAME       CHAR(25) NOT NULL,
                            R_COMMENT    VARCHAR(152)) WITH (%s);'''%(table_name, self.sql_suffix)
            self.log(cmd)
            result = self.run_query(cmd)
            self.log('CREATE TABLE')


            # create region external table
            cmd = '''CREATE external web TABLE %s  ( R_REGIONKEY  INTEGER ,
                            R_NAME       CHAR(25) ,
                            R_COMMENT    VARCHAR(152)) 
                        execute 'bash -c \'$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T r -s %s\''
                        on 1 format 'text' (delimiter '|');'''%(e_table_name, self.scale)


            self.log(cmd)
            result = self.run_query(cmd)
            self.log('CREATE TABLE')

            # insert data to region table from e_region table

            cmd = 'insert into %s select * from %s;'%(table_name, e_table_name)
            self.log(cmd)
            beg_time = datetime.now()
            result = self.run_query(cmd)
            end_time = datetime.now()
            duration = end_time - beg_time
            self.log('INSERT  ' + result)
            self.log('%s ms'%(duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))
 
        except Exception, e :
            self.log('[Fail]: Load region table failed')
            self.log(str(e))
            return False


    def create_load_part_table(self):
        # drop table if exist
        self.log('[INFO]: start load part table')
        try:
            table_name = 'part' + self.tbl_suffix
            e_table_name = 'e_' + 'part' + self.tbl_suffix
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
            self.log('CREATE TABLE')


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
                        execute 'bash -c \'$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T P -s %s -N %s -n $((GP_SEGMENT_ID + 1))\''
                        on %s format 'text' (delimiter '|');'''%(e_table_name, self.scale, self.npsegs, self.npsegs)

            self.log(cmd)
            result = self.run_query(cmd)
            self.log('CREATE TABLE')

            # insert data to part table from e_part table

            cmd = 'insert into %s select * from %s;'%(table_name, e_table_name)
            self.log(cmd)
            beg_time = datetime.now()
            result = self.run_query(cmd)
            end_time = datetime.now()
            duration = end_time - beg_time
            self.log('INSERT  ' + result)
            self.log('%s ms'%(duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))

        except Exception, e :
            self.log('[Fail]: Load part table failed')
            self.log(str(e))
            return False


    def create_load_supplier_table(self):
        # drop table if exist
        self.log('[INFO]: start load supplier table')
        try:
            table_name = 'supplier' + self.tbl_suffix
            e_table_name = 'e_' + 'supplier' + self.tbl_suffix
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
            self.log('CREATE TABLE')


            # create supplier external table
            cmd = '''CREATE external web TABLE %s ( S_SUPPKEY     INTEGER ,
                             S_NAME        CHAR(25) ,
                             S_ADDRESS     VARCHAR(40) ,
                             S_NATIONKEY   INTEGER ,
                             S_PHONE       CHAR(15) ,
                             S_ACCTBAL     DECIMAL(15,2) ,
                             S_COMMENT     VARCHAR(101) ) 
                        execute 'bash -c \'$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T s -s %s -N %s -n $((GP_SEGMENT_ID + 1))\''
                        on %s format 'text' (delimiter '|');'''%(e_table_name, self.scale, self.npsegs, self.npsegs)

            self.log(cmd)
            result = self.run_query(cmd)
            self.log('CREATE TABLE')

            # insert data to supplier table from e_supplier table

            cmd = 'insert into %s select * from %s;'%(table_name, e_table_name)
            self.log(cmd)
            beg_time = datetime.now()
            result = self.run_query(cmd)
            end_time = datetime.now()
            duration = end_time - beg_time
            self.log('INSERT  ' + result)
            self.log('%s ms'%(duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))

        except Exception, e :
            self.log('[Fail]: Load supplier table failed')
            self.log(str(e))
            return False


    def create_load_partsupp_table(self):
        # drop table if exist
        self.log('[INFO]: start load partsupp table')
        try:
            table_name = 'partsupp' + self.tbl_suffix
            e_table_name = 'e_' + 'partsupp' + self.tbl_suffix
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
            self.log('CREATE TABLE')

            # create partsupp external table
            cmd = '''CREATE external web TABLE %s ( PS_PARTKEY     INTEGER ,
                             PS_SUPPKEY     INTEGER ,
                             PS_AVAILQTY    INTEGER ,
                             PS_SUPPLYCOST  DECIMAL(15,2)  ,
                             PS_COMMENT     VARCHAR(199) ) 
                        execute 'bash -c \'$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T S -s %s -N %s -n $((GP_SEGMENT_ID + 1))\''
                        on %s format 'text' (delimiter '|');'''%(e_table_name, self.scale, self.npsegs, self.npsegs)

            self.log(cmd)
            result = self.run_query(cmd)
            self.log('CREATE TABLE')

            # insert data to partsupp table from e_partsupp table

            cmd = 'insert into %s select * from %s;'%(table_name, e_table_name)
            self.log(cmd)
            beg_time = datetime.now()
            result = self.run_query(cmd)
            end_time = datetime.now()
            duration = end_time - beg_time
            self.log('INSERT  ' + result)
            self.log('%s ms'%(duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))

        except Exception, e :
            self.log('[Fail]: Load partsupp table failed')
            self.log(str(e))
            return False


        pass

    def create_load_customer_table(self):
        # drop table if exist
        self.log('[INFO]: start load customer table')
        try:
            table_name = 'customer' + self.tbl_suffix
            e_table_name = 'e_' + 'customer' + self.tbl_suffix
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
            self.log('CREATE TABLE')


            # create customer external table
            cmd = '''create external web table %s (N_NATIONKEY  INTEGER ,
                                N_NAME       CHAR(25) ,
                                N_REGIONKEY  INTEGER ,
                                N_COMMENT    VARCHAR(152)) 
                            execute 'bash -c \'$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T n -s %s\'' on 1 format 'text' (delimiter '|');'''%(e_table_name, self.scale)

            cmd = '''CREATE external web TABLE %s ( C_CUSTKEY     INTEGER ,
                             C_NAME        VARCHAR(25) ,
                             C_ADDRESS     VARCHAR(40) ,
                             C_NATIONKEY   INTEGER ,
                             C_PHONE       CHAR(15) ,
                             C_ACCTBAL     DECIMAL(15,2) ,
                             C_MKTSEGMENT  CHAR(10) ,
                             C_COMMENT     VARCHAR(117) ) 
                        execute 'bash -c \'$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T c -s %s -N %s -n $((GP_SEGMENT_ID + 1))\''
                        on %s format 'text' (delimiter '|');'''%(e_table_name, self.scale, self.npsegs, self.npsegs)

            self.log(cmd)
            result = self.run_query(cmd)
            self.log('CREATE TABLE')

            # insert data to customer table from e_customer table

            cmd = 'insert into %s select * from %s;'%(table_name, e_table_name)
            self.log(cmd)
            beg_time = datetime.now()
            result = self.run_query(cmd)
            end_time = datetime.now()
            duration = end_time - beg_time
            self.log('INSERT  ' + result)
            self.log('%s ms'%(duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))

        except Exception, e :
            self.log('[Fail]: Load customer table failed')
            self.log(str(e))
            return False


        pass
     
    def create_load_orders_table(self):
        # drop table if exist
        self.log('[INFO]: start load orders table')
        try:
            table_name = 'orders' + self.tbl_suffix
            e_table_name = 'e_' + 'orders' + self.tbl_suffix
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
            self.log('CREATE TABLE')


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
                        execute 'bash -c \'$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T O -s %s -N %s -n $((GP_SEGMENT_ID + 1))\''
                        on %s format 'text' (delimiter '|')
                        log errors into %s_errtbl segment reject limit 100 percent;'''%(e_table_name, self.scale, self.npsegs, self.npsegs, table_name)


            self.log(cmd)
            result = self.run_query(cmd)
            self.log('CREATE TABLE')

            # insert data to orders table from e_orders table

            cmd = 'insert into %s select * from %s;'%(table_name, e_table_name)
            self.log(cmd)
            beg_time = datetime.now()
            result = self.run_query(cmd)
            end_time = datetime.now()
            duration = end_time - beg_time
            self.log('INSERT  ' + result)
            self.log('%s ms'%(duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))

        except Exception, e :
            self.log('[Fail]: Load orders table failed')
            self.log(str(e))
            return False


        pass

    def create_load_lineitem_table(self):
        # drop table if exist
        self.log('[INFO]: start load lineitem table')
        try:
            table_name = 'lineitem' + self.tbl_suffix
            e_table_name = 'e_' + 'lineitem' + self.tbl_suffix
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
            self.log('CREATE TABLE')

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
                              EXECUTE 'bash -c \'$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T L -s %s -N %s -n $((GP_SEGMENT_ID + 1))\'' 
                              on %s format 'text' (delimiter '|')
                              log errors into %s_errtbl segment reject limit 100 percent;'''%(e_table_name, self.scale, self.npsegs, self.npsegs, table_name)


            self.log(cmd)
            result = self.run_query(cmd)
            self.log('CREATE TABLE')

            # insert data to lineitem table from e_lineitem table
            cmd = 'insert into %s select * from %s;'%(table_name, e_table_name)
            self.log(cmd)
            beg_time = datetime.now()
            result = self.run_query(cmd)
            end_time = datetime.now()
            duration = end_time - beg_time
            self.log('INSERT  ' + result)
            self.log('%s ms'%(duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds))

        except Exception, e :
            self.log('[Fail]: Load lineitem table failed')
            self.log(str(e))
            return False
     
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
            self.log('VACUUM ANALYZE failure: %s' % (str(e)))
            return False
            
    def load(self):
        for table in self.tables:
            func = 'self.create_load_' + table + '_table'
            eval(func)()
        self.vacuum_analyze()
