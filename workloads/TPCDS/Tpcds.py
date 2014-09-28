import os
import sys
import commands, socket
from datetime import datetime, date, timedelta

try:
    from workloads.Workload import *
except ImportError:
    sys.stderr.write('TPCH needs workloads/Workload.py\n')
    sys.exit(2)

try:
    from pygresql import pg
except ImportError:
    sys.stderr.write('TPCH needs pygresql\n')
    sys.exit(2)

try:
    from lib.PSQL import psql
except ImportError:
    sys.stderr.write('TPCH needs psql in lib/PSQL.py\n')
    sys.exit(2)

try:
    from utils.Log import Log
except ImportError:
    sys.stderr.write('TPCH needs Log in lib/utils/Log.py\n')
    sys.exit(2)

try:
    from utils.Report import Report
except ImportError:
    sys.stderr.write('LSP needs Report in lib/utils/Report.py\n')
    sys.exit(2)

class Tpcds(Workload):
    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id)
        self.pwd = os.path.abspath(os.path.dirname(__file__))

    def setup(self):
        pass

    def get_partition_suffix(self, num_partitions = 128, table_name = ''):
        beg_date = date(1992, 01, 01)
        end_date = date(1998, 12, 31)
        duration_days = int(round(float((end_date - beg_date).days) / float(num_partitions)))

        part = ''

        if table_name == 'lineitem':
            part = '''PARTITION BY RANGE(l_shipdate)\n    (\n'''
        elif table_name == 'orders':
            part = '''PARTITION BY RANGE(o_orderdate)\n    (\n'''
                
        for i in range(1, num_partitions+1):
            beg_cur = beg_date + timedelta(days = (i-1)*duration_days)
            end_cur = beg_date + timedelta(days = i*duration_days)

            part += '''        PARTITION p1_%s START (\'%s\'::date) END (\'%s\'::date) EVERY (\'%s days\'::interval) WITH (tablename=\'%s_part_1_prt_p1_%s\', %s )''' % (i, beg_cur, end_cur, duration_days, table_name + '_' + self.tbl_suffix, i, self.sql_suffix)
            
            if i != num_partitions:
                part += ''',\n'''
            else:
                part += '''\n'''

        part += '''    )'''
                
        return part 

    def replace_sql(self, sql, table_name):
        sql = sql.replace('TABLESUFFIX', self.tbl_suffix)
        sql = sql.replace('SQLSUFFIX', self.sql_suffix)
        sql = sql.replace('SCALEFACTOR', str(self.scale_factor))
        sql = sql.replace('NUMSEGMENTS', str(self.nsegs))
        if self.partitions == 0 or self.partitions is None:
            sql = sql.replace('PARTITIONS', '')
        else:
            part_suffix = self.get_partition_suffix(num_partitions = self.partitions, table_name = table_name)
            sql = sql.replace('PARTITIONS', part_suffix)
        return sql

    def load_data(self):
        # check if the database exist
        try: 
            cnx = pg.connect(dbname = self.database_name)
        except Exception, e:
            cnx = pg.connect(dbname = 'postgres')
            cnx.query('CREATE DATABASE %s;' % (self.database_name))
        finally:
            cnx.close()

        self.output('-- Start loading data')

        tables = ['nation', 'region', 'part', 'supplier', 'partsupp', 'customer', 'orders','lineitem' ,'revenue']
        if not self.load_data_flag:
            beg_time = str(datetime.now()).split('.')[0]
            for table_name in tables:
                self.output('   Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, 'SKIP', 0))
                self.report('   Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, 'SKIP', 0)) 
                self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, 'Loading', '%s', 1, 1, 'SKIP', '%s', '%s', 0, NULL, NULL, NULL);" 
                    % (self.tr_id, self.s_id, table_name, beg_time, beg_time))
        else:
            self.load_setup()
            self.load_generate()
            self.load_loading()
            self.load_clean_up()

        self.output('-- Complete loading data')      
    
    def load_setup(self):
        self._check_hostfile()
        self._check_data_gen()
        sys.exit(2)

    def _check_hostfile(self):
        # add hostfile_master
        hostfile_master = self.pwd + os.sep + 'hostfile_master'
        master_host_name = config.getMasterHostName()
        with open(hostfile_master,'w') as f:
            f.write(str(master_host_name) + '\n')

        # add hostfile_seg
        hostfile_seg = self.pwd + os.sep + 'hostfile_seg'
        seg_hostname_list = config.getSegHostNames()
        seg_names = ''
        for seg_hostname in seg_hostname_list:
            seg_names = seg_names + seg_hostname + '\n'
        with open(hostfile_seg,'w') as f:
            f.write(seg_names)
        
    def _check_data_gen(self):
        # Check data_gen folder
        self.data_gen_folder = os.path.join(self.pwd, 'data_gen')
        if not os.path.exists(self.data_gen_folder):
            print('data_gen folder does not exist. Exit. ')
            sys.exit(2)
        else:
            print('data_gen folder exist.')
        
        # Check tpcds_idx file
        self.tpcds_idx = os.path.join(self.pwd, 'tpcds.idx')
        if not os.path.exists(self.tpcds_idx):
            print('tpcds.idx does not exist. Exit. ')
            sys.exit(2)
        else:
            print('tpcds.idx exist.')
            
        self._compile_data_gen()

    def _compile_data_gen(self):
        # temporarily 
        if os.path.exists(os.path.join(self.pwd, 'dsdgen')):
            print 'dsdgen already exist.'
            return
        
        command = 'cd %s; make clean; make' % (self.data_gen_folder)
        status, output = commands.getstatusoutput(command)    
        if status != 0:
            print('Error happens in compile data gen code.')
            print('output: %s'%output)
            sys.exit()
        else:
            print('Compile data gen code.')
            command2 = 'cd %s; cp dsdgen %s'%(self.data_gen_folder,self.pwd)
            s2, o2 = commands.getstatusoutput(command2)
            if s2!=0:
                print('Error happen in copy dsdgen.')
                sys.exit()
            else:
                print('Copy dsdgen to pwd.')
    
    def load_generate(self):
        # prepare temp folders, scp data_gen code(date_gen and tpcds.idx),  **data_gen_segment***
        pass

    def load_loading(self):
        # create table, 
        pass

    def load_clean_up(self):
        pass

    def execute(self):
        self.output('-- Start running workload %s' % (self.workload_name))
        self.report('-- Start running workload %s' % (self.workload_name))

        # setup
        self.setup()

        # load data
        self.load_data()

        # vacuum_analyze
        self.vacuum_analyze()

        # run workload concurrently and loop by iteration
        self.run_workload()

        # clean up 
        self.clean_up()
        
        self.output('-- Complete running workload %s' % (self.workload_name))
        self.report('-- Complete running workload %s' % (self.workload_name))

