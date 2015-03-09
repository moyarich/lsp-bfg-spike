import os
import sys
import time
import commands
from datetime import datetime
import random
import hashlib
from multiprocessing import Process, Queue, Value , Array

try:
    from lib.PSQL import psql
except ImportError:
    sys.stderr.write('Workload needs psql in lib/PSQL.py\n')
    sys.exit(2)

try:
    from lib.utils.Check import check
except ImportError:
    sys.stderr.write('Workload needs check in lib/utils/Check.py\n')
    sys.exit(2)

try:
    from lib.Config import config
except ImportError:
    sys.stderr.write('Workload needs config in lib/Config.py\n')
    sys.exit(2)

try:
    from utils.Log import Log
except ImportError:
    sys.stderr.write('Workload needs Log in lib/utils/Log.py\n')
    sys.exit(2)

try:
    from utils.Report import Report
except ImportError:
    sys.stderr.write('Workload needs Report in lib/utils/Report.py\n')
    sys.exit(2)

try:
    import gl
except ImportError:
    sys.stderr.write('Workload needs gl.py in lsp_home\n')
    sys.exit(2)

class Workload(object):

    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, tr_id, user):
        # initialize common variables
        self.cs_id = cs_id
        self.tr_id = tr_id
        self.us_id = 0
        self.s_id = 0
        self.adj_s_id = 0

        self.user = user
        
        # check user_id if exist in backend database
        if self.cs_id != 0:
            self.us_id = check.check_id(result_id = 'us_id', table_name = 'hst.users', search_condition = "us_name = '%s'" % ('gpadmin')) #(self.user))
            if self.us_id is None:
                sys.stderr.write('The db user name is wrong!\n')
                sys.exit(2)

        self.continue_flag = True
        # should always run the workload by default
        self.should_stop = False
        # set workload source directory
        self.workload_directory = workload_directory
         
        # required fields, workload_name, database_name, user
        try:
            self.workload_name = workload_specification['workload_name'].strip()
            self.database_name = workload_specification['database_name'].strip()
            self.database_name = self.database_name + '_' + self.user
        except Exception, e:
            print('Please add %s option in schedule file.' % (str(e)) )
            sys.exit(2)
        # prepare folders and files,
        self.__prep_folders_and_files(workload_directory, report_directory, report_sql_file)

        # table settings
        self.data_volume_type = None
        self.data_volume_size = None
        self.append_only = None
        self.distributed_randomly = None
        self.orientation = None
        self.row_group_size = -1
        self.page_size = -1
        self.compression_type = None
        self.compression_level = -1
        self.partitions = 0
        self.seg_num = 2

        self.scale_factor = None
        self.ans_directory = ''
        self.__get_table_settings(workload_specification)

        # get table suffix, sql suffix, check_condition, wl_values from table_settings
        self.tbl_suffix = ''
        self.sql_suffix = ''      
        self.check_condition = ''
        self.wl_values = ''

        # get how to run a workload
        self.load_data_flag = None
        self.run_workload_flag = None
        self.run_workload_mode = None
        self.num_concurrency = None
        self.stream_mode = None
        self.num_iteration = None
        self.__get_run_mode(workload_specification)
        self.__set_info()

    def __prep_folders_and_files(self, workload_directory, report_directory, report_sql_file):
        # prepare report directory for workload
        if report_directory != '':
            self.report_directory = os.path.join(report_directory, self.workload_name + '_' + self.user)
        else:
            print 'Test report directory is not available before preparing report directory for workload %s' % (self.workload_name)
            exit(2)
        os.system('mkdir -p %s' % (self.report_directory))
        # set output log and report
        self.output_file = os.path.join(self.report_directory, 'output.csv')

        # prepare query result directory for workload
        self.result_directory = self.report_directory + os.sep + 'queries_result'
        os.system('mkdir -p %s' % (self.result_directory))
        os.system('rm -rf %s/*' % (self.result_directory))

        # get report.sql file and tmp folder
        self.report_sql_file = report_sql_file
        #self.tmp_folder = report_sql_file.replace('report.sql', 'tmp')
        self.tmp_folder = self.report_directory + os.sep + 'tmp'
        os.system('mkdir -p %s' % (self.tmp_folder))
        
    def __get_table_settings(self, workload_specification):
        ts = workload_specification['table_setting']

        # Calculate scale factor for workload
        try:
            self.data_volume_type = ts['data_volume_type'].upper()
            self.data_volume_size = ts['data_volume_size']
        except Exception, e:
            print('Please add %s option in schedule file.' % (str(e)) )
            sys.exit(2)
        
        # Need to make it univerally applicable instead of hard-code number of segments
        nnodes = len(config.getSegHostNames())
        if nnodes == 0:
            print 'getSegHostNames from gp_segment_configuration error.'
            sys.exit(2)
        
        if 'seg_num' in ts.keys():
            self.seg_num = ts['seg_num']
        self.nsegs =  nnodes * self.seg_num
        
        if self.data_volume_type == 'TOTAL':
            self.scale_factor = self.data_volume_size
        elif self.data_volume_type == 'PER_NODE':
            self.scale_factor = self.data_volume_size * nnodes
        elif self.data_volume_type == 'PER_SEGMENT':
            self.scale_factor = self.data_volume_size * self.nsegs
        else:
            self.output('Error in calculating data volumn for workloads %s: data_volume_type=%s, data_volume_size=%s' % (self.workload_name, self.data_volume_type, self.data_volume_size))
            sys.exit(2)

        # get ans directory base on self.scale_factor
        self.ans_directory = self.workload_directory + os.sep + 'queries_ans' + os.sep + 'queries_ans_%dg' % (self.scale_factor)
        if not os.path.exists(self.ans_directory):
            self.output('%s ans_directory:%s does not exists' % (self.workload_name, self.ans_directory))

        # Parse table setting
        ts_keys = ts.keys()

        if 'append_only' in ts_keys:
            self.append_only = ts['append_only']
            assert self.append_only in [True, False]
        
        self.orientation = 'ROW'
        if 'orientation' in ts_keys:
            self.orientation = ts['orientation'].upper()
            assert self.orientation in ['PARQUET', 'ROW', 'COLUMN']
        
        if 'row_group_size' in ts_keys: # and ts['row_group_size'].isdigit():
            self.row_group_size = int(ts['row_group_size'])
            
        if 'page_size' in ts_keys:
            self.page_size = int(ts['page_size'])
            
        if 'compression_type' in ts_keys:
            self.compression_type = ts['compression_type'].upper()
            assert self.compression_type in ['QUICKLZ', 'SNAPPY', 'GZIP', 'ZLIB']
            
        if 'compression_level' in ts_keys: # and ts['compression_level'].isdigit():
            self.compression_level = int(ts['compression_level'])
            
        if 'partitions' in ts_keys: # and ts['partitions'].isdigit():
            self.partitions = int(ts['partitions'])

        self.distributed_randomly = False
        if 'distributed_randomly' in ts_keys:
            self.distributed_randomly = ts['distributed_randomly']
            assert self.distributed_randomly in [True, False]
 
    def __get_run_mode(self, workload_specification):
        
        try:
            self.load_data_flag = workload_specification['load_data_flag']
        except Exception, e:
            self.load_data_flag = False
        assert self.load_data_flag in [True, False]
        
        try:
            self.run_workload_flag = workload_specification['run_workload_flag']
        except Exception, e:
            self.run_workload_flag = False
        assert self.run_workload_flag in [True, False]
        
        try:
            self.run_workload_mode = workload_specification['run_workload_mode'].strip().upper()
            if self.run_workload_mode not in ['SEQUENTIAL', 'RANDOM']:
                print('ERROR: Invalid value for mode of workload execution in workload %s: %s. Mast be SEQUENTIAL/RANDOM.' % (self.workload_name, self.run_workload_mode))
                sys.exit(2)
        except Exception, e:
            self.run_workload_mode = 'SEQUENTIAL'
        
        
        try:
            self.num_concurrency = int(str(workload_specification['num_concurrency']).strip())
        except Exception, e:
            self.num_concurrency = 1

        try:
            self.stream_mode = workload_specification['stream_mode']
        except Exception, e:
            self.stream_mode = False
        assert self.run_workload_flag in [True, False]

        try:
            self.num_iteration = int(str(workload_specification['num_iteration']).strip())
        except Exception, e:
            self.num_iteration = 1
    
    def __set_info(self):
        tbl_suffix = ''
        sql_suffix = ''
        # init tpch specific configuration such as tpch table_settings

        if self.append_only in [None, True]:
            tbl_suffix = tbl_suffix + 'ao'
            sql_suffix = sql_suffix + 'appendonly = true'
            # add distributed randomly
            if self.distributed_randomly:
                adj_distributed_randomly = 'FALSE'
            else:
                adj_distributed_randomly = 'TRUE'

            tbl_suffix = tbl_suffix + '_' + self.orientation
            sql_suffix = sql_suffix + ', '+ 'orientation = ' + self.orientation

            if self.orientation in ['ROW', 'COLUMN']:
                # group size, page_size
                self.page_size = -1
                self.row_group_size = -1

                if self.compression_type is None:
                    tbl_suffix = tbl_suffix + '_nocomp'
                    self.compression_type = 'None'
                    self.compression_level = -1
                elif self.compression_type == 'QUICKLZ':
                    self.compression_level = 1
                    tbl_suffix = tbl_suffix + '_' + self.compression_type + str(self.compression_level)
                    sql_suffix = sql_suffix + ', ' + 'compresstype = ' + self.compression_type  + ', ' + 'compresslevel = ' + str(self.compression_level)
                elif self.compression_type == 'ZLIB':
                    if (self.compression_level is None) or (self.compression_level < 1) or (self.compression_level > 9):
                        self.compression_level = 1
                    tbl_suffix = tbl_suffix + '_' + self.compression_type + str(self.compression_level)
                    sql_suffix = sql_suffix + ', ' + 'compresstype = ' + self.compression_type  + ', ' + 'compresslevel = ' + str(self.compression_level)
                else:
                    tbl_suffix = tbl_suffix + '_nocomp'
            else:
                # PARQUET
                if self.row_group_size is None or self.page_size is None:
                    self.row_group_size = 8388608
                    self.page_size = 1048576

                sql_suffix = sql_suffix + ', ' + 'pagesize = %s, rowgroupsize = %s' % (self.page_size, self.row_group_size)

                if self.compression_type == 'SNAPPY':
                    self.compression_level = -1
                    tbl_suffix = tbl_suffix + '_' + self.compression_type
                    sql_suffix = sql_suffix + ', ' + 'compresstype = ' + self.compression_type
                elif self.compression_type == 'GZIP':
                    if (self.compression_level is None) or (self.compression_level < 1) or (self.compression_level > 9):
                        self.compression_level = 1
                    tbl_suffix = tbl_suffix + '_' + self.compression_type + str(self.compression_level)
                    sql_suffix = sql_suffix + ', ' + 'compresstype = ' + self.compression_type  + ', ' + 'compresslevel = ' + str(self.compression_level)
                else:
                    tbl_suffix = tbl_suffix + '_nocomp'

            if self.partitions > 0:
                tbl_suffix += '_part'
            else:
                tbl_suffix += '_nopart'
        
        else:
            print 'not support heap table'
            sys.exit(2)
            tbl_suffix = tbl_suffix + 'heap'
            sql_suffix = ''

        self.check_condition = "wl_catetory = '%s' and wl_data_volume_type = '%s' and wl_data_volume_size = %d and wl_appendonly = '%s' and wl_disrandomly = '%s' and wl_orientation = '%s' and wl_row_group_size = %d and wl_page_size = %d and \
        wl_compression_type = '%s' and wl_compression_level = %d and wl_partitions = %d and wl_iteration = %d and wl_concurrency = %d and wl_query_order= '%s'" \
        % (self.workload_name.split('_')[0].upper(), self.data_volume_type, self.data_volume_size, self.append_only, self.distributed_randomly, self.orientation, self.row_group_size, self.page_size, self.compression_type, self.compression_level,
            self.partitions, self.num_iteration, self.num_concurrency, self.run_workload_mode)

        adj_check_condition = "wl_catetory = '%s' and wl_data_volume_type = '%s' and wl_data_volume_size = %d and wl_appendonly = '%s' and wl_disrandomly = '%s' and wl_orientation = '%s' and wl_row_group_size = %d and wl_page_size = %d and \
        wl_compression_type = '%s' and wl_compression_level = %d and wl_partitions = %d and wl_iteration = %d and wl_concurrency = %d and wl_query_order= '%s'" \
        % (self.workload_name.split('_')[0].upper(), self.data_volume_type, self.data_volume_size, self.append_only, adj_distributed_randomly, self.orientation, self.row_group_size, self.page_size, self.compression_type, self.compression_level,
            self.partitions, self.num_iteration, self.num_concurrency, self.run_workload_mode)

        self.wl_values = "'%s', '%s', '%s', %d, '%s', '%s', '%s', %d, %d, '%s', %d, %d, %d, %d, '%s'" \
        % (self.workload_name, self.workload_name.split('_')[0].upper(), self.data_volume_type, self.data_volume_size, self.append_only, self.distributed_randomly, self.orientation, self.row_group_size, self.page_size, self.compression_type, self.compression_level,
            self.partitions, self.num_iteration, self.num_concurrency, self.run_workload_mode)

        adj_wl_values = "'%s', '%s', '%s', %d, '%s', '%s', '%s', %d, %d, '%s', %d, %d, %d, %d, '%s'" \
        % (self.workload_name, self.workload_name.split('_')[0].upper(), self.data_volume_type, self.data_volume_size, self.append_only, adj_distributed_randomly, self.orientation, self.row_group_size, self.page_size, self.compression_type, self.compression_level,
            self.partitions, self.num_iteration, self.num_concurrency, self.run_workload_mode)

        if self.cs_id != 0:
            # check wl_id if exist
            self.wl_id = check.check_id(result_id = 'wl_id', table_name = 'hst.workload', search_condition = self.check_condition)
            if self.wl_id is None:
                check.insert_new_record(table_name = 'hst.workload',
                                        col_list = 'wl_name, wl_catetory, wl_data_volume_type, wl_data_volume_size, wl_appendonly, wl_disrandomly, wl_orientation, wl_row_group_size, wl_page_size, wl_compression_type, wl_compression_level, wl_partitions, wl_iteration, wl_concurrency, wl_query_order',
                                        values = self.wl_values)
                self.wl_id = check.get_max_id(result_id = 'wl_id', table_name = 'hst.workload')
                
            # check s_id if exist
            self.s_id = check.check_id(result_id = 's_id', table_name = 'hst.scenario', 
                                       search_condition = 'cs_id = %d and wl_id = %d and us_id = %d' % (self.cs_id, self.wl_id, self.us_id))
            if self.s_id is None:
                check.insert_new_record(table_name = 'hst.scenario', col_list = 'cs_id, wl_id, us_id', 
                                        values = '%d, %d, %d' % (self.cs_id, self.wl_id, self.us_id))
                self.s_id = check.get_max_id(result_id = 's_id', table_name = 'hst.scenario')
            #get tr_id
            #self.tr_id = check.get_max_id(result_id = 'tr_id', table_name = 'hst.test_run')

            # check adjust scenario check
            adj_wl_id = check.check_id(result_id = 'wl_id', table_name = 'hst.workload', search_condition = adj_check_condition)
            if adj_wl_id is None:
                self.adj_s_id = -1
            else:
                self.adj_s_id = check.check_id(result_id = 's_id', table_name = 'hst.scenario', 
                                       search_condition = 'cs_id = %d and wl_id = %d and us_id = %d' % (self.cs_id, adj_wl_id, self.us_id))
                if self.adj_s_id is None:
                    self.adj_s_id = -1
        
        self.tbl_suffix = tbl_suffix.lower()
        self.sql_suffix = sql_suffix

   
    def setup(self):
        '''Setup prerequisites for workload'''
        pass

    def output(self, msg):
        Log(self.output_file, msg)

    def report_sql(self, msg):
        Report(self.report_sql_file, msg)

    def check_query_result(self, ans_file, result_file):
        (status, output) = commands.getstatusoutput('diff %s %s' % (ans_file, result_file) )
        if output == '':
            return True
        else:
            with open(result_file.split('.')[0] + '.diff', 'w') as f:
                f.write('diff %s %s' % (ans_file, result_file) + '\n' + output)
            return False

    
    def load_data(self):
        '''Load data for workload'''
        pass

    
    def run_queries(self, iteration, stream, query_files = ''):
        queries_directory = self.workload_directory + os.sep + 'queries'
        if not os.path.exists(queries_directory):
            print 'Not find the queries_directory for %s' % (self.workload_name)
            sys.exit(2)

        if query_files == '':
            query_files = [file for file in os.listdir(queries_directory) if file.endswith('.sql')]
            if self.run_workload_mode == 'SEQUENTIAL':
                query_files = sorted(query_files)
            else:
                random.shuffle(query_files)

        # run all sql files in queries directory
        for qf_name in query_files:
            con_id = -1
            if self.continue_flag:
                if self.run_workload_flag:
                    with open(os.path.join(queries_directory, qf_name),'r') as f:
                        query = f.read()
                    if gl.suffix:
                        query = query.replace('TABLESUFFIX', self.tbl_suffix)
                    else:
                        query = query.replace('_TABLESUFFIX', '')
                    query = query.replace('SQLSUFFIX', self.sql_suffix)

                    # get con_id use this query
                    unique_string1 = '%s_%s_%d_%d_' % (self.workload_name, self.user, iteration, stream) + qf_name
                    unique_string2 = '%' + unique_string1 + '%'
                    get_con_id_sql = "select '***', '%s', sess_id from pg_stat_activity where current_query like '%s';" % (unique_string1, unique_string2)

                    with open(self.tmp_folder + os.sep + '%d_%d_' % (iteration, stream) + qf_name, 'w') as f:
                        f.write(query)
                        f.write(get_con_id_sql)

                    self.output(query)
                    beg_time = datetime.now()
                    (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + '%d_%d_' % (iteration, stream) + qf_name, dbname = self.database_name, username = self.user, flag = '-t -A')
                    end_time = datetime.now()
                    
                    if ok and str(result).find('psql: FATAL:') == -1 and str(result).find('ERROR:') == -1:
                        status = 'SUCCESS'
                        con_id = int(result[0].split('***')[1].split('|')[2].strip())
                        # generate output and md5 file
                        with open(self.result_directory + os.sep + '%d_%d_' % (iteration, stream) + qf_name.split('.')[0] + '.output', 'w') as f:
                            f.write(str(result[0].split('***')[0]))
                        with open(self.result_directory + os.sep + '%d_%d_' % (iteration, stream) + qf_name.split('.')[0] + '.output', 'r') as f:
                            result = f.read()
                            md5code = hashlib.md5(result.encode('utf-8')).hexdigest()
                        with open(self.result_directory + os.sep + '%d_%d_' % (iteration, stream) + qf_name.split('.')[0] + '.md5', 'w') as f:
                            f.write(md5code)
                        
                        if gl.check_result:
                            ans_file = self.ans_directory + os.sep + qf_name.split('.')[0] + '.ans'
                            md5_file = self.ans_directory + os.sep + qf_name.split('.')[0] + '.md5'
                            if os.path.exists(ans_file):
                                self.output('Check query result use ans file')
                                if not self.check_query_result(ans_file = ans_file, result_file = self.result_directory + os.sep + '%d_%d_' % (iteration, stream) + qf_name.split('.')[0] + '.output'):
                                    status = 'ERROR'
                            elif os.path.exists(md5_file):
                                self.output('Check query result use md5 file')
                                if not self.check_query_result(ans_file = md5_file, result_file = self.result_directory + os.sep + '%d_%d_' % (iteration, stream) + qf_name.split('.')[0] + '.md5'):
                                    status = 'ERROR'
                            else:
                                self.output('No answer file')
                                status = 'ERROR'
                    else:
                        status = 'ERROR'
                        self.output('\n'.join(result))
                else:
                    status = 'SKIP'
                    beg_time = datetime.now()
                    end_time = beg_time
            else:
                status = 'ERROR'
                beg_time = datetime.now()
                end_time = beg_time
                
            duration = end_time - beg_time
            duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds/1000     
            beg_time = str(beg_time)
            end_time = str(end_time)
            self.output('   Execution=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (qf_name.replace('.sql', ''), iteration, stream, status, duration))
            self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, %d, 'Execution', '%s', %d, %d, '%s', '%s', '%s', %d, NULL, NULL, NULL, %d);" 
                % (self.tr_id, self.s_id, con_id, qf_name.replace('.sql', ''), iteration, stream, status, beg_time, end_time, duration, self.adj_s_id))
                              
    def run_workload(self):
        niteration = 1
        while niteration <= self.num_iteration:
            self.output('-- Start iteration %d' % (niteration))
            AllWorkers = []
            nconcurrency = 1
            while nconcurrency <= self.num_concurrency:
                self.output('-- Start stream %s' % (nconcurrency))
                p = Process(target = self.run_queries, args = (niteration, nconcurrency))
                AllWorkers.append(p)
                nconcurrency += 1
                p.start()

            self.should_stop = False
            while True and not self.should_stop:
                for p in AllWorkers[:]:
                    p.join(timeout = 0.3)
                    if p.is_alive():
                        pass
                    else:
                        AllWorkers.remove(p)

                if len(AllWorkers) == 0:
                    self.should_stop = True
                    continue

                if len(AllWorkers) != 0:
                    time.sleep(2)

            self.output('-- Complete iteration %d' % (niteration))
            niteration += 1

    
    def run_queries_by_stream(self, iteration, stream):
        queries_directory = self.report_directory + os.sep + 'queries_stream'
        queries_finish_dir = self.report_directory + os.sep + 'queries_finish'
        lock_file = self.report_directory + os.sep + 'queries_stream' + os.sep + 'run.lock'
        if not os.path.exists(queries_directory):
            print 'Not find the queries_directory for %s' % (self.workload_name)
            sys.exit(2)

        while(1):
            (status, output) = commands.getstatusoutput( 'rm %s' % (lock_file) )
            if status != 0:
                time.sleep(1)
                continue

            query_files = [file for file in os.listdir(queries_directory) if file.endswith('.sql')]
            if self.run_workload_mode == 'SEQUENTIAL':
                query_files = sorted(query_files)
            else:
                random.shuffle(query_files)

            if len(query_files) != 0:
                qf_name = query_files[0]
            else:
                self.output('Stream%d is finish. ' %(stream))
                os.system( 'touch %s' % (lock_file))
                break

            con_id = -1
            with open(os.path.join(queries_directory, qf_name), 'r') as f:
                query = f.read()
                os.system('mv %s/%s %s' % (queries_directory, qf_name, queries_finish_dir))
                os.system('touch %s' %(lock_file))
            
            if self.continue_flag:
                if self.run_workload_flag:
                    if gl.suffix:
                        query = query.replace('TABLESUFFIX', self.tbl_suffix)
                    else:
                        query = query.replace('_TABLESUFFIX', '')
                    query = query.replace('SQLSUFFIX', self.sql_suffix)

                    # get con_id use this query
                    unique_string1 = '%s_%s_%d_%d_' % (self.workload_name, self.user, iteration, stream) + qf_name
                    unique_string2 = '%' + unique_string1 + '%'
                    get_con_id_sql = "select '***', '%s', sess_id from pg_stat_activity where current_query like '%s';" % (unique_string1, unique_string2)

                    with open(self.tmp_folder + os.sep + '%d_%d_' % (iteration, stream) + qf_name, 'w') as f:
                        f.write(query)
                        f.write(get_con_id_sql)

                    self.output(query)

                    beg_time = datetime.now()
                    (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + '%d_%d_' % (iteration, stream) + qf_name, dbname = self.database_name, flag = '-t -A', username = self.user)
                    end_time = datetime.now()
                    
                    if ok and str(result).find('psql: FATAL:') == -1 and str(result).find('ERROR:') == -1 and str(result).find('PANIC:') == -1:
                        status = 'SUCCESS'
                        con_id = int(result[0].split('***')[1].split('|')[2].strip())
                        # generate output and md5 file
                        with open(self.result_directory + os.sep + '%d_%d_' % (iteration, stream) + qf_name.split('.')[0] + '.output', 'w') as f:
                            f.write(str(result[0].split('***')[0]))
                        with open(self.result_directory + os.sep + '%d_%d_' % (iteration, stream) + qf_name.split('.')[0] + '.output', 'r') as f:
                            result = f.read()
                            md5code = hashlib.md5(result.encode('utf-8')).hexdigest()
                        with open(self.result_directory + os.sep + '%d_%d_' % (iteration, stream) + qf_name.split('.')[0] + '.md5', 'w') as f:
                            f.write(md5code)
                        
                        if gl.check_result:
                            ans_file = self.ans_directory + os.sep + qf_name.split('.')[0] + '.ans'
                            md5_file = self.ans_directory + os.sep + qf_name.split('.')[0] + '.md5'
                            if os.path.exists(ans_file):
                                self.output('Check query result use ans file')
                                if not self.check_query_result(ans_file = ans_file, result_file = self.result_directory + os.sep + '%d_%d_' % (iteration, stream) + qf_name.split('.')[0] + '.output'):
                                    status = 'ERROR'
                            elif os.path.exists(md5_file):
                                self.output('Check query result use md5 file')
                                if not self.check_query_result(ans_file = md5_file, result_file = self.result_directory + os.sep + '%d_%d_' % (iteration, stream) + qf_name.split('.')[0] + '.md5'):
                                    status = 'ERROR'
                            else:
                                self.output('No answer file')
                                status = 'ERROR'
                    else:
                        status = 'ERROR'
                        self.output('\n'.join(result))
                else:
                    status = 'SKIP'
                    beg_time = datetime.now()
                    end_time = beg_time
            else:
                status = 'ERROR'
                beg_time = datetime.now()
                end_time = beg_time
                
            duration = end_time - beg_time
            duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds/1000     
            beg_time = str(beg_time)
            end_time = str(end_time)
            self.output('   Execution=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (qf_name.replace('.sql', ''), iteration, stream, status, duration))
            self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, %d, 'Execution', '%s', %d, %d, '%s', '%s', '%s', %d, NULL, NULL, NULL, %d);" 
                % (self.tr_id, self.s_id, con_id, qf_name.replace('.sql', ''), iteration, stream, status, beg_time, end_time, duration, self.adj_s_id))
            #time.sleep(0.1)                       
    
    def run_workload_by_stream(self):
        os.system( 'mkdir -p %s; mkdir -p %s' % ( self.report_directory+ os.sep + 'queries_stream', self.report_directory + os.sep + 'queries_finish') )
        os.system( 'cp %s/* %s' % (self.workload_directory + os.sep + 'queries', self.report_directory + os.sep + 'queries_stream') )
        os.system( 'touch %s' % (self.report_directory + os.sep + 'queries_stream' + os.sep + 'run.lock') )
        niteration = 1
        while niteration <= self.num_iteration:
            self.output('-- Start iteration %d' % (niteration))
            AllWorkers = []
            nconcurrency = 1
            while nconcurrency <= self.num_concurrency:
                self.output('-- Start stream %s' % (nconcurrency))
                p = Process(target = self.run_queries_by_stream, args = (niteration, nconcurrency))
                AllWorkers.append(p)
                nconcurrency += 1
                p.start()

            self.should_stop = False
            while True and not self.should_stop:
                for p in AllWorkers[:]:
                    p.join(timeout = 0.3)
                    if p.is_alive():
                        pass
                    else:
                        AllWorkers.remove(p)

                if len(AllWorkers) == 0:
                    self.should_stop = True
                else:
                    time.sleep(2)

            cmd = 'mv %s/* %s' % (self.report_directory + os.sep + 'queries_finish', self.report_directory + os.sep + 'queries_stream')
            os.system(cmd)
            self.output(cmd)
            self.output('-- Complete iteration %d' % (niteration))
            niteration += 1

        cmd = 'rm -rf %s %s' % (self.report_directory + os.sep + 'queries_finish', self.report_directory + os.sep + 'queries_stream')
        os.system(cmd)
        self.output(cmd)  

    def vacuum_analyze(self):
        self.output('-- Start vacuum analyze')     
        con_id = -1
        if self.continue_flag:
            if self.load_data_flag:
                #sql = 'VACUUM ANALYZE;'
                sql = 'ANALYZE;'
                self.output(sql)
                sql_filename = 'vacuum.sql'
                # get con_id
                sql_file = '%' + sql_filename + '%'
                get_con_id_sql = "select '***', '%s', sess_id from pg_stat_activity where current_query like '%s';" % (sql_filename , sql_file)
                
                with open(self.tmp_folder + os.sep + sql_filename, 'w') as f:
                    f.write(sql)
                    f.write(get_con_id_sql)

                beg_time = datetime.now()
                (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + sql_filename, dbname = self.database_name, flag = '-t -A') # , username = self.user,)
                end_time = datetime.now()
                self.output(result[0].split('***')[0])

                if ok and str(result).find('ERROR:') == -1 and str(result).find('FATAL:') == -1:
                    status = 'SUCCESS'
                    con_id = int(result[0].split('***')[1].split('|')[2].strip())
                else:
                    status = 'ERROR'
                    self.continue_flag = False
            else:
                status = 'SKIP'
                beg_time = datetime.now()
                end_time = beg_time
        else:
            status = 'ERROR'
            beg_time = datetime.now()
            end_time = beg_time

        duration = end_time - beg_time
        duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds/1000
        beg_time = str(beg_time)
        end_time = str(end_time)
 
        self.output('   VACUUM ANALYZE   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (1, 1, status, duration))
        self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, %d, 'Vacuum_analyze', 'Vacuum_analyze', 1, 1, '%s', '%s', '%s', %d, NULL, NULL, NULL, %d);" 
            % (self.tr_id, self.s_id, con_id, status, beg_time, end_time, duration, self.adj_s_id))
        
        self.output('-- Complete vacuum analyze')

    
    def clean_up(self):
        pass

    def grand_revoke_privileges(self, filename = ''):
        if self.run_workload_flag and self.user != 'gpadmin':
            with open(self.workload_directory + '/data/' + filename , 'r') as f:
                query = f.read()
            if gl.suffix:
                query = query.replace('TABLESUFFIX', self.tbl_suffix)
            else:
                query = query.replace('_TABLESUFFIX', '')
            query = query.replace('ROLENAME', self.user)

            file_path = self.tmp_folder + os.sep + '%s_%s_' % (self.database_name, self.user) + filename
            with open(file_path, 'w') as f:
                f.write(query)

            (ok, output) = psql.runfile(ifile = file_path, dbname = self.database_name, username = 'gpadmin', flag = '-t -A')
            if not ok:
                print query
                print '\n'.join(output)
                sys.exit(2)
            self.output(query)
            self.output('\n'.join(output))


    def execute(self):
        self.output('-- Start running workload %s' % (self.workload_name))

        # setup
        self.setup()

        # load data
        self.load_data()

        # grant privileges
        self.grand_revoke_privileges(filename = 'grant.sql')

        # vacuum_analyze
        self.vacuum_analyze()

        # run workload concurrently and loop by iteration
        if self.stream_mode:
            self.run_workload_by_stream()
        else:
            self.run_workload()

        # revoke privileges
        self.grand_revoke_privileges(filename = 'revoke.sql')

        # clean up 
        self.clean_up()
        
        self.output('-- Complete running workload %s' % (self.workload_name))
        
