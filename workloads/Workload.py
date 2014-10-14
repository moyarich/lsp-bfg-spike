import os
import sys
import time
from datetime import datetime
import random
import hashlib
from multiprocessing import Process, Queue, Value , Array

# pkgs = [('lib.PSQL', 'psql', 'lib/PSQL.py'), ('lib.utils.Check', 'check', 'lib/utils/Check.py'), \
#         ('lib.Config', 'config', 'lib/Config.py'), \
#         ('utils.Log', 'Log', 'lib/utils/Log.py'), ('utils.Report', 'Report', 'lib/utils/Report.py')]

#for pkg in pkgs:
#    try:
#        exec('from %s import %s ' % (pkg[0], pkg[1]))
#    except Import Error:
#        sys.stderr.write('Workload need %s in %s' % ([pkg[1], pkg[2]))

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

class Workload(object):
    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, validation):
        # initialize common propertities for workload
        self.cs_id = cs_id
        self.us_id = 0
        self.tr_id = 0
        self.s_id = 0
        self.validation = validation
        self.continue_flag = True

        self.workload_name = workload_specification['workload_name'].strip()
        self.database_name = workload_specification['database_name'].strip()

        # prepare report directory for workload
        if report_directory != '':
            self.report_directory = os.path.join(report_directory, self.workload_name)
        else:
            print 'Test report directory is not available before preparing report directory for workload %s' % (self.workload_name)
            exit(2)
        os.system('mkdir -p %s' % (self.report_directory))
        # set output log and report
        self.output_file = os.path.join(self.report_directory, 'output.csv')

        # set report.sql file
        self.report_sql_file = report_sql_file
        
        self.user = workload_specification['user'].strip()
        # check us_id if exist
        if self.cs_id != 0:
            self.us_id = check.check_id(result_id = 'us_id', table_name = 'hst.users', search_condition = "us_name = '%s'" % (self.user))
            if self.us_id is None:
                sys.stderr.write('The db user name is wrong!\n')
                sys.exit(2)
        
        try:
            self.load_data_flag = str(workload_specification['load_data_flag']).strip().upper()
        except Exception, e:
            self.load_data_flag = 'FALSE'
        # check flag for data loading
        if self.load_data_flag == 'TRUE':
            self.load_data_flag = True
        elif self.load_data_flag == 'FALSE':
            self.load_data_flag = False
        else:
            self.output('ERROR: Invalid value for data loading flag in workload %s: %s. Must be TRUE/FALSE.' % (self.workload_name, self.load_data_flag))
            exit(-1)
        
        try:
            self.run_workload_flag = str(workload_specification['run_workload_flag']).strip().upper()
        except Exception, e:
            self.run_workload_flag = 'FALSE'
        # check flag for workload execution
        if self.run_workload_flag == 'TRUE':
            self.run_workload_flag = True
        elif self.run_workload_flag == 'FALSE':
            self.run_workload_flag = False
        else:
            self.output('ERROR: Invalid value for data loading flag in workload %s: %s. Must be TRUE/FALSE.' % (self.workload_name, self.run_workload_flag))
            exit(-1)
        
        # get table setting and set table suffix, sql suffix, check_condition, and wl_values
        self.get_table_setting(workload_specification)

        self.run_workload_mode = workload_specification['run_workload_mode'].strip().upper()
        
        try:
            self.num_concurrency = int(str(workload_specification['num_concurrency']).strip())
        except Exception, e:
            self.num_concurrency = 1

        try:
            self.num_iteration = int(str(workload_specification['num_iteration']).strip())
        except Exception, e:
            self.num_iteration = 1
        
        self.check_condition += ' and wl_iteration = %d and wl_concurrency = %d' % (self.num_iteration, self.num_concurrency)
        self.wl_values += ', %d, %d' % (self.num_iteration, self.num_concurrency)

        # set workload source directory
        self.workload_directory = workload_directory

        # prepare result directory for workload
        self.result_directory = self.workload_directory + os.sep + 'queries_result'
        os.system('mkdir -p %s' % (self.result_directory))
        os.system('rm -rf %s/*' % (self.result_directory))

        self.ans_directory = self.workload_directory + os.sep + 'queries_ans_%dg' % (self.scale_factor)
        if not os.path.exists(self.ans_directory):
            self.output('%s ans_directory:%s does not exists' % (self.workload_name, self.ans_directory))

        # check mode for workload execution
        if self.run_workload_mode == 'SEQUENTIAL':
            pass
        elif self.run_workload_mode == 'RANDOM':
            pass
        else:
            self.output('ERROR: Invalid value for mode of workload execution in workload %s: %s. Mast be SEQUENTIAL/RANDOM.' % (self.workload_name, self.run_workload_mode))
            exit(-1)
        self.check_condition += " and wl_query_order = '%s'" % (self.run_workload_mode)
        self.wl_values += ", '%s'" % (self.run_workload_mode)

        
        if self.cs_id != 0:
            # check wl_id if exist
            self.wl_id = check.check_id(result_id = 'wl_id', table_name = 'hst.workload', search_condition = self.check_condition)
            if self.wl_id is None:
                check.insert_new_record(table_name = 'hst.workload',
                                        col_list = 'wl_name, wl_catetory, wl_data_volume_type, wl_data_volume_size, wl_appendonly, wl_orientation, wl_row_group_size, wl_page_size, wl_compression_type, wl_compression_level, wl_partitions, wl_iteration, wl_concurrency, wl_query_order',
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
            self.tr_id = check.get_max_id(result_id = 'tr_id', table_name = 'hst.test_run')

        # should always run the workload by default
        self.should_stop = False

    def get_table_setting(self, workload_specification):
        # init tpch specific configuration such as tpch table_settings
        self.check_condition = "wl_name = '%s' and wl_catetory = '%s'" % (self.workload_name, self.workload_name.split('_')[0].upper())
        
        self.wl_values = "'%s', '%s'" % (self.workload_name, self.workload_name.split('_')[0].upper())
        
        ts = workload_specification['table_setting']

        # Calculate scale factor for workload
        self.data_volume_type = ts['data_volume_type'].upper()
        self.check_condition += " and wl_data_volume_type = '%s'" % (self.data_volume_type)
        self.wl_values += ", '%s'" % (self.data_volume_type)
        
        
        self.data_volume_size = ts['data_volume_size']
        self.check_condition += " and wl_data_volume_size = %d" % (self.data_volume_size)
        self.wl_values += ", %d" % (self.data_volume_size)
        
        # Need to make it univerally applicable instead of hard-code number of segments
        self.nsegs =  config.getNPrimarySegments()

        self.scale_factor = 1
        if self.data_volume_type == 'TOTAL':
            self.scale_factor = self.data_volume_size
        elif self.data_volume_type == 'PER_NODE':
            nnodes = len(config.getSegHostNames())
            self.scale_factor = self.data_volume_size * nnodes
        elif self.data_volume_type == 'PER_SEGMENT':
            self.scale_factor = self.data_volume_size * self.nsegs
        else:
            self.output('Error in calculating data volumn for workloads %s: data_volume_type=%s, data_volume_size=%s' % (self.workload_name, self.data_volume_type, self.data_volume_size))
            exit(-1)

        # Parse table setting
        ts_keys = ts.keys()

        self.append_only = True
        if 'append_only' in ts_keys:
            self.append_only = ts['append_only']
            assert self.append_only in [True, False]
        self.check_condition += " and wl_appendonly = %s" % (str(self.append_only).upper())
        self.wl_values += ", '%s'" % (str(self.append_only).upper())

        self.orientation = 'ROW'
        if 'orientation' in ts_keys:
            self.orientation = ts['orientation'].upper()
            assert self.orientation in ['PARQUET', 'ROW', 'COLUMN']
        self.check_condition += " and wl_orientation = '%s'" % (self.orientation)
        self.wl_values += ", '%s'" % (self.orientation)

        self.row_group_size = None
        if 'row_group_size' in ts_keys:
            self.row_group_size = int(ts['row_group_size'])
            self.check_condition += ' and wl_row_group_size = %d' % (self.row_group_size) 
            self.wl_values += ', %d' % (self.row_group_size)
        else:
            self.wl_values += ', NULL'

        self.page_size = None
        if 'page_size' in ts_keys:
            self.page_size = int(ts['page_size'])
            self.check_condition += ' and wl_page_size = %d' % (self.page_size)
            self.wl_values += ', %d' % (self.page_size)
        else:
            self.wl_values += ', NULL'

        self.compression_type = None
        if 'compression_type' in ts_keys:
            self.compression_type = ts['compression_type'].upper()
            self.check_condition += " and wl_compression_type = '%s'" % (self.compression_type)
            self.wl_values += ", '%s'" % (self.compression_type)
        else:
            self.wl_values += ", ''"
        
        self.compression_level = None
        if 'compression_level' in ts_keys:
            self.compression_level = int(ts['compression_level'])
            self.check_condition += ' and wl_compression_level = %d' % (self.compression_level)
            self.wl_values += ', %d' % (self.compression_level)
        else:
            self.wl_values += ', NULL'

        self.partitions = 0
        if 'partitions' in ts_keys:
            self.partitions = int(ts['partitions'])
        self.check_condition += ' and wl_partitions = %d' % (self.partitions)
        self.wl_values += ', %d' % (self.partitions)

        # prepare name with suffix for table and corresponding sql statement to create it
        tbl_suffix = ''
        sql_suffix = ''

        if self.append_only in [None, True]:
            tbl_suffix = tbl_suffix + 'ao'
            sql_suffix = sql_suffix + 'appendonly = true'
        else:
            tbl_suffix = tbl_suffix + 'heap'
            sql_suffix = sql_suffix + 'appendonly = false'

        tbl_suffix = tbl_suffix + '_' + self.orientation
        sql_suffix = sql_suffix + ', '+ 'orientation = ' + self.orientation

        if self.orientation == 'PARQUET':
            sql_suffix = sql_suffix + ', ' + 'pagesize = %s, rowgroupsize = %s' % (self.page_size, self.row_group_size)

        if self.compression_type is not None:
            if self.compression_level is not None:
                tbl_suffix = tbl_suffix + '_' + self.compression_type + str(self.compression_level)
                sql_suffix = sql_suffix + ', ' + 'compresstype = ' + self.compression_type 
                sql_suffix = sql_suffix + ', ' + 'compresslevel = ' + str(self.compression_level)
            else:
                tbl_suffix = tbl_suffix + '_' + self.compression_type
                sql_suffix = sql_suffix + ', ' + 'compresstype = ' + self.compression_type
        else:
            tbl_suffix = tbl_suffix + '_nocomp'

        if self.partitions > 0:
            tbl_suffix += '_part'
        else:
            tbl_suffix += '_nopart'

        self.tbl_suffix = tbl_suffix.lower()
        self.sql_suffix = sql_suffix

    def setup(self):
        '''Setup prerequisites for workload'''
        pass

    def output(self, msg):
        Log(self.output_file, msg)

    def report_sql(self, msg):
        Report(self.report_sql_file, msg)

    def validation_query_result(self, ans_file, result_file):
        import commands
        (status, output) = commands.getstatusoutput('diff %s %s' % (ans_file, result_file) )
        if status == 0:
            if output == '':
                return True
            else:
                with open(result_file.split('.')[0] + '.diff', 'w') as f:
                    f.write('diff between %s and %s: ' % (ans_file, result_file) + '\n' + output)
                return False
        else:
            with open(result_file.split('.')[0] + '.diff', 'w') as f:
                    f.write('diff error:' + str(status) + ' output: '+ output)
            return False

    def load_data(self):
        '''Load data for workload'''
        pass

    def run_queries(self, iteration, stream):
        queries_directory = self.workload_directory + os.sep + 'queries'
        if not os.path.exists(queries_directory):
            print 'Not find the queries_directory for %s' % (self.workload_name)
            sys.exit(2)

        query_files = [file for file in os.listdir(queries_directory) if file.endswith('.sql')]
        if self.run_workload_mode == 'SEQUENTIAL':
            query_files = sorted(query_files)
        else:
            random.shuffle(query_files)

        # run all sql files in queries directory
        for qf_name in query_files:
            if self.continue_flag:
                if self.run_workload_flag:
                    with open(os.path.join(queries_directory, qf_name),'r') as f:
                        query = f.read()
                    query = query.replace('TABLESUFFIX', self.tbl_suffix)
                    with open('run_query_tmp.sql','w') as f:
                        f.write(query)

                    self.output(query)
                    beg_time = datetime.now()
                    (ok, result) = psql.runfile(ifile = 'run_query_tmp.sql', dbname = self.database_name, flag = '-t -A')
                    end_time = datetime.now()
                    
                    if ok:
                        status = 'SUCCESS'
                        with open(self.result_directory + os.sep + qf_name.split('.')[0] + '.output', 'w') as f:
                            f.write(str(result[0]))
                        with open(self.result_directory + os.sep + qf_name.split('.')[0] + '.output', 'r') as f:
                            result = f.read()
                            md5code = hashlib.md5(result).hexdigest()
                        with open(self.result_directory + os.sep + qf_name.split('.')[0] + '.md5', 'w') as f:
                            f.write(md5code)
                        
                        if self.validation:
                            ans_file = self.ans_directory + os.sep + qf_name.split('.')[0] + '.ans'
                            md5_file = self.ans_directory + os.sep + qf_name.split('.')[0] + '.md5'
                            if os.path.exists(ans_file):
                                self.output('Validation use ans file')
                                if not self.validation_query_result(ans_file = ans_file, result_file = self.result_directory + os.sep + qf_name.split('.')[0] + '.output'):
                                    status = 'ERROR'
                            elif os.path.exists(md5_file):
                                self.output('Validation use md5 file')
                                if not self.validation_query_result(ans_file = md5_file, result_file = self.result_directory + os.sep + qf_name.split('.')[0] + '.md5'):
                                    status = 'ERROR'
                            else:
                                self.output('No answer file')
                                status = 'ERROR'
                    else:
                        status = 'ERROR'
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
            beg_time = str(beg_time).split('.')[0]
            end_time = str(end_time).split('.')[0]
            self.output('   Execution=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (qf_name.replace('.sql', ''), iteration, stream, status, duration))
            self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, 'Execution', '%s', %d, %d, '%s', '%s', '%s', %d, NULL, NULL, NULL);" 
                % (self.tr_id, self.s_id, qf_name.replace('.sql', ''), iteration, stream, status, beg_time, end_time, duration))
                
                
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

    def vacuum_analyze(self):
        self.output('-- Start vacuum analyze')     
        
        if self.continue_flag:
            if self.load_data_flag:
                sql = 'VACUUM ANALYZE;'
                self.output(sql)
                beg_time = datetime.now()
                (ok, result) = psql.runcmd(cmd = sql, dbname = self.database_name)
                end_time = datetime.now()
                self.output('RESULT: ' + str(result))

                if ok and str(result).find('ERROR') == -1:
                    status = 'SUCCESS'
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
        beg_time = str(beg_time).split('.')[0]
        end_time = str(end_time).split('.')[0]
 
        self.output('   VACUUM ANALYZE   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (1, 1, status, duration))
        self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, 'Vacuum_analyze', 'Vacuum_analyze', 1, 1, '%s', '%s', '%s', %d, NULL, NULL, NULL);" 
            % (self.tr_id, self.s_id, status, beg_time, end_time, duration))
        
        self.output('-- Complete vacuum analyze')

    def clean_up(self):
        pass

    def execute(self):
        pass
