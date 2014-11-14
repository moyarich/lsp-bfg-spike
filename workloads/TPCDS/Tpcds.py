import os
import sys
import commands, socket, shutil
from datetime import datetime, date, timedelta

try:
    from workloads.Workload import *
except ImportError:
    sys.stderr.write('TPCDS needs workloads/Workload.py\n')
    sys.exit(2)

try:
    from pygresql import pg
except ImportError:
    sys.stderr.write('TPCDS needs pygresql\n')
    sys.exit(2)

try:
    from lib.PSQL import psql
except ImportError:
    sys.stderr.write('TPCDS needs psql in lib/PSQL.py\n')
    sys.exit(2)

try:
    from lib.Config import config
except ImportError:
    sys.stderr.write('TPCDS needs config in lib/Config.py\n')
    sys.exit(2)

try:
    import gl
except ImportError:
    sys.stderr.write('TPCDS needs gl.py in lib/\n')
    sys.exit(2)

command_template = """
import subprocess, os, time

children = %s
parallel_setting = %s;
scale = %s
data_dir = '%s'


process_pool = []
process_name = {}

for child in children:
    cmd = './dsdgen -scale ' + str(scale) + ' -dir ' + data_dir + ' -parallel ' + str(parallel_setting) + ' -child ' + str(child)
    process = subprocess.Popen(cmd.split(' '))
    process_pool.append(process)
    process_name[process] = 'Process_' + str(child) + '_' + str(parallel_setting)
    
while True:
    finished_pool = []
    finish_generating = True
    for process in process_pool:
        if process.poll() is None:
            finish_generating = False;
            break;
        else:
            finished_pool.append(process)
    # remove finished
    for p in finished_pool:
        process_pool.remove(p)

    if finish_generating:
        break
    else:
        # 30 seconds
        time.sleep(30)

# write dat files
with open('dat_files.txt','w') as f:
    files = []
    for file in os.listdir(data_dir):
        if file.endswith('.dat'):
            files.append(file)
    f.write('\\n'.join(files))
    f.write('\\n')
"""

class Tpcds(Workload):
    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, user): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, user)
        self.hostfile_master = os.path.join(self.tmp_folder, 'hostfile_master')
        self.hostfile_seg = os.path.join(self.tmp_folder, 'hostfile_seg')
        self.seg_hostname_list = None
        self.seg_host_num = 1
        self.tmp_tpcds_folder = '/data/tmp/tpcds_loading/'
        self.tmp_tpcds_data_folder = '/data/tmp/tpcds_loading/data'

    def setup(self):
        pass
        
    def load_data(self):
        self.output('\n-- Start loading data')

        if self.load_data_flag:
            cmd = 'drop database if exists %s;' % (self.database_name)
            (ok, output) = psql.runcmd(cmd = cmd)
            if not ok:
                print cmd
                print '\n'.join(output)
                sys.exit(2)
            self.output(cmd)
            self.output('\n'.join(output))

            cmd = 'create database %s;' % (self.database_name)
            (ok, output) = psql.runcmd(cmd = cmd, username = self.user)
            if not ok:
                print cmd
                print '\n'.join(output)
                sys.exit(2)
            self.output(cmd)
            self.output('\n'.join(output))

        tables = ['call_center', 'catalog_page', 'catalog_returns', 'catalog_sales', 'customer', 'customer_address',
        'customer_demographics', 'date_dim', 'household_demographics', 'income_band', 'inventory', 'item',
        'promotion', 'reason', 'ship_mode', 'store', 'store_returns', 'store_sales',
        'time_dim', 'warehouse','web_page', 'web_returns', 'web_sales', 'web_site']
        
        if not self.load_data_flag:
            beg_time = str(datetime.now()).split('.')[0]
            for table_name in tables:
                self.output('   Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, 'SKIP', 0))
                self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, 'Loading', '%s', 1, 1, 'SKIP', '%s', '%s', 0, NULL, NULL, NULL);" 
                    % (self.tr_id, self.s_id, table_name, beg_time, beg_time))
        else:
            self.load_setup()
            self.load_generate()
            self.load_loading(tables = tables)
        self.output('-- Complete loading data')      
    
    
    def load_setup(self):
        self.output('\n--Check files hostfile_master and hostfile_seg')
        self._prepare_hostfile()
        
        self.output('\n--Check files dsdgen and tpcds.idx')
        self._prepare_data_gen()

    def _prepare_hostfile(self):
        # prep hostfile_master
        master_host_name = config.getMasterHostName()
        with open(self.hostfile_master, 'w') as f:
            f.write(str(master_host_name) + '\n')

        # prep hostfile_seg
        self.seg_hostname_list = config.getSegHostNames()
        self.seg_host_num = len(self.seg_hostname_list)
        with open(self.hostfile_seg, 'w') as f:
            f.write('\n'.join(self.seg_hostname_list))
        
    def _prepare_data_gen(self):
        # Check if dsdgen file exists, else make it
        if os.path.exists(os.path.join(self.workload_directory, 'dsdgen')):
            pass
        else:
            data_gen_folder = os.path.join(self.workload_directory, 'data_gen')
            if not os.path.exists(data_gen_folder):
                print('data_gen folder does not exist. Exit. ')
                sys.exit(2)
            
            command = 'cd %s; make clean; make' % (data_gen_folder)
            (status, output) = commands.getstatusoutput(command)    
            if status != 0:
                print('Error happens in compile data gen code.')
                print('output: %s' % (output))
                sys.exit(2)
            else:
                print('Compile data gen code.')
                command2 = 'cd %s; cp dsdgen %s' % (data_gen_folder, self.workload_directory)
                (s2, o2) = commands.getstatusoutput(command2)
                if s2 != 0:
                    print('Error happen in copy dsdgen.')
                    sys.exit(2)
                else:
                    print('Copy dsdgen to pwd.')
        
        # Check if tpcds_idx file exists
        self.tpcds_idx = os.path.join(self.workload_directory, 'tpcds.idx')
        if not os.path.exists(self.tpcds_idx):
            print('tpcds.idx does not exist. Exit. ')
            sys.exit(2)
            

    def load_generate(self):
        """
        copy dsdgen to each host and generate data in parallel 
        """
        self.output('\n--Prepare tmp folder')
        self._prepare_tmp_folder()
        
        self.output('\n--Scp dsdgen and tpcds.idx to hostfile_seg')
        self._scp_data_gen_code()
        
        self.output('\n-- Generate data on every segments')
        self._data_gen_segment()
    
    def _prepare_tmp_folder(self):
        # mkdir in each segment
        cmd = "gpssh -f %s -e 'mkdir -p %s; mkdir -p %s'" % (self.hostfile_seg, self.tmp_tpcds_folder, self.tmp_tpcds_data_folder)
        (status, output) = commands.getstatusoutput(cmd)
        if status != 0:
            print('gpssh to prepare folder failed. ')
            print(cmd)
            print(output)
            sys.exit(2)
        else:
            self.output('tmp folder prepared.')

    def _scp_data_gen_code(self):
        cmd1 = 'gpscp -f %s %s =:%s' % (self.hostfile_seg, os.path.join(self.workload_directory, 'dsdgen'), self.tmp_tpcds_folder)
        cmd2 = 'gpscp -f %s %s =:%s' % (self.hostfile_seg, self.tpcds_idx, self.tmp_tpcds_folder)
        cmd3 ="gpssh -f %s -e 'chmod 755 %s; chmod 755 %s'" \
        % (self.hostfile_seg, os.path.join(self.tmp_tpcds_folder, 'dsdgen'), os.path.join(self.tmp_tpcds_folder, 'tpcds.idx'))
        
        (s1, o1) = commands.getstatusoutput(cmd1)
        if s1 != 0:
            print('gpscp dsdgen failed.')
            print(cmd1)
            print(o1)
            sys.exit(2);
            
        (s2, o2) = commands.getstatusoutput(cmd2)
        if s2 != 0:
            print('gpscp tpcds.idx failed.')
            print(cmd2)
            print(o2)
            sys.exit(2);
        
        (s3, o3) = commands.getstatusoutput(cmd3)
        if s3 != 0:
            print('chmod dsdgen and tpcds.idx failed.')
            print(cmd3)
            print(o3)
            sys.exit(2)
       
    def _data_gen_segment(self):
        total_paralle = self.nsegs
        seg_num = self.nsegs / self.seg_host_num
        count = 1
        for cur_host in self.seg_hostname_list:
            self.output('generate script for %s' % (cur_host))
            # generate  for each segment.
            child = '[' + str(count)
            count += 1
            i = 1
            while(i < seg_num):
                child = child + ',' + str(count)
                i += 1
                count += 1
            child += ']'

            python_script_name = 'tpcds_generate_data.py'
            python_script_file = os.path.join(self.tmp_folder, python_script_name)
            with open(python_script_file, 'w') as f:
                f.write(command_template
                    % (child, total_paralle, self.scale_factor, self.tmp_tpcds_data_folder))
            
            cmd1 = 'gpscp -h %s %s =:%s'%(cur_host, python_script_file, self.tmp_tpcds_folder)
            (s1, o1) = commands.getstatusoutput(cmd1)
            if s1 != 0:
                print('Error happen in scp seg python script.')
                print(o1)
                sys.exit(2)
            
            cmd2 = "gpssh -h %s -e 'cd %s;chmod 755 %s'"%(cur_host,self.tmp_tpcds_folder, python_script_name)
            (s2, o2) = commands.getstatusoutput(cmd2)
            if s2 != 0:
                print('Error happen in chmod seg python script.')
                print(o2)
                sys.exit(2)
                
        cmd = 'cd %s; python %s' %(self.tmp_tpcds_folder, python_script_name)        
        command = "gpssh -f %s -e '%s'" % (self.hostfile_seg, cmd)
        (status, output) = commands.getstatusoutput(command)
        if status != 0:
            print('execute generate script in segment failed ')
            print(command)
            print(output)
            sys.exit(2)
        else:
            self.output('execute generate script in segment %s succeed' % (str(self.seg_hostname_list)))

    def load_loading(self, tables):
        self.output('\n--Start gpfdist')
        self._start_gpfdist()
        cmd = "gpssh -f %s -e 'ps -ef | grep gpfdist'" % (self.hostfile_seg)   
        (status, output) = commands.getstatusoutput(cmd)
        self.output(output)

        data_directory = self.workload_directory + os.sep + 'data'
        if not os.path.exists(data_directory):
            self.output('ERROR: Cannot find DDL to create tables for TPCDS: %s does not exists' % (data_directory))
            sys.exit(2)

        gpfdist_map = {}
        for item in tables:
            gpfdist_map[item] = []
        
        for cur_host in self.seg_hostname_list:
            cmd = "gpssh -h %s -e 'cat %s'" % (cur_host, os.path.join(self.tmp_tpcds_folder, 'dat_files.txt'))
            dat_file_suffix = '.dat'
            
            (status, output) = commands.getstatusoutput(cmd)
            if status != 0:
                print('Error happen in ls data dir in %s' % (cur_host))
                print(output)
                sys.exit(2)
            else:
                lines = output.split('\n')
                for line in lines:
                    if line.find(dat_file_suffix) != -1:
                        file_name = line.split(' ')[-1].strip()
                        tmp_name = file_name[:file_name.rindex('_')]
                        table_name = tmp_name[:tmp_name.rindex('_')]
                        if table_name not in gpfdist_map.keys():
                            if table_name.find('dbgen_version') == -1:
                                print('Error: %s not find in gpfdist_map' % (table_name))
                                sys.exit(2)
                        else:
                            gpfdist_map[table_name].append("'gpfdist://%s:%s/%s'" % (cur_host, self.gpfdist_port, file_name))

        for table_name in tables:
            if len(gpfdist_map[table_name]) == 0:
                print(table_name + ' has no data files, generate data wrong in workload ' + self.workload_name)
            self.output(table_name + ':' + str(len(gpfdist_map[table_name])) + ' data files')
        
        self.output('\n--Start loading data into tables')
        # run all sql in each loading data file
        for table_name in tables:
            if self.continue_flag:
                with open(os.path.join(data_directory, table_name + '.sql'), 'r') as f:
                    cmd = f.read()
                cmd = self.replace_sql(sql = cmd, table_name = table_name)
                location = "LOCATION(" + ','.join(gpfdist_map[table_name]) + ")"
                cmd = cmd.replace('LOCATION', location)
                with open(self.tmp_folder + os.sep + table_name + '.sql', 'w') as f:
                    f.write(cmd)

                self.output(cmd)    
                beg_time = datetime.now()
                (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + table_name + '.sql', dbname = self.database_name)
                end_time = datetime.now()
                self.output('\n'.join(result))
                
                if ok and str(result).find('ERROR') == -1 and str(result).find('FATAL') == -1: 
                    status = 'SUCCESS'    
                else:
                    status = 'ERROR'
                    beg_time = datetime.now()
                    end_time = beg_time
                    self.continue_flag = False
            else:
                status = 'ERROR'
                beg_time = datetime.now()
                end_time = beg_time

            duration = end_time - beg_time
            duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds /1000
            beg_time = str(beg_time).split('.')[0]
            end_time = str(end_time).split('.')[0]
            
            self.output('   Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, status, duration))
            self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, 'Loading', '%s', 1, 1, '%s', '%s', '%s', %d, NULL, NULL, NULL);" 
                % (self.tr_id, self.s_id, table_name, status, beg_time, end_time, duration))

    def _start_gpfdist(self):       
        # find port 
        self.gpfdist_port = self._getOpenPort()
        self.output('GPFDIST PORT: %s' % self.gpfdist_port)

        gphome = os.getenv('GPHOME')
        source_path = gphome + os.sep + 'greenplum_path.sh'

        cmd = 'source %s; gpfdist -d %s -p %s -l %s/fdist.%s.log &' \
        % (source_path, self.tmp_tpcds_data_folder, self.gpfdist_port, self.tmp_tpcds_data_folder, self.gpfdist_port)        
        command = "gpssh -f %s -e '%s'" % (self.hostfile_seg, cmd)
        self.output(command)
        (status, output) = commands.getstatusoutput(command)
        if status != 0:
            print ('gpfdist on segments failed. ')
            print (output)
            sys.exit(2)
        else:
            self.output(output)
            self.output('gpfdist on segments succeed. ')
    
    def _getOpenPort(self, port = 8050):
        defaultPort = port
        tryAgain = True
        s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
        s.bind( ( "localhost",0) ) 
        addr, defaultPort = s.getsockname()
        s.close()
        return defaultPort
    


    def clean_up(self):
        self.output('\n--Stop gpfdist')
        self._stop_gpfdist()
        self.output('\n--Delete tmp data folder')
        self._delete_data()
        
    def _stop_gpfdist(self):
        cmd = "ps -ef|grep gpfdist|grep %s|grep -v grep|awk \'{print $2}\'|xargs kill -9" % (self.gpfdist_port)
        command = "gpssh -f %s -e \"%s\"" % (self.hostfile_seg, cmd)
        self.output(command)
        (status, output) = commands.getstatusoutput(command)
        self.output(output)
        self.output('kill gpfdist on segments succeed. ')

    def _delete_data(self):
        # mkdir in each segment
        cmd = "gpssh -f %s -e 'cd %s; rm -rf *'" % (self.hostfile_seg, self.tmp_tpcds_folder)
        (status, output) = commands.getstatusoutput(cmd)
        if status != 0:
            print('gpssh to delete data folder failed. ')
            print(output)
            sys.exit(2)
        else:
            self.output('delete data folder succeed.')



    def replace_sql(self, sql, table_name):
        if gl.suffix:
            sql = sql.replace('TABLESUFFIX', self.tbl_suffix)
        else:
            sql = sql.replace('_TABLESUFFIX', '')

        if self.sql_suffix != '':
            sql = sql.replace('SQLSUFFIX', self.sql_suffix)
        else:
            sql = sql.replace('WITH (SQLSUFFIX)', self.sql_suffix)
            
        sql = sql.replace('SCALEFACTOR', str(self.scale_factor))
        sql = sql.replace('NUMSEGMENTS', str(self.nsegs))

        if self.distributed_randomly:
            import re
            old_string = re.search(r'DISTRIBUTED BY\(\S+\)', sql).group()
            sql = sql.replace(old_string, 'DISTRIBUTED RANDOMLY')

        tables = [ 'catalog_returns', 'catalog_sales', 'date_dim',  'inventory', 'store_returns', 'store_sales', 'web_returns', 'web_sales']
        if (self.partitions == 0 or self.partitions is None) and (table_name in tables):
            beg_index = sql.index('PARTITION BY')
            end_index = sql.index(';', beg_index, )
            partitions_string = sql[beg_index:end_index]
            sql = sql.replace(partitions_string, '') 

        return sql
    
    def execute(self):
        self.output('-- Start running workload %s' % (self.workload_name))

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

