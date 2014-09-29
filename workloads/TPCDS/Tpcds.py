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

command_template = """
import subprocess, os, time

children = [%s, %s, %s, %s]
parallel_setting = %s;
scale = %s
data_dir = '%s'


process_pool = []
process_name = {}

for child in children:
    cmd = './dsdgen -scale '+str(scale)+' -dir '+data_dir+' -parallel '+str(parallel_setting)+' -child '+str(child)
    print 'in localhost cmd = ' + cmd
    process = subprocess.Popen(cmd.split(' '))
    process_pool.append(process)
    process_name[process] = 'Process_' + str(child) + '_' + str(parallel_setting)
    
with open('status.txt','w') as f:
    f.write('generating')
    
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
        # 20 minutes
        time.sleep(30)
        
# update status
with open('status.txt','w') as f:
    f.write('done\\n')

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
    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id)
        self.pwd = os.path.abspath(os.path.dirname(__file__))
        self.schema_folder = os.path.join(self.pwd, 'schema')
        self.hostfile_master = os.path.join(self.pwd, 'hostfile_master')
        self.hostfile_seg = os.path.join(self.pwd, 'hostfile_seg')
        self.seg_hostname_list = None
        self.host_num = 1
        self.tmp_tpcds_folder = '/data/tmp/tpcds_loading/'
        self.tmp_tpcds_data_folder = '/data/tmp/tpcds_loading/data'

    def setup(self):
        pass

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

    def _check_hostfile(self):
        # add hostfile_master
        master_host_name = config.getMasterHostName()
        with open(self.hostfile_master, 'w') as f:
            f.write(str(master_host_name) + '\n')

        # add hostfile_seg
        self.seg_hostname_list = config.getSegHostNames()
        self.host_num = len(self.seg_hostname_list)
        seg_names = ''
        for seg_hostname in self.seg_hostname_list:
            seg_names = seg_names + seg_hostname + '\n'
        with open(self.hostfile_seg, 'w') as f:
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
        if os.path.exists(os.path.join(self.pwd, 'dsdgen')):
            print 'dsdgen already exist.'
            return
        
        command = 'cd %s; make clean; make' % (self.data_gen_folder)
        (status, output) = commands.getstatusoutput(command)    
        if status != 0:
            print('Error happens in compile data gen code.')
            print('output: %s' % (output))
            sys.exit(2)
        else:
            print('Compile data gen code.')
            command2 = 'cd %s; cp dsdgen %s' % (self.data_gen_folder,self.pwd)
            (s2, o2) = commands.getstatusoutput(command2)
            if s2 != 0:
                print('Error happen in copy dsdgen.')
                sys.exit(2)
            else:
                print('Copy dsdgen to pwd.')
    
    

    def load_generate(self):
        """
        copy dsdgen to each host and generate data in parallel 
        """
        self._prepare_folder()
        self._scp_data_gen_code()
        self._data_gen_segment()
    
    def _prepare_folder(self):
        # mkdir in each segment
        cmd = "gpssh -f %s -e 'mkdir -p %s; mkdir -p %s'" % (self.hostfile_seg, self.tmp_tpcds_folder, self.tmp_tpcds_data_folder)
        print ('Execute: ' + cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if status != 0:
            print('gpssh to prepare folder failed. ')
            print(output)
            sys.exit(2)
        else:
            print('folder prepared.')

    def _scp_data_gen_code(self):
        cmd1 = 'gpscp -f %s %s =:%s' % (self.hostfile_seg, os.path.join(self.pwd, 'dsdgen'), self.tmp_tpcds_folder)
        cmd2 = 'gpscp -f %s %s =:%s' % (self.hostfile_seg, self.tpcds_idx, self.tmp_tpcds_folder)
        cmd3 ="gpssh -f %s -e 'chmod 755 %s; chmod 755 %s'" \
        % (self.hostfile_seg, os.path.join(self.tmp_tpcds_folder, 'dsdgen'), os.path.join(self.tmp_tpcds_folder, 'tpcds.idx'))
        
        print('Execute: ' + cmd1)
        (s1, o1) = commands.getstatusoutput(cmd1)
        if s1 != 0:
            print('gpscp dsdgen failed.')
            print(o1)
            sys.exit(2);
        else:
            print('gpscp dsdgen finished.')
            
        print('Execute: ' + cmd2)
        (s2, o2) = commands.getstatusoutput(cmd2)
        if s2 != 0:
            print('gpscp tpcds.idx failed.')
            print(o2)
            sys.exit(2);
        else:
            print('gpscp tpcds.idx finished.')
        
        print('Execute: ' + cmd3)    
        (s3, o3) = commands.getstatusoutput(cmd3)
        if s3 != 0:
            print('chmod dsdgen and tpcds.idx failed.')
            print(o3)
            sys.exit(2)
       
    def _data_gen_segment(self):
        total_paralle = self.host_num * 4
        seg_hosts = []
        for cur_host in self.seg_hostname_list:
            print ('generate script for %s' % (cur_host))
            # generate python command for each segment.
            child_1 = 1
            child_2 = 2
            child_3 = 3
            child_4 = 4
            python_script_base_name = cur_host+'.py'
            host_python_script = os.path.join(self.pwd, cur_host+'.py')
            with open(host_python_script, 'w') as f:
                f.write(command_template
                    % (child_1, child_2, child_3, child_4, total_paralle, self.scale_factor, self.tmp_tpcds_data_folder))
            
            cmd1 = 'gpscp -h %s %s =:%s'%(cur_host, host_python_script, self.tmp_tpcds_folder)
            (s1, o1) = commands.getstatusoutput(cmd1)
            if s1 != 0:
                print('Error happen in scp seg python script.')
                print(o1)
                sys.exit()
            
            cmd2 = "gpssh -h %s -e 'cd %s;chmod 755 %s'"%(cur_host,self.tmp_tpcds_folder, python_script_base_name)
            (s2, o2) = commands.getstatusoutput(cmd2)
            if s2 !=0:
                print('Error happen in chmod seg python script.')
                print(o2)
                sys.exit()
                
            cmd = 'cd %s; python %s > ./%s 2>&1' %(self.tmp_tpcds_folder, python_script_base_name, python_script_base_name + '.out')        
            command = "gpssh -h %s -e '%s'" % (cur_host, cmd)
            (status, output) = commands.getstatusoutput(command)
            if status != 0:
                print ('execute generate script in segment failed ')
                print (output)
                sys.exit()
            else:
                print (output)
                print ('execute generate script in segment succeed')
            seg_hosts.append(cur_host)
            
            
        #wait for finish
        total_minutes = 0
        check_cmd_template = "gpssh -h %s -e 'cat " + os.path.join(self.tmp_tpcds_folder,'status.txt') + "'"
        while True:                
            finished_pool = []
            finish_generating = True
            for cur_host in seg_hosts:
                # check if that is done
                check_cmd =check_cmd_template % cur_host
                (s1, o1) = commands.getstatusoutput(check_cmd)
                if s1 != 0:
                    print ('check status.txt failed.')
                    print(o1)
                    sys.exit()
                
                if o1.find('done') != -1:
                    print('%s finished' % (cur_host))
                    finished_pool.append(cur_host)
                else:
                    finish_generating = False
                    break
            
            # remove finished
            for p in finished_pool:
                seg_hosts.remove(p)

            if finish_generating:
                print ('Data generation finished in all segments.')
                print ('total generation time: %s minutes' % (total_minutes))
                break
            else:
                print ('Data generation still going on. Wait another 0.5 minutes')
                time.sleep(30)
                total_minutes += 0.5       

    


    def load_loading(self):
        """
        loading tpcds
        """
        self._create_schema()
        self._start_gpfdist()
        self._create_external_table()
        self._copy_data()

    def _create_schema(self):
        create_schema = ''
        if self.partitions > 0:
            create_schema = os.path.join(self.schema_folder,'ddl_tpcds_partition.sql')
        else:
            create_schema = os.path.join(self.schema_folder,'ddl_tpcds_no_partition.sql')
        
        command = 'psql -d %s -a -f %s' % (self.database_name, create_schema)
        print ('Execute: %s' % (command))
        (status, output) = commands.getstatusoutput(command)
        if status != 0:
            print ('Fail to create schema. ')
            print (output)
            sys.exit(2)
        else:
            print ('Successfully create schema.')

    def _start_gpfdist(self):       
        # find port 
        self.gpfdist_port = self._getOpenPort()
        print ('GPFDIST PORT: %s' % (self.gpfdist_port))
        
        cmd = 'gpfdist -d %s -p %s -l %s/fdist.%s.log &' \
        % (self.tmp_tpcds_data_folder, self.gpfdist_port, self.tmp_tpcds_data_folder, self.gpfdist_port)        
        command = "gpssh -f %s -e '%s'" %(self.hostfile_seg, cmd)
        print command
        (status, output) = commands.getstatusoutput(command)
        if status != 0:
            print ('gpfdist on segments failed. ')
            print (output)
            sys.exit(2)
        else:
            print (output)
            print ('gpfdist on segments succeed. ')   
    
    def _getOpenPort(self,port = 8050):
        defaultPort = port
        tryAgain = True
        s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
        s.bind( ( "localhost",0) ) 
        addr, defaultPort = s.getsockname()
        s.close()
        return defaultPort

    def _create_external_table(self):
        # find data files in each segment host     
        data_files = [
              'call_center',  #0
              'catalog_page',  #1
              'catalog_returns', #2
              'catalog_sales', #3
              'customer', #4
              'customer_address', #5
              'customer_demographics', #6
              'date_dim', #7
              'household_demographics', #8
              'income_band', #9
              'inventory', #10
              'item', #11
              'promotion', #12
              'reason', #13
              'ship_mode', #14
              'store',#15
              'store_returns',#16
              'store_sales',#17
              'time_dim',#18
              'warehouse',#19
              'web_page',#20
              'web_returns',#21
              'web_sales',#22
              'web_site',#23
              ]
        gpfdist_map = {}
        for item in data_files:
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
        
        for key in gpfdist_map.keys():
            print ('%s: %s dat files'%(key, len(gpfdist_map[key])))
        
        # modify the prep_external_table_script
        external_script = os.path.join(self.schema_folder, 'prep_external_tables2.sql')
        shutil.copyfile(os.path.join(self.schema_folder, 'prep_external_tables.sql'), external_script)
        for key in gpfdist_map.keys():
            self.sed('LOCATION_%s_ext'%key,"LOCATION("+','.join(gpfdist_map[key])+")", external_script)
        
        load_external_command = 'psql -d %s -a -f %s'%(self.database_name, external_script)
        print('Execute command: ' + load_external_command)
        (s2, o2) = commands.getstatusoutput(load_external_command)
        if s2 != 0:
            print('Error in prep external tables.')
            print(o2)
            sys.exit(2)
        else:
            print('Successfully prep external tables.')
            print(o2)
    
    def _copy_data(self):
        copy_script = ''
        if self.partitions > 0:
            copy_script = os.path.join(self.schema_folder,'copy_partition.sql')
        else:
            copy_script = os.path.join(self.schema_folder,'copy_no_partition.sql')
        
        command = 'psql -d %s -a -f %s' % (self.database_name, copy_script)
        print ('Execute: %s' % (command))
        (status, output) = commands.getstatusoutput(command)
        if status != 0:
            print ('Fail to copy data into table.')
            print (output)
            sys.exit(2)
        else:
            print (output)
            print ('Successfully copy data into table.') 

    def cmdstr(self, string):
        dir=''
        for i in string:
            if i == os.sep:
                dir = dir + '\/'
            elif i=='\\' :
                dir = dir + '\\\\\\'
            else:
                dir = dir + i
        return dir
    
    def sed(self, string1,string2,filename):
        str1=self.cmdstr(string1)
        str2=self.cmdstr(string2)
        test=r'sed -i "s/%s/%s/g" %s' % (str1, str2, filename) 
        os.system(test)   

    


    def load_clean_up(self):
        self._stop_gpfdist()
        self._delete_data()
        
    def _stop_gpfdist(self):
        cmd = "ps -ef|grep gpfdist|grep %s|grep -v grep|awk \'{print $2}\'|xargs kill -9" % (self.gpfdist_port)
        command = "gpssh -f %s -e \"%s\"" % (self.hostfile_seg, cmd)
        print command
        (status, output) = commands.getstatusoutput(command)
        if status != 0:
            print('kill gpfdist on segments error. ')
            print(output)
            sys.exit(2)
        else:
            print('kill gpfdist on segments succeed. ')

    def _delete_data(self):
        # mkdir in each segment
        cmd = "gpssh -f %s -e 'cd %s; rm -rf *'" % (self.hostfile_seg, self.tmp_tpcds_folder)
        print ('Execute: ' + cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if status != 0:
            print('gpssh to delete data folder failed. ')
            print(output)
            sys.exit(2)
        else:
            print('delete data folder succeed.')
    
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

