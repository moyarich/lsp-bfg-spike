import os, sys, time, shutil, commands, socket


class TPCDSLoading:
    
    def __init__(self, database_name = 'tpcds_1g', scale_factor = 1, partition = False, host_num = 1, no_pk = False):
        ''' initialization '''
        self.pwd = os.path.abspath(os.path.dirname(__file__))
        self.db_name = database_name
        self.scale_factor = scale_factor
        self.partition = partition
        self.host_num = host_num
        self.no_pk = no_pk
        self.schema_folder = os.path.join(self.pwd, 'schema')
        self.tmp_tpcds_folder = '/data1/tmp/tpcds_loading/'
        self.tmp_tpcds_data_folder = '/data1/tmp/tpcds_loading/data'
        
    def cleanup(self):
        self.stop_gpfdist()
        self._delete_data()
        
    def _delete_data(self):
        # mkdir in each segment
        cmd = "gpssh -f %s -e 'cd %s; rm -rf *'"%(self.hostfile,self.tmp_tpcds_folder)
        print ('Execute: '+cmd)
        status, output = commands.getstatusoutput(cmd)
        if status != 0:
            print('gpssh to delete data folder failed. ')
            print(output)
            sys.exit()
        else:
            print('delete data folder succeed.')
        
    def _getOpenPort(self,port = 8050):
        defaultPort = port
        tryAgain = True
        s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
        s.bind( ( "localhost",0) ) 
        addr, defaultPort = s.getsockname()
        s.close()
        return defaultPort
    
    def stop_gpfdist(self):
        cmd = "ps -ef|grep gpfdist|grep %s|grep -v grep|awk \'{print $2}\'|xargs kill -9"%self.gpfdist_port
        command = "gpssh -f %s -e \"%s\""%(self.hostfile, cmd)
        status, output = commands.getstatusoutput(command)
        #print (output)
        print ('kill gpfdist on segments succeed. ')
        
        
    def setUp(self):
        """
        initialization check
        """

        self._check_data_gen()
        self._check_hostfile()
        
    def generate(self):
        """
        copy dsdgen to each host and generate data in parallel 
        """
        self._prepare_folder()
        self._scp_data_gen_code()
        self._data_gen_segment()
        
    def dataloading(self):
        """
        loading tpcds
        """
        self._create_schema()
        self._start_gpfdist()
        self._create_external_table()
        self._copy_data()
    
    def _create_schema(self):
        create_schema = ''
        if self.partition:
            create_schema = os.path.join(self.schema_folder,'ddl_tpcds_partition.sql')
        else:
            create_schema = os.path.join(self.schema_folder,'ddl_tpcds_no_partition.sql')
        
        command = 'psql -d %s -a -f %s'%(self.db_name, create_schema)
        print ('Execute: %s'%command)
        status, output = commands.getstatusoutput(command)
        if status!=0:
            print ('Fail to create schema. ')
            print (output)
            sys.exit()
        else:
            print ('Successfully create schema.')
    
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
        
        for num in range(1, self.host_num+1):
            cur_host = 'sdw%s'%num
            cmd = "gpssh -h %s -e 'cat %s'"%(cur_host, os.path.join(self.tmp_tpcds_folder, 'dat_files.txt'))
            
            dat_file_suffix = '.dat'
            
            status, output = commands.getstatusoutput(cmd)
            if status!=0:
                print('Error happen in ls data dir in %s'%cur_host)
                print(output)
                sys.exit()
            else:
                lines = output.split('\n')
                for line in lines:
                    if line.find(dat_file_suffix)!=-1:
                        file_name = line.split(' ')[-1].strip()
                        tmp_name = file_name[:file_name.rindex('_')]
                        table_name = tmp_name[:tmp_name.rindex('_')]
                        if table_name not in gpfdist_map.keys():
                            if table_name.find('dbgen_version')==-1:
                                print('Error: %s not find in gpfdist_map'%table_name)
                                sys.exit()
                        else:
                            gpfdist_map[table_name].append("'gpfdist://%s:%s/%s'"%(cur_host, self.gpfdist_port, file_name))
        
        for key in gpfdist_map.keys():
            print ('%s: %s dat files'%(key, len(gpfdist_map[key])))
        
        # modify the prep_external_table_script
        external_script = os.path.join(self.schema_folder,'prep_external_tables2.sql')
        shutil.copyfile(os.path.join(self.schema_folder,'prep_external_tables.sql'), external_script)
        for key in gpfdist_map.keys():
            self.sed('LOCATION_%s_ext'%key,"LOCATION("+','.join(gpfdist_map[key])+")", external_script)
        
        load_external_command = 'psql -d %s -a -f %s'%(self.db_name, external_script)
        print('Execute command: '+load_external_command)
        s2, o2 = commands.getstatusoutput(load_external_command)
        if s2!=0:
            print('Error in prep external tables.')
            print(o2)
            sys.exit()
        else:
            print('Successfully prep external tables.')
            print(o2)
        
        
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
        # Add this for mac only
#        test=r'sed -i "" "s/%s/%s/g" %s'%(str1,str2,filename) 
        test=r'sed -i "s/%s/%s/g" %s'%(str1,str2,filename) 
        # Add this for mac only
#        print('Execute sed command: '+test);
        os.system(test)   
        
        
        
    
    def _start_gpfdist(self):       
        # find port 
        self.gpfdist_port = self._getOpenPort()
        print ('GPFDIST PORT: %s'%self.gpfdist_port)
        
        cmd = 'gpfdist -d %s -p %s -l %s/fdist.%s.log &'%(self.tmp_tpcds_data_folder, self.gpfdist_port, self.tmp_tpcds_data_folder, self.gpfdist_port)        
        command = "gpssh -f %s -e '%s'"%(self.hostfile, cmd)
        status, output = commands.getstatusoutput(command)
        if status != 0:
            print ('gpfdist on segments failed. ')
            print (output)
            sys.exit()
        else:
            print (output)
            print ('gpfdist on segments succeed. ')
    
    def _copy_data(self):
        copy_script = ''
        if self.partition:
            if self.no_pk:
                copy_script = os.path.join(self.schema_folder,'copy_partition_no_pk.sql')
            else:
                copy_script = os.path.join(self.schema_folder,'copy_partition.sql')
        else:
            if self.no_pk:
                copy_script = os.path.join(self.schema_folder,'copy_no_partition_no_pk.sql')
            else:
                copy_script = os.path.join(self.schema_folder,'copy_no_partition.sql')
        
        command = 'psql -d %s -a -f %s'%(self.db_name, copy_script)
        print ('Execute: %s'%command)
        status, output = commands.getstatusoutput(command)
        if status!=0:
            print ('Fail to copy data into table.')
            print (output)
            sys.exit()
        else:
            print (output)
            print ('Successfully copy data into table.')

        
    def _prepare_folder(self):
        
        # mkdir in each segment
        cmd = "gpssh -f %s -e 'mkdir -p %s'"%(self.hostfile,self.tmp_tpcds_folder)
        print ('Execute: '+cmd)
        status, output = commands.getstatusoutput(cmd)
        if status != 0:
            print('gpssh to prep folder failed. ')
            print(output)
            sys.exit()
        else:
            print('folder prepared.')

        cmd2 = "gpssh -f %s -e 'mkdir -p %s'"%(self.hostfile,self.tmp_tpcds_data_folder)
        print ('Execute: '+cmd2)
        status, output = commands.getstatusoutput(cmd2)
        if status != 0:
            print('gpssh to prep folder failed. ')
            print(output)
            sys.exit()
        else:
            print('folder prepared.')
 
    def _scp_data_gen_code(self):
        cmd1 = 'gpscp -f %s %s =:%s'%(self.hostfile,os.path.join(self.pwd,'dsdgen'),self.tmp_tpcds_folder)
        cmd2 = 'gpscp -f %s %s =:%s'%(self.hostfile,self.tpcds_idx,self.tmp_tpcds_folder)
        
        cmd3 ="gpssh -f %s -e 'chmod 755 %s; chmod 755 %s'"%(self.hostfile, os.path.join(self.tmp_tpcds_folder,'dsdgen'), os.path.join(self.tmp_tpcds_folder,'tpcds.idx'))
        
        print('Execute: '+cmd1)
        s1, o1 = commands.getstatusoutput(cmd1)
        if s1!= 0:
            print('gpscp dsdgen failed.')
            print(o1)
            sys.exit();
        else:
            print('gpscp dsdgen finished.')
            
        print('Execute: '+cmd2)
        s2, o2 = commands.getstatusoutput(cmd2)
        if s2!= 0:
            print('gpscp tpcds.idx failed.')
            print(o2)
            sys.exit();
        else:
            print('gpscp tpcds.idx finished.')
            
        s3, o3 = commands.getstatusoutput(cmd3)
        if s3!= 0:
            print('chmod dsdgen and tpcds.idx failed.')
            print(o3)
            sys.exit()
       
    def _data_gen_segment(self):
        
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
    process = subprocess.Popen(cmd.split(' '))
    process_pool.append(process)
    process_name[process] = 'Process_'+str(child)+'_'+str(parallel_setting)
    
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
        time.sleep(1200)
        
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
        total_paralle = self.host_num * 4
        seg_hosts = []
        for i in range(1, self.host_num+1):
            cur_host = 'sdw%s'%i
            print ('generate script for %s'%cur_host)
            # generate python command for each segment.
            child_1 = 4*i - 3
            child_2 = 4*i - 2
            child_3 = 4*i - 1
            child_4 = 4*i
            python_script_base_name = cur_host+'.py'
            host_python_script = os.path.join(self.pwd, cur_host+'.py')
            with open(host_python_script, 'w') as f:
                f.write(command_template%(child_1, child_2, child_3, child_4, total_paralle, self.scale_factor, self.tmp_tpcds_data_folder))
            
            cmd1 = 'gpscp -h %s %s =:%s'%(cur_host, host_python_script, self.tmp_tpcds_folder)
            s1, o1 = commands.getstatusoutput(cmd1)
            if s1 != 0:
                print('Error happen in scp seg python script.')
                print(o1)
                sys.exit()
            
            cmd2 = "gpssh -h %s -e 'cd %s;chmod 755 %s'"%(cur_host,self.tmp_tpcds_folder, python_script_base_name)
            s2, o2 = commands.getstatusoutput(cmd2)
            if s2 !=0:
                print('Error happen in chmod seg python script.')
                print(o2)
                sys.exit()
                
            cmd = 'cd %s; python %s &'%(self.tmp_tpcds_folder, python_script_base_name)        
            command = "gpssh -h %s -e '%s'"%(cur_host, cmd)
            status, output = commands.getstatusoutput(command)
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
        check_cmd_template = "gpssh -h %s -e 'cat "+os.path.join(self.tmp_tpcds_folder,'status.txt')+"'"
        while True:                
            finished_pool = []
            finish_generating = True
            for cur_host in seg_hosts:
                # check if that is done
                check_cmd =check_cmd_template%cur_host
                s1, o1 = commands.getstatusoutput(check_cmd)
                if s1 != 0:
                    print ('check status.txt failed.')
                    print(o1)
                    sys.exit()
                
                if o1.find('done')!=-1:
                    print('%s finished'%cur_host)
                    finished_pool.append(cur_host)
                else:
                    finish_generating = False
                    break
            
            # remove finished
            for p in finished_pool:
                seg_hosts.remove(p)

            if finish_generating:
                print ('Data generation finished in all segments.')
                print ('total generation time: %s minutes'%total_minutes)
                break
            else:
                print ('Data generation still going on. Wait another 20 minutes')
                time.sleep(1200)
                total_minutes += 20   
            
        
    def _check_hostfile(self):
        # check hostfile 
        self.hostfile = os.path.join(self.pwd,'hostfile')
        if not os.path.exists(self.hostfile):
            print('hostfile does not exist. Exit.')
            sys.exit()
        else:
            print('hostfile exist.')
        hosts = [line.strip() for line in open(self.hostfile)]
        
        if len(hosts)!=self.host_num:
            print('hostfile does not match host_num')
            sys.exit()
        else:
            for i in range(1, self.host_num+1):
                if 'sdw%s'%i not in hosts:
                    print('sdw%s is not in hosts'%i)
                    sys.exit()
            print('hostfile match host_num')
        
    def _check_data_gen(self):
        # Check data_gen folder
        self.data_gen_folder = os.path.join(self.pwd, 'data_gen')
        if not os.path.exists(self.data_gen_folder):
            print('data_gen folder does not exist. Exit. ')
            sys.exit()
        else:
            print('data_gen folder exist.')
        
        # Check tpcds_idx file
        self.tpcds_idx = os.path.join(self.pwd, 'tpcds.idx')
        if not os.path.exists(self.tpcds_idx):
            print('tpcds.idx does not exist. Exit. ')
            sys.exit()
        else:
            print('tpcds.idx exist.')
            
        self._compile_data_gen()
        
    def _compile_data_gen(self):
        # temporarily 
        if os.path.exists(os.path.join(self.pwd,'dsdgen')):
            print 'dsdgen already exist.'
            return
        
        command = 'cd %s; make clean; make'%self.data_gen_folder
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
            
            

def main():
    args = sys.argv
    db_name = args[1]
    scale = int(args[2])
    haspartition = True
    if args[3] == 'True':
        haspartition = True
    else:
        haspartition = False
    host = int(args[4])
    tpcds_loading = TPCDSLoading(database_name = db_name, scale_factor = scale, partition = haspartition, host_num = host)
    tpcds_loading.setUp()
    tpcds_loading.generate()
    tpcds_loading.dataloading()
    tpcds_loading.cleanup()
    
if __name__=='__main__':
    main()
