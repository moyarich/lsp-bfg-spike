import os, sys, commands, socket
from datetime import datetime

try:
    from workloads.Workload import *
except ImportError:
    sys.stderr.write('Retail needs workloads/Workload.py\n')
    sys.exit(2)

try:
    from pygresql import pg
except ImportError:
    sys.stderr.write('Retail needs pygresql\n')
    sys.exit(2)

try:
    from lib.PSQL import psql
except ImportError:
    sys.stderr.write('Retail needs psql in lib/PSQL.py\n')
    sys.exit(2)

def run_sqlfile(dbname , sqlfile):
        os.system("psql -a -d %s -f %s"%(dbname,sqlfile))

def cmdstr(string):
    dir=''
    for i in string:
        if i == os.sep:
            dir = dir + '\/'
        elif i=='\\' :
            dir = dir + '\\\\\\'
        else:
            dir = dir + i

    return dir

def sed(string1,string2,filename):
    str1=cmdstr(string1)
    str2=cmdstr(string2)
    test=r'sed -i "s/%s/%s/g" %s'%(str1,str2,filename) 
    os.system(test)


class Retail(Workload):
    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, validation): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, validation)
        self.scripts_dir = self.workload_directory + os.sep + 'scripts'

    def setup(self):
        # check if the database exist
        try: 
            cnx = pg.connect(dbname = self.database_name)
        except Exception, e:
            cnx = pg.connect(dbname = 'postgres')
            cnx.query('CREATE DATABASE %s;' % (self.database_name))
        finally:
            cnx.close()

    def load_data1(self):
        self.check_seeds()

        run_sqlfile(self.database_name, self.scripts_dir + '/prep_database.sql')

        list=os.popen('psql -d %s -c \"SELECT hostname FROM pg_catalog.gp_segment_configuration  GROUP BY hostname ORDER by hostname;\"'%dbname).readlines()
        lists=[i.strip() for i in list if 'hostname' not in i and '---' not in i and 'row' not in i and i !='\n']
        hostfile=''
        for host in lists:
            hostfile = hostfile + '-h %s '%host
        
        port=self.getOpenPort()
        hostname=socket.gethostname()

        sed('//HOST:PORT','//%s:%s'%(hostname,port),self.scripts_dir+'/prep_external_tables.sql')
        run_sqlfile(self.database_name, self.scripts_dir + '/prep_external_tables.sql')
        sed('//.*:[0-9]*','//%s:%s'%('HOST','PORT'),self.scripts_dir+'/prep_external_tables.sql')

        os.system("gpfdist -d %s -p %s -l %s/fdist.%s.log &"%(self.tmp_folder,port,self.tmp_folder,port))

        gphome=os.environ['GPHOME']
        box_muller = self.workload_directory + os.sep + 'box_muller'
        os.system("cd %s;make clean;make install"%box_muller)
        os.system("gpscp %s %s/bm.so =:%s/lib/postgresql/"%(hostfile,box_muller,gphome))

        run_sqlfile(self.database_name, self.scripts_dir + '/prep_UDFs.sql')
        os.system(self.scripts_dir + '/prep_GUCs.sh')

        run_sqlfile(self.database_name, self.scripts_dir + '/prep_dimensions.sql')

        sed('PATH_OF_DCA_DEMO_CONF_SQL','\i %s/dca_demo_conf.sql'%self.scripts_dir ,self.scripts_dir + '/prep_facts.sql')    
        run_sqlfile(self.database_name, self.scripts_dir + '/prep_facts.sql')
        sed('.*_conf.sql','PATH_OF_DCA_DEMO_CONF_SQL' ,self.scripts_dir + '/prep_facts.sql')    

        sed('PATH_OF_DCA_DEMO_CONF_SQL','\i %s/dca_demo_conf.sql'%self.scripts_dir ,self.scripts_dir + '/gen_order_base.sql')    
        run_sqlfile(self.database_name, self.scripts_dir + '/gen_order_base.sql')
        sed('.*_conf.sql','PATH_OF_DCA_DEMO_CONF_SQL' ,self.scripts_dir + '/gen_order_base.sql')    

        sed('PATH_OF_DCA_DEMO_CONF_SQL','\i %s/dca_demo_conf.sql'%self.scripts_dir ,self.scripts_dir + '/gen_facts.sql')    
        run_sqlfile(self.database_name, self.scripts_dir + '/gen_facts.sql')
        sed('.*_conf.sql','PATH_OF_DCA_DEMO_CONF_SQL' ,self.scripts_dir + '/gen_facts.sql')    

        os.system('ps -ef|grep gpfdist|grep %s|grep -v grep|awk \'{print $2}\'|xargs kill -9'%port)

    def load_data(self):

        if self.load_data_flag:
            beg_time = datetime.now()
            end_time = beg_time
            status = 'ERROE'
            if self.check_seeds() and self.prep_e_tables() and self.prep_udfs():
                status = 'SUCCESS'
                os.system(self.scripts_dir + os.sep + 'prep_GUCs.sh')

                # dca_demo_conf set the data scale 
                with open(self.scripts_dir + os.sep + 'dca_demo_conf.sql', 'r') as f:
                    sql = f.read()
                    scale = int (8500000000 / 10.4 / 1024 * self.scale_factor)
                    sql = sql.replace('8500000000', str(scale))
                with open(self.tmp_folder + os.sep + 'dca_demo_conf.sql', 'w') as f:
                    f.write(sql)
                
                scripts = ['prep_dimensions.sql', 'prep_facts.sql', 'gen_order_base.sql', 'gen_facts.sql']
                for script in scripts:
                    self.output('------ start %s ------' % (script))

                    with open(self.scripts_dir + os.sep + script, 'r') as f:
                        sql = f.read()
                    sql = sql.replace('PATH_OF_DCA_DEMO_CONF_SQL', '\i %s/dca_demo_conf.sql' % (self.tmp_folder))
                    sql = sql.replace('SQLSUFFIX', self.sql_suffix)
                    with open(self.tmp_folder + os.sep + script, 'w') as f:
                        f.write(sql)   
                    
                    (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + script, dbname = self.database_name, flag = '-e')
                    self.output('\n'.join(result))

                    if ok and str(result).find('ERROR') == -1: 
                        end_time = datetime.now()    
                    else:
                        status = 'ERROR'
                        self.output('%s is error!' % (script))
                        end_time = datetime.now()
                        break  
        
        else:
            status = 'SKIP'
            beg_time = datetime.now()
            end_time = beg_time
                
        duration = end_time - beg_time
        duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds /1000
        beg_time = str(beg_time).split('.')[0]
        end_time = str(end_time).split('.')[0]
        
        self.output('   Loading   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (1, 1, status, duration))
        self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, 'Loading', '%s', %d, 1, '%s', '%s', '%s', %d, NULL, NULL, NULL);" 
            % (self.tr_id, self.s_id, 'retail_dw_' + self.tbl_suffix, 1, status, beg_time, end_time, duration))

    def prep_e_tables(self):
        (ok, result) = psql.runfile(ifile = self.scripts_dir + os.sep + 'prep_database.sql', dbname = self.database_name)
        self.output('------ create schema ------\n' + '\n'.join(result))
        if not ok or str(result).find('ERROR') != -1:
            return False
        
        self.port = self.getOpenPort()
        hostname = socket.gethostname()
        
        with open(self.scripts_dir + os.sep + 'prep_external_tables.sql', 'r') as f:
            sql = f.read()
            sql = sql.replace('//HOST:PORT','//%s:%s' % (hostname, self.port) )
        with open(self.tmp_folder + os.sep + 'prep_external_tables.sql', 'w') as f:
            f.write(sql)

        self.output('------ prep_external_tables ------')
        (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + 'prep_external_tables.sql', dbname = self.database_name, flag = '-e')
        self.output('/n'.join(result))

        if ok and str(result).find('ERROR') == -1:
            cmd = 'gpfdist -d %s -p %s -l %s/fdist.%s.log &' % (self.tmp_folder, self.port, self.tmp_folder, self.port)
            self.output(cmd)
            result = os.system(cmd)
            self.output(str(result))
            return True    
        else:
            self.output('prep_external_tables error. ')
            return False

    def prep_udfs(self):
        # make
        box_muller = self.workload_directory + os.sep + 'box_muller'
        gphome=os.environ['GPHOME']

        cmd = 'cd %s;make clean;make install' % (box_muller)
        self.output(cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if status != 0:
            self.output('error: ' + output)
            return False
        else:
            self.output('make success. ')

        host_list = os.popen('psql -d %s -c \"SELECT hostname FROM pg_catalog.gp_segment_configuration GROUP BY hostname ORDER by hostname;\"'% (self.database_name)).readlines()
        lists = [i.strip() for i in host_list if 'hostname' not in i and '---' not in i and 'row' not in i and i !='\n']
        hostfile = ''
        for host in lists:
            hostfile = hostfile + '-h %s ' % (host)

        cmd = 'gpscp %s %s/bm.so =:%s/lib/postgresql/' % (hostfile, box_muller, gphome)
        self.output(cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if status != 0:
            self.output('error: ' + output)
            return False
        else:
            self.output('gpscp success. ')

        (ok, result) = psql.runfile(ifile = self.scripts_dir + os.sep + 'prep_UDFs.sql', dbname = self.database_name)
        self.output('------ perp udfs ------\n' + '\n'.join(result))
        if not ok or str(result).find('ERROR') != -1:
            self.output('prep_UDFs error. ')
            return False
        else:
            self.output('prep_UDFs success. ')

        return True

    def check_seeds(self):
        (status, output) = commands.getstatusoutput("cd %s;tar -zvxf seeds.tar.gz -C %s" % (self.workload_directory, self.tmp_folder))
        
        files = ['female_first_names.txt', 'male_first_names.txt', 'products_full.dat', \
        'state_sales_tax.dat', 'street_names.dat', 'surnames.dat', 'websites.dat', 'zip_codes.dat']
        for seeds_file in files:
            if os.path.exists(self.tmp_folder + os.sep + seeds_file):
                pass
            else:
                self.output('error: %s not exists.' % (seeds_file))
                return False
        self.output('check seeds files success. ')
        return True

    def getOpenPort(self, port = 8050):
        defaultPort = port
        s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
        s.bind( ( "localhost", 0) ) 
        addr, defaultPort = s.getsockname()
        s.close()
        return defaultPort


    def clean_up(self):
        cmd = 'ps -ef|grep gpfdist|grep %s|grep -v grep|awk \'{print $2}\'|xargs kill -9' % (self.port)
        (status, output) = commands.getstatusoutput(cmd)
        self.output(cmd)
        self.output(output)

    def execute(self):
        self.output('-- Start running workload %s' % (self.workload_name))

        # setup
        self.setup()

        # load data
        self.load_data()

        # run workload concurrently and loop by iteration
        self.run_workload()

        # clean up 
        self.clean_up()
        
        self.output('-- Complete running workload %s' % (self.workload_name))