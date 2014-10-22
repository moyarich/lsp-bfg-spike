import os, sys, commands, socket, shutil
from datetime import datetime, date, timedelta

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

    def check_seeds(self):
        os.system("cd %s;tar -zvxf seeds.tar.gz -C %s" % (self.workload_directory, self.tmp_folder))
        files = ['female_first_names.txt', 'male_first_names.txt', 'products_full.dat', 'state_sales_tax.dat', \
        'street_names.dat', 'surnames.dat', 'websites.dat', 'zip_codes.dat']
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
        tryAgain = True
        s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
        while tryAgain:
            try:
                s.bind( ( "localhost", defaultPort ) )
            except:
                defaultPort += 1
            finally:
                tryAgain = False
                s.close()
                
        return defaultPort

    def load_data(self):
        if self.load_data_flag:
            beg_time = datetime.now()
            if self.prep_e_tables() and self.prep_udfs() and self.check_seeds():
                status = 'SUCCESS'

                # dca_demo_conf set the data scale 
                with open(self.scripts_dir + os.sep + 'dca_demo_conf.sql', 'r') as f:
                    cmd = f.read()
                    scale = int (8500000000 / 10.4 / 1024 * self.scale_factor)
                    cmd = cmd.replace('8500000000', str(scale))
                with open(self.tmp_folder + os.sep + 'dca_demo_conf.sql', 'w') as f:
                    f.write(cmd)
                
                scripts = ['prep_dimensions.sql', 'prep_facts.sql', 'gen_order_base.sql', 'gen_facts.sql']
                for script in scripts:
                    self.output('------ Start %s ------' % (script))
                    with open(self.scripts_dir + os.sep + script, 'r') as f:
                        cmd = f.read()
                    cmd = cmd.replace('PATH_OF_DCA_DEMO_CONF_SQL', '\i %s/dca_demo_conf.sql' % (self.tmp_folder))
                    with open(self.tmp_folder + os.sep + script, 'w') as f:
                        f.write(cmd)

                 #   self.output(cmd)    
                    
                    (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + script, dbname = self.database_name)
                    self.output('RESULT: ' + result[0])

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
        self.output('create schema: \n' + result[0])
        if not ok or str(result).find('ERROR') != -1:
            return False
        
        self.port = self.getOpenPort()
        hostname = socket.gethostname()
        
        with open(self.scripts_dir + os.sep + 'prep_external_tables.sql', 'r') as f:
            cmd = f.read()
            cmd = cmd.replace('//HOST:PORT','//%s:%s' % (hostname, self.port) )
            with open(self.tmp_folder + os.sep + 'prep_external_tables.sql', 'w') as f:
                f.write(cmd)

 #           self.output(cmd)      
            (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + 'prep_external_tables.sql', dbname = self.database_name)
            self.output('RESULT:\n' + result[0])

            if ok and str(result).find('ERROR') == -1:
                os.system("gpfdist -d %s -p %s -l %s/fdist.%s.log &" % (self.tmp_folder, self.port, self.tmp_folder, self.port))
                return True    
            else:
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
        self.output('perp udfs result:\n' + result[0])
        if not ok or str(result).find('ERROR') != -1:
            self.output('prep udfs error. ')
            return False
        else:
            self.output('prep_UDFs success. ')

        os.system(self.scripts_dir + os.sep + 'prep_GUCs.sh')
        return True

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