import os, sys, commands, socket, shutil
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

class Retail(Workload):
	def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, validation): 
     	# init base common setting such as dbname, load_data, run_workload , niteration etc
    	Workload.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, validation)

	def setup(self):
		# check if the database exist
        try: 
            cnx = pg.connect(dbname = self.database_name)
        except Exception, e:
            cnx = pg.connect(dbname = 'postgres')
            cnx.query('CREATE DATABASE %s;' % (self.database_name))
        finally:
            cnx.close()

        box_muller = self.workload_directory + os.sep + 'box_muller'
        gphome=os.environ['GPHOME']
    	os.system("cd %s;make clean;make install" % (box_muller))
    	os.system("gpscp -f %s %s/bm.so =:%s/lib/postgresql/" % (hostfile, box_muller, gphome))

	def check_seeds(dir):
		files=['female_first_names.txt','male_first_names.txt','products_full.dat','state_sales_tax.dat', \
		'street_names.dat','surnames.dat','websites.dat','zip_codes.dat']
		for seeds_file in files:
		    if os.path.exists(dir + seeds_file):
		        pass
		    else:
		        return False
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
		for table_name in tables:
            if self.load_data_flag or self.run_workload_flag:
                with open(data_directory + os.sep + table_name + '.sql', 'r') as f:
                    cmd = f.read()
                cmd = self.replace_sql(sql = cmd, table_name = table_name, num = niteration)
                with open(self.tmp_folder + os.sep + 'gpfdist_loading_temp.sql', 'w') as f:
                    f.write(cmd)

                self.output(cmd)    
                beg_time = datetime.now()
                (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + 'gpfdist_loading_temp.sql', dbname = self.database_name)
                end_time = datetime.now()
                self.output('RESULT: ' + str(result))

                if ok and str(result).find('ERROR') == -1: 
                    status = 'SUCCESS'    
                else:
                    status = 'ERROR'
            
            else:
                status = 'SKIP'
                beg_time = datetime.now()
                end_time = beg_time
                
            duration = end_time - beg_time
            duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds /1000
            beg_time = str(beg_time).split('.')[0]
            end_time = str(end_time).split('.')[0]
            
            self.output('   Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, niteration, 1, status, duration))
            self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, 'Loading', '%s', %d, 1, '%s', '%s', '%s', %d, NULL, NULL, NULL);" 
                % (self.tr_id, self.s_id, table_name, niteration, status, beg_time, end_time, duration))

	def clean_up(self):
		pass

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