import os
import sys
from datetime import datetime, date, timedelta
import yaml
from pygresql import pg
import commands

try:
    from workloads.Workload import *
except ImportError:
    sys.stderr.write('TPCH needs workloads/Workload.py\n')
    sys.exit(2)

try:
    from lib.PSQL import psql
except ImportError:
    sys.stderr.write('TPCH needs psql in lib/PSQL.py\n')
    sys.exit(2)

try:
    import gl
except ImportError:
    sys.stderr.write('TPCH needs gl.py in lsp_home\n')
    sys.exit(2)

class Query:
	def __init__(self,querycontent,userlist):
		self._name = querycontent['query_name']
		self._sql = querycontent['sql']
		self._type = querycontent['type']
		self._runnum = querycontent['runnum']
		self._concurrencynum = querycontent['concurrencynum']
		if querycontent['user']!= None:
			self._user = querycontent['user']
		else:
			self._user = userlist[random.randint(0,len(userlist)-1)]

class Rqtpch(Workload):

    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, user): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, user)
	self.queryfile = self.workload_directory + os.sep + 'YML' + os.sep + workload_specification['query_file'] + '.yml'
	self.database_name = workload_specification['database_name'].strip()
   
    def parse_yaml(self):
	self.querylist = []
	with open (self.queryfile,"r")	as yamlfile:
		yaml_parser = yaml.load(yamlfile)
	self.runworkload_mode = yaml_parser['runworkload_mode']
	self.query_list = yaml_parser['query_list'].split(',')
	self.user_list = yaml_parser['user_list']
	for queryname in self.query_list:
		queryname = queryname.strip()
		for i in range(0,len(yaml_parser['query_content'])):
			if queryname == yaml_parser['query_content'][i]['query_name']:
				query = Query(yaml_parser['query_content'][i], self.user_list)
				self.querylist.append(query)
				break

    def generateSql(self):
	Tablename = "customer"
	QueryMap = {
            	"SELECT": "SELECT COUNT(*) FROM %s;" %(Tablename),
		"COPY": "COPY %s FROM '$TINCREPO/mpp/hawq/tests/transaction/hawq_isolation/setup/tbl_data'; "%(TableName),
            	#"INSERT":"INSERT INTO %s select generate_series(20000,30000),generate_series(20000,30000),generate_series(20000,30000);"%(TableName),
            	"INSERT": "INSERT INTO %s VALUES(1,'b','b',1,'b',1.1,'b','b');"%(TableName),
		"VACUUM" :"VACUUM %s;"%(TableName),
            	"ANALYZE" :"ANALYZE %s;"%(TableName),
            	"ALTER TABLE" :"ALTER TABLE %s  set with ( reorganize='true') distributed randomly;"%(TableName),
            	"DROP TABLE" :"DROP TABLE %s;"%(TableName),
            	"TRUNCATE" :"TRUNCATE %s;"%(TableName),
            	"VACUUM FULL" :"VACUUM FULL %s;"%(TableName)
            	}

	for query in self.querylist:
		if query._sql == None:
			query._sql = QueryMap[query._type]
    

    def run_query(self,query,puser):
	if self.continue_flag:
		if self.run_workload_flag:
			self.output(query._sql)
			beg_time = datetime.now()	
			cmd = 'psql -d %s -U %s -c "%s"' % (self.database_name,puser,query._sql)
			(ok,result) = commands.getstatusoutput(cmd)
			end_time = datetime.now()
	
			if str(result).find('ERROR')!=-1 or str(result).find('FATAL')!=-1 or str(result).find('PANIC')!=-1:
				flag = False
			else:
				flag = True
			if ok!=0 or flag==False:
				status = "ERROR"
				self.output('\n'.join(result))
			else:
				status = "SUCCESS"
				with open(self.result_directory + os.sep + query._name + '.output', 'w') as f:
					f.write(str(result))
				with open(self.result_directory + os.sep + query._name + '.output', 'r') as f:
					result = f.read()
					md5code = hashlib.md5(result.encode('utf-8')).hexdigest()
				with open(self.result_directory + os.sep + query._name.split('.')[0] + '.md5', 'w') as f:
					f.write(md5code)
				if gl.check_result:
					ans_file = self.ans_directory + os.sep + query._name.split('.')[0] + '.ans'
					md5_file = self.ans_directory + os.sep + query._name.split('.')[0] + '.md5'
					if os.path.exists(ans_file):
						self.output('Check query result use ans file')
						if not self.check_query_result(ans_file = ans_file, result_file = self.result_directory + os.sep + qf_name.split('.')[0] + '.output'):
							status = 'ERROR'
					elif os.path.exists(md5_file):
						self.output('Check query result use md5 file')
						if not self.check_query_result(ans_file = md5_file, result_file = self.result_directory + os.sep + qf_name.split('.')[0] + '.md5'):
							status = 'ERROR'
					else:
                                		self.output('No answer file')
                                		status = 'ERROR'
		else:
			status ='SKIP'
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
	self.output('   Execution=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (query._name, query._runnum, query._concurrencynum, status, duration))
	self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, 'Execution', '%s', %d, %d, '%s', '%s', '%s', %d, NULL, NULL, NULL);"% (self.tr_id, self.s_id, query._name, query._runnum,query._concurrencynum, status, beg_time, end_time, duration))


    def get_partition_suffix(self, num_partitions = 128, table_name = ''):
        beg_date = date(1992, 01, 01)
        end_date = date(1998, 12, 31)
        duration_days = int(round(float((end_date - beg_date).days) / float(num_partitions)))

        part = ''

        if table_name == 'lineitem':
            part = '''PARTITION BY RANGE(l_shipdate)\n    (\n'''
        elif table_name == 'orders':
            part = '''PARTITION BY RANGE(o_orderdate)\n    (\n'''

        if gl.suffix:
            table_name = table_name + '_' + self.tbl_suffix
                
        for i in range(1, num_partitions+1):
            beg_cur = beg_date + timedelta(days = (i-1)*duration_days)
            end_cur = beg_date + timedelta(days = i*duration_days)

            part += '''        PARTITION p1_%s START (\'%s\'::date) END (\'%s\'::date) EVERY (\'%s days\'::interval) WITH (tablename=\'%s_part_1_prt_p1_%s\', %s )''' % (i, beg_cur, end_cur, duration_days, table_name, i, self.sql_suffix)
            
            if i != num_partitions:
                part += ''',\n'''
            else:
                part += '''\n'''

        part += '''    )'''
                
        return part 

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

        if self.distributed_randomly and table_name != 'revenue':
            import re
            old_string = re.search(r'DISTRIBUTED BY\(\S+\)', sql).group()
            sql = sql.replace(old_string, 'DISTRIBUTED RANDOMLY')

        if self.partitions == 0 or self.partitions is None:
            sql = sql.replace('PARTITIONS', '')
        else:
            part_suffix = self.get_partition_suffix(num_partitions = self.partitions, table_name = table_name)
            sql = sql.replace('PARTITIONS', part_suffix)
        
        return sql


    def load_data(self):
        self.output('-- Start loading data')

        # get the data dir
        data_directory = self.workload_directory + os.sep + 'data'
        if not os.path.exists(data_directory):
            self.output('ERROR: Cannot find DDL to create tables for TPC-H: %s does not exists' % (data_directory))
            sys.exit(2)

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

        tables = ['nation', 'region', 'part', 'supplier', 'partsupp', 'customer', 'orders','lineitem' ,'revenue']
        for table_name in tables:
            if self.continue_flag:
                if self.load_data_flag:
                    with open(data_directory + os.sep + table_name + '.sql', 'r') as f:
                        cmd = f.read()
                    cmd = self.replace_sql(sql = cmd, table_name = table_name)
                    with open(self.tmp_folder + os.sep + table_name + '.sql', 'w') as f:
                        f.write(cmd)

                    self.output(cmd)
                    beg_time = datetime.now()
                    (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + table_name + '.sql', dbname = self.database_name, username = self.user)
                    end_time = datetime.now()
                    self.output('\n'.join(result))

                    if ok and str(result).find('ERROR') == -1 and str(result).find('FATAL') == -1:
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
            duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds /1000
            beg_time = str(beg_time).split('.')[0]
            end_time = str(end_time).split('.')[0]         
            self.output('   Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, status, duration))
            self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, 'Loading', '%s', 1, 1, '%s', '%s', '%s', %d, NULL, NULL, NULL);" 
                % (self.tr_id, self.s_id, table_name, status, beg_time, end_time, duration))
               
        self.output('-- Complete loading data')

        
    def run_queries(self,query,iteration,stream):
	niteration = 1
	while niteration <= query._runnum:
		self.output('-- Start iteration %d' % (niteration))
		nconcurrency = 1
		AllWorkers = []
		while nconcurrency <= query._concurrencynum:
			self.output('-- Start stream %s' % (nconcurrency))
			p = Process(target = self.run_query, args = (query, query._user))
			print query._type
			print nconcurrency
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
   
    def run_workload(self):
	if self.runworkload_mode=='S':
		for query in self.querylist:
			iteration = query._runnum
			stream = query._concurrencynum
			print iteration
			print stream
			print "##############"
			self.run_queries(query,iteration,stream)
	elif self.runworkload_mode=='C':
		print 'C'
		for query in self.querylist:
			iteration = query._runnum
			stream = query._concurrencynum
			print iteration
			print stream
			self.run_queries(query,iteration,stream)
			p = Process(target = self.run_queries, args = (query,iteration,stream))
			p.start()
 
    def execute(self):
        self.output('-- Start running workload %s' % (self.workload_name))
	
	self.parse_yaml()
        # setup
        self.setup()

        # load data
        self.load_data()

        # vacuum_analyze
	self.generateSql()

        self.vacuum_analyze()

        # run workload concurrently and loop by iteration
        self.run_workload()

        # clean up 
        self.clean_up()
        
        self.output('-- Complete running workload %s' % (self.workload_name))
