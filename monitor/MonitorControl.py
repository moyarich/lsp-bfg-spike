#!/usr/bin/env python
import os,sys,commands,time
from datetime import datetime
from multiprocessing import Process
from pygresql import pg


class Monitor_control():
	
	def __init__(self):
		self.query_record = {}
		self.current_query_record = []
		
		self.seg_script = ''

		# prep report folder on master and tmp folder on seg host
		self.init_time = datetime.now().strftime('%Y%m%d-%H%M%S')
		self.seg_tmp_folder = '/tmp/monitor_report/' + self.init_time
		self.report_folder = os.getcwd() + os.sep + 'monitor_report' + os.sep + self.init_time
		self.run_lock = self.report_folder + os.sep + 'run.lock'
		
		self.hostfile_seg = self.report_folder + os.sep + 'hostfile_seg'

		(s,o) = commands.getstatusoutput('hostname')
		self.hostname = o.strip()

		self.count = 1
		self.sep = '|'

	def _get_monitor_seg_script_path(self):
		for one_path in sys.path:
			if one_path.endswith('monitor'):
				self.seg_script = one_path + os.sep + 'MonitorSeg.py'
				if os.path.exists(self.seg_script):
					return 0
		print 'not find MonitorSeg.py.'
		sys.exit()

	def _get_seg_list(self, hostfile = 'hostfile_seg'):
		cmd = ''' psql -d postgres -t -A -c "select distinct hostname from gp_segment_configuration where content <> -1 and role = 'p';" '''
		(status, output) = commands.getstatusoutput(cmd)
		
		if status != 0:
			print ('Unable to select gp_segment_configuration in monitor_control. ')
			sys.exit()
		
		with open(hostfile, 'w') as fnode:
			fnode.write(output + '\n')

	def setup(self):
		os.system( 'mkdir -p %s' % (self.report_folder + os.sep + 'seg_log') )
		os.system( 'touch %s' % (self.run_lock) )
		
		self._get_monitor_seg_script_path()
		self._get_seg_list(hostfile = self.hostfile_seg)
		# make tmp dir on every seg host
		cmd = " gpssh -f %s -e 'mkdir -p %s; touch %s/run.lock' " % (self.hostfile_seg, self.seg_tmp_folder, self.seg_tmp_folder)
		(s, o) = commands.getstatusoutput(cmd)
		if s != 0:
			print ('perp monitor report folder on seg host error.')
			print s,o
			sys.exit()

		# gpscp seg monitor script to every seg host
		cmd = 'gpscp -f %s %s =:%s' % (self.hostfile_seg, self.seg_script, self.seg_tmp_folder)
		(s, o) = commands.getstatusoutput(cmd)
		if s != 0:
			print ('gpscp monitor node script to every node error.')
			print s,o
			sys.exit()

	def report(self, filename, msg, mode = 'a'):
		if msg != '':
		    fp = open(filename, mode)  
		    fp.write(msg)
		    fp.flush()
		    fp.close()
	

	'''
	ps -eo pid,ppid,pcpu,vsz,rss,pmem,state,command | grep postgres | grep -vE "bin/postgres|logger|stats|writer|checkpoint|seqserver|WAL|ftsprobe|sweeper|sh -c|bash|grep|seg|pg_stat_activity|resource manager"
	 PID   PPID  %CPU  VSZ   RSS  %MEM STATE CMD          
	10836  3817  0.5 655800 27068  0.6  S  postgres: port  5432, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(34512) con202 127.0.0.1(34512) cmd1 SELECT
	10836  3817  0.5 655800 27068  0.6  S  postgres: port  5432, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(34512) con202 127.0.0.1(34512) cmd1 idle
index  0     1    2    3      4     5   6    7       8      9      10               11                     12             13       14            15    16          
	'''
	
	def __get_qd_mem(self):
		filter_string = 'bin/postgres|logger|stats|writer|checkpoint|seqserver|WAL|ftsprobe|sweeper|sh -c|bash|grep|seg|pg_stat_activity|resource manager'
		grep_string1 = 'postgres'
		cmd = ''' ps -eo pid,ppid,pcpu,vsz,rss,pmem,state,command | grep %s | grep -vE "%s" ''' % (grep_string1, filter_string)
		(status, output) = commands.getstatusoutput(cmd)
		if status != 0 or output == '':
			print 'error code: ' + str(status) + ' output: ' + output + ' in qd_mem_cpu'
			return None
		
		line_item = output.splitlines()
		output_string = ['', '']
		now_time = str(datetime.now())
		for line in line_item:
			temp = line.split()
			if len(temp) < 17:
				continue
			# hostname, count, time_point, pid, ppid, con_id, cmd, status, rss, pmem, pcpu	  
			one_item = self.hostname + self.sep + str(self.count)  + self.sep +  now_time + self.sep + temp[0] + self.sep + temp[1] + self.sep +  temp[13][3:] + self.sep + \
			temp[15] + self.sep + temp[16] + self.sep + str(int(temp[4])/1024) + self.sep + temp[5] + self.sep + temp[2]

			output_string[0] = output_string[0] + one_item + '\n'
			#output_string[1] = output_string[1] + sql_item + '\n'

		self.count = self.count + 1
		return output_string
	
	def get_qd_mem(self, filename = ['', ''], interval = 5):
		count = 0
		while(os.path.exists(self.run_lock) and count < 300):

			result = self.__get_qd_mem()
			if result is None:
				count = count + 1
				time.sleep(1)
				continue
			
			self.report(filename = filename[0], msg = result[0])
			#self.report(filename = filename[1], msg = result[1])

			time.sleep(interval)



	# record all query in memory
	def __get_qd_info1(self):
		# -R '***' set record separator '***' (default: newline)
		cmd = ''' psql -d postgres -t -A -R '***' -c "select sess_id,query_start,procpid,usename,datname,current_query from pg_stat_activity where current_query not like '%from pg_stat_activity%' order by sess_id,query_start,procpid;" '''
		#cmd = ''' psql -d postgres -t -c "select sess_id,query_start,procpid,usename,datname from pg_stat_activity order by sess_id,query_start;" '''
		(status, output) = commands.getstatusoutput(cmd)
		if status != 0 or output == '':
			print 'error code: ' + str(status) + ' output: ' + output + ' in qd_info'
			return None

		''' sess_id  query_start  procpid  usename  datname  current_query '''
		line_item = output.split('***')
		output_string = ['', '']
		
		for line in line_item:
			line = line.split('|')
			if line[1] == '':
				continue
			try:
				query_start_time = datetime.strptime(line[1][:-3].strip(), "%Y-%m-%d %H:%M:%S.%f")
			except Exception, e:
				print 'time error ' + str(line)
				continue

			if ( line[0] not in self.query_record.keys() ) or ( line[0] in self.query_record.keys() and query_start_time > self.query_record[line[0]] ):
				self.query_record[line[0]] = query_start_time
				
				one_item = line[0] + self.sep + str(query_start_time) + self.sep + line[2] + self.sep + line[3] + self.sep + line[4]
				sql_item = "insert into moni.qd_info values (%s, '%s', %s, '%s', '%s');" \
				% (line[0], str(query_start_time), line[2], line[3], line[4])
				
				output_string[0] = output_string[0] + one_item + '\n'
				output_string[1] = output_string[1] + sql_item + '\n'

		return output_string

	# only record current query in memory
	def __get_qd_info(self):
		now_time = datetime.now()
		# -R '***' set record separator '***' (default: newline)
		cmd = ''' psql -d postgres -t -A -R '***' -c "select sess_id,query_start,usename,datname from pg_stat_activity where current_query not like '%from pg_stat_activity%' order by sess_id,query_start,procpid;" '''
		#cmd = ''' psql -d postgres -t -c "select sess_id,query_start,procpid,usename,datname from pg_stat_activity order by sess_id,query_start;" '''
		(status, output) = commands.getstatusoutput(cmd)
		if status != 0 or output == '':
			print 'error code: ' + str(status) + ' output: ' + output + ' in qd_info'
			return None

		'''line_item = sess_id|query_start|procpid|usename|datname '''
		all_items = output.split('***')
		output_string = ['', '']

		
		for current_query in self.current_query_record:
			if current_query not in all_items:
				line = current_query.split('|')
				if line[1] == '':
					continue
				try:
					query_start_time = datetime.strptime(line[1][:-3].strip(), "%Y-%m-%d %H:%M:%S.%f")
				except Exception, e:
					print 'time error ' + str(line)
					continue

				one_item = line[0] + self.sep + str(query_start_time) + self.sep + str(now_time) + self.sep +line[2] + self.sep + line[3]
				#sql_item = "insert into moni.qd_info values (%s, '%s', '%s', '%s', '%s');" \
				#% (line[0], str(query_start_time), str(now_time), line[2], line[3])
				
				output_string[0] = output_string[0] + one_item + '\n'
				#output_string[1] = output_string[1] + sql_item + '\n'

				self.current_query_record.remove(current_query)

		for line_item in all_items:
			if line_item not in self.current_query_record:
				self.current_query_record.append(line_item)

		return output_string

	def get_qd_info(self, filename = ['', ''], interval = 1):
		count = 0
		while(os.path.exists(self.run_lock)):
			result = self.__get_qd_info()
			if result is None:
				count = count + 1
				time.sleep(2)
				continue

			self.report(filename = filename[0], msg = result[0])
			#self.report(filename = filename[1], msg = result[1])

			time.sleep(interval)

		now_time = datetime.now()
		if len(self.current_query_record) != 0:
			#print len(self.current_query_record)
			output_string = ['', '']
			for current_query in self.current_query_record:
				line = current_query.split('|')
				if line[1] == '':
					continue
				try:
					query_start_time = datetime.strptime(line[1][:-3].strip(), "%Y-%m-%d %H:%M:%S.%f")
				except Exception, e:
					print 'time error ' + str(line)
					continue

				one_item = line[0] + self.sep + str(query_start_time) + self.sep + str(now_time) + self.sep +line[2] + self.sep + line[3]
				#sql_item = "insert into moni.qd_info values (%s, '%s', '%s', '%s', '%s');" \
				#% (line[0], str(query_start_time), str(now_time), line[2], line[3])
				
				output_string[0] = output_string[0] + one_item + '\n'
				#output_string[1] = output_string[1] + sql_item + '\n'
		
		self.report(filename = filename[0], msg = output_string[0])
		#self.report(filename = filename[1], msg = output_string[1])



	def stop(self):
		os.system('rm -rf %s' % (self.run_lock))
		cmd = " gpssh -f %s -e 'rm -rf %s/run.lock' " % (self.hostfile_seg, self.seg_tmp_folder)
		print cmd
		(s, o) = commands.getstatusoutput(cmd)
		print s, o


	def start(self):
		self.setup()
		cmd = " gpssh -f %s -e 'cd %s; nohup python -u MonitorSeg.py %s %s > monitor.log 2>&1 &' " % (self.hostfile_seg, self.seg_tmp_folder, self.report_folder, self.hostname)
		(s, o) = commands.getstatusoutput(cmd)
		print s, o

		prefix = self.report_folder + os.sep
		p1 = Process( target = self.get_qd_info, args = ( [prefix+'qd_info.data', prefix+'_qd_info.sql', ''], ) )
		p2 = Process( target = self.get_qd_mem, args = ( [prefix+'qd_mem_cpu.data', prefix+'_qd_mem.sql', ''], ) )
		p1.start()
		p2.start()

monitor_control = Monitor_control()

if __name__ == "__main__" :
	monitor_control.start()