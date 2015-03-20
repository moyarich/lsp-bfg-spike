#!/usr/bin/env python
import os,sys,commands,time,socket
from datetime import datetime
from multiprocessing import Process
from pygresql import pg

gphome = os.getenv('GPHOME')
if gphome.endswith('/'):
	gphome = gphome[:-1]
pexpect_dir = gphome + os.sep + 'bin' + os.sep + 'lib'
if pexpect_dir not in sys.path:
    sys.path.append(pexpect_dir)

try:
    import pexpect
except ImportError:
    sys.stderr.write('scp ssh needs pexpect\n')
    sys.exit(2)


class Monitor_control():
	
	def __init__(self, mode = 'local', interval = 5, timeout = 120, remote_host = 'gpdb63.qa.dh.greenplum.com', run_id = 0):
		assert mode in ['local', 'remote']
		self.mode = mode
		self.interval = int(interval)
		self.timeout = int(timeout)
		self.remote_host = remote_host
		self.run_id = int(run_id)
		
		self.query_record = {}
		self.current_query_record = []
		
		self.seg_script = ''
		self.local_schema_script = ''

		# prep report folder on master and tmp folder on seg host
		self.init_time = datetime.now().strftime('%Y%m%d-%H%M%S')
		self.seg_tmp_folder = '/tmp/monitor_report/' + self.init_time
		self.report_folder = os.getcwd() + os.sep + 'monitor_report' + os.sep + self.init_time
		self.run_lock = self.report_folder + os.sep + 'run.lock'
		
		self.hostfile_seg = self.report_folder + os.sep + 'hostfile_seg'

		(s,o) = commands.getstatusoutput('hostname')
		self.hostname = o.strip()

		self.sep = '|'
		
	def report(self, filename, msg, mode = 'a'):
		if msg != '':
		    fp = open(filename, mode)
		    fp.write(msg)
		    fp.flush()
		    fp.close()

	# ssh gpadmin@gpdb63.qa.dh.greenplum.com -e "pwd;ls"
	# scp qe_mem_cpu.data gpadmin@gpdb63.qa.dh.greenplum.com:~/
	def ssh_command(self, cmd, password = 'changeme'):
		ssh_newkey = 'Are you sure you want to continue connecting'
		child = pexpect.spawn(cmd, timeout = 600)
		try:
			i = child.expect([pexpect.TIMEOUT, ssh_newkey, 'password:'])
		except Exception,e:
			return child.before
		else:
			if i == 0:
				print str(os.getpid()) + ': ', 'ERROR!'
				print str(os.getpid()) + ': ', 'SSH could not login. Here is what SSH said:'
				print str(os.getpid()) + ': ', child.before, child.after
				return None
			# SSH does not have the public key. Just accept it.
			if i == 1:
				child.sendline ('yes')
				try:
					j = child.expect([pexpect.TIMEOUT, 'password:'])
				except Exception,e:
					return child.before
				else:
					# Timeout
					if j == 0:
						print str(os.getpid()) + ': ', 'ERROR!'
						print str(os.getpid()) + ': ', 'SSH could not login. Here is what SSH said:'
						print str(os.getpid()) + ': ', child.before, child.after
						return None
					else:
						child.sendline(password)
			if i == 2:
				child.sendline(password)

		child.expect(pexpect.EOF)
		return child.before

	def remote_sql(self, sql):
		#os.system( 'mkdir -p %s' % (self.report_folder + os.sep + 'seg_log') )
		with open(self.report_folder + os.sep + 'monitor_temp.sql', 'w') as fsql:
			fsql.write(sql)

		cmd1 = "scp %s gpadmin@%s:%s" % (self.report_folder + os.sep + 'monitor_temp.sql', self.remote_host, '/tmp/')
		result1 = self.ssh_command(cmd = cmd1)

		cmd2 = 'ssh gpadmin@%s "source ~/psql.sh; psql -d postgres -f %s"' % (self.remote_host, '/tmp/monitor_temp.sql')
		result2 = self.ssh_command(cmd = cmd2)


	def scp_data(self, filename):
		if not os.path.exists(self.report_folder + os.sep + filename):
			print str(os.getpid()) + '%s is not exists when copy it to remote database.' % (filename)
			return None

		table_name = filename[filename.find('qd'):filename.rindex('_')]
		if self.mode == 'local':
			cmd = '''psql -d postgres -c "COPY moni.%s FROM '%s' WITH DELIMITER '|';" ''' % (table_name, self.report_folder + os.sep + filename)
			(s, o) = commands.getstatusoutput(cmd)
			if o.find('COPY') != -1 and o.find('ERROR') == -1:
				print str(os.getpid()) + ': ', 'copy file %s success.'% (filename), o.strip()
			else:
				print str(os.getpid()) + ': ', cmd, o.strip()
		else:
			count = 0
			while (count < 10):
				time.sleep(count)
				filepath = self.report_folder + os.sep + filename
				cmd1 = "scp %s gpadmin@%s:%s" % (filepath, self.remote_host, self.seg_tmp_folder)
				result1 = self.ssh_command(cmd = cmd1)

				cmd2 = "COPY hst.%s FROM '%s' WITH DELIMITER '|';" % (table_name, self.seg_tmp_folder + os.sep + filename)
				copy_file = filename[:-5] + '.sql'
				with open (self.report_folder + os.sep + copy_file, 'w') as fcopy:
					fcopy.write(cmd2)

				cmd3 = "scp %s gpadmin@%s:%s" % (self.report_folder + os.sep + copy_file, self.remote_host, self.seg_tmp_folder)
				result3 = self.ssh_command(cmd = cmd3)

				cmd4 = 'ssh gpadmin@%s "source ~/psql.sh; cd %s; psql -d hawq_cov -f %s; rm -rf %s"' % (self.remote_host, self.seg_tmp_folder, copy_file, copy_file)
				result4 = self.ssh_command(cmd = cmd4)
				if result4.find('COPY') != -1 and result4.find('ERROR') == -1:
					print str(os.getpid()) + ': ', 'copy file %s success in %d times.'% (filename, count + 1), result4
					break
				else:
					count += 1
			
			if count == 10:
				print str(os.getpid()) + ': ', 'copy file %s error for %d times, the last time error is below: '% (filename, count)
				print str(os.getpid()) + ': ', cmd1, '\n', result1
				print str(os.getpid()) + ': ', cmd2
				print str(os.getpid()) + ': ', cmd3, '\n', result3
				print str(os.getpid()) + ': ', cmd4, '\n', result4

	def setup(self):
		self._get_monitor_seg_script_path()
		
		os.system( 'mkdir -p %s' % (self.report_folder + os.sep + 'seg_log') )
		self._get_seg_list(hostfile = self.hostfile_seg)
		
		os.system( 'touch %s' % (self.run_lock) )
		
		# make tmp dir on every seg host
		monitor_info = self.mode + '|' + self.report_folder + '|' + self.hostname + '|' + self.remote_host + '|' + str(self.interval) + '|' + str(self.timeout) + '|' + str(self.run_id)
		for hostname in self.host_list:
			seg_info = hostname + '|' + monitor_info
			cmd = ''' gpssh -h %s -e "mkdir -p %s; echo '%s' > %s/run.lock" ''' % (hostname, self.seg_tmp_folder, seg_info, self.seg_tmp_folder)
			(s, o) = commands.getstatusoutput(cmd)
			if s != 0:
				print str(os.getpid()) + ': ', 'perp monitor report folder on seg host %s error.' % (hostname)
				print str(os.getpid()) + ': ', s, o
				sys.exit(2)

		# gpscp seg monitor script to every seg host
		cmd = 'gpscp -f %s %s =:%s' % (self.hostfile_seg, self.seg_script, self.seg_tmp_folder)
		(s, o) = commands.getstatusoutput(cmd)
		if s != 0:
			print str(os.getpid()) + ':', 'gpscp monitor node script to every node error.'
			print str(os.getpid()) + ':', s, o
			sys.exit(2)

		if self.mode == 'remote':
			cmd = 'ssh gpadmin@%s "mkdir -p %s"' % (self.remote_host, self.seg_tmp_folder)
			result = self.ssh_command(cmd = cmd)
		else:
			cmd = "psql -d postgres -f %s" % (self.local_schema_script)
			(s, o) = commands.getstatusoutput(cmd)
			print str(os.getpid()) + ': ', '\n', o

	def _get_monitor_seg_script_path(self):
		for one_path in sys.path:
			if one_path.endswith('monitor'):
				self.seg_script = one_path + os.sep + 'MonitorSeg.py'
				self.local_schema_script = one_path + os.sep + 'schema.sql'
				if os.path.exists(self.seg_script):
					return 0
		print str(os.getpid()) + ': ', 'not find MonitorSeg.py when setup monitor. '
		sys.exit(2)

	def _get_seg_list(self, hostfile = 'hostfile_seg'):
		cmd = ''' psql -d postgres -t -A -c "select distinct hostname from gp_segment_configuration where role = 'p';" '''
		(status, output) = commands.getstatusoutput(cmd)
		
		if status != 0:
			print str(os.getpid()) + ': ', 'Unable to select gp_segment_configuration in monitor_control.'
			sys.exit(2)
		
		with open(hostfile, 'w') as fnode:
			fnode.write(output + '\n')
		self.host_list = output.split('\n')
		

	# only record current query in memory
	def _get_qd_info(self):
		now_time = datetime.now()
		if self.mode == 'local':
			sql = "select sess_id,query_start,usename,datname from pg_stat_activity where current_query not like '%from pg_stat_activity%' and datname not like 'postgres' order by sess_id,query_start;"
		else:
			sql = "select sess_id,query_start,usename,datname from pg_stat_activity where current_query not like '%from pg_stat_activity%' and datname not like 'postgres' order by sess_id,query_start;"
		# -R '***' set record separator '***' (default: newline)
		cmd = ''' psql -d postgres -t -A -R '***' -c "%s" ''' % (sql)
		(status, output) = commands.getstatusoutput(cmd)
		if status != 0 or output == '':
			#print str(os.getpid()) + ':'  + 'error code: ' + str(status) + ' output: ' + output + ' in qd_info'
			return None

		''' line_item = sess_id|query_start|usename|datname|current_query '''
		all_items = output.split('***')
		output_string = ''
		
		del_query = []
		for current_query in self.current_query_record:
			if current_query not in all_items:
				del_query.append(current_query)
				line = current_query.split('|')
				if len(line) != 4 or line[1] == '':
					print 'error:', line
					continue
				try:
					query_start_time = datetime.strptime(line[1][:-3].strip(), "%Y-%m-%d %H:%M:%S.%f")
				except Exception, e:
					print str(os.getpid()) + ':', line, '\n', str(e)
					continue

				one_item = str(self.run_id) + self.sep + line[0] + self.sep + str(query_start_time) + self.sep + str(now_time) + self.sep + line[2] + self.sep + line[3] + self.sep #+ line[4].strip().replace('\n', ' ')
				output_string = output_string + one_item + '\n'
		
		for current_query in del_query:
			self.current_query_record.remove(current_query)

		for line_item in all_items:
			if line_item not in self.current_query_record:
				self.current_query_record.append(line_item)

		return output_string
	
	def get_qd_info(self, interval = 1):
		count = 0
		file_no = 1
		filename = self.hostname + '_qd_info_' + str(file_no) + '.data'

		while(os.path.exists(self.run_lock)):
			if count == self.timeout:
				p1 = Process( target = self.scp_data, args = (filename, ) )
				p1.start()
				count = 0
				file_no += 1
				filename = self.hostname + '_qd_info_' + str(file_no) + '.data'

			result = self._get_qd_info()
			if result is None:
				time.sleep(1)
				continue

			if result != '':
				self.report(filename = self.report_folder + os.sep + filename, msg = result)
				count += 1
			time.sleep(interval)

		now_time = datetime.now()
		if len(self.current_query_record) != 0:
			output_string = ''
			for current_query in self.current_query_record:
				line = current_query.split('|')
				if len(line) != 4 or line[1] == '':
					continue
				try:
					query_start_time = datetime.strptime(line[1][:-3].strip(), "%Y-%m-%d %H:%M:%S.%f")
				except Exception, e:
					#print str(os.getpid()) + ':'  + 'time error ' + str(line)
					continue

				one_item = str(self.run_id) + self.sep + line[0] + self.sep + str(query_start_time) + self.sep + str(now_time) + self.sep +line[2] + self.sep + line[3] + self.sep #+ line[4].strip().replace('\n', ' ')
				output_string = output_string + one_item + '\n'
		
			self.report(filename = self.report_folder + os.sep + filename, msg = output_string)
			time.sleep(15)
			self.scp_data(filename = filename)
		 
		print str(os.getpid()) + ': ', 'get_qd_info process normally stop.', 'Total ', file_no, ' files'
	

	'''
	ps -eo pid,ppid,pcpu,vsz,rss,pmem,state,command | grep postgres | grep -vE "bin/postgres|logger|stats|writer|checkpoint|seqserver|WAL|ftsprobe|sweeper|sh -c|bash|grep|seg|pg_stat_activity|resource manager|psql -d postgres|Metadata Cache process"
	 PID   PPID  %CPU  VSZ   RSS  %MEM STATE CMD          
	10836  3817  0.5 655800 27068  0.6  S  postgres: port  5432, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(34512) con202 127.0.0.1(34512) cmd1 SELECT
	10836  3817  0.5 655800 27068  0.6  S  postgres: port  5432, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(34512) con202 127.0.0.1(34512) cmd1 idle
index  0     1    2    3      4     5   6    7       8      9      10               11                     12             13       14            15    16          
	'''

	def _get_qd_mem_cpu(self, timeslot, now_time): 
		filter_string = 'bin/postgres|logger|stats|writer|checkpoint|seqserver|WAL|ftsprobe|sweeper|sh -c|bash|grep|seg|pg_stat_activity|resource manager|psql -d postgres|HAWQ|build|NULL|Metadata Cache process'
		cmd = ''' ps -eo pid,ppid,pcpu,vsz,rss,pmem,state,command | grep postgres | grep -vE "%s" ''' % (filter_string)
		(status, output) = commands.getstatusoutput(cmd)
		if status != 0 or output == '':
			pass
		
		line_item = output.splitlines()
		output_string = ''
		
		for line in line_item:
			temp = line.split()
			if len(temp) != 17 or temp[13][:3] != 'con':
				continue
			try:
				# tr_id, hostname, timeslot, real_time, pid, ppid, con_id, cmd, status, rss, pmem, pcpu	  
				one_item = str(self.run_id) + self.sep + self.hostname + self.sep + str(timeslot)  + self.sep +  now_time + self.sep + temp[0] + self.sep + temp[1] + self.sep +  temp[13][3:] + self.sep + \
				temp[15] + self.sep + temp[16] + self.sep + str(int(temp[4])/1024) + self.sep + temp[5] + self.sep + temp[2]
			except Exception, e:
				#print str(os.getpid()) + ':'  + temp
				continue

			output_string = output_string + one_item + '\n'

		return output_string

	
	def get_qd_data(self, function = 'self._get_qd_mem_cpu'):
		count = 1   # control scp data with self.timeout
		file_no = 1
		filename = self.hostname + '_' + function[10:] + '_' + str(file_no) + '.data'
		
		while(os.path.exists(self.run_lock)): 
			timeslot = (file_no - 1) * self.timeout + count
			now_time = str(datetime.now())
			result = eval(function + '(timeslot, now_time)')

			if count > self.timeout:
				p1 = Process( target = self.scp_data, args = (filename, ) )
				p1.start()
				count = 1
				file_no += 1
				filename = self.hostname + '_' + function[10:] + '_' + str(file_no) + '.data'

			self.report(filename = self.report_folder + os.sep + filename, msg = result)
			count += 1
			time.sleep(self.interval)

		time.sleep(15)
		self.scp_data(filename = filename)

		print str(os.getpid()) + ': ', '%s normally stop.' % (function[10:]), 'Total ', file_no, ' files'

	
	def get_qd_data_sync(self, function = 'self._get_qd_mem_cpu'):
		count = 1   # control scp data with self.timeout
		file_no = 1
		filename = self.hostname + '_' + function[10:] + '_' + str(file_no) + '.data'
		
		while(os.path.exists(self.run_lock)): 
			timeslot = (file_no - 1) * self.timeout + count
			now_time = str(datetime.now())
			result = eval(function + '(timeslot, now_time)')

			if count > self.timeout:
				p1 = Process( target = self.scp_data, args = (filename, ) )
				p1.start()
				count = 1
				file_no += 1
				filename = self.hostname + '_' + function[10:] + '_' + str(file_no) + '.data'

			self.report(filename = self.report_folder + os.sep + filename, msg = result)
			count += 1

			for addr in self.host_list:
				try:
					sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					sock.connect((addr, 8001))
					sock.send( now_time + '*' + str(timeslot) )
					sock.close()
				except Exception, e:
					print addr, 'error:', str(e), 
					cmd = " gpssh -h %s -e 'cd %s; nohup python -u MonitorSeg.py %s sync >> monitor.log 2>&1 &' " % (addr, self.seg_tmp_folder, pexpect_dir)
					(s, o) = commands.getstatusoutput(cmd)
					print str(os.getpid()) + ': ', '\n', o

			time.sleep(self.interval)

		time.sleep(15)
		self.scp_data(filename = filename)

		for addr in self.host_list:
			try:
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect((addr, 8001))
				sock.send( 'stop' + '*' + str(timeslot))
				#data=sock.recv(1024)
				#print data
				sock.close()
			except Exception, e:
				print addr, 'error when stop monitor.'

		print str(os.getpid()) + ': ', '%s normally stop.' % (function[10:]), 'Total ', file_no, ' files'


	def stop(self):
		while( os.path.exists(self.run_lock) ):
			os.system('rm -rf %s' % (self.run_lock))
		print str(os.getpid()) + ': ', 'remove monitor run.lock file and stop.'

		cmd = "gpssh -f %s -e 'rm -rf %s/run.lock'" % (self.hostfile_seg, self.seg_tmp_folder)
		(s, o) = commands.getstatusoutput(cmd)
		print str(os.getpid()) + ': ', cmd, '\n', o.strip()
		time.sleep(60)


	def start(self, mode = 'async'):
		assert mode in ['async', 'sync']
		self.setup()
		if mode == 'async':
			cmd = " gpssh -f %s -e 'cd %s; nohup python -u MonitorSeg.py %s async > monitor.log 2>&1 &' " % (self.hostfile_seg, self.seg_tmp_folder, pexpect_dir)
			(s, o) = commands.getstatusoutput(cmd)
			print str(os.getpid()) + ': ', '\n', o
			p1 = Process( target = self.get_qd_info, args = (1, ) )
			p2 = Process( target = self.get_qd_data, args = () )
			p1.start()
			p2.start()
		else:
			cmd = " gpssh -f %s -e 'cd %s; nohup python -u MonitorSeg.py %s sync > monitor.log 2>&1 &' " % (self.hostfile_seg, self.seg_tmp_folder, pexpect_dir)
			(s, o) = commands.getstatusoutput(cmd)
			print str(os.getpid()) + ': ', '\n', o
			p1 = Process( target = self.get_qd_info, args = (1, ) )
			p2 = Process( target = self.get_qd_data_sync, args = () )
			p1.start()
			p2.start()



#monitor_control = Monitor_control()#(mode = 'remote')

if __name__ == "__main__":
	m = Monitor_control()
	sql = 'select version();'
	m.remote_sql(sql = sql)