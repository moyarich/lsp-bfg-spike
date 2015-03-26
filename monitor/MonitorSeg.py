#!/usr/bin/env python
import os,sys,commands,time
from datetime import datetime
import subprocess
from multiprocessing import Process

pexpect_dir = sys.argv[1]
if pexpect_dir not in sys.path:
    sys.path.append(pexpect_dir)

try:
    import pexpect
except ImportError:
    sys.stderr.write('scp ssh needs pexpect\n')
    sys.exit(2)

class Monitor_seg():

	def __init__(self):
		self.pwd = os.getcwd()
		
		(s, o) = commands.getstatusoutput('cat run.lock')
		seg_info = o.strip().split('|')
		self.hostname = seg_info[0]
		self.mode = seg_info[1]
		self.master_folder = seg_info[2]
		self.master_host = seg_info[3]
		self.remote_host = seg_info[4]
		self.interval = int(seg_info[5])
		self.timeout = int(seg_info[6])
		self.run_id = int(seg_info[7])

		self.sep = '|'

	def report(self, filename, msg):
		if msg != '':
		    fp = open(filename, 'a')  
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


	def scp_data(self, filename):
		if not os.path.exists(filename):
			print str(os.getpid()) + ': ', '%s is not exists when copy it remote database.' % (filename)
			return None

		if self.mode == 'local':
			host = self.master_host
			folder = self.master_folder
			db = 'postgres'
			schema = 'moni'
			source = ''
		else:
			host = self.remote_host
			folder = self.pwd
			db = 'hawq_cov'
			schema = 'hst'
			source = 'source ~/psql.sh;'
		
		count = 0
		while (count < 10):
			time.sleep(count)

			cmd1 = "scp %s gpadmin@%s:%s" % (filename, host, folder)
			result1 = self.ssh_command(cmd = cmd1)

			table_name = filename[filename.find('qe'):filename.rindex('_')]

			cmd2 = "COPY %s.%s FROM '%s' WITH DELIMITER '|';" % (schema, table_name, folder + os.sep + filename)
			copy_file = filename[:-5] + '.sql'
			with open (copy_file, 'w') as fcopy:
				fcopy.write(cmd2)

			cmd3 = "scp %s gpadmin@%s:%s" % (copy_file, host, folder)
			result3 = self.ssh_command(cmd = cmd3)

			cmd4 = 'ssh gpadmin@%s "%s cd %s; psql -d %s -f %s; rm -rf %s"' % (host, source, folder, db, copy_file, copy_file)
			result4 = self.ssh_command(cmd = cmd4)
			if result4.find('COPY') != -1 and result4.find('ERROR') == -1:
				print str(os.getpid()) + ': ' ,  'copy file %s success in %d times. '% (filename, count + 1), result4
				break
			else:
				count += 1
			
		if count == 10:
			print str(os.getpid()) + ': ', 'copy file %s error for %d times, the last time error is below: '% (filename, count)
			print str(os.getpid()) + ': ', cmd1, '\n', result1
			print str(os.getpid()) + ': ', cmd2
			print str(os.getpid()) + ': ', cmd3, '\n', result3
			print str(os.getpid()) + ': ', cmd4, '\n', result4

	
	'''
	ps -eo pid,pcpu,vsz,rss,pmem,state,command | grep postgres | grep seg | grep -vE "bin/postgres|logger|stats|writer|checkpoint|seqserver|WAL|ftsprobe|sweeper|sh -c|bash|grep|seg-|resource manager"
	   pid %CPU  VSZ  RSS  %MEM STATE CMD          
	  1034  1.0 656480 16676  0.4 S postgres: port 40001, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(51217) con64 seg1 idle
	  1676  0.0 538684 12280  0.3 S postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(37854) con81 seg0 cmd6 MPPEXEC INSERT             
	  1675  0.0 538692 12380  0.3 S postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(37855) con81 seg0 cmd6 slice1 MPPEXEC INSERT                          
 	  1035  0.8 658804 20844  0.5 S postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(43204) con82 seg0 cmd2 slice1 MPPEXEC SELECT
index  0    1      2     3     4  5    6       7     8       9             10                        11           12     13  14    15      16     17
	'''
	def _get_qe_mem_cpu(self, timeslot, real_time = None):
		filter_string = 'bin/postgres|logger|stats|writer|checkpoint|seqserver|WAL|ftsprobe|sweeper|sh -c|bash|grep|seg-|resource manager'
		cmd = ''' ps -eo pid,pcpu,vsz,rss,pmem,state,command | grep postgres | grep seg | grep -vE "%s" ''' % (filter_string)
		#print str(os.getpid()) + ': ' ,  cmd
		(status, output) = commands.getstatusoutput(cmd)
		if status != 0 or output == '':
			#pass
			print str(os.getpid()) + ': ', timeslot, ': return code = ', status, ' output = ', output, ' in qe_mem_cpu'
		
		line_item = output.splitlines()
		if real_time is not None:
			now_time = str(real_time)
		else:
			now_time = str(datetime.now())
		output_string = ''
		#print str(os.getpid()) + ': ', output
		
		for line in line_item:
			temp = line.split()
			if len(temp) < 15 or temp[12][:3] != 'con' or temp[13][:3] != 'seg':
				continue
			# tr_id, hostname, timeslot, real_time, pid, con_id, seg_id, cmd, slice, status, rss, pmem, pcpu
			try:
				if temp[14] == 'idle':
					one_item = str(self.run_id) + self.sep + self.hostname + self.sep + str(timeslot) + self.sep + now_time + self.sep + temp[0] + self.sep + temp[12][3:] + self.sep + temp[13] + self.sep + 'NULL' + self.sep + 'NUll' + self.sep + temp[14] + self.sep + str(int(temp[3])/1024) + self.sep + temp[4] + self.sep + temp[1]
				elif temp[15].find('slice') != -1:
					one_item = str(self.run_id) + self.sep + self.hostname + self.sep + str(timeslot) + self.sep + now_time + self.sep + temp[0] + self.sep + temp[12][3:] + self.sep + temp[13] + self.sep + temp[14] + self.sep + temp[15] + self.sep + temp[17] + self.sep + str(int(temp[3])/1024) + self.sep + temp[4] + self.sep + temp[1]
				else:
					one_item = str(self.run_id) + self.sep + self.hostname + self.sep + str(timeslot) + self.sep + now_time + self.sep + temp[0] + self.sep + temp[12][3:] + self.sep + temp[13] + self.sep + temp[14] + self.sep + 'NULL' + self.sep + temp[16] + self.sep + str(int(temp[3])/1024) + self.sep + temp[4] + self.sep + temp[1]
			except Exception, e:
				print str(os.getpid()) + ': ', temp, '\n', str(e)
				continue
			output_string = output_string + one_item + '\n'
		
		return output_string
	
	def get_qe_data(self, function = 'self._get_qe_mem_cpu'):
		file_no = 1
		count = 1   # control scp data with self.timeout
		filename = self.hostname + '_' + function[10:] + '_' + str(file_no) + '.data'
		
		while(os.path.exists('run.lock')):
			timeslot = (file_no - 1) * self.timeout + count
			result = eval(function + '(timeslot)')

			if count > self.timeout:
				p1 = Process( target = self.scp_data, args = (filename, ) )
				p1.start()
				count = 1
				file_no = file_no + 1
				filename = self.hostname + '_' + function[10:] + '_' + str(file_no) + '.data'
			
			self.report(filename = filename, msg = result)
			count += 1
			time.sleep(self.interval)

		time.sleep(15)
		self.scp_data(filename = filename)

		print str(os.getpid()) + ': ', '%s normally stop.' % (function[10:]), 'Total ', file_no, ' files'
		os.system('rm -rf run.lock')
	
	def get_qe_data_sync(self, function = 'self._get_qe_mem_cpu', timeslot = 0, real_time = ''):
		beg_time = datetime.now()
		file_no = (timeslot - 1) / self.timeout + 1
		filename = self.hostname + '_' + function[10:] + '_' + str(file_no) + '.data'
		
		if now_time == 'stop':
			time.sleep(15)
			self.scp_data(filename = filename)
			print str(os.getpid()) + ': ', '%s normally stop.' % (function[10:]), 'Total ', file_no, ' files'
			os.system('rm -rf run.lock')
			return 0

		result = eval(function + '(timeslot, now_time)')
		self.report(filename = filename, msg = result)
		if (timeslot % self.timeout) == 0:
			p1 = Process( target = self.scp_data, args = (filename, ) )
			p1.start()

		end_time = datetime.now()
		duration = end_time - beg_time
		duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds/1000
		print timeslot, ': ', duration, 'ms'
    	

monitor_seg = Monitor_seg()

if __name__ == "__main__" :
	if sys.argv[2] == 'async':
		p1 = Process( target = monitor_seg.get_qe_data, args = ('self._get_qe_mem_cpu', ) )
		p1.start()
		p1.join()
		print str(os.getpid()) + ': ', 'async collenct qe data process stop.'
		cmd = "gpscp -h %s monitor.log =:%s/seg_log/%s.log" % (monitor_seg.master_host, monitor_seg.master_folder, monitor_seg.hostname)
		print str(os.getpid()) + ': ', cmd
		os.system(cmd)
	else:
		import socket   
		s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)  
		s.bind((monitor_seg.hostname, 8001))  
		s.listen(1)
		while 1:
			conn,addr = s.accept()
			data = conn.recv(1024).split('*')
			now_time = data[0]
			timeslot = int(data[1])
			monitor_seg.get_qe_data_sync(timeslot = timeslot, real_time = now_time)
			if data[0] == 'stop':
				print 'get master stop signal.'
				break
			#conn.send('done')
		conn.close()
		cmd = "gpscp -h %s monitor.log =:%s/seg_log/%s.log" % (monitor_seg.master_host, monitor_seg.master_folder, monitor_seg.hostname)
		print str(os.getpid()) + ': ', cmd
		os.system(cmd)
	#os.system('rm -rf /tmp/monitor_report/*')
	cmd = 'rm -rf /tmp/monitor_report/%s' % (os.getcwd().split('/')[-1])
	#os.system(cmd)

	
	
