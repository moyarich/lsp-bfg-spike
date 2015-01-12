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

	def __init__(self, timeout = 120):
		self.master_folder = sys.argv[2]
		self.master_host = sys.argv[3]
		self.remote_host = 'gpdb63.qa.dh.greenplum.com'

		self.local = True
		if sys.argv[4] == 'remote':
			self.local = False
			self.remote_host = sys.argv[5]
		
		self.pwd = os.getcwd()
		(s, o) = commands.getstatusoutput('hostname')
		self.hostname = o.strip()
		self.timeout = timeout
		self.count = 1
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
		        print 'ERROR!'
		        print 'SSH could not login. Here is what SSH said:'
		        print child.before, child.after
		        return None
		    # SSH does not have the public key. Just accept it.
		    if i == 1: 
		        child.sendline ('yes')
		        j = child.expect([pexpect.TIMEOUT, 'password: '])
		        # Timeout
		        if j == 0: 
		            print 'ERROR!'
		            print 'SSH could not login. Here is what SSH said:'
		            print child.before, child.after
		            return None
		        else:
		        	child.sendline(password)
		    if i == 2:
		    	child.sendline(password)
	    
	    child.expect(pexpect.EOF)
	    return child.before


	'''
	 PID    USER    PR  NI  VIRT  RES  SHR S %CPU %MEM    TIME+  COMMAND
	30705 gpadmin   39  19  528m  12m 6408 S  0.0  0.3   0:00.00 postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(38011) con1140 seg6 cmd2 slice4 MPPEXEC SELECT
	35019 gpadmin   39  19  526m 9944 5964 S  0.0  0.3   0:00.00 postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(40991) con1376 seg0 cmd6 MPPEXEC INSERT                                                                                                                                                   
    35021 gpadmin   39  19  526m 9.8m 5996 S  0.0  0.3   0:00.00 postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(40992) con1376 seg0 cmd6 slice1 MPPEXEC INSERT                                                                                                                                            
	30713 gpadmin   39  19  526m  10m 6604 S  0.0  0.3   0:00.00 postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(38015) con1140 seg3 idle 
index 0      1      2   3    4     5   6   7   8    9      10     11         12   13     14              15                       16            17     18   19    20     21      22
	'''
	def _get_qe_mem_cpu_by_top(self):
		filter_string = 'bin/postgres|logger|stats|writer|checkpoint|seqserver|WAL|ftsprobe|sweeper|sh -c|bash|grep|seg-|resource manager'
		grep_string1 = 'postgres'
		grep_string2 = 'seg'
		cmd = ''' top -n 1 -b -c | grep postgres | grep seg | grep -vE "bin/postgres|logger|stats|writer|checkpoint|seqserver|WAL|ftsprobe|sweeper|sh -c|bash|grep|resource manager" ''' 
		p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
		output, error = p.communicate()
		
		if error is not None or output == '':
			print 'error: ' + str(error) + ' output: ' + output + 'in qe_mem_cpu'
			return None
		
		line_item = output.splitlines()
		now_time = str(datetime.now())
		output_string = ['', '']
		
		for line in line_item:
			temp = line.split()
			if len(temp) < 20:
				continue

			# hostname, count, time_point, pid, con_id, seg_id, cmd, slice, status, rss, pmem, pcpu
			if temp[19] == 'idle':
				one_item = self.hostname + '\t' + str(self.count) + '\t' + now_time + '\t' + temp[0] + '\t' + temp[17] + '\t' + temp[18] + '\t' + temp[19] + '\t' + 'NUll' + '\t' + 'NULL' + '\t' + temp[5]+ '\t' + temp[9] + '\t' + temp[8]
			elif temp[20].find('slice') != -1:
				one_item = self.hostname + '\t' + str(self.count) + '\t' + now_time + '\t' + temp[0] + '\t' + temp[17] + '\t' + temp[18] + '\t' + temp[19] + '\t' + temp[20] + '\t' + temp[22] + '\t' + temp[5] + '\t' + temp[9] + '\t' + temp[8]
			else:
				one_item = self.hostname + '\t' + str(self.count) + '\t' + now_time + '\t' + temp[0] + '\t' + temp[17] + '\t' + temp[18] + '\t' + temp[19 ] + '\t' + 'NULL' + '\t' + temp[21] + '\t' + temp[5] + '\t' + temp[9] + '\t' + temp[8]

			col_item = one_item.split('\t')

			sql_item = "insert into moni.qe_mem_cpu values ('%s', %s, '%s', %s, %s, '%s', '%s', '%s', '%s', %s, %s, %s);" \
				% (col_item[0], col_item[1], col_item[2], col_item[3], col_item[4][3:], col_item[5], col_item[6], col_item[7], col_item[8], col_item[9], col_item[10], col_item[11])

			output_string[0] = output_string[0] + '\n' + one_item
			output_string[1] = output_string[1] + '\n' + sql_item
		
		self.count = self.count + 1
		return output_string

	

	'''
	ps -eo pid,pcpu,vsz,rss,pmem,state,command | grep postgres | grep seg | grep -vE "bin/postgres|logger|stats|writer|checkpoint|seqserver|WAL|ftsprobe|sweeper|sh -c|bash|grep|seg-|resource manager"
	   pid %CPU  VSZ  RSS  %MEM STATE CMD          
	  1034  1.0 656480 16676  0.4 S postgres: port 40001, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(51217) con64 seg1 idle
	  1676  0.0 538684 12280  0.3 S postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(37854) con81 seg0 cmd6 MPPEXEC INSERT             
	  1675  0.0 538692 12380  0.3 S postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(37855) con81 seg0 cmd6 slice1 MPPEXEC INSERT                          
 	  1035  0.8 658804 20844  0.5 S postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(43204) con82 seg0 cmd2 slice1 MPPEXEC SELECT
index  0    1      2     3     4  5    6       7     8       9             10                        11           12     13  14    15      16     17
	'''
	def _get_qe_mem_cpu(self):
		filter_string = 'bin/postgres|logger|stats|writer|checkpoint|seqserver|WAL|ftsprobe|sweeper|sh -c|bash|grep|seg-|resource manager'
		cmd = ''' ps -eo pid,pcpu,vsz,rss,pmem,state,command | grep postgres | grep seg | grep -vE "%s" ''' % (filter_string)
		(status, output) = commands.getstatusoutput(cmd)
		if status != 0 or output == '':
			print 'return code: ' + str(status) + ' output: ' + output + ' in qe_mem_cpu'
			return None
		
		line_item = output.splitlines()
		now_time = str(datetime.now())
		output_string = ['', '']
		
		for line in line_item:
			temp = line.split()
			if len(temp) < 15:
				continue
			# hostname, count, time_point, pid, con_id, seg_id, cmd, slice, status, rss, pmem, pcpu
			try:
				if temp[14] == 'idle':
					one_item = self.hostname + self.sep + str(self.count) + self.sep + now_time + self.sep + temp[0] + self.sep + temp[12][3:] + self.sep + temp[13] + self.sep + 'NULL' + self.sep + 'NUll' + self.sep + temp[14] + self.sep + str(int(temp[3])/1024) + self.sep + temp[4] + self.sep + temp[1]
				elif temp[15].find('slice') != -1:
					one_item = self.hostname + self.sep + str(self.count) + self.sep + now_time + self.sep + temp[0] + self.sep + temp[12][3:] + self.sep + temp[13] + self.sep + temp[14] + self.sep + temp[15] + self.sep + temp[17] + self.sep + str(int(temp[3])/1024) + self.sep + temp[4] + self.sep + temp[1]
				else:
					one_item = self.hostname + self.sep + str(self.count) + self.sep + now_time + self.sep + temp[0] + self.sep + temp[12][3:] + self.sep + temp[13] + self.sep + temp[14] + self.sep + 'NULL' + self.sep + temp[16] + self.sep + str(int(temp[3])/1024) + self.sep + temp[4] + self.sep + temp[1]
			except Exception, e:
				print temp, '\n', str(e)
				continue
			
			#col_item = one_item.split('\t')
			#sql_item = "insert into moni.qe_mem_cpu values ('%s', %s, '%s', %s, %s, '%s', '%s', '%s', '%s', %s, %s, %s);" \
			#	% (col_item[0], col_item[1], col_item[2], col_item[3], col_item[4][3:], col_item[5], col_item[6], col_item[7], col_item[8], col_item[9], col_item[10], col_item[11])

			output_string[0] = output_string[0] + one_item + '\n'
			#output_string[1] = output_string[1] + sql_item + '\n'
		
		self.count = self.count + 1
		return output_string
	
	
	def get_qe_data(self, function = 'self._get_qe_mem_cpu()', interval = 5):
		stop_count = 0
		file_no = 1
		count = 0   # control scp data with self.timeout
		filename = self.hostname + '_' + function[10:-2] + '_' + str(file_no) + '.data'
		
		while(os.path.exists('run.lock') and stop_count < 300):
			if count == self.timeout:
				p1 = Process( target = self.scp_data, args = (filename, ) )
				p1.start()
				count = 0
				file_no = file_no + 1
				filename = self.hostname + '_' + function[function.find('qe'):-2] + '_' + str(file_no) + '.data'

			result = eval(function)
			if result is None:
				stop_count = stop_count + 1
				time.sleep(1)
				continue
			
			self.report(filename = filename, msg = result[0])
			#self.report(filename = filename[1], msg = result[1]) 
			count = count + 1
			time.sleep(interval)

		time.sleep(15)
		self.scp_data(filename = filename)
		print 'file_no = ', file_no

		cmd = "gpscp -h %s monitor.log =:%s/seg_log/%s.log" % (self.master_host, self.master_folder, self.hostname)
		print cmd
		os.system(cmd)
		
		#os.system('rm -rf /tmp/monitor_report/*')


	def scp_data(self, filename):
		if self.local:
			host = self.master_host
			folder = self.master_folder
			source = ''
		else:
			host = self.remote_host
			folder = self.pwd
			source = 'source ~/psql.sh;'
		
		cmd = "scp %s gpadmin@%s:%s" % (filename, host, folder)
		print cmd
		result = self.ssh_command(cmd = cmd)
		print result

		table_name = filename[filename.find('qe'):filename.rindex('_')]

		cmd = "COPY moni.%s FROM '%s' WITH DELIMITER '|';" % (table_name, folder + os.sep + filename)
		copy_file = self.hostname + '_qe_mem_cpu.copy'
		with open (copy_file, 'w') as fcopy:
			fcopy.write(cmd)

		cmd = "scp %s gpadmin@%s:%s" % (copy_file, host, folder)
		print cmd
		result = self.ssh_command(cmd = cmd)
		print result

		cmd = 'ssh gpadmin@%s "%s cd %s; psql -d postgres -f %s; rm -rf %s"' % (host, source, folder, copy_file, copy_file)
		print cmd
		result = self.ssh_command(cmd = cmd)
		print result


	def gpscp_data(self, filename):
		cmd = "gpscp -h %s %s =:%s" % (self.master_host, filename, self.master_folder)
		print cmd
		(s, o) = commands.getstatusoutput(cmd)
		print 'return code = ', s, '\n', o

		cmd = "COPY moni.qe_mem_cpu FROM '%s' WITH DELIMITER '|';" % (self.master_folder + os.sep + filename)
		copy_file = self.hostname + '_qe_mem_cpu.copy'
		with open (copy_file, 'w') as fcopy:
			fcopy.write(cmd)

		cmd = "gpscp -h %s %s =:%s" % (self.master_host, copy_file, self.master_folder)
		print cmd
		(s, o) = commands.getstatusoutput(cmd)
		print 'return code = ', s, '\n', o

		cmd = 'gpssh -h %s -e "cd %s; psql -d postgres -f %s; rm -rf %s"' % (self.master_host, self.master_folder, copy_file, copy_file)
		print cmd
		(s, o) = commands.getstatusoutput(cmd)
		print 'return code = ', s, '\n', o


monitor_seg = Monitor_seg()

if __name__ == "__main__" :
	p1 = Process( target = monitor_seg.get_qe_data, args = ('self._get_qe_mem_cpu()', 5) )
	p1.start()
	