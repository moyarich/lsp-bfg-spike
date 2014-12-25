#!/usr/bin/env python
import os,sys,commands,time
from datetime import datetime
import subprocess

class Monitor_seg():

	def __init__(self):
		self.master_dir = sys.argv[1]
		self.master_name = sys.argv[2]
		self.pwd = os.getcwd()
		self.count = 1
		(s,o) = commands.getstatusoutput('hostname')
		self.hostname = o.strip()
		self.sep = '\t|'


	def report(self, filename, msg):
		if msg != '':
		    fp = open(filename, 'a')  
		    fp.write(msg)               
		    fp.flush()
		    fp.close()



	'''
	 PID    USER    PR  NI  VIRT  RES  SHR S %CPU %MEM    TIME+  COMMAND
	30705 gpadmin   39  19  528m  12m 6408 S  0.0  0.3   0:00.00 postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(38011) con1140 seg6 cmd2 slice4 MPPEXEC SELECT
	35019 gpadmin   39  19  526m 9944 5964 S  0.0  0.3   0:00.00 postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(40991) con1376 seg0 cmd6 MPPEXEC INSERT                                                                                                                                                   
    35021 gpadmin   39  19  526m 9.8m 5996 S  0.0  0.3   0:00.00 postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(40992) con1376 seg0 cmd6 slice1 MPPEXEC INSERT                                                                                                                                            
	30713 gpadmin   39  19  526m  10m 6604 S  0.0  0.3   0:00.00 postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(38015) con1140 seg3 idle 
index 0      1      2   3    4     5   6   7   8    9      10     11         12   13     14              15                       16            17     18   19    20     21      22
	'''
	def __get_qe_mem_cpu_by_top(self):
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
	   pid %CPU  VSZ  RSS  %MEM STATE CMD          
	  1034  1.0 656480 16676  0.4 S postgres: port 40001, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(51217) con64 seg1 idle
	  1676  0.0 538684 12280  0.3 S postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(37854) con81 seg0 cmd6 MPPEXEC INSERT             
	  1675  0.0 538692 12380  0.3 S postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(37855) con81 seg0 cmd6 slice1 MPPEXEC INSERT                          
 	  1035  0.8 658804 20844  0.5 S postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(43204) con82 seg0 cmd2 slice1 MPPEXEC SELECT
index  0    1      2     3     4  5    6       7     8       9             10                        11           12     13  14    15      16     17
	'''
	def __get_qe_mem_cpu_by_ps(self):
		filter_string = 'bin/postgres|logger|stats|writer|checkpoint|seqserver|WAL|ftsprobe|sweeper|sh -c|bash|grep|seg-|resource manager'
		grep_string1 = 'postgres'
		grep_string2 = 'seg'
		cmd = ''' ps -eo pid,pcpu,vsz,rss,pmem,state,command | grep %s | grep %s | grep -vE "%s" ''' % (grep_string1, grep_string2, filter_string)
		(status, output) = commands.getstatusoutput(cmd)
		if status != 0 or output == '':
			print 'error code: ' + str(status) + ' output: ' + output + 'in qe_mem_cpu'
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
					one_item = self.hostname + self.sep + str(self.count) + self.sep + now_time + self.sep + temp[0] + self.sep + temp[12][3:] + self.sep + temp[13] + self.sep + temp[14] + self.sep + 'NUll' + self.sep + 'NULL' + self.sep + str(int(temp[3])/1024) + self.sep + temp[4] + self.sep + temp[1]
				elif temp[15].find('slice') != -1:
					one_item = self.hostname + self.sep + str(self.count) + self.sep + now_time + self.sep + temp[0] + self.sep + temp[12][3:] + self.sep + temp[13] + self.sep + temp[14] + self.sep + temp[15] + self.sep + temp[17] + self.sep + str(int(temp[3])/1024) + self.sep + temp[4] + self.sep + temp[1]
				else:
					one_item = self.hostname + self.sep + str(self.count) + self.sep + now_time + self.sep + temp[0] + self.sep + temp[12][3:] + self.sep + temp[13] + self.sep + temp[14] + self.sep + 'NULL' + self.sep + temp[16] + self.sep + str(int(temp[3])/1024) + self.sep + temp[4] + self.sep + temp[1]
			except Exception, e:
				print temp
				continue
			

			#col_item = one_item.split('\t')

			#sql_item = "insert into moni.qe_mem_cpu values ('%s', %s, '%s', %s, %s, '%s', '%s', '%s', '%s', %s, %s, %s);" \
			#	% (col_item[0], col_item[1], col_item[2], col_item[3], col_item[4][3:], col_item[5], col_item[6], col_item[7], col_item[8], col_item[9], col_item[10], col_item[11])

			output_string[0] = output_string[0] + one_item + '\n'
			#output_string[1] = output_string[1] + sql_item + '\n'
		self.count = self.count + 1

		return output_string
	
	
	def get_qe_mem_cpu(self, filename = ['', ''], interval = 5):
		count = 0
		while(os.path.exists('run.lock') and count < 300):
			result = self.__get_qe_mem_cpu_by_ps()
			if result is None:
				count = count + 1
				time.sleep(1)
				continue
			
			self.report(filename = filename[0], msg = result[0])
			#self.report(filename = filename[1], msg = result[1])

			time.sleep(interval)

		cmd = "gpscp -h %s %s =:%s" % (self.master_name, filename[0], self.master_dir)
		print cmd
		(s, o) = commands.getstatusoutput(cmd)
		print s,o

		cmd = "gpscp -h %s monitor.log =:%s/%s_monitor.log" % (self.master_name, self.master_dir, self.hostname)
		print cmd
		(s, o) = commands.getstatusoutput(cmd)
		print s,o

		os.system('rm -rf ../*')

	
	def start(self, status_file):
		with open(status_file, 'r') as fstatus:
			status = fstatus.read()


monitor_seg = Monitor_seg()

if __name__ == "__main__" :
	#'_' + datetime.now().strftime('%Y%m%d-%H%M%S') + 
	monitor_seg.get_qe_mem_cpu(filename = [monitor_seg.hostname + '_qe_mem_cpu.data', ''], interval = 5)
	