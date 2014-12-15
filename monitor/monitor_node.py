#!/usr/bin/env python
import os,sys,commands,time
from datetime import datetime

class Monitor_node():

	def __init__(self):
		pass

	def report(self, filename, msg):
		if msg != '':
		    fp = open(filename, 'a')  
		    fp.write(msg)
		    fp.write('\n')
		    fp.flush()
		    fp.close()
	
	'''
	 pid %CPU  VSZ  RSS  %MEM STATE CMD          
	1034 1.0 656480 16676  0.4 S postgres: port 40001, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(51217) con649 seg1 idle                           
 	1035 0.8 658804 20844  0.5 S postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(43204) con649 seg0 cmd2 slice1 MPPEXEC SELECT 
	'''
	
	def __get_qe_mem(self):
		filter_string = 'bin/postgres|logger|stats|writer|checkpoint|seqserver|WAL|ftsprobe|sweeper|sh -c|bash|grep|seg-'
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
			#time_point, con_id, seg_id, status, rss, pmem, pcpu
			try:
				one_item = now_time + '\t' + temp[0] + '\t' + temp[12] + '\t' + temp[13] + '\t' + temp[14] + '\t' + str(int(temp[3])/1024) + 'm' + '\t' + temp[4] + '\t' + temp[1]
			except Exception, e:
				continue

			#sql_item = "insert into moni.qe_mem_cpu values ('%s', %s, %s, '%s', %s, %s, %s);" \
			#	% (now_time, temp[11][3:], temp[12][3:], temp[13], str(int(temp[2])/1024), temp[3], temp[0])

			output_string[0] = output_string[0] + '\n' + one_item
			#output_string[1] = output_string[1] + '\n' + sql_item

		return output_string
	
	def get_qe_mem(self, filename = ['', ''], interval = 5):
		count = 0
		while(count < 10):
			result = self.__get_qe_mem()
			if result is None:
				count = count + 1
				time.sleep(2)
				continue
			
			self.report(filename = filename[0], msg = result[0])
			#self.report(filename = filename[1], msg = result[1])

			time.sleep(interval)

monitor_node = Monitor_node()

if __name__ == "__main__" :
	mn = Monitor_node()
	#monitor.get_qe_mem(filename = datetime.now().strftime('%Y%m%d-%H%M%S')+'_qe_mem.log', interval = 3)
	