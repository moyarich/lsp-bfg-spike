#!/usr/bin/env python
import os,sys,commands,time
from datetime import datetime

class Monitor_node():

	def __init__(self):
		pass

	def report(self, filename, msg):
	    fp = open(filename, 'a')  
	    fp.write(msg)
	    fp.write('\n')
	    fp.flush()
	    fp.close()
	
	'''
	%CPU  VSZ  RSS  %MEM STATE CMD          
	0.0 566612 20840  0.5 R postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(58558) con279 seg0 cmd2 slice7 MPPEXEC SELECT     
	0.0 566612 22864  0.5 R postgres: port 40001, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(36042) con279 seg1 cmd2 slice7 MPPEXEC SELECT         
	2.5 640936 18776  0.4 S postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(58564) con279 seg0 idle                           
	2.5 640936 18776  0.4 S postgres: port 40001, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(36048) con279 seg1 idle
	'''
	
	def __get_qe_mem(self):
		filter_string = 'bin/postgres|logger|stats|writer|checkpoint|seqserver|WAL|ftsprobe|sweeper|sh -c|bash|grep'
		grep_string1 = 'postgres'
		grep_string2 = 'seg'
		cmd = ''' ps -eo pcpu,vsz,rss,pmem,state,command | grep %s | grep %s | grep -vE "%s" ''' % (grep_string1, grep_string2, filter_string)
		(status, output) = commands.getstatusoutput(cmd)
		if status != 0 or output == '':
			print 'error code: ' + str(status) + ' output: ' + output
			return ''
		
		line_item = output.splitlines()
		now_time = str(datetime.now())
		output_string = ''
		for line in line_item:
			temp = line.split()
			one_item = now_time + '\t' + temp[11][3:] + '\t' + temp[12] + '\t' + temp[13] + '\t' + str(int(temp[2])/1024) + '\t' + temp[3] + '\t' + temp[0]
			output_string = output_string + '\n' + one_item
		return output_string
	
	def get_qe_mem(self, filename, interval):
		count = 0
		while(True):
			if count == 10:
				break
			result = self.__get_qe_mem()
			if result == '':
				count = count + 1
				time.sleep(2)
				continue
			else:
				self.report(filename = filename, msg = result)
			time.sleep(interval)

monitor_node = Monitor_node()

if __name__ == "__main__" :
	mn = Monitor_node()
	#monitor.get_qe_mem(filename = datetime.now().strftime('%Y%m%d-%H%M%S')+'_qe_mem.log', interval = 3)
	