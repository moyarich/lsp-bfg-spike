#!/usr/bin/env python
import os,sys,commands

class Monitor_node():
	def __init__(self):
		pass

	'''
	%CPU  VSZ  RSS  %MEM CMD          
	0.0 566612 20840  0.5 postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(58558) con279 seg0 cmd2 slice7 MPPEXEC SELECT     
	0.0 566612 22864  0.5 postgres: port 40001, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(36042) con279 seg1 cmd2 slice7 MPPEXEC SELECT         
	2.5 640936 18776  0.4 postgres: port 40000, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(58564) con279 seg0 idle                           
	2.5 640936 18776  0.4 postgres: port 40001, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(36048) con279 seg1 idle
	'''
	
	def fetch_qe_mem(self):
		filter_string = 'bin/postgres|logger|stats|writer|checkpoint|seqserver|WAL|ftsprobe|sweeper|sh -c|bash|grep'
		grep_string1 = 'postgres'
		grep_string2 = 'seg'
		cmd = ''' ps -eo pcpu,vsz,rss,pmem,state,command | grep %s | grep %s | grep -vE "%s" ''' % (grep_string1, grep_string2, filter_string)
		(status, output) = commands.getstatusoutput(cmd)
		if status != 0 or output == '':
			return 'error code: ' + str(status) + ' output: ' + output
		
		print output
		line_item = output.splitlines()
		for line in line_item:
			temp = line.split()
			out_string = ''
			out_string = temp[11] + ' ' + temp[12] + ' ' + str(int(temp[2])/1024) + ' ' + temp[0]
			print out_string
		#for line in line_item:
		#	temp = line.split("postgres")
		#print line_item[0].split()
	

if __name__ == "__main__" :
	monitor = Monitor_node()

	monitor.fetch_qe_mem()