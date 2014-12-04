#!/usr/bin/env python
import os,sys,commands,time
from datetime import datetime

class Monitor_master():

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
	4.0 799100 27772  0.7 S postgres: port  5432, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(56830) con335 127.0.0.1(56830) cmd1 SELECT            
 	4.6 799100 27756  0.7 S postgres: port  5432, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(56847) con336 127.0.0.1(56847) cmd1 SELECT
	'''
	
	def __get_qd_mem(self):
		filter_string = 'bin/postgres|logger|stats|writer|checkpoint|seqserver|WAL|ftsprobe|sweeper|sh -c|bash|grep|seg'
		grep_string1 = 'postgres'
		cmd = ''' ps -eo pcpu,vsz,rss,pmem,state,command | grep %s | grep -vE "%s" ''' % (grep_string1, filter_string)
		(status, output) = commands.getstatusoutput(cmd)
		if status != 0 or output == '':
			print 'error code: ' + str(status) + ' output: ' + output
			return ''
		
		line_item = output.splitlines()
		output_string = ''
		now_time = str(datetime.now())
		for line in line_item:
			temp = line.split()
			try:
				one_item = now_time + ' ' + temp[11] + ' ' + temp[13] + ' ' + str(int(temp[2])/1024) + ' ' + temp[0]
			except Exception, e:
				one_item = line
			finally:
				output_string = output_string + '\n' + one_item

		return output_string
	
	def get_qd_mem(self, filename = '', interval = 5):
		count = 0
		while(True):
			if count == 10:
				break
			result = self.__get_qd_mem()
			if result == '':
				count = count + 1
				time.sleep(2)
				continue
			else:
				self.report(filename = filename, msg = result)

			time.sleep(interval)

	#def get_qd_info(self, filename = '', interval = 5):

	def get_qd_info(self, filename = '', interval = 5):
		cmd = ''' psql -d postgres -t -c "select sess_id,procpid,usename,datname,query_start from pg_stat_activity where current_query not like '%from pg_stat_activity%' order by sess_id;" '''
		(status, output) = commands.getstatusoutput(cmd)
		if status != 0 or output == '':
			print 'error code: ' + str(status) + ' output: ' + output
			return '' 

		''' sess_id  procpid  usename  datname  query_start '''
		line_item = output.splitlines()
		output_string = ''
		for line in line_item:
			print line
			#line = line.replace('|', '').split()
			#one_item = line[0] + ' ' + line[1] + ' ' + line[2] + ' ' + line[3] + ' ' + line[4]
			#output_string = output_string + '\n' + one_item

		return output_string


if __name__ == "__main__" :
	monitor = Monitor_master()
	#monitor.get_qd_mem(filename = datetime.now().strftime('%Y%m%d-%H%M%S')+'_qd_mem.log', interval = 4)
	print monitor.get_qd_info()