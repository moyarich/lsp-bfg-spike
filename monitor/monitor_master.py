#!/usr/bin/env python
import os,sys,commands,time
from datetime import datetime
from multiprocessing import Process
from pygresql import pg
from monitor_node import Monitor_node


class Monitor_master():
	def __init__(self):
		self.query_record = {}

	def report(self, filename, msg):
	    fp = open(filename, 'a')  
	    fp.write(msg)
	    fp.flush()
	    fp.close()
	
	'''
	%CPU  VSZ  RSS  %MEM STATE CMD          
	4.0 799100 27772  0.7 S postgres: port  5432, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(56830) con335 127.0.0.1(56830) cmd1 SELECT            
 	4.6 799100 27756  0.7 S postgres: port  5432, gpadmin gpsqltest_tpch_ao_row_gpadmin 127.0.0.1(56847) con336 127.0.0.1(56847) cmd1 SELECT
	'''
	
	def __get_qd_mem(self):
		filter_string = 'bin/postgres|logger|stats|writer|checkpoint|seqserver|WAL|ftsprobe|sweeper|sh -c|bash|grep|seg|pg_stat_activity'
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
				one_item = now_time + '\t' + temp[11][3:] + '\t' + temp[13] + '\t' + str(int(temp[2])/1024) + '\t' + temp[3] + '\t' + temp[0]
			except Exception, e:
				continue
			output_string = output_string + one_item + '\n'

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


	def __get_qd_info(self):
		# -R '***' set record separator '***' (default: newline)
		cmd = ''' psql -d postgres -t -A -R '***' -c "select sess_id,query_start,procpid,usename,datname,current_query from pg_stat_activity where current_query not like '%from pg_stat_activity%' order by sess_id,query_start,procpid;" '''
		#cmd = ''' psql -d postgres -t -c "select sess_id,query_start,procpid,usename,datname from pg_stat_activity order by sess_id,query_start;" '''
		(status, output) = commands.getstatusoutput(cmd)
		if status != 0 or output == '':
			print 'error code: ' + str(status) + ' output: ' + output
			return 'error' 

		''' sess_id  query_start  procpid  usename  datname  current_query '''
		line_item = output.split('***')
		output_string = ['', '']
		for line in line_item:
			line = line.split('|')
			try:
				query_start_time = datetime.strptime(line[1].split('+')[0].strip(), "%Y-%m-%d %H:%M:%S.%f")
			except Exception, e:
				print 'time error ' + str(line)
				continue

			if ( line[0] not in self.query_record.keys() ) or ( line[0] in self.query_record.keys() and query_start_time > self.query_record[line[0]] ):
				self.query_record[line[0]] = query_start_time
				one_item = line[0] + '\t' + str(query_start_time) + '\t' + line[2] + '\t' + line[3] + '\t' + line[4]
				sql_string = "insert into moni.qd_info values (%s, '%s', %s, '%s', '%s');" \
				% (line[0], str(query_start_time), line[2], line[3], line[4])
				output_string[0] = output_string[0] + one_item + '\n'
				output_string[1] = output_string[1] + sql_string + '\n'


		return output_string

	def get_qd_info(self, filename = ['', ''], interval = 3):
		count = 0
		while(True):
			if count == 10:
				break
			result = self.__get_qd_info()
			if result == 'error':
				count = count + 1
				time.sleep(2)
				continue
			elif result[0] != '' and result[1] != '':
				self.report(filename = filename[0], msg = result[0])
				self.report(filename = filename[1], msg = result[1])

			time.sleep(interval)


if __name__ == "__main__" :
	monitor = Monitor_master()
	#monitor.get_qd_mem(filename = datetime.now().strftime('%Y%m%d-%H%M%S')+'_qd_mem.log', interval = 4)
	p1 = Process(target = monitor.get_qd_info, args = ( [ datetime.now().strftime('%Y%m%d-%H%M%S')+'_qd_info.log', datetime.now().strftime('%Y%m%d-%H%M%S')+'_qd_info.sql'], 1))
#	p2 = Process(target = monitor.get_qd_mem, args = (datetime.now().strftime('%Y%m%d-%H%M%S')+'_qd_mem.log', 3))
#	p3 = Process(target = Monitor_node().get_qe_mem, args = (datetime.now().strftime('%Y%m%d-%H%M%S')+'_qe_mem.log', 3))
	p1.start()
#	p2.start()
#	p3.start()

	#monitor.get_qd_info(filename = datetime.now().strftime('%Y%m%d-%H%M%S')+'_qd_info.log', interval = 2)
	