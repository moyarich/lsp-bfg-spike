import os, sys, re, string, random
import time, datetime
import ConfigParser, commands
from time import strftime
from random import randint
from subprocess import Popen, PIPE
from datetime import datetime, timedelta

class check_hawq_stress():
	def __init__(self):
		pass

	def setUp(self):
        ''' Setup the test case '''
        pass

    def tearDown(self):
        ''' Tear down the test case '''
        pass

    def __fetch_system_configuration(self):
        '''Fetch GANGLIA, self.rrdtool, HAWQ, HDFS parameters'''
        ganglia_config_file = "./backup/ganglia_" + str(TEST_TYPE) + ".cfg"
        hdfs_config_file = "./hdfs_" + str(TEST_TYPE) + ".cfg"
        self.__fetch_hdfs_configuration(hdfs_config_file)
        self.__fetch_hawq_configuration()


    def __fetch_hdfs_configuration(self, config_file):
        '''Fetch namenode, datanode info and logs dir of hdfs.'''
        config = ConfigParser.ConfigParser()
        config.read(config_file)
        self.hdfs_path = config.get('HDFS','path')
        self.nn = config.get('HDFS','namenode')
        self.snn = config.get('HDFS','second_namenode')
        dns = config.get('HDFS','datanode')
        self.dn = [ chunk.strip() for chunk in dns.split(',')]

    def __fetch_hawq_configuration(self):
        '''Fetch master hostname, segments hostname, data directory of HAWQ.'''
        self.hawq_master = []
        self.hawq_segments = []
        self.hawq_paths = []
        self.hawq_config = []
        
        sql = "SELECT hostname from gp_segment_configuration where content=-1 and role='p';"
        (ok, out) = psql.runcmd( dbname=GPDTBS, cmd=sql , ofile='-', pFlags='-q -t' ) 
        if not ok:
            print out
            raise Exception("Failed to get HAWQ master hostname.")
        else:
            for line in out:
                line = line.strip()
                if line:
                    self.hawq_master.append(line)  

        sql = "SELECT hostname from gp_segment_configuration where content>=0;"
        (ok, out) = psql.runcmd( dbname=GPDTBS, cmd=sql , ofile='-', pFlags='-q -t' ) 
        if not ok:
            print out
            raise Exception("Failed to get HAWQ segments hostname.")
        else:
            for line in out:
                line = line.strip()
                if line:
                    self.hawq_segments.append(line)  

        sql = "SELECT gsc.hostname, pfe.fselocation FROM gp_segment_configuration gsc, pg_filespace_entry pfe WHERE gsc.dbid = pfe.fsedbid AND pfe.fselocation NOT LIKE 'hdfs%' ORDER BY pfe.fselocation;"
        (ok, out) = psql.runcmd( dbname=GPDTBS, cmd=sql , ofile='-', pFlags='-q -t' )

        if not ok:
            print out
            raise Exception("Failed to get HAWQ configuration paths.")
        else:
            for line in out:
                line = line.strip()
                if line:
                    (host, path) = line.split( '|' )
                    self.hawq_config.append( (host.strip(), path.strip()) )
                    self.hawq_paths.append( path.strip() ) 

    def __search_key_in_log(self, host, path, key):
        '''Search key in logs using grep'''
        cmd = "gpssh -h %s -e 'grep -i %s %s'" % (host, key, path)
        (ok, out) = shell.run( cmd )
        return (ok, out)

    def __escape_search_key(self, searchKey):
        '''Escape the search key in case of special characters'''
        searchKeyEscaped = ''
        for i in range(0, len(searchKey)):
            if searchKey[i] in [',', '\\', '{', '}', '[', ']']:
                searchKeyEscaped += '\\'+searchKey[i]
            else:
                searchKeyEscaped += searchKey[i]

        return searchKeyEscaped

    def __produce_wildcard_for_seg_paths(self, strlist):
        length = min(len(strlist[0]), len(strlist[1]))
        num = len(strlist)
        wild_card = ''
        for i in range(0, length):
            flag = False 
            for j in range(0, num-1):
                if strlist[j][i] == strlist[j+1][i]:
                    if j == num - 2:
                        flag = True 
                        continue
                    else:
                        break
            if flag:
                wild_card += strlist[0][i]
            else:
                wild_card += '*'
      
        return wild_card

    def __analyze_hawq_logs(self, searchKeyArray):
        '''Analyze HAWQ logs using gplogfilter'''
        find_any = False
        find_one = False
        bt = datetime.fromtimestamp(int(START_STAMP)).strftime('%Y-%m-%d %H:%M:%S')
        et = datetime.fromtimestamp(int(END_STAMP)).strftime('%Y-%m-%d %H:%M:%S')

        searchKeyRegrex = self.__escape_search_key(searchKeyArray[0])
        for i in range(1, len(searchKeyArray)):
            searchKeyEscaped = self.__escape_search_key(searchKeyArray[i])
            searchKeyRegrex += '|%s' % (searchKeyEscaped)

        cmd = "gplogfilter -b %s -e %s -m \'%s\'" % (bt, et, searchKeyRegrex)
        status, output = commands.getstatusoutput(cmd)
        matchLines = re.findall('match:       [^0]+', output)
        
        if (len(matchLines)):
            find_one = True
            print "Logs for \'%s\' on master:" % (searchKeyRegrex)
            print output

        segs_hosts = ''
        for host in self.hawq_segments:
            segs_hosts += '-h %s' % (host)
        segs_path = self.__produce_wildcard_for_seg_paths(self.hawq_paths)
        cmd = "gpssh %s -e \"gplogfilter -b \'%s\' -e \'%s\' -m \'%s\' %s\"" % (segs_hosts, bt, et, searchKeyRegrex, segs_path)
        status, output = commands.getstatusoutput(cmd)
        matchLines = re.findall('match:       [^0]+', output)
        
        if (len(matchLines)):
            find_one = True
            print "Logs for \'%s\' on segments:" % (searchKeyRegrex)
            print output

        if find_one:
            find_any = True
        else:
            print "\nNo %s found" % (searchKeyRegrex)

        if find_any:
            self.fail()

    def __analyze_hdfs_logs(self, searchKeyArray, hosts_paths):
        '''Search keys in hdfs logs, including: error, exception, etc.''' 
        find_any = False
        for key in searchKeyArray:
            print "Searching for %s" % key
            find_one = False
            for (host, path) in hosts_paths:
                (find, out) = self.__search_key_in_log( host, path+"/logs/*.log", key)
                if find:
                    print "Logs for \'%s\' on %s in %s:" % (key, host, path)
                    print out
                    find_one = True
            if find_one:
                print "\nFound %s" % ( key )
                find_any = True
            else:
                print "\nNo %s found" % ( key )

        if find_any:
            self.fail()

    def __compute_cpu_usage(self, hosts):
        '''Compute cpu usage using rrdtools'''
        num_hosts = len(hosts)
        num_samples = (int(END_STAMP) - int(START_STAMP))/int(self.resolution) + 1

        cpu_usage_all_host = []
        for i in range(0, num_hosts):
            cmd = '%s fetch %s/%s/cpu_idle.rrd AVERAGE -r %s -s %s -e %s' % (self.rrdtool, self.ganglia_data_path, hosts[i], self.resolution, START_STAMP, END_STAMP)
            status, output = commands.getstatusoutput(cmd)
 
            cpu_usage_one_host = [ 100 - float(value) for value in re.findall('\d\.\d+[eE]{1}[\+\-]{1}\d+', output)]
            
            cpu_usage_all_host.append(cpu_usage_one_host)

        sum_cpu = 0
        for cpu_usage_one_host in cpu_usage_all_host:
            sum_cpu += sum( cpu_usage_one_host )
        acc_cpu = round( float(sum_cpu) / float(num_hosts*num_samples) )

        return acc_cpu

    def __compute_memory_usage(self, hosts):
        '''Compute memory usage using rrdtools'''
        num_hosts = len(hosts)
        num_samples = (int(END_STAMP) - int(START_STAMP))/int(self.resolution) + 1
        mem_usage_all_host = []

        for i in range(0, num_hosts):
            cmd = '%s fetch %s/%s/mem_total.rrd AVERAGE -r %s -s %s -e %s' % (self.rrdtool, self.ganglia_data_path, hosts[i], self.resolution, START_STAMP, END_STAMP)
            status, output = commands.getstatusoutput(cmd)
            mem_total_one_host = [float(value) for value in re.findall('\d\.\d+[eE]{1}[\+\-]{1}\d+', output)]
            cmd = '%s fetch %s/%s/mem_cached.rrd AVERAGE -r %s -s %s -e %s' % (self.rrdtool, self.ganglia_data_path, hosts[i], self.resolution, START_STAMP, END_STAMP)
            status, output = commands.getstatusoutput(cmd)
            mem_cached_one_host = [float(value) for value in re.findall('\d\.\d+[eE]{1}[\+\-]{1}\d+', output)]
            cmd = '%s fetch %s/%s/mem_free.rrd AVERAGE -r %s -s %s -e %s' % (self.rrdtool, self.ganglia_data_path, hosts[i], self.resolution, START_STAMP, END_STAMP)
            status, output = commands.getstatusoutput(cmd)
            mem_free_one_host = [float(value) for value in re.findall('\d\.\d+[eE]{1}[\+\-]{1}\d+', output)]
            mem_usage_one_host = [100*(mem_total_one_host[i] - mem_cached_one_host[i] - mem_free_one_host[i])/mem_total_one_host[i] for i in range(0, len(mem_total_one_host))]
            mem_usage_all_host.append(mem_usage_one_host)

        sum_mem = 0
        for mem_usage_one_host in mem_usage_all_host:
            sum_mem += sum( mem_usage_one_host )

        acc_mem = round( float(sum_mem) / float(num_hosts*num_samples) )
        return acc_mem

    def __compute_disk_io(self, hosts):
        '''Compute disk io using rrdtools.'''
        num_hosts = len(hosts)
        num_samples = (int(END_STAMP) - int(START_STAMP))/int(self.resolution) + 1
        max_disk_write = 250
        max_disk_read = 200
        bytes_factor = 1024

        dsk_write_all_host = []
        dsk_read_all_host = []
        for i in range(0, num_hosts):
            cmd = '%s fetch %s/%s/bytes_written.rrd AVERAGE -r %s -s %s -e %s' % (self.rrdtool, self.ganglia_data_path, hosts[i], self.resolution, START_STAMP, END_STAMP)
            status, output = commands.getstatusoutput(cmd)
            dsk_write_one_host = [100*(float(value)/bytes_factor/max_disk_write/1024) for value in re.findall('\d\.\d+[eE]{1}[\+\-]{1}\d+', output)]
            cmd = '%s fetch %s/%s/bytes_read.rrd AVERAGE -r %s -s %s -e %s' % (self.rrdtool, self.ganglia_data_path, hosts[i], self.resolution, START_STAMP, END_STAMP)
            status, output = commands.getstatusoutput(cmd)
            dsk_read_one_host = [100*(float(value)/bytes_factor/max_disk_read/1024) for value in re.findall('\d\.\d+[eE]{1}[\+\-]{1}\d+', output)]
            dsk_write_all_host.append( dsk_write_one_host )
            dsk_read_all_host.append( dsk_read_one_host )

        sum_dsk_write = 0
        for dsk_write_one_host in dsk_write_all_host:
            sum_dsk_write += sum( dsk_write_one_host )

        sum_dsk_read = 0
        for dsk_read_one_host in dsk_read_all_host:
            sum_dsk_read += sum( dsk_read_one_host )

        acc_dsk_write = round( float(sum_dsk_write) / float(num_hosts*num_samples) )
        acc_dsk_read  = round( float(sum_dsk_read ) / float(num_hosts*num_samples) )

        return [acc_dsk_write, acc_dsk_read]

    def __compute_network_io(self, hosts):
        '''Compute network io using rrdtools.'''
        num_hosts = len(hosts)
        num_samples = (int(END_STAMP) - int(START_STAMP))/int(self.resolution) + 1
        max_net_write = 100  #100M/s
        max_net_read = 100  #100M/s
        bytes_factor = 1024*1024

        net_write_all_host = []
        net_read_all_host = []
        for i in range(0, num_hosts):
            cmd = '%s fetch %s/%s/bytes_out.rrd AVERAGE -r %s -s %s -e %s' % (self.rrdtool, self.ganglia_data_path, hosts[i], self.resolution, START_STAMP, END_STAMP)
            status, output = commands.getstatusoutput(cmd)
            net_write_one_host = [100*(float(value)/bytes_factor/max_net_write) for value in re.findall('\d\.\d+[eE]{1}[\+\-]{1}\d+', output)]
            cmd = '%s fetch %s/%s/bytes_in.rrd AVERAGE -r %s -s %s -e %s' % (self.rrdtool, self.ganglia_data_path, hosts[i], self.resolution, START_STAMP, END_STAMP)
            status, output = commands.getstatusoutput(cmd)
            net_read_one_host = [100*(float(value)/bytes_factor/max_net_read) for value in re.findall('\d\.\d+[eE]{1}[\+\-]{1}\d+', output)]
            net_write_all_host.append( net_write_one_host )
            net_read_all_host.append( net_read_one_host )

        sum_net_write = 0
        for net_write_one_host in net_write_all_host:
            sum_net_write += sum( net_write_one_host )

        sum_net_read = 0
        for net_read_one_host in net_read_all_host:
            sum_net_read += sum( net_read_one_host )

        acc_net_write = round( float(sum_net_write) / float(num_hosts*num_samples) )
        acc_net_read  = round( float(sum_net_read ) / float(num_hosts*num_samples) )

        return [acc_net_write, acc_net_read]

    def __compute_memory_usage_slope(self, hosts):
        '''Compute memory usage slope.'''
        num_hosts = len(hosts)
        num_samples = (int(END_STAMP) - int(START_STAMP))/int(self.resolution) + 1
        bytes_factor = 1024*1024

        mem_used_all_host = []
        for i in range(0, num_hosts):
            cmd = '%s fetch %s/%s/mem_total.rrd AVERAGE -r %s -s %s -e %s' % (self.rrdtool, self.ganglia_data_path, hosts[i], self.resolution, START_STAMP, END_STAMP)
            status, output = commands.getstatusoutput(cmd)
            mem_total_one_host = [float(value) for value in re.findall('\d\.\d+[eE]{1}[\+\-]{1}\d+', output)]
            if len(mem_total_one_host) < num_samples:
                num_samples = len(mem_total_one_host)

            cmd = '%s fetch %s/%s/mem_cached.rrd AVERAGE -r %s -s %s -e %s' % (self.rrdtool, self.ganglia_data_path, hosts[i], self.resolution, START_STAMP, END_STAMP)
            status, output = commands.getstatusoutput(cmd)
            mem_cached_one_host = [float(value) for value in re.findall('\d\.\d+[eE]{1}[\+\-]{1}\d+', output)]
            if len(mem_cached_one_host) < num_samples:
                num_samples = len(mem_cached_one_host)

            cmd = '%s fetch %s/%s/mem_free.rrd AVERAGE -r %s -s %s -e %s' % (self.rrdtool, self.ganglia_data_path, hosts[i], self.resolution, START_STAMP, END_STAMP)
            status, output = commands.getstatusoutput(cmd)
            mem_free_one_host = [float(value) for value in re.findall('\d\.\d+[eE]{1}[\+\-]{1}\d+', output)]
            if len(mem_free_one_host) < num_samples:
                num_samples = len(mem_free_one_host)

            if num_samples < 2:
                print "Only %d samples for memory leak analysis, skipping ..."
                return -90

            mem_used_one_host = [(mem_total_one_host[i] - mem_cached_one_host[i] - mem_free_one_host[i])/bytes_factor for i in range(0, len(mem_total_one_host))]
            mem_used_all_host.append(mem_used_one_host)
        
        mem_used_all_sample = []
        for i in range(0, num_samples):
            mem_used_one_sample = 0.0
            for mem_used_one_host in mem_used_all_host:
                mem_used_one_sample += mem_used_one_host[i]
            mem_used_one_sample /= len(hosts)
            mem_used_all_sample.append(mem_used_one_sample)

        avg_xy = 0.0
        avg_x = 0.0
        avg_y = 0.0
        avg_x2 = 0.0
        for i in range(0, num_samples):
            avg_xy += i * mem_used_all_sample[i]
            avg_x += i
            avg_y += mem_used_all_sample[i]
            avg_x2 += i * i
        avg_xy /= num_samples
        avg_x /= num_samples
        avg_y /= num_samples
        avg_x2 /= num_samples

        if ( math.fabs( avg_x2 - avg_x * avg_x ) < 1e-3 ):
            slope = 90
        else:
            slope = int ( math.degrees( math.atan( ( avg_xy - avg_x * avg_y ) / ( avg_x2 - avg_x * avg_x ) ) ) )
        return slope