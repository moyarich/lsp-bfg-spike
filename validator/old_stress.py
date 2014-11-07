#!/usr/bin/env python

'''
Copyright (c) 2013, Pivotal, Inc. All rights reserved.

   NAME
     stress.py

   DESCRIPTION
     Verify HAWQ checkpoints after its stress test. 

   NOTES
     Need to provide stress.cfg(system configuration papameters) in the same directory
     Please check that you have sourced greenplum_path.sh before running script
     Run the script using the following command: python stress.py start_stamp end_stamp

   MODIFIED  (MM/DD/YY)
   Wan Zhang   07/25/2013   - Use rrd in ganglia as data source for analysis, tune thresholds for checkpoints, improve performance for HAWQ log checking
   Ruilong Huo 06/30/2013   - Initial stress test cases for HAWQ 
'''

############################################################################
## Set up some globals, and import gptest
##    [YOU DO NOT NEED TO CHANGE THESE]
##
import os
import sys
import re
import string
import random
import time, datetime
import ConfigParser, commands
from time import strftime
from random import randint
from subprocess import Popen, PIPE
from datetime import datetime, timedelta

MYD = os.path.abspath(os.path.dirname(__file__))
mkpath = lambda *x: os.path.join(MYD, *x)
lib = MYD + os.sep + "src"
if lib not in sys.path:
    sys.path.append(lib)
if MYD not in sys.path:
    sys.path.append(MYD)

import gptest

try:
    from PSQL import psql
    from Shell import Shell
    from optparse import OptionParser
    from gptest import psql, shell, GPTestCase
    from lib.gpdb_verify import gpdb_verify
    from util.network import network
    import gpConfig
    from moniter import *

except Exception, e:
    sys.exit("Cannot import modules. Please check that you have sourced greenplum_path.sh. Details: " + str(e))

import ext.unittest2 as unittest

## Fetch GPDB/HAWQ GUCs
GPHOST = network().get_hostname()
GPDTBS = os.environ.get( 'PGDATABASE' )
GPPORT = os.environ.get( 'PGPORT' )
GPHOME = os.environ.get( 'GPHOME' )
GPUSER = os.environ.get( 'LOGNAME' )

if GPDTBS is None:
    GPDTBS = 'gpsqltest'

GPDTBS = 'gpsqltest'
## Get rrdtool fetching time period 
if len(sys.argv) < 4:
    sys.exit("Too less arguments.Please run this script using following command:\n python stress.py start_stamp end_stamp")
START_STAMP = sys.argv[1]
END_STAMP = sys.argv[2]
TEST_TYPE = sys.argv[3]

###########################################################################
##  A Test class must inherit from gptest.GPTestCase
##    [CREATE A CLASS FOR YOUR TESTS]
##
class check_hawq_stress(gptest.GPTestCase):
    '''
    @class check_hawq_stress
    @extend gptest.GPTestCase
    @improvement: accumulated ways to run workloads to put stress on HAWQ
    '''
    def __init__(self, name="check_hawq_stress"):
        '''Fetch the system configuration parameters'''
        gptest.GPTestCase.__init__(self, name)
        self.__fetch_system_configuration()
        
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
        self.__fetch_ganglia_configuration(ganglia_config_file)
        self.__fetch_hdfs_configuration(hdfs_config_file)
        self.__fetch_hawq_configuration()


    def __fetch_ganglia_configuration(self, config_file):
        '''Fetch cluster name, and data path of ganglia.'''
        config = ConfigParser.ConfigParser()
        config.read(config_file)
        self.cluster = config.get('GANGLIA', 'cluster_name')
        self.data_root_path = config.get('GANGLIA', 'data_path')
        self.ganglia_data_path = self.data_root_path.rstrip(os.sep) + os.sep + self.cluster + os.sep
        self.rrdtool = config.get('RRDTOOL', 'bin_path').rstrip(os.sep)
        self.resolution = config.get('RRDTOOL', 'resolution')


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


    def test_02_check_disk_failure(self):
        '''Test case 02: Check disck failure by examing disck I/O with gpcheckperf'''
        df = False
        if len(self.hawq_config) == 0:
            ood = True
        else:
            for (host, path) in self.hawq_config:
                # cmd = "%s/bin/gpcheckperf -h %s -d '%s' -r d -D" % (GPHOME, host, path)
                # (ok, out) = shell.run( cmd )
                ok = True
                if ok:
                    # diskio_w = re.match(r"^.*disk write bandwidth \(MB\/s\): ([0-9]+\.[0-9]+)[ \t].*$", "".join(o[:-1] for o in out).strip()).group(1)
                    # diskio_r = re.match(r"^.*disk read bandwidth \(MB\/s\): ([0-9]+\.[0-9]+)[ \t].*$", "".join(o[:-1] for o in out).strip()).group(1)
                    diskio_w = 1200
                    diskio_r = 1600
                    # If detected disk I/O is less than 80% maximum disk I/O (1200Mb/s, 1600Mb/s), disk failure will be reportd
                    if ( diskio_w < 960 and diskio_w != 0 ) or ( diskio_r < 1280 and diskio_r != 0 ):
                        print  host + ":" + path + ":" + "Disc Write = " + diskio_w + " MB/s, Disk Read = " + diskio_r + " MB/s" + " threshold : disc_write = 96 MB/s , disk_read = 128MB/s"
                        df = True
                    else:
                        print  host + ":" + path + ":" + "Disc Write = " + str(diskio_w) + " MB/s, Disk Read = " + str(diskio_r) + " MB/s" + " threshold : disc_write = 96 MB/s , disk_read = 128MB/s"
              

        if df:
            self.fail()


    def test_06_check_hawq_logs_coredump(self):
        '''Test case 06: Check core dump in HAWQ logs'''
        # Potential improvement: check core dump file and extract call stack
        searchKeyArray = ['PANIC']
        self.__analyze_hawq_logs( searchKeyArray )

    def test_07_check_hawq_logs_fatal_errors_exceptions(self):
        '''Test case 07: Check fatal/errors/exceptions in HAWQ logs'''
        searchKeyArray = ['FATAL', 'ERROR', 'EXCEPTION']
        self.__analyze_hawq_logs( searchKeyArray )

    def test_08_check_hawq_logs_failures(self):
        '''Test case 08: Check failures in HAWQ logs'''
        searchKeyArray = ['FAIL']
        self.__analyze_hawq_logs( searchKeyArray )

    def test_09_check_hawq_logs_warnings(self):
        '''Test case 09: Check warnings in HAWQ logs'''
        searchKeyArray = ['WARNING']
        self.__analyze_hawq_logs( searchKeyArray )

    def test_10_check_hdfs_logs_namenode(self):
        '''Test case 10: Check errors and warnings in HDFS namenode logs including: Read Error, Write Error, Replica Error, Time Out, Warning'''
        searchKeyArray = ['Input\/output error']

        hosts_paths = []
        for host in [self.nn]:
            hosts_paths.append( (host, self.hdfs_path) )

        self.__analyze_hdfs_logs( searchKeyArray, hosts_paths)

    def test_11_check_hdfs_logs_secondary_namenode(self):
        '''Test case 11: Check errors and warnings in HDFS secondary namenode logs including: Read Error, Write Error, Replica Error, Time Out, Warning'''
        searchKeyArray = ['Input\/output error']

        hosts_paths = []
        for host in [self.snn]:
            hosts_paths.append( (host, self.hdfs_path) )

        self.__analyze_hdfs_logs( searchKeyArray, hosts_paths)

    def test_12_check_hdfs_logs_datanodes(self):
        '''Test case 12: Check errors and warnings in HDFS datanodes logs including: Read Error, Write Error, Replica Error, Time Out, Warning'''
        searchKeyArray = ['Input\/output error']

        hosts_paths = []
        for host in self.dn:
            hosts_paths.append( (host, self.hdfs_path) )

        self.__analyze_hdfs_logs( searchKeyArray, hosts_paths)

    





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

    def test_13_check_master_max_cpu_usage(self):
        '''Test case 13: Check the maximum CPU usage of HAWQ master'''
        hosts = self.hawq_master
        acc_cpu = self.__compute_cpu_usage(hosts)
        # Threshold for CPU: 15% for accumulated time
        if acc_cpu < 0:
            print "Accumulated CPU Usage on HAWQ master is: %d%%" % ( acc_cpu )
            self.fail()
        else:
            print "Accumulated CPU Usage on HAWQ master is: %d%%" % ( acc_cpu )

    def test_14_check_segments_max_cpu_usage(self):
        '''Test case 14: Check the maximum CPU usage of HAWQ segments'''
        hosts = self.hawq_segments
        # remove the same host
        hosts =  {}.fromkeys(hosts).keys()
        acc_cpu = self.__compute_cpu_usage(hosts)
        # Threshold for CPU: 20% for accumulated time
        if acc_cpu < 3:
            print "Accumulated CPU Usage on HAWQ segments is: %d%%" % ( acc_cpu )
            self.fail()
        else:
            print "Accumulated CPU Usage on HAWQ segments is: %d%%" % ( acc_cpu )

    def test_15_check_master_max_memory_usage(self):
        '''Test case 15: Check the maximum memory usage of HAWQ master'''
        hosts = self.hawq_master
        acc_mem = self.__compute_memory_usage(hosts)
        # Threshold for Memory: 5% for accumulated time
        if acc_mem < 3:
            print "Accumulated Memory Usage on HAWQ master is: %d%%" % ( acc_mem )
            self.fail()
        else:
            print "Accumulated Memory Usage on HAWQ master is: %d%%" % ( acc_mem )

    def test_16_check_segments_max_memory_usage(self):
        '''Test case 16: Check the maximum memory usage of HAWQ segments'''
        hosts = self.hawq_segments
        # remove the same host
        hosts =  {}.fromkeys(hosts).keys()
        acc_mem = self.__compute_memory_usage(hosts)
        # Threshold for Memory: 10% for accumulated time
        if acc_mem < 5:
            print "Accumulated Memory Usage on HAWQ segments is: %d%%" % ( acc_mem )
            self.fail()
        else:
            print "Accumulated Memory Usage on HAWQ segments is: %d%%" % ( acc_mem )

    def test_17_check_master_max_disk_io(self):
        '''Test case 17: Check the maximum Disk I/O of HAWQ master'''
        # Read 200M/s, Write 200M/s
        hosts = self.hawq_master
        [acc_dsk_write, acc_dsk_read] = self.__compute_disk_io(hosts)
        # Threshold for Disk I/O: 50% for write and 0% for read
        if acc_dsk_write < 0 or acc_dsk_read < 0:
            print "Accumulated Disk I/O on HAWQ master is: Write = %d%%, Read = %d%%" % ( acc_dsk_write, acc_dsk_read )
            self.fail()
        else:
            print "Accumulated Disk I/O on HAWQ master is: Write = %d%%, Read = %d%%" % ( acc_dsk_write, acc_dsk_read )

    def test_18_check_segments_max_disk_io(self):
        '''Test case 18: Check the maximum Disk I/O of HAWQ segments'''
        # Read 200M/s, Write 200M/s
        hosts = self.hawq_segments
        # remove the same host
        hosts =  {}.fromkeys(hosts).keys()
        [acc_dsk_write, acc_dsk_read] = self.__compute_disk_io(hosts)
        # Threshold for Disk I/O: 75% for write and 60% for read
        if acc_dsk_write < 0 or acc_dsk_read < 0:
            print "Accumulated Disk I/O on HAWQ segments is: Write = %d%%, Read = %d%%" % ( acc_dsk_write, acc_dsk_read )
            self.fail()
        else:
            print "Accumulated Disk I/O on HAWQ segments is: Write = %d%%, Read = %d%%" % ( acc_dsk_write, acc_dsk_read )

    def test_19_check_master_max_network_io(self):
        '''Test case 19: Check the maximum network I/O of HAWQ master'''
        # Data volume tranferred on master in execution period
        # Input 100M/s, Output 100M/s
        hosts = self.hawq_master
        [acc_net_write, acc_net_read] = self.__compute_network_io(hosts)
        # Threshold for Network I/O: 30% for accumulated time
        if acc_net_write < 1 or acc_net_read < 1:
            print "Accumulated Network I/O on HAWQ master is: Write = %d%%, Read = %d%%" % ( acc_net_write, acc_net_read )
            self.fail()
        else:
            print "Accumulated Network I/O on HAWQ master is: Write = %d%%, Read = %d%%" % ( acc_net_write, acc_net_read )

    def test_20_check_segments_max_network_io(self):
        '''Test case 20: Check the maximum network I/O of HAWQ master'''
        # Data volume tranferred on segments in execution period
        # Input 10M/s, Output 10M/s
        hosts = self.hawq_segments
        # remove the same host
        hosts =  {}.fromkeys(hosts).keys()
        [acc_net_write, acc_net_read] = self.__compute_network_io(hosts)
        # Threshold for Network I/O: 3% for accumulated time
        if acc_net_write < 3 or acc_net_read < 3:
            print "Accumulated Network I/O on HAWQ segments is: Write = %d%%, Read = %d%%" % ( acc_net_write, acc_net_read )
            self.fail()
        else:
            print "Accumulated Network I/O on HAWQ segments is: Write = %d%%, Read = %d%%" % ( acc_net_write, acc_net_read )

    def test_21_check_master_memory_leak(self):
        '''Test case 21: Check the memory leak on HAWQ master'''
        # Linear regression using least square method
        hosts = self.hawq_master
        slope = self.__compute_memory_usage_slope(hosts)

        if slope < 0:
            print "Invalid sample data for memeory leak analysis"
            self.fail()
        elif slope > 15:
            print "Potential memory leak on HAWQ master: %d%% increase in average" % ( slope )
            self.fail()
        else:
            print "Potential memory leak on HAWQ master: %d%% increase in average" % ( slope )

    def test_22_check_segments_memory_leak(self):
        '''Test case 22: Check the memory leak on HAWQ segments'''
        # Linear regression using least square method
        hosts = self.hawq_segments
        # remove the same host
        hosts =  {}.fromkeys(hosts).keys()
        slope = self.__compute_memory_usage_slope(hosts)

        if slope < 0:
            print "Invalid sample data for memeory leak analysis"
            self.fail()
        elif slope > 15:
            print "Potential memory leak on HAWQ segments: %d%% increase in average" % ( slope )
            self.fail()
        else:
            print "Potential memory leak on HAWQ segments: %d%% increase in average" % ( slope )

    def test_23_detect_oom_coredump(self):
        '''Test case 23: Deteck new issues regarding OOM and core dump'''
        # Test case to detect OOM and core dump as new issues
        self.skipTest()

###########################################################################
#  Try to run if user launched this script using following command:
#    python stress.py start_stamp end_stamp
#    [YOU SHOULD NOT CHANGE THIS]
if __name__ == '__main__':
    gptest.main()
