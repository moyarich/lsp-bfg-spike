#!/usr/bin/env python
import os, sys, re, string, random
import time, datetime
import ConfigParser, commands
from time import strftime
from random import randint
from subprocess import Popen, PIPE
from datetime import datetime, timedelta

try:
    import yaml
except ImportError:
    sys.stderr.write('Stress needs pyyaml. You can get it from http://pyyaml.org.\n') 
    sys.exit(2)

LSP_HOME = os.path.abspath(os.path.dirname(__file__))
os.environ['LSP_HOME'] = LSP_HOME

if LSP_HOME not in sys.path:
    sys.path.append(LSP_HOME)

LIB_DIR = LSP_HOME + os.sep + 'lib'
if LIB_DIR not in sys.path:
    sys.path.append(LIB_DIR)

try:
    from lib.PSQL import psql
except ImportError:
    sys.stderr.write('Stress needs psql in lib/PSQL.py in Workload.py\n')
    sys.exit(2)

try:
    from lib.Config import config
except ImportError:
    sys.stderr.write('Stress needs config in lib/Config.py\n')
    sys.exit(2)

class Check_hawq_stress():
    def __init__(self):
        self.check_stress = LSP_HOME + '/validator/check_stress'
        self.__fetch_hdfs_configuration()
        self.__fetch_hawq_configuration()

    def __fetch_hdfs_configuration(self):
        '''Fetch namenode, datanode info and logs dir of hdfs.'''
        hdfs_conf_file = LSP_HOME + os.sep + 'validator/check_stress/hdfs_stress.yml'
        with open(hdfs_conf_file, 'r') as fhdfs_conf:
            hdfs_conf_parser = yaml.load(fhdfs_conf)

        self.hdfs_path = hdfs_conf_parser['path'].strip()
        self.namenode = [ hdfs_conf_parser['namenode'].strip() ]
        self.second_namenode = [ hdfs_conf_parser['second_namenode'].strip() ]
        self.datanode = [ dn.strip() for dn in hdfs_conf_parser['datanode'].split(',') ]

       # print self.hdfs_path
       # print self.namenode
       # print self.second_namenode
       # print self.datanode

    def __fetch_hawq_configuration(self):
        '''Fetch master hostname, segments hostname, data directory of HAWQ.'''
        self.hawq_master = config.getMasterHostName()
        self.hawq_segments = config.getSegHostNames()
        self.hawq_paths = []
        self.hawq_config = {}

        sql = "SELECT gsc.hostname, pfe.fselocation FROM gp_segment_configuration gsc, pg_filespace_entry pfe WHERE gsc.dbid = pfe.fsedbid AND pfe.fselocation NOT LIKE 'hdfs%' ORDER BY pfe.fselocation;"
        (ok, out) = psql.runcmd( dbname = 'postgres', cmd = sql , ofile = '-', flag = '-q -t' )

        if not ok:
            print out
            raise Exception("Failed to get HAWQ configuration paths.")
        else:
            for line in out:
                line = line.strip()
                if line:
                    (host, path) = line.split( '|' )
                    if not self.hawq_config.has_key(host.strip()):
                        self.hawq_config[host.strip()] = []
                    self.hawq_config[host.strip()].append(path.strip())
                        
                   # self.hawq_config.append( (host.strip(), path.strip()) )
                    self.hawq_paths.append( path.strip() ) 

        print self.hawq_master
        print self.hawq_segments
        print self.hawq_config


    def __search_key_in_log(self, host, key, path):
        '''Search key in logs using grep'''
        cmd = ''' gpssh -h %s -e "sudo grep -i '%s' %s" ''' % (host, key, path)
        (status, output) = commands.getstatusoutput(cmd)
        return (status, output)

    def __escape_search_key(self, key):
        '''Escape the search key in case of special characters'''
        searchKeyEscaped = ''
        for i in range(0, len(key)):
            if key[i] in [',', '\\', '{', '}', '[', ']']:
                searchKeyEscaped += '\\' + key[i]
            else:
                searchKeyEscaped += key[i]
        return searchKeyEscaped

    def __analyze_hdfs_logs(self, searchKeyArray = ['error'], hosts = ['localhost'], path = '/usr/local/gphd/hadoop-2.2.0-gphd-3.0.0.0'):
        '''Search keys in hdfs logs, including: error, exception, etc.''' 
        find_any = False
        for key in searchKeyArray:
            print "Searching for '%s'" % key
            find_one = False
            for host in hosts:
                (status, output) = self.__search_key_in_log( host = host, key = key, path = path + "/logs/*.log")
                result = output.split('\n', 1)
                if len(result) == 2 :
                    print "Logs for '%s' on %s in %s:" % (key, host, path + "/logs/*.log")
                  #  print result[1].split('\n', 1)[0]
                    print result[1]
                    find_one = True
                    find_any = True
            if not find_one:
                print "No '%s' found" % ( key )
        return find_any


    def __analyze_hawq_logs(self, searchKeyArray = ['error']):
        '''Analyze HAWQ logs using gplogfilter'''
        find_any = False
        now_time = time.time()
        bt = datetime.fromtimestamp(int(now_time - 3600*2)).strftime('%Y-%m-%d %H:%M:%S')
        et = datetime.fromtimestamp(int(now_time)).strftime('%Y-%m-%d %H:%M:%S')

        searchKeyRegrex = self.__escape_search_key(searchKeyArray[0])
        for i in range(1, len(searchKeyArray)):
            searchKeyEscaped = self.__escape_search_key(searchKeyArray[i])
            searchKeyRegrex += '|%s' % (searchKeyEscaped)

        cmd = "gplogfilter -b '%s' -e '%s' -m '%s'" % (bt, et, searchKeyRegrex)
        #print cmd
        (status, output) = commands.getstatusoutput(cmd)
        matchLines = re.findall('match:       [^0]+', output)
        #print '\n'.join(matchLines)
        
        if (len(matchLines)):
            find_any = True
            print "Logs for '%s' on master: " % (searchKeyRegrex)
            print output
        else:
            print "No '%s' found on master" % (searchKeyRegrex)


        gphome = os.getenv('GPHOME')
        for host in self.hawq_segments:
            for log_path in self.hawq_config[host]:
                cmd = ''' gpssh -h %s -e "cd %s; source greenplum_path.sh; gplogfilter -b '%s' -e '%s' -m '%s' %s "''' % (host, gphome, bt, et, searchKeyRegrex, log_path + '/pg_log/*.csv')
                #print cmd
                (status, output) = commands.getstatusoutput(cmd)
                
                if status != 0:
                    print cmd
                    print output
                else:
                    matchLines = re.findall('match:       [^0]+', output)
                    
                    if (len(matchLines)):
                        find_any = True
                        print "Logs for '%s' on segments %s: " % (searchKeyRegrex, host)
                        print output
                    else:
                        print "No '%s' found on segments %s" % (searchKeyRegrex, host)

        return find_any

    # ????????????
    def __produce_wildcard_for_seg_paths(self, strlist):
        length = min(len(strlist[0]), len(strlist[1]))
        print length
        num = len(strlist)
        print num
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
        print wild_card
      
        return wild_card


    def test_01_check_hawq_availability(self):
        '''Test case 03: Check availability including: utility mode, panic, cannot access, and hawq sanity test with INSERT/DROP/SELECT queries'''
        sqlFile = self.check_stress + "/check_hawq_availability.sql"
        ansFile = self.check_stress + "/check_hawq_availability.ans"
        outFile = self.check_stress + "/check_hawq_availability.out"

        cmd = "psql -a -d %s -f %s" % ('postgres', sqlFile)
        (s, o) = commands.getstatusoutput(cmd)
        if s != 0:
            print str(o)
            print('test_01_check_hawq_availability: error ')
        else:
            fo = open(outFile, 'w')
            ignore = False
            for line in o.split('\n'):
                if '-- start_ignore' in line.strip():
                    ignore = True
                    continue

                if ignore == True:
                    if '-- end_ignore' in line.strip():
                        ignore = False
                    continue
                else:
                    fo.write( line + '\n')

            fo.close()

            cmd = "diff -rq %s %s" % ( outFile, ansFile )
            (status, output) = commands.getstatusoutput(cmd)
            if status != 0 or output != '':
                print str(output)
                print('test_01_check_hawq_availability: failed ')
            else:
                print('test_01_check_hawq_availability: success ')


    def test_02_check_hawq_health(self):
        '''Test case 04: Check health including: segment down'''
        # Potential improvement: further investigation on root cause using gpcheckperf
        sql = "SELECT count(*) FROM pg_catalog.gp_segment_configuration WHERE mode<>'s'"
        (ok, out) = psql.runcmd( dbname = 'postgres', cmd = sql , ofile = '-', flag = '-q -t' )
        if not ok:
            print str(out) 
            print('test_02_check_hawq_health: error ')
        if int(out[0]) == 0:
            print('test_02_check_hawq_health: success ')
        else:
            print('test_02_check_hawq_health: %d segments is failed. ' % (int(out[0])))


    def test_03_check_out_of_disk_hawq(self):
        '''Test case 01: Check out-of-disk by examing available disk capacity'''
        ood = False
        if len(self.hawq_config) == 0:
            ood = True
        else:
            for host in self.hawq_config.keys():
                for path in self.hawq_config[host]:
                  #  cmd = "ssh %s 'df -h %s'" % (host, ' '.join(self.hawq_config[host]))
                    cmd = "ssh %s 'df -h %s'" % (host, path)
                    (status, output) = commands.getstatusoutput(cmd)
                    if status != 0:
                        print('test_03_check_out_of_disk_hawq: error ')
                        print str(output)
                    else:
                        capacity_list = re.findall(r'[0-9]+%', output)
                        for capacity in capacity_list:
                            if int(capacity.replace('%', '')) > 80:
                                ood = True
                            print host + ": " + path + ": " + capacity + " used" + ' threshold : 80%'

        if ood:
            print('test_03_check_out_of_disk_hawq: failed ')
        else:
            print('test_03_check_out_of_disk_hawq: success ')

    
    def test_04_check_out_of_disk_hdfs(self):
        (status, output) = commands.getstatusoutput('kinit -k -t /home/gpadmin/hawq-krb5.keytab hdfs/bcn-mst1@HAWQ.PIVOTAL.COM; hadoop dfsadmin -report')
        start_index = output.find('DFS Used%')
        end_index = output.find('\n', start_index)
        if start_index !=-1 and end_index != -1:
            print output[start_index:end_index] + " used" + ' threshold : 80%'
            if float( output[start_index:end_index].split(':')[1].replace('%', '') ) > 80:
                print('test_04_check_out_of_disk_hdfs: failed ')
            else:
                print('test_04_check_out_of_disk_hdfs: success ')
        else:
            print output
            print('test_04_check_out_of_disk_hdfs: error ')
    

    def test_05_check_hdfs_logs_namenode(self):
        '''Test case 05: Check errors and warnings in HDFS namenode logs including: Read Error, Write Error, Replica Error, Time Out, Warning'''
        searchKeyArray = ['Input\/output error', 'error']

        status = self.__analyze_hdfs_logs(searchKeyArray = searchKeyArray, hosts = self.namenode, path = self.hdfs_path)
        if status:
            print('test_05_check_hdfs_logs_namenode: failed ')
        else:
            print('test_05_check_hdfs_logs_namenode: success ')

    def test_06_check_hdfs_logs_secondary_namenode(self):
        '''Test case 06: Check errors and warnings in HDFS secondary namenode logs including: Read Error, Write Error, Replica Error, Time Out, Warning'''
        searchKeyArray = ['Input\/output error', 'error']

        status = self.__analyze_hdfs_logs(searchKeyArray = searchKeyArray, hosts = self.second_namenode, path = self.hdfs_path)
        if status:
            print('test_06_check_hdfs_logs_secondary_namenode: failed ')
        else:
            print('test_06_check_hdfs_logs_secondary_namenode: success ')

    def test_07_check_hdfs_logs_datanodes(self):
        '''Test case 07: Check errors and warnings in HDFS datanodes logs including: Read Error, Write Error, Replica Error, Time Out, Warning'''
        searchKeyArray = ['Input\/output error', 'error']

        status = self.__analyze_hdfs_logs(searchKeyArray = searchKeyArray, hosts = self.datanode, path = self.hdfs_path)
        if status:
            print('test_07_check_hdfs_logs_datanodes: failed ')
        else:
            print('test_07_check_hdfs_logs_datanodes: success ')      



    def test_08_check_hawq_logs_coredump(self):
        '''Test case 08: Check core dump in HAWQ logs'''
        # Potential improvement: check core dump file and extract call stack
        
        searchKeyArray = ['PANIC']
        
        if self.__analyze_hawq_logs( searchKeyArray = searchKeyArray ):
            print('test_08_check_hawq_logs_coredump: failed ')
        else:
            print('test_08_check_hawq_logs_coredump: success')

    def test_09_check_hawq_logs_fatal_errors_exceptions(self):
        '''Test case 09: Check fatal/errors/exceptions in HAWQ logs'''
        
        searchKeyArray = ['FATAL', 'ERROR', 'EXCEPTION']
        
        if self.__analyze_hawq_logs( searchKeyArray = searchKeyArray ):
            print('test_09_check_hawq_logs_fatal_errors_exceptions: failed ')
        else:
            print('test_09_check_hawq_logs_fatal_errors_exceptions: success ')

    def test_10_check_hawq_logs_failures(self):
        '''Test case 10: Check failures in HAWQ logs'''
        
        searchKeyArray = ['FAIL']
        
        if self.__analyze_hawq_logs( searchKeyArray = searchKeyArray ):
            print('test_10_check_hawq_logs_failures: failed')
        else:
            print('test_10_check_hawq_logs_failures: success')

    def test_11_check_hawq_logs_warnings(self):
        '''Test case 11: Check warnings in HAWQ logs'''
        
        searchKeyArray = ['WARNING']
        
        if self.__analyze_hawq_logs( searchKeyArray = searchKeyArray ):
            print('test_11_check_hawq_logs_warnings: failed ')
        else:
            print('test_11_check_hawq_logs_warnings: success ')
        

    def test(self):
        self.test_01_check_hawq_availability()
        self.test_02_check_hawq_health()
        self.test_03_check_out_of_disk_hawq()
        self.test_04_check_out_of_disk_hdfs()
     #   self.__analyze_hdfs_logs(searchKeyArray = ['liuq', 'error'], hosts = ['localhost', 'localhost'])
        self.test_05_check_hdfs_logs_namenode()
        self.test_06_check_hdfs_logs_secondary_namenode()
        self.test_07_check_hdfs_logs_datanodes()
      #  self.__analyze_hawq_logs()
        self.test_08_check_hawq_logs_coredump()
        self.test_09_check_hawq_logs_fatal_errors_exceptions()
        self.test_10_check_hawq_logs_failures()
        self.test_11_check_hawq_logs_warnings()


if __name__ == '__main__':
    check_stress = Check_hawq_stress()
    check_stress.test()

