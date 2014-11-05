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

        self.log_path = hdfs_conf_parser['path'].strip()
        self.namenode = [ hdfs_conf_parser['namenode'].strip() ]
        self.second_namenode = [ hdfs_conf_parser['second_namenode'].strip() ]
        self.datanode = [ dn.strip() for dn in hdfs_conf_parser['datanode'].split(',') ]

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

    def __search_key_in_log(self, host, key, path):
        '''Search key in logs using grep'''
        cmd = "gpssh -h %s -e 'grep -i %s %s'" % (host, key, path)
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

    def __analyze_hdfs_logs(self, searchKeyArray = ['error', 'exception'], hosts = ['localhost'], path = '/usr/local/gphd/hadoop-2.2.0-gphd-3.0.0.0'):
        '''Search keys in hdfs logs, including: error, exception, etc.''' 
        find_any = False
        for key in searchKeyArray:
            print "Searching for %s" % key
            find_one = False
            for host in hosts:
                (status, output) = self.__search_key_in_log( host = host, key = key, path = path+"/logs/*.log",)
                print "Logs for '%s' on %s in %s:" % (key, host, path+"/logs/*.log")
                #print out
                #find_one = True
                print 
            if find_one:
                print "\nFound %s" % ( key )
                find_any = True
            else:
                print "\nNo %s found" % ( key )

        if find_any:
            pass
            #print find


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
            print('test_01_check_hawq_availability is error.\n ')
            print o
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
                print('test_01_check_hawq_availability is failed.\n ')
                print output
            else:
                print('test_01_check_hawq_availability is success.\n ')

    def test_02_check_out_of_disk(self):
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
                        print('test_02_check_out_of_disk is error.\n ')
                        print output
                    else:
                        capacity_list = re.findall(r'[0-9]+%', output)
                        for capacity in capacity_list:
                            if int(capacity.replace('%', '')) > 80:
                                print host + ": " + path + ": " + capacity + " used" + ' threshold : 80%'
                                ood = True
                            else:
                                print host + ": " + path + ": " + capacity + " used" + ' threshold : 80%'

        if ood:
            print('test_02_check_out_of_disk is failed.\n ')
        else:
            print('test_02_check_out_of_disk is success.\n ')

    def test_03_check_hawq_health(self):
        '''Test case 04: Check health including: segment down'''
        # Potential improvement: further investigation on root cause using gpcheckperf
        sql = "SELECT count(*) FROM pg_catalog.gp_segment_configuration WHERE mode<>'s'"
        (ok, out) = psql.runcmd( dbname = 'postgres', cmd = sql , ofile = '-', flag = '-q -t' )
        if not ok:
            print('test_03_check_hawq_health is error.\n ')
            print out
        if int(out[0]) == 0:
            print('test_03_check_hawq_health is success.\n ')
        else:
            print('test_03_check_hawq_health is failed: %d segments is failed\n ' % (int(out[0])) )

    def test(self):
        #self.test_01_check_hawq_availability()
        #self.test_02_check_out_of_disk()
        self.test_03_check_hawq_health()


if __name__ == '__main__':
    check_stress = Check_hawq_stress()
    check_stress.test()

