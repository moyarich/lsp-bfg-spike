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

class Check_hawq_stress():
    def __init__(self):
        self.pwd = os.path.abspath(os.path.dirname(__file__))

    def __fetch_hdfs_configuration(self):
        '''Fetch namenode, datanode info and logs dir of hdfs.'''
        hdfs_conf_file = self.pwd + os.sep + 'check_stress/hdfs_stress.yml'
        with open(hdfs_conf_file, 'r') as fhdfs_conf:
            hdfs_conf_parser = yaml.load(fhdfs_conf)

        self.log_path = hdfs_conf_parser['path'].strip()
        self.namenode = hdfs_conf_parser['namenode'].strip()
        self.second_namenode = hdfs_conf_parser['second_namenode'].strip()
        self.datanode = [ dn.strip() for dn in hdfs_conf_parser['datanode'].split(',') ]

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

    def test(self):
        self.__fetch_hdfs_configuration()



if __name__ == '__main__':
    check_stress = Check_hawq_stress()
    check_stress.test()

