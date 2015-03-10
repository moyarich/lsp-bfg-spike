#!/usr/bin/env python
'''
PSQL utility class to execute SQL

@copyright: Copyright (c) 2011 EMC Corporation All Rights Reserved
This software is protected, without limitation, by copyright law
and international treaties. Use of this software and the intellectual
property contained therein is expressly limited to the terms and
conditions of the License Agreement under which it is provided by
or on behalf of EMC.

Examples:
1. Basic psql.runfile
- With no comments: psql.runfile(file)
- With comments: psq.runfile(file,default='-a').
This is useful if we want comments enabled in the output file.
We use directives in the olap_queries so that gpdiff.pl sorts out the output

2. Connecting to different database, default is gptest
- psq.runfile(filename, dbname='dbname')

3. psql.runfile with username and password
This is useful for resource queue. By default we are connecting as superuser
- psql.runfile(filename, username='user1') with no password needed
- psql.runfile(filename, username='user1', password='password')

4. psql.runcmd_utilitymode
To run psql in Utility Mode

5. psql.runcmd_with_catalogupdate
Latest 4.1 and MAIN does not allow to update gp_segment_configuration.
This function will allow to update gp_segment_configuration

@todo: 
1. Use logger
2. move outFile as a different class other than cdbfastUtil
3. refactor use of useodbc

'''

import os
import sys
import time

try:
    from Shell import shell
except ImportError:
    sys.stderr.write('LSP needs shell in lib/Shell.py when using PSQL\n')
    sys.exit(2)

class PSQL:
    '''
    @class PSQL
    @author: Ruilong Huo
    @organization: Pivotal DF QA
    @contact: rhuo@gopivotal.com
    '''

    def __init__(self):
        pass

    def Print(self):
        print self.__class__.__name__

    def run(self, dbname = None, ifile = None, ofile = None, cmd = None, 
            flag = '-e', timeout=900, username = None, password = None,
            PGOPTIONS = None, host = None, port = None,
            background = False):
        '''
        Run a command or file against psql. Return True if OK.
        @param dbname: database name
        @param ifile: input file
        @param cmd: command line
        @param flag: -e Run SQL with no comments (default)
                     -a Run SQL with comments and psql notice
        @param timeout: 900s default
        @param username: psql user
        @param host    : to connect to a different host
        @param port    : port where gpdb is running
        @param PGOPTIONS: connects to postgres via utility mode
        @poram background: run PSQL command in the background
        '''
        if dbname == None:
            dbname = 'postgres'
            
        #if username == None:
            #username = 'gpadmin'
          #  try:
          #      username = os.environ['PGUSER']
          #  except Exception, e:
          #      username = 'gpadmin'
            
        if password == None:
            password = ""
        else:
            password = "--password %s" % password

        if PGOPTIONS == None:
            PGOPTIONS = ""
        else:
            PGOPTIONS = "PGOPTIONS='%s'" % PGOPTIONS
            
        if host is None:
            host = "-h %s" % ('localhost')
            #host = ''
        else:
            host = "-h %s" % host

        if port is None:
            port = ""
        else:
            port = "-p %s" % port

        if cmd:
            arg = '-c "%s"' % cmd
        elif ifile:
            arg = ' -f ' + ifile
   #         if not (flag == '-q'):
    #            arg = '-e < ' + ifile
     #       if flag == '-a':
      #          arg = '-f ' + ifile
        else:
            raise PSQLError('missing cmd and ifile')

        if background:
            background = " &"
        else:
            background = ""

        if ofile == '-':
            ofile = '2>&1'
        elif not ofile:
            ofile = '> /dev/null 2>&1'
        else:
            ofile = '> %s 2>&1' % ofile

        if username == None:
            #print '%s psql -d %s %s %s %s %s %s %s' % (PGOPTIONS, dbname, host, port, flag, arg, ofile, background)
            return shell.run_timeout('%s psql -d %s %s %s %s %s %s %s' %
                        (PGOPTIONS, dbname, host, port, flag, arg, ofile, background), timeout=timeout)
        else:
            #print '%s psql -d %s %s %s -U %s %s %s %s %s' %(PGOPTIONS, dbname, host, port, username, flag, arg, ofile, background)
            return shell.run_timeout('%s psql -d %s %s %s -U %s %s %s %s %s' %
                        (PGOPTIONS, dbname, host, port, username, flag, arg, ofile, background), timeout=timeout)
            #result = shell.run_timeout('%s psql -d %s %s %s -U %s %s %s %s %s' %
            #            (PGOPTIONS, dbname, host, port, username, flag, arg, ofile, background), timeout=timeout)
            #print result
            #return result


    def runcmd(self, cmd, dbname = None, ofile = '-', flag = '', username = None, password = None,
        PGOPTIONS = None, host = None, port = None, background = False):
        '''
        Run command, psql -c cmd
        @param cmd: command line
        @param dbname: database name
        @param username: username
        @param PGOPTIONS: PGOPTIONS
        @param host: host
        @param port: port

        '''
        return self.run(cmd = cmd, dbname = dbname, ofile = ofile, flag = flag, 
                        username = username, password = password, 
                        PGOPTIONS = PGOPTIONS, host = host, port = port,
                        background = background)

    def runfile(self, ifile, flag='', timeout = 900, dbname = None, outputPath = "", outputFile = "-", 
        username = None, password = None, PGOPTIONS = None, host = None, port = None,
        background = False):
        '''
        Run SQL File, psql -f ifile
        @param ifile: Input File
        @param timeout: Timeout
        @param dbName: defaults to gptest
        @param username: username
        @param PGOPTIONS: PGOPTIONS
        @param host: host
        @param port: port
        '''
        return self.run(ifile = ifile, ofile = outputFile, flag = flag, 
                             timeout = timeout, dbname = dbname, username = username, password = password, 
                             PGOPTIONS = PGOPTIONS, host = host, port = port,
                             background = background)
    
    def list_out(self,test_out):
        """Parse the output of psql.run and returns the result as a list """
        test_list = []
        for test in test_out:
            test=test.replace('\n','')
            test=test.strip()
            if test != None and test != '' and test != '\n':
               test_list.append(test)
        return test_list

psql = PSQL()
