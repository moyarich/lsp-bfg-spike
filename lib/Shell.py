#!/usr/bin/env python
"""
------------------------------------------------------------------------------
COPYRIGHT: Copyright (c) 200x - 2010, Greenplum Inc.  All rights reserved.
PURPOSE:
LAST MODIFIED:
------------------------------------------------------------------------------
"""
#disable deprecationwarnings
import warnings
warnings.simplefilter('ignore', DeprecationWarning)

import unittest, os, popen2, time, sys, getopt, StringIO, string, platform, datetime, subprocess

MYD = os.path.abspath(os.path.dirname(__file__))
mkpath = lambda *x: os.path.join(MYD, *x)

if MYD in sys.path:
    sys.path.remove(MYD)
    sys.path.append(MYD)

# ============================================================================
class Shell:
    
    def __init__(self):
        self.lastcmd = ''
    
    def run(self, cmd, oFile=None, mode = 'a', cmdtype="cmd"):
        """
        Run a shell command. Return (True, [result]) if OK, or (False, []) otherwise.
        @params cmd: The command to run at the shell.
             oFile: an optional output file.
             mode: What to do if the output file already exists: 'a' = append;
                 'w' = write.  Defaults to append (so that the function is 
                 backwards compatible).  Yes, this is passed to the open() 
                 function, so you can theoretically pass any value that is 
                 valid for the second parameter of open().
        @change: 2010-04-11 mgilkey
                 I added an optional parameter to the function to allow the 
                 caller to specify whether to append to an existing file or 
                 overwrite if there is an existing file.
        """

        self.lastcmd = cmd
        p = os.popen(self.lastcmd)
        ret = []

        fp = None
        if oFile: #if oFile provided then append (or write) the results of the cmd 
                  #to the file
            fp = open(oFile, mode)

        for line in p:
            ret.append(line)
            if fp:
              fp.write(line)
        rc = p.close()
        if fp:
            fp.close()
        return (not rc, ret)

    
    def killall(self, procname):
	#Anu: removed the path for pkill from /bin/pkill to pkill - assuming that pkill is always in the path.
        cmd = "bash -c 'pkill %s || killall %s' > /dev/null 2>&1" % (procname, procname)
        return self.run(cmd)

    def getFilePath(self,cmd,key="sql"):
        cmdArr = cmd.split(" ")
        for val in cmdArr:
            if val.find(key,2)>0:
                return val
        return "cmd-error"

    # Run Shell execution with Timeout
    # Currently, it's checking only for PSQL. We should try to generalize for all shell
    def run_timeout(self,cmd,timeout=900,raiseError=True,getPstack=True):

        # To help with filerep debugging take the timeout out of picture
        timeout=0
       
        # If psql "-c" option, runs with no timeout
        # Also check for spaces in between -c.
        if(cmd.find("psql")>=0 and cmd.find(" -c ")>0):
            return self.run(cmd)
        else:
            process = subprocess.Popen(cmd, env=None, shell=True, executable='/bin/bash',stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            if timeout>0:
                start = datetime.datetime.now()
                while process.poll() is None:
                    time.sleep(0.1)
                    now = datetime.datetime.now()
                    if (now - start).seconds > timeout:
                        """
                        TODO: Need to refactor this from cdbfastUtil
                        sqlFile = self.getFilePath(cmd)
                        collectStackTraces(sqlFile)
                        collectResourceUsage(sqlFile)
                        checkDeadlock(sqlFile)
                        """
                        # Using send_signal(2) seems to let the process finish. Using kill instead
                        # process.send_signal(2) #Send interrupt to the parent gptorment process
                        # process.kill() # equivalent to kill SIGKILL (-9)
                        process.terminate() # equivalent to kill SIGTERM (-2)
                        os.waitpid(process.pid,0)
                        # If raiseError is True, will throw an exception so exit out the Test Case
                        # Sometimes, we just want to timeout the command execution, so return False instead of exception
                        if raiseError:
                            raise GPTestError('GPTimeout')
                        return (False,["Timeout"])
            # Fix issues with long-running loop, python is not cleaning up the process
            pmsg = process.communicate()[0]
            if process.returncode == None:
                process.terminate()
            return (True,[pmsg])

    def run_in_loop(self, cmd, loop=0, msg="Error in loop"):
        """
        Run in a loop and exit if there is an exception
        @cmd: Command
        @loop_count: Loop Count, default=0 is infinite
        @msg: Error Message
        
        @todo: Add a condition as a parameter that execute a process or function 
        that returns True or False
        """
        try:
            counter = 1
            while True:
                if loop != 0 and (counter > loop) == True:
                    break
                self.run(cmd)
                counter += 1
        except:
            print traceback.print_exc()
            self.fail(msg)

shell = Shell()


