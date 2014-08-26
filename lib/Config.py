#!/usr/bin/env python
"""
------------------------------------------------------------------------------
COPYRIGHT: Copyright (c) 200x - 2010, Greenplum Inc.  All rights reserved.
PURPOSE:
LAST MODIFIED:
------------------------------------------------------------------------------
"""

import os, sys

LSP_HOME = os.getenv('LSP_HOME')
lib = os.path.join(LSP_HOME, 'lib')
if lib not in sys.path:
    sys.path.append(lib)

MYD = os.path.abspath(os.path.dirname(__file__))
mkpath = lambda *x: os.path.join(MYD, *x)

#HACK:  fix sys.path so that standard libraries don't get over-ridden
if MYD in sys.path:
    sys.path.remove(MYD)
    sys.path.append(MYD)

try:
    from PSQL import psql
except ImportError:
    sys.stderr.write('LSP needs psql in lib/PSQL.py\n')
    sys.exit(2)

# ============================================================================
class Config:
    class Record:
        def __init__(self, line):
            line = line.split('|')
            line = [l.strip() for l in line]
            self.dbid = int(line[0])
            self.content = int(line[1])
            self.role = line[2] == 'p'
            self.preferred_role = line[3] == 'p'
            self.mode = line[4] == 's'
            self.status = line[5] == 'u'
            self.hostname = line[6]
            self.address = line[7]
            self.port = line[8]
            self.datadir = line[9]
            self.replication_port = line[10]
            self.san_mounts = line[11]
  
    def __init__(self):
        from warnings import warn
        #warn("Config.py is deprecated. Please use lib.modules.gpdb.system.Config!")
        self.record = []
        self.fill()
    
    def fill(self):
        self.record = []
        # Use psql to get gp_configuration instead of pyODB
        # Bug: Solaris pyODB, -1 is 4294967295 
        #(ok, out) = psql.run(flag = '-q -t', cmd = 'select dbid, content, role, preferred_role, mode, status, hostname, address, port, fselocation as datadir, replication_port, san_mounts from gp_segment_configuration LEFT JOIN pg_cataprint.pg_filespace_entry on (dbid = fsedbid) LEFT JOIN pg_cataprint.pg_filespace fs on (fsefsoid = fs.oid and fsname=\'pg_system\') ORDER BY content, preferred_role', ofile = '-', isODBC = False, dbname='template1') 
    
        # Anu : commented out the above query since it was returning the full config of the system (including the filespace entry) Hence changed the query to return only the cluster configuration
        cmd='''select dbid, content, role, preferred_role, mode, status, hostname, address, port, fselocation as datadir, replication_port, san_mounts from gp_segment_configuration, pg_filespace_entry, pg_cataprint.pg_filespace fs where fsefsoid = fs.oid and fsname=\'pg_system\' and gp_segment_configuration.dbid=pg_filespace_entry.fsedbid ORDER BY content, preferred_role'''
        (ok, out) = psql.runcmd(pFlags='-t -q', sql);

        print cmd + '\n'
        print 'ok =' + str(ok) + '\n'
        print '***************************\n'
        print 'out =' + str(out) 
        
        if not ok:
            print('Error %s' % out)
            sys.exit('Unable to select gp_segment_configuration')
        for line in out:
            if line.find("NOTICE") < 0:
                line = line.strip()
                if line:
                    self.record.append(Config.Record(line))
    
    def get(self):
        return self.record
    
    def hasMirror(self):
        return reduce(lambda x, y: x or y,
                      [not r.role for r in self.record])
    
    def getNPrimarySegments(self):
        n = 0
        for r in self.record:
            if r.role and r.content != -1:
                n += 1
        return n
    
    def getHosts(self, unique=True):
        list = map(lambda x: x.hostname, self.record)
        if unique:
            u = {}
            for h in list: u[h] = 1
            list = u.keys()
        return list

    def getSegHostNames(self):
        
        (ok, out) = psql.run(flag='-q -t', cmd='select distinct hostname from gp_segment_configuration where content <> -1', ofile='-', isODBC=False, dbname='template1') 

        if not ok:
            print('Error %s' % out)
            sys.exit('Unable to select gp_segment_configuration')
        hostlist = psql.list_out(out)
        return hostlist
   
    def getMastermirrorHostname(self):
        '''Returns hostname of the standby master '''
        (ok, out) = psql.run(flag='-q -t', cmd="select hostname from gp_segment_configuration where content = -1 and role = 'm'", ofile='-', isODBC=False, dbname='template1')

        if not ok:
            print('Error %s' % out)
            sys.exit('Unable to select gp_segment_configuration')
        hostname = psql.list_out(out)[0]
        return hostname

    def getHostAndPortOfSegment(self, pSegmentNumber=0, pRole='p'):

        """
        PURPOSE:
            Return a tuple that contains the host and port of the specified segment.
        PARAMETERS:
            pSegmentNumber: The segment number (0 - N-1, where N is the number
                of segments).  You can probably also pass -1 to get info about
                the master.
                Defaults to 0 for segment 0.
            pRole: 'p' for Primary.  I think you use 'm' for Mirror, but I
                haven't tested that yet.
                Defaults to 'p' for Primary.
        WARNINGS:
            1) If there are duplicate values (same segment number ("content") and
               role (primary or mirror) in the config information, we will return
               the host and port of the first match.  I don't think there should
               ever be duplicates, however.
            2) This does not warn you if the segment is down.
        """

        # For some reason the role in the Config uses True for Primary and False
        # for Mirror, rather than 'p' and 'm' as gp_segment_configuration uses.
        if pRole == 'p':
            role = True
        else:
            role = False

        # Extract the "Record" info, which includes the hostname and port for
        # each segment.
        segmentInfo = self.record
        for seg in segmentInfo:
            # DDDIAGNOSTIC
            #print seg.content, seg.role, seg.hostname, seg.port
            if seg.content == pSegmentNumber and seg.role == role:
                return (seg.hostname, seg.port, seg.datadir)

        # If we couldn't get the requested info, then return a hint that we
        # didn't get the right stuff.
        # Throwing an exception might be better!!!
        return ('DidNotFindSpecifiedSegment', 9999)

    # Ngoc: 20100419: check if we run GPDB against Multinode
    #
    def isMultinode(self):
        if (len(self.getHosts()) == 1):
            return False
        else: 
            return True
     
    # Johnny Soedomo: 20100505: check if there is MasterMirror
    #
    def hasMasterMirror(self): 
        master = 0
        for r in self.record:
            if r.content == -1:
                master += 1
        if master == 1:
            return False
        else:
            return True 

    # ramans2: 20110506: Return number of segments
    def getCountSegments(self):
        (ok, out) = psql.run(dbname='template1', cmd="select count(*) from gp_segment_configuration where content != -1 and role = 'p' and status = 'u'",
                             ofile='-', flag='-q -t')
        for line in out:
            return line

    def isMasterMirrorSynchronized(self):
        (ok, out) = psql.run(dbname='template1',
                 cmd='select summary_state from gp_master_mirroring',
                 ofile='-',
                 flag='-q -t')
        if ok:
            for line in out:
                line = line.strip()
                if line == 'Synchronized':
                    return True
        return False

    def getMasterDataDirectory(self):
        for r in self.record:
            if r.role and r.content == -1:
                return r.datadir
   
    def getMasterHost(self):
        for r in self.record:
            if r.role and r.content == -1:
                return r.hostname
   
    def getMasterStandbyHost(self):
        for r in self.record:
            if r.content == -1 and r.dbid != 1:
                print r.dbid
                return r.hostname
        return None 

config = Config()

if __name__ == "__main__":
    config = Config()
