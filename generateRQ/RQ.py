from __future__ import division
import os
import sys
import random
import yaml
import fileinput
import re
import commands

class ParameterParser:
    def __init__(self, yamlfile = './RQ.yml', report_directory = './'):
        with open(yamlfile, "r") as fyaml:
            yaml_parser = yaml.load(fyaml)
        self.height = yaml_parser['height']
        self.max_width = yaml_parser['max_width']
        self.fix_width = yaml_parser['fix_width']
        self.child_ratio = yaml_parser['child_ratio']
        if self.child_ratio == 'customize':
            self.child_ratio_value = yaml_parser['child_ratio_value'].split(',')
            self.chile_ratio_total = 0
            for v in self.child_ratio_value:
                self.chile_ratio_total += int(v)
            print "Childvalue is %s and the total is %d" %(self.child_ratio , self.chile_ratio_total)
            print self.child_ratio_value 
        self.curqueue = 1
        self.default_memory_limit_cluster = int(yaml_parser['default']['MEMORY_LIMIT_CLUSTER'])
        self.default_core_limit_cluster = int(yaml_parser['default']['CORE_LIMIT_CLUSTER'])
        self.default_leaf_active_statements_cluster = yaml_parser['default']['ACTIVE_STATEMENTS']
        self.default_resource_upper_factor = yaml_parser['default']['RESOURCE_UPPER_FACTOR']
        self.default_segment_resource_quota = yaml_parser['default']['SEGMENT_RESOURCE_QUOTA']   
        self.default_allocation_policy = yaml_parser['default']['ALLOCATION_POLICY']
      
        self.leaf_resource_upper_factor = int(yaml_parser['leaf']['RESOURCE_UPPER_FACTOR'])
        self.leaf_segment_resource_quota = yaml_parser['leaf']['SEGMENT_RESOURCE_QUOTA']
        self.leaf_allocation_policy = yaml_parser['leaf']['ALLOCATION_POLICY']
        self.leaf_active_statements_cluster = int(yaml_parser['leaf']['ACTIVE_STATEMENTS'])

        print "The height:width:fix width is %d, %d, %s" %(self.height,self.max_width,self.fix_width) 


        #open resource quene defition file
        self.report_directory = report_directory
        rqfile = "%s/RQ.sql"%self.report_directory
        userlist = "%s/userlist"%self.report_directory
        quenelist = "%s/rqlist"%self.report_directory
        self.rqfile = open(rqfile,"w")
        self.userfile = open(userlist, "w")
        self.quenefile = open(quenelist,"w")
        self.rolelist = []
        self.quenelist = []
    
    def closefile(self):
        for i in range(0, len(self.rolelist) ):
            if i <> 0:
                self.userfile.write(',')
            self.userfile.write(self.rolelist[i])
        
        for i in range(0, len(self.quenelist)):
            if i <> 0:
                self.quenefile.write(',')
            self.quenefile.write(self.quenelist[i])
        self.rqfile.close()
        self.userfile.close()
        self.quenefile.close()
    
class Node:
     def __init__(self, name, parent, percentMem, percentCore, param_name,param_value, parameters):
         self._children = []
         self._name = name
         self._parent = parent
         self.parameters = parameters
         if name == 'pg_default':
             self._resource_upper_factor = parameters.default_resource_upper_factor
             self._active_statements_cluster = parameters.default_leaf_active_statements_cluster
             self._segment_resource_quota = parameters.default_segment_resource_quota
             self._allocation_policy = parameters.leaf_allocation_policy
         else:
             self._resource_upper_factor = parameters.leaf_resource_upper_factor
             self._active_statements_cluster = parameters.leaf_active_statements_cluster
             self._segment_resource_quota = parameters.leaf_segment_resource_quota
             self._allocation_policy = parameters.leaf_allocation_policy
         self._memory_limit_cluster = percentMem
         self._core_limit_cluster = percentCore
         
         if param_name != '':
             exec("self._" + param_name.lower() + "=" + str(param_value))

     def add(self, node, isbranch):
         self._children.append(node)
         self.generaterq(node,isbranch)
 
     def getchildren(self):
         return self._children  

     def generaterq(self, node, isbranch):
         if node._name == 'pg_default':
             quenesql = "ALTER RESOURCE QUEUE pg_default WITH("
             rolename = 'role_default'
         else:
             quenesql = "CREATE RESOURCE QUEUE " + node._name + " WITH(PARENT= " + "'" + node._parent._name + "'," 
             rolename =  'role_' + str(self.parameters.curqueue)
         quenesql = quenesql + "ACTIVE_STATEMENTS=" + str(node._active_statements_cluster) + \
                       ",MEMORY_LIMIT_CLUSTER=" + str(node._memory_limit_cluster) + "%" + \
                       ",CORE_LIMIT_CLUSTER=" + str(node._core_limit_cluster) + "%" + \
                       ",RESOURCE_UPPER_FACTOR=" + str(node._resource_upper_factor) + \
                       ",SEGMENT_RESOURCE_QUOTA='" + str(node._segment_resource_quota) + "'" + \
                       ",ALLOCATION_POLICY='" + str(node._allocation_policy) + "');\n"
         if isbranch == False:
             quenesql = quenesql + "CREATE ROLE " + rolename + " WITH LOGIN RESOURCE QUEUE " + node._name + ";\n"
             self.parameters.rolelist.append(rolename)
         #print quenesql
         self.parameters.rqfile.write(quenesql)
         
    
         
     @staticmethod
     def addToNode(curNode, childrenNum, param_name, param_value,parameters, isbranch):
         parentList = []
         if curNode._name == 'pg_root':
             percentSumMem = 100 - parameters.default_memory_limit_cluster
             percentSumCore = 100 - parameters.default_core_limit_cluster
             
         else:
             percentSumMem = 100
             percentSumCore = 100
             
         percentSumMem_orignal = percentSumMem
         percentSumCore_core = percentSumCore
   
         for i in range(1, childrenNum + 1):
             if i != childrenNum:
                 if parameters.child_ratio == 'even':
                     percentMem = int(percentSumMem_orignal /childrenNum)
                 elif parameters.child_ratio == 'customize':
                     percentMem = int(percentSumMem_orignal * int(parameters.child_ratio_value[i-1])/parameters.chile_ratio_total)
                 else:
                     percentMem = random.randint(1, percentSumMem - childrenNum + i)
                 percentSumMem -= percentMem
                 percentCore = percentMem
                 percentSumCore -= percentCore
             else:
                 percentMem = percentSumMem
                 percentCore = percentSumCore
             sonName = 'queue' + str(parameters.curqueue)
             rolename = 'role' + str(parameters.curqueue)
             son = Node(sonName,curNode,percentMem,percentCore,param_name,param_value, parameters)
             curNode.add(son, isbranch)
             
             parameters.quenelist.append(sonName)
             parentList.append(son)
             parameters.curqueue += 1
         return parentList

class RQ:
    def __init__(self, yamlfile = './RQ.yml', report_directory = './', param_name = '', param_value = 0):
        self.param_name = param_name
        self.param_value = param_value
        self.count = 0
        self.parameters = ParameterParser(yamlfile, report_directory)
        self.dropuserlist = []
        
    def generateRq(self):
        parentlist = []
        pgroot = Node('pg_root','',100,100,self.param_name,self.param_value, self.parameters)
        parentlist.append(pgroot)
        current_height = 2
        
        while current_height <= self.parameters.height:
            print "----current height is %d" %current_height
            length = len(parentlist)
            for i in range(1,length+1):
                curNode = parentlist.pop(0)
                if self.parameters.fix_width == True:
                    childnum = self.parameters.max_width
                else:
                    childnum = random.randint(1,self.parameters.max_width)
                if current_height == self.parameters.height:
                    isbranch = False
                else:
                    isbranch = True
                if current_height == 2:
                    childnum = childnum - 1
                    pgdefault = Node('pg_default',pgroot,self.parameters.default_memory_limit_cluster,
                         self.parameters.default_core_limit_cluster,self.param_name,self.param_value,self.parameters)
                    pgdefault.generaterq(pgdefault, False)
                parentlist += Node.addToNode(curNode,childnum,self.param_name,self.param_value, self.parameters, isbranch)
            print "    this level node is %d" %len(parentlist) 
            current_height += 1
        self.parameters.closefile()

    def __execute_sql_cmd(self,sqlstr):
       if len(sqlstr) == 0:
           return ''
       result = commands.getoutput('psql -d postgres -q -t -c "%s"'%sqlstr)
       if str(result).find('ERROR') != -1 or str(result).find('FATAL') != -1 or str(result).find('PANIC') != -1:
           print "Drop Resource Quene/User fail!"
           print result
           sys.exit(2)
       return result
     
    def dropRole(self):
        print "Note: We will remove all user and resource quene!!!!!!!!!"
        dropuser = "select  usename  from pg_user where usesuper is false;"
        dulist = commands.getoutput('psql -d postgres -q -t -c "%s"' %dropuser).split("\n")
        for user in dulist:
            user = user.lstrip(' ').rstrip(' ')
            if len(user) > 0 :
                self.dropuserlist.append(user)
        dbstr = "select datname from pg_database where datname not in ('template0','template1', 'postgres');"
        dblist = commands.getoutput('psql -d postgres -q -t -c "%s"' %dbstr).split("\n")
        ndblist = []
        for db in dblist:
            db = db.lstrip(' ').rstrip(' ')
            if len(db) > 0 :
                ndblist.append(db)
        for user in self.dropuserlist:
            for db in ndblist:
                print "execute on db %s drop user %s" %(db, user)
                result = commands.getoutput('psql -d %s -q -t -c "DROP OWNED BY %s"' %(db, user))
        dropstr = "select 'drop role ' || usename || ';' from pg_user where usesuper is false"     

        dropsql=self.__execute_sql_cmd(dropstr)
        if dropsql != '':
            self.__execute_sql_cmd(dropsql)
        i = 0
        while True:
            dropstr = "select 'drop resource queue ' || rsqname || ';' from pg_resqueue where rsqname not in ('pg_root', 'pg_default') and rsq_status != 'branch'"
            dropsql = commands.getoutput('psql -d postgres -q -t -c "%s"' %dropstr)
            print "drop quene  %s" %dropsql
            if len(dropsql) == 0 or i > 10:
                break
            i = i + 1
            self.__execute_sql_cmd(dropsql)

    def runRq(self):      
        self.dropRole()
        result = commands.getoutput("psql -d postgres -f %s/RQ.sql" %self.parameters.report_directory)
        if str(result).find('ERROR') == -1 and str(result).find('FATAL') == -1 and str(result).find('PANIC') == -1:
            print "Create Resource Queue success!"
        else:
            print "Create Resource Queue fail!"
            print result
            sys.exit(2)
        #change mode for users
        userlist=[]
        for line in open("%s/userlist" %self.parameters.report_directory):
            if line.find(',') != -1:
                userlist = line.split(',')
            else:
                userlist.append(line)
        
        ''' 
        # gpadmin   3268     1  0 13:08 ?        00:00:07 /usr/local/hawq-2.0.0.0-12654/bin/postgres -D /data/masterdd -p 5432 --silent-mode=true -M master -i
        output = commands.getoutput("ps -ef | grep bin/postgres | grep master | grep -v 'sh -c'")
        first_index = output.find('-D') + 3
        last_index = output[first_index:-1].find(' -') + first_index
        path = output[first_index:last_index].strip() + os.sep + 'pg_hba.conf'
        print 'master dd:', path
        os.system("sed -i '/role/d' %s" %path)
        for user in userlist:
            print "bb"
            f = open(path,"a+")
            for line in f.readlines():
                print "aaa"
                print line
                if re.search(".*all.*gpadmin.*",line):
                    line = line.replace("gpadmin",user.strip())
                    line = line.replace("ident","trust")
                    f.write(line)
            f.close()
        print "hawq restart now..."
        out = commands.getoutput("hawq cluster stop; hawq cluster start")
        if out.find("fail") == -1:
            print "hawq restart success!"
        else:
            print out
            sys.exit(2)
        '''
        print userlist
        return userlist

if __name__ == '__main__':
    rq = RQ('RQ.yml')
    #rq = RQ('SRI_Multiple.yml')
    rq.generateRq()
    #rq.dropRole()
    #rq.runRq()
