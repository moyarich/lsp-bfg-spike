from __future__ import division
import os
import sys
import random
import yaml
import fileinput
import re
import commands

class RQ:
    def __init__(self, path = './RQ.yml', report_directory = './'):
        self.path = path
	self.report_directory = report_directory
	self.count = 0
	with open(self.path, "r") as fyaml:
            import yaml
            self.yaml_parser = yaml.load(fyaml)
        self.changeList = {}
	self.changeNum = 1
        for element in self.yaml_parser['default']:
            if str(self.yaml_parser['default'][element]).find(',') != -1:
                self.changeNum = len(str(self.yaml_parser['default'][element]).split(','))
                self.changeList[element] = str(self.yaml_parser['default'][element]).split(',')
        for item in self.yaml_parser['leaf']:
            if str(self.yaml_parser['leaf'][item]).find(',') != -1 :
                self.changeNum = len(str(self.yaml_parser['leaf'][item]).split(','))
                self.changeList[item] = str(self.yaml_parser['leaf'][item]).split(',')
        print 'rq init success', self.changeList	

    def generateRq(self):
	m = self.yaml_parser['height']
	n = self.yaml_parser['width']
	curnum = self.yaml_parser['nodeNum']-2
	curm = 1
	#curnum = 1022
	parentlist = []
	leaflist = []
	branchlist = []
	while curm<=m:
		if curm==1:
			pgroot = node.createNode(self.path,"pg_root",None,100,100)
		elif curm==2:
			pgdefault = node.createNode(self.path,"pg_default",pgroot,0,0)
			pgroot.add(pgdefault)
			curn = random.randint(1,n-1)
			if m==2:
                                curn = curnum
                                parentlist = node.addToNode(self.path,pgroot,curn)
                                break
                        elif m!=2 and (curnum-curn)<=0:
                                curn = curnum
                                parentlist = node.addToNode(self.path,pgroot,curn)
                                break
                        else:
                                curnum -= curn
                                parentlist = node.addToNode(self.path,pgroot,curn)
		else:
			length = len(parentlist)
			breaktag = 0
			for i in range(1,length+1):
				curNode = parentlist.pop(0)
				curn = random.randint(1,n)
				if (curnum-curn)<=0:
					curn = curnum
					parentlist += node.addToNode(self.path,curNode,curn)
					breaktag = 1
					break
				else:
					curnum -= curn
					parentlist += node.addToNode(self.path,curNode,curn)
			if breaktag==1:
				break
		curm += 1
	self.typeOfNode(pgroot,leaflist,branchlist)
	self.dump_branchlist(branchlist)
	self.dump_leaflist(leaflist)
	print 'generate rq success', m ,n

    def runRq(self):
	#execute the RQsql
	parameter = self.yaml_parser['parameter']
	if parameter != '':
		paraValue = self.yaml_parser['leaf'][parameter]
	print 'in rq self.count = ', self.count
	if self.count < self.changeNum:
                self.count += 1
		if self.count > 1:
                	self.changeRqSql(self.changeList, self.count)
		result = commands.getoutput("psql -d postgres -U gpadmin -f %s/RQ.sql"%self.report_directory)
		if str(result).find('ERROR') == -1 and str(result).find('FATAL') == -1:
        		print "Create Resource Queue success!"
		else:
        		print "Create Resource Queue fail!"
			print result
			print "#######################\n"

		#change mode for users
		for line in open("%s/userlist"%self.report_directory):
        		userlist = line.split(',')

		path = "/data/masterdd/pg_hba.conf"
		os.system("sed -i '/role/d' %s"%path)
		for user in userlist:
        		f = open(path,"a+")
        		for line in f.readlines():
        	       		if re.search(".*all.*gpadmin.*",line):
                        		line = line.replace("gpadmin",user.strip())
                        		line = line.replace("ident","trust")
                        		f.write(line)
        		f.close()

		print "hawq restart now..."
		out = commands.getoutput("hawq cluster stop")
		out = commands.getoutput("hawq cluster start")
		print "hawq restart success!"

		#add users to the sqlfile

		#path =  "%s/sqlfile"%self.report_directory
		#filelist = os.listdir(path)
		#for file in filelist:
        	#	filepath = path + os.sep + file
        	#	i = random.randint(0,len(userlist)-1)
        	#	os.system("sed -i '1i\\\\\c - %s' %s" % (userlist[i], filepath))
		self.dropRole()
		self.list = []
		for user in userlist:
			userdict = {user:paraValue}
			self.list.append(userdict)
                return self.list
        else:
		rqsql = "%s/RQ.sql"%self.report_directory
		os.system("sed -i '/WITH/ d' %s"%rqsql)
		dropresult = commands.getoutput("psql -d postgres -U gpadmin -f %s/RQ.sql"%self.report_directory)
		if str(dropresult).find('ERROR') != -1 and str(dropresult).find('FATAL') != -1:
                        print "Drop Resource Queue success!"
                else:
                        print "Drop Resource Queue fail!"
			print dropresult
			print "#####################\n"
                return []
		
		



    def dropRole(self):
	path = "%s/RQ.sql"%self.report_directory
	for line in fileinput.input(path, inplace = 1):
		line = re.sub(r'^DROP.*()', r'\1',line.strip())
               	print line
	
	for rqs in open("%s/rqlist"%self.report_directory):
                rqlist = rqs.split(',')
        for rq in rqlist:
		if rq != '':
                	droprq = "DROP RESOURCE QUEUE %s;\n"%rq
                	os.system("sed -i '1i %s' %s" % (droprq, path))

	for users in open("%s/userlist"%self.report_directory):
		userlist = users.split(',')

	for user in userlist:
                dropuser = "DROP ROLE %s;\n"%user
                os.system("sed -i '1i %s' %s" % (dropuser, path))

    def changeRqSql(self,changeList, count):
	for key in changeList:
		value = changeList[key]
		print value
		print key
		print value[count-1]
	for lineitem in fileinput.input("%s/RQ.sql"%self.report_directory, inplace=1):
		lineitem = re.sub(r'(%s)=.,'%key, r'\1=%s,'%value[count-1].strip(), lineitem.strip())
		print lineitem
	
    def typeOfNode(self,node,leaflist,branchlist):
	if len(node._children)==0:
		leaflist.append(node)
	else:
		branchlist.append(node)
		for i in node._children:
			self.typeOfNode(i,leaflist,branchlist)
	#put the resource queue in the rqlist file
	f = open("%s/rqlist"%self.report_directory,"w")
	rq = ""
	for i in range(1,len(branchlist)):
		if i==1:
			rq += str(branchlist[i]._name)
		else:
			rq += ",%s"%branchlist[i]._name
	for j in range(1,len(leaflist)):
		rq += ",%s"%leaflist[j]._name
	f.write(rq)

    def dump_branchlist(self,list):
    	if os.path.exists("%s/RQ.sql"%self.report_directory):
        	os.system("rm %s/RQ.sql"%self.report_directory)
	fl = open("%s/RQ.sql"%self.report_directory,"w")
	print "branchlist" + str(len(list))
	sql = ""
	for i in range(1,len(list)):
		sqltmp = "CREATE RESOURCE QUEUE " + list[i]._name + " WITH(\
PARENT= " + "'" + list[i]._parent + "'" + \
",MEMORY_LIMIT_CLUSTER=" + str(list[i]._memory_limit_cluster) + "%" +  \
",CORE_LIMIT_CLUSTER=" + str(list[i]._core_limit_cluster) + "%" +  \
",RESOURCE_UPPER_FACTOR=" + str(list[i]._resource_upper_factor) + \
",ALLOCATION_POLICY='" + str(list[i]._allocation_policy) + "');\n"
		sql = sql + sqltmp
	fl.write(sql)
	fl.close()

    def dump_leaflist(self,list):
	userlist = ""
	filename = "%s/RQ.sql"%self.report_directory 
	print "leaflist" + str(len(list)-1)

	fll = open("%s/RQ.sql"%self.report_directory,"a")
	sql1 = ""
	for i in range(1,len(list)):
		sql = "CREATE RESOURCE QUEUE "+ list[i]._name + " WITH(\
PARENT= " + "'" + list[i]._parent + "'" +\
",ACTIVE_STATEMENTS=" + str(list[i]._active_statements_cluster) + \
",MEMORY_LIMIT_CLUSTER=" + str(list[i]._memory_limit_cluster) + "%" + \
",CORE_LIMIT_CLUSTER=" + str(list[i]._core_limit_cluster) + "%" + \
",RESOURCE_UPPER_FACTOR=" + str(list[i]._resource_upper_factor) + \
",SEGMENT_RESOURCE_QUOTA='" + str(list[i]._segment_resource_quota) + "'" + \
",ALLOCATION_POLICY='" + str(list[i]._allocation_policy) + "');\n" + \
"CREATE ROLE role" + str(i) + " WITH LOGIN RESOURCE QUEUE " + str(list[i]._name) + ";\n"
		sql1 += sql 
		if i==1:
			userlist += "role" + str(i)
		else:
			username = ", role" + str(i)
			userlist += username

	file = open("%s/userlist"%self.report_directory,"w")
	file.write(userlist)
	file.close()
	#for line in fileinput.input("amy.yml",inplace=1):
	#	line = re.sub(r'^user_list.*',userlist,line.strip())
	#	print line
	#fileinput.close()
	fll.write(sql1)
	fll.close()
	
	default= "ALTER RESOURCE QUEUE pg_default WITH(MEMORY_LIMIT_CLUSTER = " + str(list[0]._memory_limit_cluster) + "%" + ", CORE_LIMIT_CLUSTER = " + str(list[0]._core_limit_cluster) + "%);"
	os.system("sed -i '1i %s' %s"%(default, filename))

curqueue = 1
class node:
    def __init__(self, path=''):
        self._children = []
 	self._name = ''
	self._parent = ''
	self._active_statements_cluster = ''
	self._memory_limit_cluster = ''
	self._core_limit_cluster = ''
	self._resource_upper_factor = ''
	self._segment_resource_quota = ''
	self._allocation_policy = ''

	self.path = path

	with open(self.path,"r") as yamlfile:
                self.yaml_parser = yaml.load(yamlfile)

    def add(self, node):
    	self._children.append(node)
    
    def getchildren(self):
        return self._children
    

    @staticmethod
    def createNode(path,name,parent,percentMem,percentCore):
	new = node(path)
	if parent == None:
		new._parent = ""
	else:
		new._parent = parent._name
        new._name = name
	new._resource_upper_factor = new.listJudge(new._resource_upper_factor,new.yaml_parser['leaf']['RESOURCE_UPPER_FACTOR'])
	new._segment_resource_quota = new.listJudge(new._segment_resource_quota,new.yaml_parser['leaf']['SEGMENT_RESOURCE_QUOTA'])
        new._allocation_policy = new.listJudge(new._allocation_policy,new.yaml_parser['leaf']['ALLOCATION_POLICY'])
	new._active_statements_cluster = new.listJudge(new._active_statements_cluster,new.yaml_parser['leaf']['ACTIVE_STATEMENTS'])
	if name == 'pg_default':
		new._memory_limit_cluster = new.listJudge(new._memory_limit_cluster,new.yaml_parser['default']['MEMORY_LIMIT_CLUSTER'])
		new._core_limit_cluster = new.listJudge(new._core_limit_cluster,new.yaml_parser['default']['CORE_LIMIT_CLUSTER'])
	elif new._parent == 'pg_root' and name != 'pg_default':
		#initialize the node according to parent
		if str(new.yaml_parser['default']['MEMORY_LIMIT_CLUSTER']).find(',') != -1:
			memoryList = new.yaml_parser['default']['MEMORY_LIMIT_CLUSTER'].split(',')
			new._memory_limit_cluster = int((100 - int(memoryList[0])) * (percentMem / 100.0)) 
		else:
			new._memory_limit_cluster = int((100 - int(new.yaml_parser['default']['MEMORY_LIMIT_CLUSTER'])) * (percentMem / 100.0))
		if str(new.yaml_parser['default']['CORE_LIMIT_CLUSTER']).find(',') != -1:
			coreList = new.yaml_parser['default']['CORE_LIMIT_CLUSTER'].split(',')
			new._core_limit_cluster = int((100 - int(coreList[0])) * (percentCore / 100.0))
		else:
			new._core_limit_cluster = int((100 - int(new.yaml_parser['default']['CORE_LIMIT_CLUSTER'])) * (percentCore / 100.0))
	else:
		new._memory_limit_cluster = percentMem
		new._core_limit_cluster = percentCore
		
	return new
		
    
    @staticmethod
    def addToNode(path,curNode,childrenNum):
	global curqueue
	parentList = []
	percentSumMem = 100
	percentSumCore = 100
	for i in range(1,childrenNum+1):
		if i != childrenNum:
			percentMem = random.randint(1, percentSumMem)
			percentSumMem -= percentMem
			percentCore = percentMem
			percentSumCore -= percentCore
			#percentCore = random.randint(0, percentSumCore)
			#percentSumCore -= percentCore
		else:
			percentMem = percentSumMem
			percentCore = percentSumCore
		sonName = 'queue' + str(curqueue)
		curqueue += 1
		son = node.createNode(path,sonName,curNode,percentMem,percentCore)
		curNode.add(son)
		parentList.append(son)
	return parentList

    def listJudge(self, value, yaml):
	if str(yaml).find(',') != -1:
		yamllist = yaml.split(',')
		value = yamllist[0]
        else:
		value = yaml
	return value



if __name__ == '__main__':

	rq = RQ()
	rq.generateRq()
	print rq.runRq()
	print rq.runRq()
	print rq.runRq()
