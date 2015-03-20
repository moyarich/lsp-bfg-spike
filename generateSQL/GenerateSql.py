import os, sys
import re
import yaml
import random
import pdb
import Queue

isolationlevellist = ["SERIALIZABLE", "READ COMMITTED","READ UNCOMMITTED"]
commitstring = ["COMMIT", "ROLLBACK"]

BlockMaps = {
            "SELECT": ("ALTER TABLE","TRUNCATE","DROP TABLE","VACUUM FULL") ,
            "COPY": ("ALTER TABLE","DROP TABLE","TRUNCATE","VACUUM FULL"),
            "INSERT":("ALTER TABLE","DROP TABLE","TRUNCATE","VACUUM FULL"),
            "VACUUM" :("VACUUM","ANALYZE","ALTER TABLE","DROP TABLE","TRUNCATE","VACUUM FULL"),
            "ANALYZE" :("VACUUM","ANALYZE","ALTER TABLE","DROP TABLE","TRUNCATE","VACUUM FULL"),
            "ALTER TABLE" :("SELECT","INSERT","COPY","VACUUM","ANALYZE","ALTER TABLE","DROP TABLE","TRUNCATE","VACUUM FULL"),
            "DROP TABLE" :("SELECT","INSERT","COPY","VACUUM","ANALYZE","ALTER TABLE","DROP TABLE","TRUNCATE","VACUUM FULL"),
            "TRUNCATE" :("SELECT","INSERT","COPY","VACUUM","ANALYZE","ALTER TABLE","DROP TABLE","TRUNCATE","VACUUM FULL"),
            "VACUUM FULL" :("SELECT","INSERT","COPY","VACUUM","ANALYZE","ALTER TABLE","DROP TABLE","TRUNCATE","VACUUM FULL")
            }

ShortFormMap = {
        "SELECT":"SEL",
        "COPY":"CP",
        "INSERT":"INS",
        "VACUUM":"VAC",
        "ANALYZE":"ANA",
        "ALTER TABLE":"ALT",
        "DROP TABLE":"DRP",
        "TRUNCATE":"TRU",
        "VACUUM FULL":"VACF"
        }

def Block_Matched(type1,type2):
    if type2.upper() in BlockMaps[type1.upper()]:
        return True
    return False

class GenerateSql:
        def __init__(self):
		with open (os.getcwd() + "/sql.yml","r")  as yamlfile:
                       	yaml_parser = yaml.load(yamlfile)
		self.typelist = yaml_parser['Type_list'].split(',')
               	self.querycontent = yaml_parser['Query_content']
		self.components = []
                self.commited = {}
                self.began = {}
                self.toCommit = Queue.Queue()
                self.toAdd = Queue.Queue()

	def addComponent(self, id ,type, sql):
		query = sql
		index = len(self.components)
		new = queryItem(id,index,type,query)
		self.components.append(new)
		self.commited[id] = 0
		self.began[id] = 0

        def addBegin(self,item):
		if self.began[item.sessionId] == 0:
            		if item.type != "VACUUM" and item.type != "VACUUM FULL":
                		if self.isolationlevel == "RANDOM":
                    			isolation = isolationlevellist[random.randint(0, len(isolationlevellist) -1)]
                		else:
                    			isolation = self.isolationlevel
                		line = str(item.sessionId) + ":BEGIN transaction isolation level %s;" %(isolation)
				print line
				self.write(line)
            		self.began[item.sessionId] = 1

        def addCommit(self, item):
		if self.commited[item.sessionId] == 0:
            		if item.type != "VACUUM" and item.type != "VACUUM FULL":
                		if self.commitstring == "RANDOM":
                    			commitstr = commitstring[random.randint(0, len(commitstring) -1)]
                		else:
                    			commitstr = self.commitstring

                		line = str(item.sessionId) + ":" + commitstr + ";"
				print line
				self.write(line)
            		for bd in item.blocked:
                		bd.blockedby -=1
                		if bd.blockedby is 0:
                    			line = str(bd.sessionId) + "<:"
                    			if bd.type == "VACUUM" or bd.type == "VACUUM FULL":
                        			self.addCommit(bd)
					print line
					print "block"
                    			self.write(line)
        	self.commited[item.sessionId] = 1

        def addQuery(self, item):
		need_Ampersand = False
	        blocklist = []
        	for comp in self.components[:item.index]:
            		if item.sessionId != comp.sessionId and Block_Matched(comp.type,item.type) and self.commited[comp.sessionId] == 0:
                		if comp.Ampersand is True or comp.type != "VACUUM" and comp.type != "VACUUM FULL"  :
                    			need_Ampersand = True
                    			comp.blocked.append(item)
                    			item.blockedby += 1
                    			blocklist.append(comp.type)
        	if len(blocklist) != 0 :
            		blockinfo = "----Item %s is blocked by (%s) --- " %(item.type, ','.join(blocklist))
            		print blockinfo
			self.write(blockinfo)

        	if need_Ampersand:
            		line =  str(item.sessionId) + "&:" + item.query
            		item.Ampersand = True
			print line
            		self.write(line)
        	else :
            		line = str(item.sessionId) + ":" + item.query
			print line
			self.write(line)


        def write(self, line, filename=''):
                if filename == '':
			filename = os.getcwd() + os.sep + self.filename
		file = open(filename,"a")
                file.write(line + '\n')
                file.close()


        def GenSqlFile(self):

		for item in self.components:
            		self.addBegin(item)
            	for item in self.components:
               		self.addQuery(item)
            	for item in self.components:
                	self.addCommit(item)

        def splitSql(self):
                maxsqlnum = int(self.sqlnum[0])
                for i in range(0, len(self.sqlnum)):
                        sqlnum = self.sqlnum[i]
                        if maxsqlnum < int(sqlnum):
                                maxsqlnum = int(sqlnum)

                self.filename = os.getcwd() + "/%s"%self.filename
                for i in range(1,maxsqlnum+1):
                        pattern = '^%s:.*'%i
                        filename = ShortFormMap[self.type.upper()] + str(i) + '.sql'
                        f = open(os.getcwd() + "/sqlfile/%s"%filename,"a")
                        print "$$$$$$$$$"
                        print self.filename
                        print "$$$$$$$$$"
                        for line in open(self.filename,"r"):
                                match = re.search(pattern, line)
                                if match != None:
                                        newline = re.sub(r'^%s:(.*)'%i,r'\1',line)
                                        f.write(newline)

	def execute(self):
		for i in range(0,len(self.querycontent)):
                        ###???
                        self.type = self.querycontent[i]['query_name']
                        self.replaceclass = self.querycontent[i]['handle_class']
                        self.sedstring = self.querycontent[i]['sed_string_list'].split(',')
                        self.sqltemplates = self.querycontent[i]['sql_templates']
			self.sqlfile = self.querycontent[i]['sql_file']
                        self.sedprefix = self.querycontent[i]['sed_string_dynamic']
                        self.sqlnum = self.querycontent[i]['sql_num'].split(",")
			if self.sqlnum[1] == '':
				num = self.sqlnum[0]
				self.sqlnum = []
				self.sqlnum.append(num)
                        self.isolationlevel = self.querycontent[i]['Isolationlevel']
                        self.commitstring = self.querycontent[i]['Commit_type']
                        self.tablename = []
                        shortform = ShortFormMap[self.type.upper()]
                        self.filename = shortform + '.sql'
                        #self.ansfilename = shortform + '.ans'
                        #self.write('', self.ansfilename)
			if self.replaceclass == "replaceString":
				replace = ReplaceString()
				if self.sqltemplates != None :
					self.sqltemplates = replace.replaceString(self.sedstring, self.sqltemplates)
					self.sqltemplates = self.sqltemplates.split('|')
				else:
					self.sqlfile = self.sqlfile.split(',')
					self.sqltemplates = str(self.sqltemplates)
                                        self.sqltemplates = ''
					if self.sqlfile[1] == '':
						length = len(self.sqlfile) - 1
					else:
						length = len(self.sqlfile)
					for i in range(0, length):
						file = self.sqlfile[i].strip()
						file = os.getcwd() + "/file/%s"%file
						f = open(file,"r")
						sql = ''
						for sql1 in f.readlines():
							sql1 = sql1.strip()
							sql1 += ' '
							sql += sql1
						if i != len(self.sqlfile)-1:
							sql = sql + '|'
						self.sqltemplates += sql
					self.sqltemplates = replace.replaceString(self.sedstring, self.sqltemplates)
					self.sqltemplates = self.sqltemplates.split('|') 
				if self.sedprefix['tablename']!=None or self.sedprefix['value']!=None:
					sqllist = {}
					taglist = []
                        		replace.sedStringPrefix(self.sedprefix, self.sqltemplates, self.sqlnum, self.sedstring, sqllist, taglist)
					for i in range(0,len(self.sqltemplates)):
                        			if i in taglist:
                                			for id in range(1, len(sqllist[i])+1):
                            		            		self.addComponent(id, self.type.upper(), sqllist[i][id-1])
                        			else:
                                			for num in range(1, int(self.sqlnum[i])+1):
                                        			self.addComponent(num, self.type.upper(), self.sqltemplates[i])
				else:
                        		for i in range(0, len(self.sqlnum)):
                                		sqlnum = self.sqlnum[i]
                                		sql = self.sqltemplates[i]
                                		for num in range(1, int(sqlnum)+1):
                                        		self.addComponent(num, self.type.upper(), sql)
			self.GenSqlFile()
                	self.splitSql()
			self.components = []

	def checkResult(self):
        	pass
		
class ReplaceString:
	def __init__(self):
		pass
	def replaceString(self, sedstring, sqltemplates):
		if sedstring[1] == '':
			length = len(sedstring) -1
		else:
			length = len(sedstring)
		for i in range(0,length):
        		items = sedstring[i].split(":")
                	sqltemplates = sqltemplates.replace(items[0].strip(),items[1].strip())
		return sqltemplates	

	def sedStringPrefix(self, sedprefix, sqltemplates, sqlnum, sedstring, sqllist, taglist):
		for (key, value) in sedprefix.items():
			if key == 'tablename' and value != None:
				tablename = value.split(',')
				for t in range(0,len(tablename)):
					for i in range(0, len(sqltemplates)):
						table = self.findRealValue(tablename[t], sedstring)
						if sqltemplates[i].find(table) != -1:
							if i in taglist:
								sqllist[i] = self.increamentTable(table, sqllist[i], sqlnum[i])
							else:
								sqllist[i] = self.increamentTable(table, sqltemplates[i], sqlnum[i])
							taglist.append(i)	
			elif key == 'value' and value != None:
				valuelist = value.split(',')
				for t in range(0,len(valuelist)):
					for i in range(0, len(sqltemplates)):
						v = self.findRealValue(valuelist[t].strip(), sedstring)
						if sqltemplates[i].find(v) != -1:
							if i in taglist:
								sqllist[i] = self.increamentValue(v, sqllist[i], sqlnum[i], 2)
							else:
								sqllist[i] = self.increamentValue(v, sqltemplates[i], sqlnum[i], 2)
							taglist.append(i)
					
	def findRealValue(self, value, sedstring):
		for index in range(0,len(sedstring)):
                	if sedstring[index].find(value) != -1:
                		value = sedstring[index][sedstring[index].find(value)+len(value)+1:]
				return value
	 
	def increamentTable(self, value, sql, iteration):
		firstvalue = value
		sqllist = []
		if type(sql) == str :
			sqllist.append(sql)
			for i in range(1,int(iteration)):
				value = firstvalue + str(i)
				sql = sql.replace(firstvalue,value)
				sqllist.append(sql)
			return sqllist		 
		else:
			for s in range(0,len(sql)):
				value = firstvalue + str(s)
				sql[s] = sql[s].replace(firstvalue,value)
				sqllist.append(sql[s])
			return sqllist

	def increamentValue(self, value, sql, iteration, increamental):
		sqllist = []
		value = int(value)
		if type(sql) == str:
			sqllist.append(sql)
			for i in range(1, int(iteration)):
				firstvalue = value	
				value += increamental
				sql = sql.replace(str(firstvalue),str(value))
				sqllist.append(sql)
			return sqllist
		else:
			firstvalue = value
			for s in range(0, len(sql)):
				value += increamental
				sql[s] = sql[s].replace(str(firstvalue),str(value))
				sqllist.append(sql[s])
			return sqllist

class queryItem:
	def __init__(self, id, index, type, query):
		self.sessionId = id
		self.index = index
		self.blockedby = 0
		self.type = type
		self.query = query
		self.blocked = []
		self.Ampersand = False
	
	def DecBlockedBy(self):
		self.blockby = -1
		if self.blockedby is 0:
			g_output.addCommit(self)

	def AddBlockedBy(self):
		self.blockby += 1


if __name__ == '__main__':
	sql = GenerateSql()
	sql.execute()
