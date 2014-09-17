import sys

try:
    from lib.RemoteCommand import remotecmd
except ImportError:
    sys.stderr.write('Check needs remotecmd in lib/RemoteCommand.py \n')
    sys.exit(2)

def file(filename, msg):
    fp = open(filename, 'w')   
    fp.write(msg)
    fp.flush()
    fp.close()

def remote_psql_file(sql, user, host, password):
    file(filename = '/tmp/temp.sql', msg = sql)
    remotecmd.scp_command(from_user = '', from_host = '', from_file = '/tmp/temp.sql',
        to_user = 'gpadmin@', to_host = 'gpdb63.qa.dh.greenplum.com', to_file = ':/tmp/', password = 'changeme')
    cmd = 'source psql.sh && psql -d hawq_cov -t -q -f /tmp/temp.sql'
    result = remotecmd.ssh_command(user = user, host = host, password = password, command = cmd)
    return result

class Check:
    def __init__(self):
        self.user = 'gpadmin'
        self.host = 'gpdb63.qa.dh.greenplum.com'
        self.password = 'changeme'

    def check_id(self, result_id, table_name, search_condition):
        sql = 'select %s from %s where %s;' % (result_id, table_name, search_condition)
        result = remote_psql_file(sql = sql, user = self.user, host = self.host, password = self.password)
        result = str(result).strip()
        if result.isdigit():
            return int(result)
        else:
            return None

    def get_max_id(self, result_id, table_name):
        sql = 'select max(%s) from %s ;' % (result_id, table_name)
        result = remote_psql_file(sql = sql, user = self.user, host = self.host, password = self.password)
        result = str(result).strip()
        if result.isdigit():
            return int(result)
        else:
            return 0

    def insert_new_record(self, table_name, col_list = None, values = ''):
        if col_list is None:
            sql = "Insert into %s values (%s);" % (table_name, values)
        else:
            sql = "Insert into %s (%s) values (%s);" % (table_name, col_list, values)
        result = remote_psql_file(sql = sql, user = self.user, host = self.host, password = self.password)

    def update_record(self, table_name, set_content = '', search_condition = ''):
        sql = "update %s set %s where %s;" % (table_name, set_content, search_condition)
        result = remote_psql_file(sql = sql, user = self.user, host = self.host, password = self.password)
        
    def get_result(self, col_list = '',table_list = '', search_condition = ''):
        sql = "select %s from %s %s;" % (col_list, table_list, search_condition)
        result = remote_psql_file(sql = sql, user = self.user, host = self.host, password = self.password)
        return result

check = Check()