import sys

try:
    from lib.PSQL import psql
except ImportError:
    sys.stderr.write('LSP needs psql in lib/utils/Check.py \n')
    sys.exit(2)

class Check:
    def __init__(self):
        pass

    def check_id(self, result_id, table_name, search_condition):
        cmd = 'select %s from %s where %s;' % (result_id, table_name, search_condition)
        (ok, result) = psql.runcmd(cmd = cmd, dbname = 'hawq_cov', username = 'hawq_cov',host = 'gpdb63.qa.dh.greenplum.com', port = 5430)
        if ok:
            cs_id = str(result[0]).strip()
            if cs_id.isdigit():
                return int(cs_id)
            else:
                return None
        else:
            sys.stderr.write('Failed to connect to db %s, the host is %s\n' % ('hawq_cov', 'gpdb63.qa.dh.greenplum.com'))
            sys.exit(2)

    def get_max_id(self, result_id, table_name):
        cmd = 'select max(%s) from %s ;' % (result_id, table_name)
        (ok, result) = psql.runcmd(cmd = cmd, dbname = 'hawq_cov', username = 'hawq_cov', host = 'gpdb63.qa.dh.greenplum.com', port = 5430)
        if ok:
            cs_id = str(result[0]).strip()
            if cs_id.isdigit():
                return int(cs_id)
            else:
                return 0
        else:
            sys.stderr.write("Failed to get max_id in the table '%s', the db is %s, the host is %s\n" % (table_name, 'hawq_cov', 'gpdb63.qa.dh.greenplum.com'))
            sys.exit(2)

    def insert_new_record(self, table_name, col_list = '', values = ''):
        cmd = "Insert into %s %s values (%s);" % (table_name, col_list, values)
        (ok, result) = psql.runcmd(cmd = cmd, dbname = 'hawq_cov', username = 'hawq_cov', host = 'gpdb63.qa.dh.greenplum.com', port = 5430)
        if not ok:
            print cmd
            print result
            sys.stderr.write("Failed to insert new record into table '%s', the db is %s, the host is %s\n" % (table_name, 'hawq_cov', 'gpdb63.qa.dh.greenplum.com'))
            sys.exit(2)
        return True

    def update_record(self, table_name, set_content = '', search_condition = ''):
        cmd = "update %s set %s where %s;" % (table_name, set_content, search_condition)
        (ok, result) = psql.runcmd(cmd = cmd, dbname = 'hawq_cov', username = 'hawq_cov',host = 'gpdb63.qa.dh.greenplum.com', port = 5430)
        if not ok:
            print cmd
            print result
            sys.stderr.write("Failed to update record in table '%s', the db is %s, the host is %s\n" % (table_name, 'hawq_cov', 'gpdb63.qa.dh.greenplum.com'))
            sys.exit(2)
        return True

    def get_result(self, col_list = '',table_list = '', search_condition = ''):
        cmd = "select %s from %s %s;" % (col_list, table_list, search_condition)
        (ok, result) = psql.runcmd(cmd = cmd, dbname = 'hawq_cov', username = 'hawq_cov',host = 'gpdb63.qa.dh.greenplum.com', port = 5430)
        if not ok:
            print cmd
            print result
            sys.stderr.write("Failed to update record in table '%s', the db is %s, the host is %s\n" % (table_name, 'hawq_cov', 'gpdb63.qa.dh.greenplum.com'))
            sys.exit(2)
        else:
            return result;

check = Check()