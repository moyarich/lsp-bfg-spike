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
        (ok, result) = psql.runcmd(cmd = cmd, dbname = 'hawq_cov', username = 'hawq_cov', password = None,
             host = 'gpdb63.qa.dh.greenplum.com', port = 5430, background = False)
        if ok:
            cs_id = str(result[0]).strip()
            if cs_id.isdigit():
                return int(cs_id)
            else:
                return None
        else:
            sys.stderr.write('Failed to connect to db %s, the host is %s\n' % ('hawq_cov', 'gpdb63.qa.dh.greenplum.com'))
            sys.exit(2)

    def max_id(self, result_id, table_name):
        cmd = 'select max(%s) from %s ;' % (result_id, table_name)
        (ok, result) = psql.runcmd(cmd = cmd, dbname = 'hawq_cov', username = 'hawq_cov', password = None,
             host = 'gpdb63.qa.dh.greenplum.com', port = 5430, background = False)
        if ok:
            cs_id = str(result[0]).strip()
            if cs_id.isdigit():
                return int(cs_id)
            else:
                return 0
        else:
            sys.stderr.write("Failed to select max_id in the table '%s', the db is %s, the host is %s\n" % (table_name, 'hawq_cov', 'gpdb63.qa.dh.greenplum.com'))
            sys.exit(2)

    def insert_new_record(self, table_name, values):
        cmd = "Insert into %s values(%s)" % (table_name, values)
        print cmd
        (ok, result) = psql.runcmd(cmd = cmd, dbname = 'hawq_cov', username = 'hawq_cov', password = None,
             host = 'gpdb63.qa.dh.greenplum.com', port = 5430, background = False)
        if not ok:
            sys.stderr.write("Failed to insert new record into table '%s', the db is %s, the host is %s\n" % (table_name, 'hawq_cov', 'gpdb63.qa.dh.greenplum.com'))
            sys.exit(2)
        return True

check = Check()