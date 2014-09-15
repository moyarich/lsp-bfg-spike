#!/usr/bin/expect
spawn ssh gpadmin@gpdb63.qa.dh.greenplum.com "source psql.sh && psql -d hawq_cov -f report.sql"
expect "password:"
send "changeme\r"
expect eof
exit