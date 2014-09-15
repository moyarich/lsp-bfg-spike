#!/usr/bin/expect

$host = $arg[1]
$file = $arg[2]

spawn scp /home/gpadmin/Dev/gpsql/private/liuq8/test/lsp/report/20140915-103207/report.sql gpadmin@gpdb63.qa.dh.greenplum.com:~
expect "password:"
send "changeme\r"
expect eof
exit
