#!/bin/sh

source ~/.bashrc
source ~/qa.sh

hawqconfig -c default_segment_num -v 128
hawqconfig -c hawq_resourcemanager_query_vsegment_number_per_segment_limit -v 8
hawqconfig -c hawq_resourceenforcer_cpu_enable -v false

hawq stop cluster -a
hawq start cluster -a
psql -d postgres -c "select * from gp_segment_configuration;" >config
hawqconfig -s default_segment_num >>config
hawqconfig -s hawq_resourcemanager_query_vsegment_number_per_segment_limit >>config
hawqconfig -s hawq_resourceenforcer_cpu_enable >>config
sudo /usr/phd/current/hadoop-client/bin/hdfs dfsadmin -report -live  >>config

cd /data/gpadmin/pulse2-agent/agents/agent1/work/HAWQ-2.X-SystemTest/rhel5_x86_64/lsp/workloads/TPCH
rm -rf queries
cp -r queries_1 queries
cd /data/gpadmin/pulse2-agent/agents/agent1/work/HAWQ-2.X-SystemTest/rhel5_x86_64/lsp
mv report report_bak
psql -d postgres -c "drop table if exists test; create table test(a int); insert into test values (1);"
python -u lsp.py -s concurrent_inv -a  > ./concurrent_inv.log 2>&1
mv report report_1

cd /data/gpadmin/pulse2-agent/agents/agent1/work/HAWQ-2.X-SystemTest/rhel5_x86_64/lsp/workloads/TPCH
rm -rf queries
cp -r queries_9 queries
cd /data/gpadmin/pulse2-agent/agents/agent1/work/HAWQ-2.X-SystemTest/rhel5_x86_64/lsp
psql -d postgres -c "select * from gp_segment_configuration;" >>config
#sudo  /usr/phd/current/hadoop-client/bin/hdfs dfsadmin -report -live  >>config
psql -d postgres -c "drop table if exists test; create table test(a int); insert into test values (1);"
python -u lsp.py -s concurrent_inv_q9 -a  > ./concurrent_inv_09.log 2>&1
mv report report_9
