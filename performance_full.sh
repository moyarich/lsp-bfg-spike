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
psql -d postgres -c "drop table if exists test; create table test(a int); insert into test values (1);"

python -u lsp.py -s performance_tpch_10g -m 5 -a -c  > ./performance_tpch_10g.log 2>&1
sleep 10
python -u lsp.py -s performance_tpch_200g_full -m 30 -a -c > ./performance_tpch_200g.log 2>&1
sleep 10
#python -u lsp.py -s performance_xmarq_200g -m 10 -a > ./performance_xmarq_200g.log 2>&1
#sleep 10
python -u lsp.py -s performance_tpch_stream -m 60 -a -c > ./performance_tpch_stream.log 2>&1
sleep 10
python -u lsp.py -s performance_tpch_concurrent -m 60 -a -c -r 4 > ./performance_tpch_concurrent.log 2>&1

