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
sleep 10

python -u lsp.py -s tpcds_sanity -m 10 -a -c  > ./tpcds 2>&1
sleep 10
python -u lsp.py -s performance_tpch_10g -m 5 -a -c  > ./performance_tpch_10g.log 2>&1
sleep 10
python -u lsp.py -s performance_tpch_200g_sanity -m 30 -a -c  > ./performance_tpch_200g.log 2>&1
sleep 10
python -u lsp.py -s performance_tpch_concurrent_sanity -m 60 -a -c > ./performance_tpch_concurrent.log 2>&1
sleep 10
#4. 2. 1 ratio concurrent to run tpch 10G 
python -u lsp.py -s resourcequene_tpch_ratio_10g -m 30 -a -c -r 5  > ./resourcequene_tpch_ratio_10g 2>&1


