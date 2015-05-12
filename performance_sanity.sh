#!/bin/sh

source ~/.bashrc
source ~/qa.sh



hawqconfig -c default_segment_num -v 64
hawqconfig -c hawq_resourcemanager_query_vsegment_number_per_segment_limit -v 4
hawq stop cluster -a
hawq start cluster -a
psql -d postgres -c "drop table if exists test; create table test(a int); insert into test values (1);"
python -u lsp.py -s performance_tpch_10g -m 5 -a -c  > ./performance_tpch_10g.log 2>&1
sleep 10
python -u lsp.py -s performance_tpch_200g -m 30 -a -c -r 2 > ./performance_tpch_200g.log 2>&1
mkdir report/summary_bak
mv report/summary_report*  report/summary_bak/

hawqconfig -c default_segment_num -v 128
hawqconfig -c hawq_resourcemanager_query_vsegment_number_per_segment_limit -v 8
#hawqconfig -c hawq_resourceenforcer_cpu_enable -v true
hawq stop cluster -a
hawq start cluster -a
psql -d postgres -c "drop table if exists test; creatdde table test(a int); insert into test values (1);"
python -u lsp.py -s performance_tpch_10g -m 5 -a -c  > ./performance_tpch_10g.log 2>&1
sleep 10
python -u lsp.py -s performance_tpch_200g -m 30 -a -c  > ./performance_tpch_200g.log 2>&1
sleep 10
python -u lsp.py -s performance_tpch_stream_sanity -c -r 3 > ./performance_tpch_stream.log 2>&1
sleep 10
python -u lsp.py -s resourcequene_tpch_ratio_10g  -c > ./resourcequene_tpch_ratio_10g 2>&1

