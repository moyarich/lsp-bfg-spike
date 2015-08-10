#!/bin/sh

source ~/.bashrc
source ~/qa.sh

psql -d postgres -c "select * from gp_segment_configuration;" >config
hawqconfig -s default_segment_num >>config
hawqconfig -s hawq_resourcemanager_query_vsegment_number_per_segment_limit >>config
hawqconfig -s hawq_resourceenforcer_cpu_enable >>config
psql -d postgres -c "drop table if exists test; create table test(a int); insert into test values (1);"


python -u lsp.py -s performance_tpch_10g  -a -c  > ./performance_tpch_10g.log 2>&1
sleep 5
python -u lsp.py -s performance_tpch_200g  -a -c > ./performance_tpch_200g.log 2>&1
sleep 5

## Resource quene
python -u lsp.py -s resourcequene_tpch_ratio_10g -a -c  > ./resourcequene_tpch_ratio_10g 2>&1


