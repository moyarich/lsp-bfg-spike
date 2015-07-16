#!/bin/sh

source ~/.bashrc
source ~/qa.sh

#hawqconfig -c default_segment_num -v 120
hawqconfig -c hawq_resourcemanager_query_vsegment_number_per_segment_limit -v 8
hawqconfig -c hawq_resourceenforcer_cpu_enable -v false

hawq stop cluster -a
hawq start cluster -a
psql -d postgres -c "select * from gp_segment_configuration;" >config
hawqconfig -s default_segment_num >>config
hawqconfig -s hawq_resourcemanager_query_vsegment_number_per_segment_limit >>config
hawqconfig -s hawq_resourceenforcer_cpu_enable >>config

##### TPCH
python -u lsp.py -s performance_tpch_10g  -a   > ./performance_tpch_10g.log 2>&1
sleep 10
python -u lsp.py -s performance_tpch_200g  -a  > ./performance_tpch_200g.log 2>&1
sleep 10
python -u lsp.py -s performance_xmarq_200g  -a > ./performance_xmarq_200g.log 2>&1
sleep 10
python -u lsp.py -s performance_tpch_stream  -a  > ./performance_tpch_stream.log 2>&1
sleep 10
python -u lsp.py -s performance_tpch_concurrent  -a   > ./performance_tpch_concurrent.log 2>&1
sleep 10


## Resource quene
python -u lsp.py -s resourcequene_tpch_ratio_10g  -a  -r 7 > ./resourcequene_tpch_ratio_10g 2>&1


### TPCDS
hawqconfig -c hawq_resourcemanager_query_vsegment_number_per_segment_limit -v 9
hawq stop cluster -a
hawq start cluster -a
python -u lsp.py -s tpcds  -a   > ./tpcds 2>&1
sleep 10
