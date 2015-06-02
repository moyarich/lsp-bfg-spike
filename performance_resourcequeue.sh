#!/bin/sh
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
###Prepare datavvi resourcequene_sri_multiple
python -u lsp.py -s resourcequene_load >./resourcequene_sri_multiple 2>&1

#reuse data and  run workload
python -u lsp.py -s resourcequene_tpch_multiplelevel -a -c   > ./resourcequene_tpch_multiplelevel 2>&1

#4. 2. 1 ratio concurrent to run tpch 10G 
python -u lsp.py -s resourcequene_tpch_ratio_10g -a -c  > ./resourcequene_tpch_ratio_10g 2>&1


#4. 2. 1 ratio concurrent to run tpch 200G 
python -u lsp.py -s resourcequene_tpch_ratio_200g -a -c > ./resourcequene_tpch_ratio_200g 2>&1

#100 stream run single row insert
python -u lsp.py -s resourcequene_sri_multiple   > ./resourcequene_sri_multiple 2>&1
