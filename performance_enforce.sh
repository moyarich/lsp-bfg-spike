#!/bin/sh

source ~/.bashrc
source ~/qa.sh

hawqconfig -c -n hawq_resourcemanager_query_vsegment_number_per_segment_limit -v 4
hawqconfig -c -n hawq_resourceenforcer_cpu_enable -v true
hawq cluster stop
hawq cluster start
python -u lsp.py -s performance_tpch_10g -m 5 -a -c  > ./performance_tpch_10g.log 2>&1
