#!/bin/sh

source ~/.bashrc
source ~/qa.sh

hawqconfig -c -n hawq_resourcemanager_query_vsegment_number_per_segment_limit -v 4
hawqconfig -c -n coredump_on_memerror -v true
hawqconfig -c -n gp_vmem_protect_limit -v 32768
hawq cluster stop
hawq cluster start
python -u lsp.py -s performance_tpch_10g  > ./performance_tpch_10g.log 2>&1
sleep 10
python -u lsp.py -s performance_tpch_200g_inv > ./performance_tpch_200g.log 2>&1

