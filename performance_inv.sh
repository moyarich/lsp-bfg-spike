#!/bin/sh

source ~/.bashrc
source ~/qa.sh

hawqconfig -c hawq_resourcemanager_query_vsegment_number_per_segment_limit -v 4
hawqconfig -c coredump_on_memerror -v true
hawq cluster stop
hawq cluster start
python -u lsp.py -s load > ./load.log 2>&1
sleep 10
python -u lsp.py -s tpcds  > ./tpcds.log 2>&1

