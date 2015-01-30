#!/bin/sh

source ~/.bashrc
source ~/qa.sh


gpconfig -c hash_dist_init_segment_num -v 64 --skipvalidation > gpconfig.log 2>&1
gpconfig -c util_segment_num -v 64 --skipvalidation >> gpconfig.log 2>&1
gpstop -ar >> gpconfig.log 2>&1
python -u lsp.py -s performance_tpch_10g.yml -m 5 -a -c > ./performance_tpch_10g.log 2>&1
sleep 10
python -u lsp.py -s performance_tpch_200g.yml -m 30 -a -c > ./performance_tpch_200g.log 2>&1
sleep 10
python -u lsp.py -s performance_xmarq_200g.yml -m 10 -a > ./performance_xmarq_200g.log 2>&1
sleep 10
python -u lsp.py -s performance_tpch_concurrent.yml -m 60 -a -c -r 4 > ./performance_tpch_concurrent 2>&1
