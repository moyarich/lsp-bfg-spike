#!/bin/sh

source ~/.bashrc
source ~/qa.sh

# python -u lsp.py -s performance_full_regression_single_stream_tpch,performance_full_regression_concurrent_streams_tpch -a > ./performance_full_regression_tpch.log 2>&1

# python -u lsp.py -s performance_full_regression_single_stream_tpch -a > ./performance_full_regression_tpch.log 2>&1

# python -u lsp.py -s performance_sanity -a > ./performance_sanity.log 2>&1

gpconfig -c hash_dist_init_segment_num -v 64 --skipvalidation > gpconfig.log 2>&1
gpconfig -c util_segment_num -v 64 --skipvalidation >> gpconfig.log 2>&1
gpstop -ar >> gpconfig.log 2>&1
#gpconfig -c split_read_size_mb -v 512 --skipvalidation
#gpstop -u
python -u lsp.py -s performance1 -m 5 -a -c > ./performance1.log 2>&1
sleep 10
python -u lsp.py -s performance2 -m 30 -a -c -r 2 > ./performance2.log 2>&1
#sleep 10
#python -u lsp.py -s performance3 -a > ./performance3.log 2>&1
#sleep 10
#python -u lsp.py -s performance4 -a -c > ./performance4.log 2>&1
#sleep 10
#python -u lsp.py -s performance5 -a -c > ./performance5.log 2>&1
#python -u lsp.py -s performance1,performance2,performance3 -a > ./performance.log 2>&1
