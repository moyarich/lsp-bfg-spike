#!/bin/sh

source ~/.bashrc
source ~/qa.sh

# python -u lsp.py -s performance_full_regression_single_stream_tpch,performance_full_regression_concurrent_streams_tpch -a > ./performance_full_regression_tpch.log 2>&1

# python -u lsp.py -s performance_full_regression_single_stream_tpch -a > ./performance_full_regression_tpch.log 2>&1

# python -u lsp.py -s performance_sanity -a > ./performance_sanity.log 2>&1
ps -ef | grep postgres > ps.log 2>&1
echo $GPHOME >> ps.log 2>&1
gpconfig -c hash_dist_init_segment_num -v 64 --skipvalidation > gpconfig1.log 2>&1
gpstop -ar > gpconfig2.log 2>&1
#gpconfig -c split_read_size_mb -v 512 --skipvalidation
#gpstop -u
#python -u lsp.py -s performance1 -a -c > ./performance1.log 2>&1
#sleep 10
#set split_read_size_mb = 8192;
#gpstop -ar
#python -u lsp.py -s performance2 -a -c > ./performance2.log 2>&1
#sleep 10
#python -u lsp.py -s performance3 -a > ./performance3.log 2>&1
#sleep 10
#python -u lsp.py -s performance4 -a -c > ./performance4.log 2>&1
#sleep 10
#python -u lsp.py -s performance5 -a -c > ./performance5.log 2>&1
#python -u lsp.py -s performance1,performance2,performance3 -a > ./performance.log 2>&1
