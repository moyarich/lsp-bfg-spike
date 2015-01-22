#!/bin/sh

source ~/.bashrc
source ~/qa.sh

# python -u lsp.py -s performance_full_regression_single_stream_tpch,performance_full_regression_concurrent_streams_tpch -a > ./performance_full_regression_tpch.log 2>&1

# python -u lsp.py -s performance_full_regression_single_stream_tpch -a > ./performance_full_regression_tpch.log 2>&1

# python -u lsp.py -s performance_sanity -a > ./performance_sanity.log 2>&1

#gpconfig -c split_read_size_mb -v 512 --skipvalidation
#gpstop -u
python -u lsp.py -s performance1 -m 5 -a > ./performance1.log 2>&1
#sleep 10
#set split_read_size_mb = 8192;
#gpstop -ar
#python -u lsp.py -s performance2 -m 30 -a > ./performance2.log 2>&1
#sleep 10
#python -u lsp.py -s performance3 -m 10 -a > ./performance3.log 2>&1
#sleep 10
#python -u lsp.py -s performance4 -m 60 -a -r 4 > ./performance4.log 2>&1
#python -u lsp.py -s performance1,performance2,performance3 -a > ./performance.log 2>&1
