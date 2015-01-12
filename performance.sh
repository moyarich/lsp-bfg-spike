#!/bin/sh

source ~/.bashrc
source ~/qa.sh

# python -u lsp.py -s performance_full_regression_single_stream_tpch,performance_full_regression_concurrent_streams_tpch -a > ./performance_full_regression_tpch.log 2>&1

# python -u lsp.py -s performance_full_regression_single_stream_tpch -a > ./performance_full_regression_tpch.log 2>&1

# python -u lsp.py -s performance_sanity -a > ./performance_sanity.log 2>&1

#python -u lsp.py -s tpcds -a > ./tpcds.log 2>&1

python -u lsp.py -s performance1 -a > ./performance1.log 2>&1
#sleep 10
#python -u lsp.py -s performance2 -a -m > ./performance2.log 2>&1
