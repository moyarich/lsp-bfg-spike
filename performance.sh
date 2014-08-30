#!/bin/sh

source ~/.bashrc
source ~/hawq_env.sh

pytohn -u lsp.py -s performance_full_regression_single_stream_tpch      > ./performance_full_regression_single_stream_tpch.log      2>&1

# python -u lsp.py -s performance_full_regression_concurrent_streams_tpch > ./performance_full_regression_concurrent_streams_tpch.log 2>&1
