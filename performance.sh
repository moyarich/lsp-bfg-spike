#!/bin/sh

source ~/.bashrc
source ~/qa.sh

# python -u lsp.py -s performance_full_regression_single_stream_tpch,performance_full_regression_concurrent_streams_tpch > ./performance_full_regression_tpch.log 2>&1

# python -u lsp.py -s performance_full_regression_single_stream_tpch > ./performance_full_regression_tpch.log 2>&1

# python -u lsp.py -s performance_sanity > ./performance_sanity.log 2>&1

python -u lsp.py -s tpcds > ./tpcds.log 2>&1
