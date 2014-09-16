#!/bin/sh

source ~/.bashrc
source ~/qa.sh

python -u lsp.py -c 'HAWQ main performance on dca22' -s performance_full_regression_single_stream_tpch,performance_full_regression_concurrent_streams_tpch > ./performance_full_regression_tpch.log 2>&1
