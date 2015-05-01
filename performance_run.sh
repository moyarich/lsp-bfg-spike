#!/bin/sh

source ~/.bashrc
source ~/qa.sh

python -u lsp.py -s performance_run  -a -c  > ./performance_tpch_run.log 2>&1
