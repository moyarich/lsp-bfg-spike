#!/bin/sh

source ~/.bashrc
source ~/qa.sh

hawqconfig -c default_segment_num -v 64
hawq stop cluster -a
hawq start cluster -a
python -u lsp.py -s performance_run -a -c  > ./performance_tpch_run.log 2>&1
