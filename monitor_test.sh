#!/bin/sh

source ~/.bashrc
source ~/qa.sh

python -u lsp.py -s test > monitor_test1.log 2>&1
sleep 10
python -u lsp.py -s test1 -m > monitor_test2.log 2>&1
