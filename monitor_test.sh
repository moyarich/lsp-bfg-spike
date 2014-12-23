#!/bin/sh

source ~/.bashrc
source ~/qa.sh

python -u lsp.py -s test -m > monitor_test.log 2>&1
