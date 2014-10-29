#!/bin/sh

source ~/.bashrc
source ~/qa.sh

python -u lsp.py -s stress_load > ./stress_load.log 2>&1 
sleep 10
nohup python -u lsp.py -s stress_run > ./stress_run.log 2>&1 &