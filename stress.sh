#!/bin/sh

source ~/.bashrc
source ~/qa.sh

### Daily stress test 
# python -u lsp.py -s system_load -b > ./system_load.log 2>&1 
# sleep 10
# nohup python -u lsp.py -s system_run_random_2_iteration_2_concurrency -b -c > ./system_run_random_2_iteration_2_concurrency.log 2>&1 &


### QUAYL stress test
python -u lsp.py -s stress_load > ./stress_load.log 2>&1 
#sleep 10
#nohup python -u lsp.py -s stress_run > ./stress_run.log 2>&1 &