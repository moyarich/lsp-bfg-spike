#!/bin/sh

source ~/.bashrc
source ~/qa.sh


hawqconfig -c hash_dist_init_segment_num -v 32
hawqconfig -c util_segment_num -v 32 
hawq cluster stop
hawq cluster start

gpssh -f ~/hostfile -e "sudo chmod 666 /usr/phd/3.0.0.0-249/hadoop/etc/hadoop/slaves"
gpscp -f ~/hostfile expand/slaves_8 =:/usr/phd/3.0.0.0-249/hadoop/etc/hadoop/slaves 
localhdfs stop HA
localhdfs start HA

##########Current There are 8 Nodes. Load Data into them##############################
echo '#################################################'  > ./performance_tpch_10g_8.log 2>&1
echo '8 Nodes with HAWQ and HDFS'  >> ./performance_tpch_10g_8.log 2>&1
./nodeconfig.sh stop BOTH "gva-w16 gva-w15 gva-w14 gva-w13 gva-w12 gva-w11 gva-w10 gva-w9"  >> ./performance_tpch_10g_8.log 2>&1
for i in gva-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done  >> ./performance_tpch_10g_8.log 2>&1
sleep 300 
psql -d postgres -c "select * from gp_segment_configuration;"  >> ./performance_tpch_10g_8.log 2>&1
python -u lsp.py -s performance_tpch_10g  >> ./performance_tpch_10g_8.log 2>&1

echo '#################################################'
hawq cluster stop -M fast  > ./performance_tpch_10g_16node.log 2>&1
hawq cluster start  >> ./performance_tpch_10g_16node.log 2>&1
gpscp -f ~/hostfile expand/slaves_16 =:/usr/phd/3.0.0.0-249/hadoop/etc/hadoop/slaves  >> ./performance_tpch_10g_16node.log 2>&1
localhdfs stop HA  >> ./performance_tpch_10g_16node.log 2>&1
localhdfs start HA  >> ./performance_tpch_10g_16node.log 2>&1
for i in gva-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done  >> ./performance_tpch_10g_16node.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;"  >> ./performance_tpch_10g_16node.log 2>&1
python -u lsp.py -s performance_tpch_10g_noload  >> ./performance_tpch_10g_16node.log 2>&1
echo '#################################################'

