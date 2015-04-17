#!/bin/sh

source ~/.bashrc
source ~/qa.sh

hawq cluster stop
hawq cluster start


##########Current There are 16 Nodes. Load Data into them##############################
echo '#################################################'
echo '16 Nodes with HAWQ and HDFS'
for i in bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;"
python -u lsp.py -s performance_tpch_10g  > ./performance_tpch_10g_16.log 2>&1

echo '#################################################'

########## Shut down one HAWQ nodes
echo '#################################################'
echo '15 Nodes with HAWQ and 16 nodes with HDFS'
./nodeconfig.sh stop HAWQ "bcn-w16"
for i in bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;"
python -u lsp.py -s performance_tpch_10g_noload  > ./performance_tpch_10g_15hawq.log 2>&1
echo '#################################################'


########## Shut down one HDFS nodes
echo '#################################################'
echo '15 Nodes with HAWQ and 16 nodes with HDFS'
./nodeconfig.sh stop HDFS "bcn-w16"
./nodeconfig.sh start HAWQ "bcn-w16"
for i in bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;"
python -u lsp.py -s performance_tpch_10g_noload  > ./performance_tpch_10g_15hdfs.log 2>&1
echo '#################################################'


########## Shut down one HAWQ/HDFS nodes
echo '#################################################'
echo '15 Nodes with HAWQ and 16 nodes with HDFS'
./nodeconfig.sh stop BOTH "bcn-w16"
for i in bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;"
python -u lsp.py -s performance_tpch_10g_noload  > ./performance_tpch_10g_15both.log 2>&1
echo '#################################################'


########## Shut down two HAWQ nodes
echo '#################################################'
echo '15 Nodes with HAWQ and 16 nodes with HDFS'
./nodeconfig.sh stop HAWQ "bcn-w16 bcn-w15"
for i in bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;"
python -u lsp.py -s performance_tpch_10g_noload  > ./performance_tpch_10g_14hawq.log 2>&1
echo '#################################################'


########## Shut down one HDFS nodes
echo '#################################################'
echo '15 Nodes with HAWQ and 16 nodes with HDFS'
./nodeconfig.sh stop HDFS "bcn-w16 bcn-w15"
./nodeconfig.sh start HAWQ "bcn-w16 bcn-w15"
for i in bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;"
python -u lsp.py -s performance_tpch_10g_noload  > ./performance_tpch_10g_14hdfs.log 2>&1
echo '#################################################'


########## Shut down one HAWQ/HDFS nodes
echo '#################################################'
echo '15 Nodes with HAWQ and 16 nodes with HDFS'
./nodeconfig.sh stop BOTH "bcn-w16 bcn-w15"
for i in bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;"
python -u lsp.py -s performance_tpch_10g_noload  > ./performance_tpch_10g_14both.log 2>&1
echo '#################################################'


hawqconfig -c hash_dist_init_segment_num -v 32
hawqconfig -c util_segment_num -v 32 
hawq cluster stop
hawq cluster start
##########Current There are 8 Nodes. Load Data into them##############################
echo '#################################################'
echo '8 Nodes with HAWQ and HDFS'
./nodeconfig.sh stop BOTH "bcn-w16 bcn-w15 bcn-w14 bcn-w13 bcn-w12 bcn-w11 bcn-w10 bcn-w9"
for i in bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;"
python -u lsp.py -s performance_tpch_10g  > ./performance_tpch_10g_8.log 2>&1

echo '#################################################'
hawq cluster stop -M fast
hawq cluster start
for i in bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;"
python -u lsp.py -s performance_tpch_10g_noload  > ./performance_tpch_10g_16node.log 2>&1
echo '#################################################'


