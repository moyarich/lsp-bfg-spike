#!/bin/sh

source ~/.bashrc
source ~/qa.sh


hawqconfig -c hash_dist_init_segment_num -v 64
hawqconfig -c util_segment_num -v 64
hawq cluster stop
hawq cluster start

gpssh -f ~/hostfile -e "sudo chmod 666 /usr/phd/3.0.0.0-249/hadoop/etc/hadoop/slaves"
gpscp -f ~/hostfile expand/slaves_8 =:/usr/phd/3.0.0.0-249/hadoop/etc/hadoop/slaves 
localhdfs stop HA
localhdfs start HA

##########Current There are 8 Nodes. Load Data into them##############################
echo '#################################################'  > ./performance_tpch_nodechange_8.log 2>&1
echo '8 Nodes with HAWQ and HDFS'  >> ./performance_tpch_nodechange_8.log 2>&1
./nodeconfig.sh stop BOTH "gva-w16 gva-w15 gva-w14 gva-w13 gva-w12 gva-w11 gva-w10 gva-w9"  >> ./performance_tpch_nodechange_8.log 2>&1
for i in gva-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done  >> ./performance_tpch_nodechange_8.log 2>&1
sleep 300 
psql -d postgres -c "select * from gp_segment_configuration;"  >> ./performance_tpch_nodechange_8.log 2>&1
python -u lsp.py -s performance_tpch_nodechange  >> ./performance_tpch_nodechange_8.log 2>&1

echo '#################################################'
hawq cluster stop -M fast  > ./performance_tpch_nodechange_16node.log 2>&1
hawq cluster start  >> ./performance_tpch_nodechange_16node.log 2>&1
gpscp -f ~/hostfile expand/slaves_16 =:/usr/phd/3.0.0.0-249/hadoop/etc/hadoop/slaves  >> ./performance_tpch_nodechange_16node.log 2>&1
localhdfs stop HA  >> ./performance_tpch_nodechange_16node.log 2>&1
localhdfs start HA  >> ./performance_tpch_nodechange_16node.log 2>&1
for i in gva-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done  >> ./performance_tpch_nodechange_16node.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;"  >> ./performance_tpch_nodechange_16node.log 2>&1
python -u lsp.py -s performance_tpch_nodechange_noload  >> ./performance_tpch_nodechange_16node.log 2>&1
echo '#################################################'

##########Current There are 16 Nodes. Load Data into them##############################
echo '#################################################' > ./performance_tpch_nodechange_16.log
echo '16 Nodes with HAWQ and HDFS'    >> ./performance_tpch_nodechange_16.log
for i in gva-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done >> ./performance_tpch_nodechange_16.log
sleep 300 
psql -d postgres -c "select * from gp_segment_configuration;" >> ./performance_tpch_nodechange_16.log  2>&1
python -u lsp.py -s performance_tpch_nodechange_noload >> ./performance_tpch_nodechange_16.log 2>&1

echo '#################################################'
########## Shut down one HAWQ nodes
echo '#################################################' >./performance_tpch_nodechange_15hawq.log  
echo '15 Nodes with HAWQ and 16 nodes with HDFS' >>./performance_tpch_nodechange_15hawq.log  
hawq cluster stop -M fast >>./performance_tpch_nodechange_15hawq.log   2>&1
hawq cluster start   >>./performance_tpch_nodechange_15hawq.log  2>&1
./nodeconfig.sh stop HAWQ "gva-w16" >>./performance_tpch_nodechange_15hawq.log  2>&1
for i in gva-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done  >>./performance_tpch_nodechange_15hawq.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;" >>./performance_tpch_nodechange_15hawq.log  2>&1
python -u lsp.py -s performance_tpch_nodechange_noload  >> ./performance_tpch_nodechange_15hawq.log 2>&1
echo '#################################################'


########## Shut down one HDFS nodes
echo '#################################################' >./performance_tpch_nodechange_15hdfs.log 2>&1
echo '16 Nodes with HAWQ and 15 nodes with HDFS' >>./performance_tpch_nodechange_15hdfs.log 2>&1
hawq cluster stop -M fast
hawq cluster start
./nodeconfig.sh stop HDFS "gva-w16" >>./performance_tpch_nodechange_15hdfs.log 2>&1
./nodeconfig.sh start HAWQ "gva-w16" >>./performance_tpch_nodechange_15hdfs.log 2>&1
for i in gva-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done >>./performance_tpch_nodechange_15hdfs.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;"  >>./performance_tpch_nodechange_15hdfs.log 2>&1
python -u lsp.py -s performance_tpch_nodechange_noload    >> ./performance_tpch_nodechange_15hdfs.log 2>&1
echo '#################################################'


########## Shut down one HAWQ/HDFS nodes
echo '#################################################' >./performance_tpch_nodechange_15both.log 2>&1
echo '15 Nodes with HAWQ and 15 nodes with HDFS' >>./performance_tpch_nodechange_15both.log 2>&1
hawq cluster stop -M fast  >>./performance_tpch_nodechange_15both.log 2>&1
hawq cluster start       >>./performance_tpch_nodechange_15both.log 2>&1
./nodeconfig.sh stop BOTH "gva-w16" >>./performance_tpch_nodechange_15both.log 2>&1
for i in gva-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done >>./performance_tpch_nodechange_15both.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;" >>./performance_tpch_nodechange_15both.log 2>&1
python -u lsp.py -s performance_tpch_nodechange_noload  >> ./performance_tpch_nodechange_15both.log 2>&1
echo '#################################################'


########## Shut down two HAWQ nodes
echo '#################################################' > ./performance_tpch_nodechange_14hawq.log 2>&1
echo '14 Nodes with HAWQ and 16 nodes with HDFS' >> ./performance_tpch_nodechange_14hawq.log 2>&1
hawq cluster stop -M fast >> ./performance_tpch_nodechange_14hawq.log 2>&1
hawq cluster start >> ./performance_tpch_nodechange_14hawq.log 2>&1
./nodeconfig.sh stop HAWQ "gva-w16 gva-w15" >> ./performance_tpch_nodechange_14hawq.log 2>&1
./nodeconfig.sh start HDFS "gva-w16" >> ./performance_tpch_nodechange_14hawq.log 2>&1
for i in gva-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done >> ./performance_tpch_nodechange_14hawq.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;" >> ./performance_tpch_nodechange_14hawq.log 2>&1
python -u lsp.py -s performance_tpch_nodechange_noload  >> ./performance_tpch_nodechange_14hawq.log 2>&1
echo '#################################################'


########## Shut down one HDFS nodes
echo '#################################################' > ./performance_tpch_nodechange_14hdfs.log 2>&1
echo '16 Nodes with HAWQ and 14 nodes with HDFS' >> ./performance_tpch_nodechange_14hdfs.log 2>&1
hawq cluster stop -M fast >> ./performance_tpch_nodechange_14hdfs.log 2>&1
hawq cluster start >> ./performance_tpch_nodechange_14hdfs.log 2>&1
./nodeconfig.sh stop HDFS "gva-w16 gva-w15" >> ./performance_tpch_nodechange_14hdfs.log 2>&1
./nodeconfig.sh start HAWQ "gva-w16 gva-w15" >> ./performance_tpch_nodechange_14hdfs.log 2>&1
for i in gva-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done >> ./performance_tpch_nodechange_14hdfs.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;" >> ./performance_tpch_nodechange_14hdfs.log 2>&1
python -u lsp.py -s performance_tpch_nodechange_noload  >> ./performance_tpch_nodechange_14hdfs.log 2>&1
echo '#################################################'


########## Shut down one HAWQ/HDFS nodes
echo '#################################################' > ./performance_tpch_nodechange_14hboth.log 2>&1
echo '14 Nodes with HAWQ and 14 nodes with HDFS'  >> ./performance_tpch_nodechange_14both.log 2>&1
hawq cluster stop -M fast  >> ./performance_tpch_nodechange_14both.log 2>&1
hawq cluster start  >> ./performance_tpch_nodechange_14both.log 2>&1
./nodeconfig.sh stop BOTH "gva-w16 gva-w15"  >> ./performance_tpch_nodechange_14both.log 2>&1
for i in gva-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done  >> ./performance_tpch_nodechange_14both.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;"  >> ./performance_tpch_nodechange_14both.log 2>&1
python -u lsp.py -s performance_tpch_nodechange_noload  >> ./performance_tpch_nodechange_14both.log 2>&1
echo '#################################################'



########## Shut down two HAWQ nodes
echo '#################################################'  > ./performance_tpch_nodechange_8hawq.log 2>&1
echo '8 Nodes with HAWQ and 16 nodes with HDFS'
hawq cluster stop -M fast    >> ./performance_tpch_nodechange_8hawq.log 2>&1
hawq cluster start   >> ./performance_tpch_nodechange_8hawq.log 2>&1
./nodeconfig.sh stop HAWQ "gva-w16 gva-w15 gva-w14 gva-w13 gva-w12 gva-w11 gva-w10 gva-w9"   >> ./performance_tpch_nodechange_8hawq.log 2>&1
./nodeconfig.sh start HDFS "gva-w16 gva-w15"   >> ./performance_tpch_nodechange_8hawq.log 2>&1
for i in gva-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done   >> ./performance_tpch_nodechange_8hawq.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;"   >> ./performance_tpch_nodechange_8hawq.log 2>&1
python -u lsp.py -s performance_tpch_nodechange_noload  >> ./performance_tpch_nodechange_8hawq.log 2>&1
echo '#################################################'


########## Shut down one HDFS nodes
echo '#################################################'   > ./performance_tpch_nodechange_8hdfs.log 2>&1
echo '16 Nodes with HAWQ and 8 nodes with HDFS' >> ./performance_tpch_nodechange_8hdfs.log 2>&1
hawq cluster stop -M fast >> ./performance_tpch_nodechange_8hdfs.log 2>&1
hawq cluster start >> ./performance_tpch_nodechange_8hdfs.log 2>&1
./nodeconfig.sh stop HDFS "gva-w16 gva-w15 gva-w14 gva-w13 gva-w12 gva-w11 gva-w10 gva-w9" >> ./performance_tpch_nodechange_8hdfs.log 2>&1
./nodeconfig.sh start HAWQ "gva-w16 gva-w15 gva-w14 gva-w13 gva-w12 gva-w11 gva-w10 gva-w9" >> ./performance_tpch_nodechange_8hdfs.log 2>&1
for i in gva-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done >> ./performance_tpch_nodechange_8hdfs.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;" >> ./performance_tpch_nodechange_8hdfs.log 2>&1
python -u lsp.py -s performance_tpch_nodechange_noload  >> ./performance_tpch_nodechange_8hdfs.log 2>&1
echo '#################################################'


########## Shut down one HAWQ/HDFS nodes
echo '#################################################' > ./performance_tpch_nodechange_8both.log 2>&1
echo '8 Nodes with HAWQ and 8 nodes with HDFS' >> ./performance_tpch_nodechange_8both.log 2>&1
hawq cluster stop -M fast  >> ./performance_tpch_nodechange_8both.log 2>&1
hawq cluster start  >> ./performance_tpch_nodechange_8both.log 2>&1
./nodeconfig.sh stop BOTH "gva-w16 gva-w15 gva-w14 gva-w13 gva-w12 gva-w11 gva-w10 gva-w9"  >> ./performance_tpch_nodechange_8both.log 2>&1
for i in gva-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done  >> ./performance_tpch_nodechange_8both.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;"  >> ./performance_tpch_nodechange_8both.log 2>&1
python -u lsp.py -s performance_tpch_nodechange_noload  >> ./performance_tpch_nodechange_8both.log 2>&1
echo '#################################################'
