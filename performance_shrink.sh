#!/bin/sh

source ~/.bashrc
source ~/qa.sh

hawq stop cluster
hawq start cluster


##########Current There are 16 Nodes. Load Data into them##############################
echo '#################################################' > ./performance_tpch_10g_16.log
echo '16 Nodes with HAWQ and HDFS'    >> ./performance_tpch_10g_16.log
for i in bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done >> ./performance_tpch_10g_16.log
sleep 300 
psql -d postgres -c "select * from gp_segment_configuration;" >> ./performance_tpch_10g_16.log  2>&1
python -u lsp.py -s performance_tpch_10g_noload >> ./performance_tpch_10g_16.log 2>&1

echo '#################################################'

########## Shut down one HAWQ nodes
echo '#################################################' >./performance_tpch_10g_15hawq.log  
echo '15 Nodes with HAWQ and 16 nodes with HDFS' >>./performance_tpch_10g_15hawq.log  
hawq stop cluster -M fast >>./performance_tpch_10g_15hawq.log   2>&1
hawq start cluster   >>./performance_tpch_10g_15hawq.log  2>&1
./nodeconfig.sh stop HAWQ "bcn-w16" >>./performance_tpch_10g_15hawq.log  2>&1
for i in bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done  >>./performance_tpch_10g_15hawq.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;" >>./performance_tpch_10g_15hawq.log  2>&1
python -u lsp.py -s performance_tpch_10g_noload  >> ./performance_tpch_10g_15hawq.log 2>&1
echo '#################################################'


########## Shut down one HDFS nodes
echo '#################################################' >./performance_tpch_10g_15hdfs.log 2>&1
echo '16 Nodes with HAWQ and 15 nodes with HDFS' >>./performance_tpch_10g_15hdfs.log 2>&1
hawq stop cluster -M fast
hawq start cluster
./nodeconfig.sh stop HDFS "bcn-w16" >>./performance_tpch_10g_15hdfs.log 2>&1
./nodeconfig.sh start HAWQ "bcn-w16" >>./performance_tpch_10g_15hdfs.log 2>&1
for i in bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done >>./performance_tpch_10g_15hdfs.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;"  >>./performance_tpch_10g_15hdfs.log 2>&1
python -u lsp.py -s performance_tpch_10g_noload    >> ./performance_tpch_10g_15hdfs.log 2>&1
echo '#################################################'


########## Shut down one HAWQ/HDFS nodes
echo '#################################################' >./performance_tpch_10g_15both.log 2>&1
echo '15 Nodes with HAWQ and 15 nodes with HDFS' >>./performance_tpch_10g_15both.log 2>&1
hawq stop cluster -M fast  >>./performance_tpch_10g_15both.log 2>&1
hawq start cluster       >>./performance_tpch_10g_15both.log 2>&1
./nodeconfig.sh stop BOTH "bcn-w16" >>./performance_tpch_10g_15both.log 2>&1
for i in bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done >>./performance_tpch_10g_15both.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;" >>./performance_tpch_10g_15both.log 2>&1
python -u lsp.py -s performance_tpch_10g_noload  >> ./performance_tpch_10g_15both.log 2>&1
echo '#################################################'


########## Shut down two HAWQ nodes
echo '#################################################' > ./performance_tpch_10g_14hawq.log 2>&1
echo '14 Nodes with HAWQ and 16 nodes with HDFS' >> ./performance_tpch_10g_14hawq.log 2>&1
hawq stop cluster -M fast >> ./performance_tpch_10g_14hawq.log 2>&1
hawq start cluster >> ./performance_tpch_10g_14hawq.log 2>&1
./nodeconfig.sh stop HAWQ "bcn-w16 bcn-w15" >> ./performance_tpch_10g_14hawq.log 2>&1
./nodeconfig.sh start HDFS "bcn-w16" >> ./performance_tpch_10g_14hawq.log 2>&1
for i in bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done >> ./performance_tpch_10g_14hawq.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;" >> ./performance_tpch_10g_14hawq.log 2>&1
python -u lsp.py -s performance_tpch_10g_noload  >> ./performance_tpch_10g_14hawq.log 2>&1
echo '#################################################'


########## Shut down one HDFS nodes
echo '#################################################' > ./performance_tpch_10g_14hdfs.log 2>&1
echo '16 Nodes with HAWQ and 14 nodes with HDFS' >> ./performance_tpch_10g_14hdfs.log 2>&1
hawq stop cluster -M fast >> ./performance_tpch_10g_14hdfs.log 2>&1
hawq start cluster >> ./performance_tpch_10g_14hdfs.log 2>&1
./nodeconfig.sh stop HDFS "bcn-w16 bcn-w15" >> ./performance_tpch_10g_14hdfs.log 2>&1
./nodeconfig.sh start HAWQ "bcn-w16 bcn-w15" >> ./performance_tpch_10g_14hdfs.log 2>&1
for i in bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done >> ./performance_tpch_10g_14hdfs.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;" >> ./performance_tpch_10g_14hdfs.log 2>&1
python -u lsp.py -s performance_tpch_10g_noload  >> ./performance_tpch_10g_14hdfs.log 2>&1
echo '#################################################'


########## Shut down one HAWQ/HDFS nodes
echo '#################################################' > ./performance_tpch_10g_14hboth.log 2>&1
echo '14 Nodes with HAWQ and 14 nodes with HDFS'  >> ./performance_tpch_10g_14both.log 2>&1
hawq stop cluster -M fast  >> ./performance_tpch_10g_14both.log 2>&1
hawq start cluster  >> ./performance_tpch_10g_14both.log 2>&1
./nodeconfig.sh stop BOTH "bcn-w16 bcn-w15"  >> ./performance_tpch_10g_14both.log 2>&1
for i in bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done  >> ./performance_tpch_10g_14both.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;"  >> ./performance_tpch_10g_14both.log 2>&1
python -u lsp.py -s performance_tpch_10g_noload  >> ./performance_tpch_10g_14both.log 2>&1
echo '#################################################'



########## Shut down two HAWQ nodes
echo '#################################################'  > ./performance_tpch_10g_8hawq.log 2>&1
echo '8 Nodes with HAWQ and 16 nodes with HDFS'
hawq stop cluster -M fast    >> ./performance_tpch_10g_8hawq.log 2>&1
hawq start cluster   >> ./performance_tpch_10g_8hawq.log 2>&1
./nodeconfig.sh stop HAWQ "bcn-w16 bcn-w15 bcn-w14 bcn-w13 bcn-w12 bcn-w11 bcn-w10 bcn-w9"   >> ./performance_tpch_10g_8hawq.log 2>&1
./nodeconfig.sh start HDFS "bcn-w16 bcn-w15"   >> ./performance_tpch_10g_8hawq.log 2>&1
for i in bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done   >> ./performance_tpch_10g_8hawq.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;"   >> ./performance_tpch_10g_8hawq.log 2>&1
python -u lsp.py -s performance_tpch_10g_noload  >> ./performance_tpch_10g_8hawq.log 2>&1
echo '#################################################'


########## Shut down one HDFS nodes
echo '#################################################'   > ./performance_tpch_10g_8hdfs.log 2>&1
echo '16 Nodes with HAWQ and 8 nodes with HDFS' >> ./performance_tpch_10g_8hdfs.log 2>&1
hawq stop cluster -M fast >> ./performance_tpch_10g_8hdfs.log 2>&1
hawq start cluster >> ./performance_tpch_10g_8hdfs.log 2>&1
./nodeconfig.sh stop HDFS "bcn-w16 bcn-w15 bcn-w14 bcn-w13 bcn-w12 bcn-w11 bcn-w10 bcn-w9" >> ./performance_tpch_10g_8hdfs.log 2>&1
./nodeconfig.sh start HAWQ "bcn-w16 bcn-w15 bcn-w14 bcn-w13 bcn-w12 bcn-w11 bcn-w10 bcn-w9" >> ./performance_tpch_10g_8hdfs.log 2>&1
for i in bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done >> ./performance_tpch_10g_8hdfs.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;" >> ./performance_tpch_10g_8hdfs.log 2>&1
python -u lsp.py -s performance_tpch_10g_noload  >> ./performance_tpch_10g_8hdfs.log 2>&1
echo '#################################################'


########## Shut down one HAWQ/HDFS nodes
echo '#################################################' > ./performance_tpch_10g_8both.log 2>&1
echo '8 Nodes with HAWQ and 8 nodes with HDFS' >> ./performance_tpch_10g_8both.log 2>&1
hawq stop cluster -M fast  >> ./performance_tpch_10g_8both.log 2>&1
hawq start cluster  >> ./performance_tpch_10g_8both.log 2>&1
./nodeconfig.sh stop BOTH "bcn-w16 bcn-w15 bcn-w14 bcn-w13 bcn-w12 bcn-w11 bcn-w10 bcn-w9"  >> ./performance_tpch_10g_8both.log 2>&1
for i in bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava |  wc -l "; done  >> ./performance_tpch_10g_8both.log 2>&1
sleep 300
psql -d postgres -c "select * from gp_segment_configuration;"  >> ./performance_tpch_10g_8both.log 2>&1
python -u lsp.py -s performance_tpch_10g_noload  >> ./performance_tpch_10g_8both.log 2>&1
echo '#################################################'
