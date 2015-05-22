#!/bin/sh

source ~/.bashrc
source ~/qa.sh

export HADOOP_PATH_VAR=/usr/phd/current/

function nodeconfig_fun() {
   if [ $1 != 'stop' ] && [ $1 != 'start' ] ; then
       echo "Please use options: start/stop."   
       exit 1
   fi
   hostlist=$3
   option=$1
   echo "hostlist is " $hostlist
   echo "option is " $option
   if [ "$2" = "HDFS" ]  || [ "$2" = "BOTH" ]; then
         for datanode in $hostlist; do
                echo "datanode is " $datanode
                ssh $datanode sudo -u hdfs $HADOOP_PATH_VAR/hadoop-client/sbin/hadoop-daemon.sh  $option  datanode
          done
   fi

   if [ "$2" = "HAWQ" ] || [ "$2" = "BOTH" ]; then
         for hawqnode in $hostlist; do
                ssh $hawqnode "source $GPHOME/greenplum_path.sh;hawq $option segment -a"
          done
   fi

   if [ "$2" = "NAMENODE" ]; then
         for namenode in $hostlist; do
                ssh $namenode sudo -u hdfs $HADOOP_PATH_VAR/hadoop-client/sbin/hadoop-daemon.sh  $option  namenode
          done
         if  [ $1 = 'start' ] ; then
             ssh $namenode sudo -u hdfs $HADOOP_PATH_VAR/hadoop-client/bin/hdfs dfsadmin -safemode leave
         fi
    fi
}

### $1 schedule $2 log name
function runworkload() {
    echo $3
    if [ "$3" = "HAWQ" ] || [ "$3" = "BOTH" ]; then   
        sleep 120
    fi
    psql -d postgres -c "select * from gp_segment_configuration;"  >> $2 2>&1
    if [ "$3" = "HDFS" ] || [ "$3" = "BOTH" ]; then  
        for((i=1;i<20;i++))
        do
            a=`sudo -u hdfs $HADOOP_PATH_VAR/hadoop-client/bin/hdfs dfsadmin -report -dead | grep "Dead datanodes" | cut -d '(' -f 2 |cut -d ')' -f 1`
            echo "No $i Fetch dead node is $a" >>$2      
            if [[ $? != 0 ]];
            then
                 echo "Hdfs error, Please check HDFS seting"
                 exit
             fi
             if [[ "$a" > 0 ]];
             then
                echo "Report the dead node $a"
                break
             fi
             python -u lsp.py -s $1  >> $2 2>&1
             sleep 30
         done
      fi
      python -u lsp.py -s $1  >> $2 2>&1
}

mv report report_bak

hawqconfig -c default_segment_num -v 128
hawqconfig -c hawq_resourcemanager_query_vsegment_number_per_segment_limit -v 8
hawqconfig -c hawq_resourceenforcer_cpu_enable -v false
hawq stop cluster -a
hawq start cluster -a
localhdfs stop HA
localhdfs start HA



#####################################
echo '16 Nodes with HAWQ and 16 nodes with HDFS' >./performance_tpch_nodechange_16both.log
runworkload performance_tpch_nodechange ./performance_tpch_nodechange_16both.log 
mv report report_16both


echo '16 Nodes with HAWQ and 15 nodes with HDFS' >./performance_tpch_nodechange_15hdfs.log
nodeconfig_fun stop HDFS "bcn-w16" >>./performance_tpch_nodechange_15hdfs.log 2>&1
runworkload performance_tpch_nodechange_noload ./performance_tpch_nodechange_15hdfs.log "HDFS"
nodeconfig_fun start HDFS "bcn-w16" >>./performance_tpch_nodechange_15hdfs.log 2>&1
mv report report_15hdfs

echo '15 Nodes with HAWQ and 16 nodes with HDFS' >./performance_tpch_nodechange_15hawq.log
nodeconfig_fun stop HAWQ "bcn-w16" >>./performance_tpch_nodechange_15hawq.log 2>&1
runworkload performance_tpch_nodechange_noload ./performance_tpch_nodechange_15hawq.log "HAWQ"
nodeconfig_fun start HAWQ "bcn-w16" >>./performance_tpch_nodechange_15hdfs.log 2>&1
mv report report_15hawq

echo '15 Nodes with HAWQ and 15 nodes with HDFS' >./performance_tpch_nodechange_15both.log
nodeconfig_fun stop BOTH "bcn-w16" >>./performance_tpch_nodechange_15both.log 2>&1
runworkload performance_tpch_nodechange_noload ./performance_tpch_nodechange_15both.log "BOTH"
nodeconfig_fun start BOTH "bcn-w16" >>./performance_tpch_nodechange_15both.log 2>&1
mv report report_15both



echo '16 Nodes with HAWQ and 14 nodes with HDFS' >./performance_tpch_nodechange_14hdfs.log
nodeconfig_fun stop HDFS "bcn-w16 bcn-w15" >>./performance_tpch_nodechange_14hdfs.log 2>&1
runworkload performance_tpch_nodechange_noload ./performance_tpch_nodechange_14hdfs.log "HDFS"
nodeconfig_fun start HDFS "bcn-w16 bcn-w15" >>./performance_tpch_nodechange_14hdfs.log 2>&1
mv report report_14hdfs

echo '14 Nodes with HAWQ and 16 nodes with HDFS' >./performance_tpch_nodechange_14hawq.log
nodeconfig_fun stop HAWQ "bcn-w16 bcn-w15" >>./performance_tpch_nodechange_14hawq.log 2>&1
runworkload performance_tpch_nodechange_noload ./performance_tpch_nodechange_14hawq.log "HAWQ"
nodeconfig_fun start HAWQ "bcn-w16 bcn-w15" >>./performance_tpch_nodechange_14hdfs.log 2>&1
mv report report_14hawq

echo '14 Nodes with HAWQ and 14 nodes with HDFS' >./performance_tpch_nodechange_14both.log
nodeconfig_fun stop BOTH "bcn-w16 bcn-w15" >>./performance_tpch_nodechange_14both.log 2>&1
runworkload performance_tpch_nodechange_noload ./performance_tpch_nodechange_14both.log "BOTH"
nodeconfig_fun start BOTH "bcn-w16 bcn-w15" >>./performance_tpch_nodechange_14both.log 2>&1
mv report report_14both


echo '16 Nodes with HAWQ and 8 nodes with HDFS' >./performance_tpch_nodechange_8hdfs.log
nodeconfig_fun stop HDFS "bcn-w16 bcn-w15 bcn-w14 bcn-w13 bcn-w12 bcn-w11 bcn-w10 bcn-w9" >>./performance_tpch_nodechange_8hdfs.log 2>&1
runworkload performance_tpch_nodechange_noload ./performance_tpch_nodechange_8hdfs.log "HDFS"
nodeconfig_fun start HDFS "bcn-w16 bcn-w15 bcn-w14 bcn-w13 bcn-w12 bcn-w11 bcn-w10 bcn-w9" >>./performance_tpch_nodechange_8hdfs.log 2>&1
mv report report_8hdfs

echo '8 Nodes with HAWQ and 16 nodes with HDFS' >./performance_tpch_nodechange_8hawq.log
nodeconfig_fun stop HAWQ "bcn-w16 bcn-w15 bcn-w14 bcn-w13 bcn-w12 bcn-w11 bcn-w10 bcn-w9" >>./performance_tpch_nodechange_8hawq.log 2>&1
runworkload performance_tpch_nodechange_noload ./performance_tpch_nodechange_8hawq.log "HAWQ" 
nodeconfig_fun start HAWQ "bcn-w16 bcn-w15 bcn-w14 bcn-w13 bcn-w12 bcn-w11 bcn-w10 bcn-w9" >>./performance_tpch_nodechange_8hdfs.log 2>&1
mv report report_8hawq

echo '8 Nodes with HAWQ and 8 nodes with HDFS' >./performance_tpch_nodechange_8both.log
nodeconfig_fun stop BOTH "bcn-w16 bcn-w15 bcn-w14 bcn-w13 bcn-w12 bcn-w11 bcn-w10 bcn-w9" >>./performance_tpch_nodechange_8both.log 2>&1
runworkload performance_tpch_nodechange_noload ./performance_tpch_nodechange_8both.log "BOTH"
nodeconfig_fun start BOTH "bcn-w16 bcn-w15 bcn-w14 bcn-w13 bcn-w12 bcn-w11 bcn-w10 bcn-w9" >>./performance_tpch_nodechange_8both.log 2>&1
mv report report_8both


#####################################################################################



hawq stop cluster -a
hawq start cluster -a
localhdfs stop HA
localhdfs start HA


##########Current There are 8 Nodes. Load Data into them##############################
echo '8 Nodes with HAWQ and HDFS'  > ./performance_tpch_nodechange_8.log 2>&1
gpscp -f ~/hostfile expand/slaves_8 =:$HADOOP_PATH_VAR/hadoop-client/etc/hadoop/slaves  >> ./performance_tpch_nodechange_8.log 2>&1
nodeconfig_fun stop BOTH "bcn-w16 bcn-w15 bcn-w14 bcn-w13 bcn-w12 bcn-w11 bcn-w10 bcn-w9"  >> ./performance_tpch_nodechange_8.log 2>&1
nodeconfig_fun stop NAMENODE "bcn-mst2 bcn-mst1"  >> ./performance_tpch_nodechange_8.log 2>&1
nodeconfig_fun start NAMENODE "bcn-mst2 bcn-mst1"  >> ./performance_tpch_nodechange_8.log 2>&1
sudo -u hdfs  /usr/phd/current/hadoop-client/bin/hdfs dfsadmin -report -live  >> ./performance_tpch_nodechange_8.log 2>&1
sleep 300 
psql -d postgres -c "select * from gp_segment_configuration;"  >> ./performance_tpch_nodechange_8.log 2>&1
python -u lsp.py -s performance_tpch_nodechange  >> ./performance_tpch_nodechange_8.log 2>&1


echo '#################################################'
echo '16 Nodes with HAWQ and HDFS'  > ./performance_tpch_nodechange_8.log 2>&1
gpscp -f ~/hostfile expand/slaves_16 =:$HADOOP_PATH_VAR/hadoop-client/etc/hadoop/slaves  > ./performance_tpch_nodechange_16node.log 2>&1
nodeconfig_fun start HAWQ "bcn-w16 bcn-w15 bcn-w14 bcn-w13 bcn-w12 bcn-w11 bcn-w10 bcn-w9"  >> ./performance_tpch_nodechange_8.log 2>&1
localhdfs stop HA  >> ./performance_tpch_nodechange_16node.log 2>&1
localhdfs start HA  >> ./performance_tpch_nodechange_16node.log 2>&1
sleep 120
sudo -u hdfs  $HADOOP_PATH_VAR/hadoop-client/bin/hdfs dfsadmin -report -live  >> ./performance_tpch_nodechange_16node.log 2>&1
psql -d postgres -c "select * from gp_segment_configuration;"  >> ./performance_tpch_nodechange_16node.log 2>&1
python -u lsp.py -s performance_tpch_nodechange_noload  >> ./performance_tpch_nodechange_16node.log 2>&1


sudo -u hdfs $HADOOP_PATH_VAR/hadoop-client/bin/hdfs  balancer  >> ./performance_tpch_nodechange_16node_balance.log 2>&1
psql -d postgres -c "select * from gp_segment_configuration;"  >> ./performance_tpch_nodechange_16node_balance.log 2>&1
python -u lsp.py -s performance_tpch_nodechange_noload  >> ./performance_tpch_nodechange_16node_balance.log 2>&1
