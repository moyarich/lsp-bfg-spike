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
      for i in bcn-mst1 bcn-mst2 bcn-w{1..16}; do ssh ${i} "hostname; ps -ef | grep [j]ava|wc"; done >> $2 2>&1 
      python -u lsp.py -s $1  >> $2 2>&1
}



hawqconfig -c default_segment_num -v 64
hawq stop cluster -a
cp expand/slaves_8 $GPHOME/etc/slaves
hawq start cluster -a
localhdfs stop HA
localhdfs start HA


##########Current There are 8 Nodes. Load Data into them##############################
gpssh -f ~/hostfile -e "sudo chmod 666 /usr/phd/current//hadoop-client/etc/hadoop/slaves"
echo '8 Nodes with HAWQ and HDFS'  > ./performance_tpch_nodechange_8.log 2>&1
gpscp -f ~/hostfile expand/slaves_8 =:$HADOOP_PATH_VAR/hadoop-client/etc/hadoop/slaves  >> ./performance_tpch_nodechange_8.log 2>&1
nodeconfig_fun stop BOTH "bcn-w16 bcn-w15 bcn-w14 bcn-w13 bcn-w12 bcn-w11 bcn-w10 bcn-w9"  >> ./performance_tpch_nodechange_8.log 2>&1
nodeconfig_fun stop NAMENODE "bcn-mst2 bcn-mst1"  >> ./performance_tpch_nodechange_8.log 2>&1
nodeconfig_fun start NAMENODE "bcn-mst2 bcn-mst1"  >> ./performance_tpch_nodechange_8.log 2>&1
sleep 300 
sudo -u hdfs  /usr/phd/current/hadoop-client/bin/hdfs dfsadmin -report -live  >> ./performance_tpch_nodechange_8.log 2>&1
psql -d postgres -c "select * from gp_segment_configuration;"  >> ./performance_tpch_nodechange_8.log 2>&1
psql -d postgres -c "drop table if exists test; create table test(a int); insert into test values (1);"
python -u lsp.py -s performance_tpch_nodechange  >> ./performance_tpch_nodechange_8.log 2>&1
sudo -u hdfs  /usr/phd/current/hadoop-client/bin/hdfs dfsadmin -report -live  >> ./performance_tpch_nodechange_8.log 2>&1

gpscp -f ~/hostfile expand/slaves_16 =:$HADOOP_PATH_VAR/hadoop-client/etc/hadoop/slaves  > ./performance_tpch_nodechange_16node.log 2>&1
cp expand/slaves_16 $GPHOME/etc/slaves
nodeconfig_fun start HAWQ "bcn-w16 bcn-w15 bcn-w14 bcn-w13 bcn-w12 bcn-w11 bcn-w10 bcn-w9"  >> ./performance_tpch_nodechange_16node.log 2>&1
localhdfs stop HA  >> ./performance_tpch_nodechange_16node.log 2>&1
localhdfs start HA  >> ./performance_tpch_nodechange_16node.log 2>&1
sleep 120
sudo -u hdfs  $HADOOP_PATH_VAR/hadoop-client/bin/hdfs dfsadmin -report -live  >> ./performance_tpch_nodechange_16node.log 2>&1
psql -d postgres -c "select * from gp_segment_configuration;"  >> ./performance_tpch_nodechange_16node.log 2>&1
psql -d postgres -c "drop table if exists test; create table test(a int); insert into test values (1);"
python -u lsp.py -s performance_tpch_nodechange_noload  >> ./performance_tpch_nodechange_16node.log 2>&1

## It is for temporary disable the bug
date > ./performance_tpch_nodechange_16node_balance.log
sudo -u hdfs $HADOOP_PATH_VAR/hadoop-client/bin/hdfs  balancer -threshold 1  >> ./performance_tpch_nodechange_16node_balance.log 2>&1
date >> ./performance_tpch_nodechange_16node_balance.log
sleep 300
sudo -u hdfs  /usr/phd/current/hadoop-client/bin/hdfs dfsadmin -report -live  >> ./performance_tpch_nodechange_16node_balance.log 2>&1
psql -d postgres -c "select * from gp_segment_configuration;"  >> ./performance_tpch_nodechange_16node_balance.log 2>&1
psql -d postgres -c "drop table if exists test; create table test(a int); insert into test values (1);"
python -u lsp.py -s performance_tpch_nodechange_noload  >> ./performance_tpch_nodechange_16node_balance.log 2>&1

psql -d postgres -c "select gp_metadata_cache_clear();"
psql -d postgres -c "select * from gp_segment_configuration;"  >> ./performance_tpch_nodechange_16node_balance_restart.log 2>&1
psql -d postgres -c "drop table if exists test; create table test(a int); insert into test values (1);"
python -u lsp.py -s performance_tpch_nodechange_noload  >> ./performance_tpch_nodechange_16node_balance_restart.log 2>&1

