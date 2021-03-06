#!/bin/sh

source ~/.bashrc
source ~/qa.sh

hawq stop cluster
hawq start cluster


source ~/.bashrc
source ~/qa.sh

export HADOOP_PATH_VAR=/usr/phd/current/
function nodeconfig () 
{
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
                ssh $hawqnode "source $GPHOME/greenplum_path.sh;hawq $option segment -a "
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

SUCCESS="Missing blocks: 0"
SIGN="CONTINUE"

### $1 log file to check, $2 log file to log
function check_success() {
    if grep -Fq "$SUCCESS" $1
    then
        echo "HDFS have completed copying missing blocks successful :  " >> $2 2>&1
        SIGN="SUCCESS"
    else
        echo "HDFS haven\`t completed copying missing blocks yet." >> $2 2>&1
        SIGN="CONTINUE"
        sleep 600
    fi
}

#### $1 log file to log.
function check_success_or_timeout(){
    TOTAL_CHECK=144
    COUNT=144
    while [ $SIGN = "CONTINUE" ]
    do
        sudo -u hdfs  /usr/phd/current/hadoop-client/bin/hdfs dfsadmin -report -live > HDFS_tmp_report.log 2>&1
        #hdfs dfsadmin -report -live > HDFS_tmp_report.log 2>&1
        check_success ./HDFS_tmp_report.log $1
        COUNT=`expr $COUNT - 1`
        date >> $1  2>&1
        echo "This is the `expr $TOTAL_CHECK - $COUNT` times check." >> $1 2>&1
        if [ $COUNT = 0 ]
        then
            SIGN="TIMEOUT"
        fi
    done
}

################ 
# $1: log file to log.
# $2: schedule name
function check_replica(){
    check_success_or_timeout $1
    if [ $SIGN = "SUCCESS" ]
    then
        date >> $1 2>&1
        echo "HDFS recover to 3 replica finished. Starting run workloads" >> $1 2>&1
        ### run workload
        python -u lsp.py -s $2 >> $1 2>&1
        ### run workload finished
        echo "Ending run workloads" >> $1 2>&1
    else
        if [ $SIGN = "TIMEOUT" ]
        then
            date >> $1 2>&1
            echo "There are always missing blocks. Cannot finish the recover work." >> $1 2>&1
            echo "Exit due to timeout." >> $1 2>&1
        else
            date >> $1 2>&1
            echo "There are unknown error." >> $1 2>&1
        fi
    fi
}
#################
# $1: Log File name
# $2: schedule name
# 
function runworkload () {
     #sleep 300
     sudo -u hdfs  /usr/phd/current/hadoop-client/bin/hdfs dfsadmin -report -live  >> $1 2>&1                                      
     psql -d postgres -c "select * from gp_segment_configuration;" >> $1 2>&1
     psql -d postgres -c "drop table if exists test; create table test(a int); insert into test values (1);"  >> $1  2>&1
     echo $2
     check_replica $1 $2
     #python -u lsp.py -s $2 >> $1 2>&1
}


##########Current There are 16 Nodes. Load Data into them##############################
echo '16 Nodes with HAWQ and HDFS'    > ./performance_tpch_nodechange_16.log
runworkload performance_tpch_nodechange_16.log performance_tpch_nodechange

########### Shut down one  nodes
echo '15 Nodes with HAWQ and 16 nodes with HDFS' >./performance_tpch_nodechange_15hawq.log 
nodeconfig.sh stop HAWQ "gva-w16" >>./performance_tpch_nodechange_15hawq.log  2>&1
runworkload ./performance_tpch_nodechange_15hawq.log performance_tpch_nodechange_noload
nodeconfig.sh start HAWQ "gva-w16" >>./performance_tpch_nodechange_15hawq.log  2>&1

echo '16 Nodes with HAWQ and 15 nodes with HDFS' >./performance_tpch_nodechange_15hdfs.log 2>&1
./nodeconfig.sh stop HDFS "gva-w16" >>./performance_tpch_nodechange_15hdfs.log 2>&1
runworkload ./performance_tpch_nodechange_15hdfs.log performance_tpch_nodechange_noload
./nodeconfig.sh start HDFS "gva-w16" >>./performance_tpch_nodechange_15hdfs.log 2>&1


echo '15 Nodes with HAWQ and 15 nodes with HDFS' >./performance_tpch_nodechange_15both.log 2>&1
nodeconfig.sh stop BOTH "gva-w16" >>./performance_tpch_nodechange_15both.log 2>&1
runworkload ./performance_tpch_nodechange_15both.log performance_tpch_nodechange_noload
nodeconfig.sh start BOTH "gva-w16" >>./performance_tpch_nodechange_15both.log 2>&1



########### Shut down two  nodes
echo '14 Nodes with HAWQ and 16 nodes with HDFS' >./performance_tpch_nodechange_14hawq.log
nodeconfig.sh stop HAWQ "gva-w16 gva-w15" >>./performance_tpch_nodechange_14hawq.log  2>&1
runworkload ./performance_tpch_nodechange_14hawq.log performance_tpch_nodechange_noload
nodeconfig.sh start HAWQ "gva-w16 gva-w15" >>./performance_tpch_nodechange_14hawq.log  2>&1

echo '16 Nodes with HAWQ and 14 nodes with HDFS' >./performance_tpch_nodechange_14hdfs.log 2>&1
./nodeconfig.sh stop HDFS "gva-w16 gva-w15" >>./performance_tpch_nodechange_14hdfs.log 2>&1
runworkload ./performance_tpch_nodechange_14hdfs.log performance_tpch_nodechange_noload
./nodeconfig.sh start HDFS "gva-w16 gva-w15" >>./performance_tpch_nodechange_14hdfs.log 2>&1


echo '14 Nodes with HAWQ and 14 nodes with HDFS' >./performance_tpch_nodechange_14both.log 2>&1
nodeconfig.sh stop BOTH "gva-w16 gva-w15" >>./performance_tpch_nodechange_14both.log 2>&1
runworkload ./performance_tpch_nodechange_14both.log performance_tpch_nodechange_noload
nodeconfig.sh start BOTH "gva-w16 gva-w15" >>./performance_tpch_nodechange_14both.log 2>&1



########### Shut down eight  nodes
echo '8 Nodes with HAWQ and 16 nodes with HDFS' >./performance_tpch_nodechange_8hawq.log
nodeconfig.sh stop HAWQ "gva-w16 gva-w15 gva-w14 gva-w13 gva-w12 gva-w11 gva-w10 gva-w9" >>./performance_tpch_nodechange_8hawq.log  2>&1
runworkload ./performance_tpch_nodechange_8hawq.log performance_tpch_nodechange_noload
nodeconfig.sh start HAWQ "gva-w16 gva-w15 gva-w14 gva-w13 gva-w12 gva-w11 gva-w10 gva-w9" >>./performance_tpch_nodechange_8hawq.log  2>&1

echo '16 Nodes with HAWQ and 8 nodes with HDFS' >./performance_tpch_nodechange_8hdfs.log 2>&1
./nodeconfig.sh stop HDFS "gva-w16 gva-w15 gva-w14 gva-w13 gva-w12 gva-w11 gva-w10 gva-w9" >>./performance_tpch_nodechange_8hdfs.log 2>&1
runworkload ./performance_tpch_nodechange_8hdfs.log performance_tpch_nodechange_noload
./nodeconfig.sh start HDFS "gva-w16 gva-w15 gva-w14 gva-w13 gva-w12 gva-w11 gva-w10 gva-w9" >>./performance_tpch_nodechange_8hdfs.log 2>&1


echo '8 Nodes with HAWQ and 8 nodes with HDFS' >./performance_tpch_nodechange_8both.log 2>&1
nodeconfig.sh stop BOTH "gva-w16 gva-w15 gva-w14 gva-w13 gva-w12 gva-w11 gva-w10 gva-w9" >>./performance_tpch_nodechange_8both.log 2>&1
runworkload ./performance_tpch_nodechange_8both.log performance_tpch_nodechange_noload
nodeconfig.sh start BOTH "gva-w16 gva-w15 gva-w14 gva-w13 gva-w12 gva-w11 gva-w10 gva-w9" >>./performance_tpch_nodechange_8both.log 2>&1

