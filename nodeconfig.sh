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
                ssh $datanode sudo -u hdfs /usr/phd/current/hadoop-client/sbin/hadoop-daemon.sh  $option  datanode
          done
   fi

   if [ "$2" = "HAWQ" ] || [ "$2" = "BOTH" ]; then
         for hawqnode in $hostlist; do
                ssh $hawqnode "source /data/pulse-agent-data/HAWQ-2.X-SystemTest/product/hawq-2.0.0.0/greenplum_path.sh;hawq $option segment"
          done
   fi

   if [ "$2" = "NAMENODE" ]; then
         for namenode in $hostlist; do
                ssh $namenode sudo -u hdfs /usr/phd/current/hadoop-client/sbin/hadoop-daemon.sh  $option  namenode
          done
         if  [ $1 = 'start' ] ; then
             ssh $namenode sudo -u hdfs sudo -u hdfs /usr/phd/current/hadoop-client/bin/hdfs dfsadmin -safemode leave
         fi
    fi

