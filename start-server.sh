#!/bin/bash

JAVA_HOME="/usr/jdk64/jdk1.7.0_67"
JAVA_OPTS="-Xms64m -Xmx256m -Dcom.sun.management.jmxremote.port=11162 -Dcom.sun.management.jmxremote.rmi.port=11162 -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
CLASS="org.apache.hadoop.contrib.ftp.HdfsOverFtpServer"
JAVA_CMD="$JAVA_HOME/jre/bin/java"
OUT_LOG="hdfs-over-ftp.out"

pid=/tmp/hdfs-over-ftp.pid

command="hdfs-over-ftp"
usage="Usage: start-server.sh"
cmd=start

case $cmd in

  (start)

    if [ -f $pid ]; then
        echo $command running as process `cat $pid`. Stop it first.
        exit 1
    fi

    echo starting $command
      $JAVA_CMD ${JAVA_OPTS} -cp .:lib/* ${CLASS} &> $OUT_LOG & echo $! > $pid
    ;;

  (*)
    echo $usage
    exit 1
    ;;
esac
