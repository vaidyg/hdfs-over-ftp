#!/bin/sh

pid=/tmp/hdfs-over-ftp.pid

command="hdfs-over-ftp"
usage="Usage: hdfs-over-ftp.sh (start|stop)"
cmd=stop

case $cmd in

  (stop)

    if [ -f $pid ]; then
        echo stopping $command
        kill `cat $pid`
    	rm $pid
      else
        echo no $command to stop
      fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;
esac
