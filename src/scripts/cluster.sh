#!/bin/bash

reset(){
  # Settings
  PORT=50000
  TIMEOUT=2000
  NODES=6
  REPLICAS=1
  # Computed vars
  ENDPORT=$((PORT+NODES))
}

# Stop previous if exist
reset
echo "Stopping old cluster if it exists..."
while [ $((PORT < ENDPORT)) != "0" ]; do
    PORT=$((PORT+1))
    #echo "Stopping $PORT"
    redis-cli -p $PORT shutdown nosave >> /dev/null 2>&1
done

# Clear previos if exist
echo "Clearing old cluster data"
rm -rf *.log
rm -rf *.rdb
rm -rf *.log
rm -rf appendonly*.aof
rm -rf dump*.rdb
rm -rf nodes*.conf
rm -rf ./appendonlydir

if [ "$1" != "stop" ]
then
  # Start new
  reset
  echo "Starting cluster"
  while [ $((PORT < ENDPORT)) != "0" ]; do
      PORT=$((PORT+1))
      echo "Starting $PORT"
      redis-server --port $PORT --cluster-enabled yes --cluster-config-file nodes-${PORT}.conf --cluster-node-timeout $TIMEOUT --appendonly yes --appendfilename appendonly-${PORT}.aof --dbfilename dump-${PORT}.rdb --logfile ${PORT}.log --daemonize yes
  done
fi

if [ "$1" != "stop" ]
then
  # Initialize them
  reset
  HOSTS=""
  while [ $((PORT < ENDPORT)) != "0" ]; do
      PORT=$((PORT+1))
      HOSTS="$HOSTS 127.0.0.1:$PORT"
  done
  echo "Creating new cluster"
  echo "yes" | redis-cli --cluster create $HOSTS --cluster-replicas $REPLICAS
fi