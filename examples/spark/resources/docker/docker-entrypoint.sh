#!/bin/bash
set -e
echo "Executing docker-entrypoint.sh"
# first arg is `-f` or `--some-option`
if [ "${1:0:1}" = '-' ]; then
	echo "First if"
	set -- cassandra -f "$@"
fi
#if it is a worker it starts cassandra 
if [ "$SPARK_MASTER" ]; then
	echo "second if-casandra"

	: ${CASSANDRA_LISTEN_ADDRESS='auto'}
	if [ "$CASSANDRA_LISTEN_ADDRESS" = 'auto' ]; then
		CASSANDRA_LISTEN_ADDRESS="$(hostname --ip-address)"
	fi

	: ${CASSANDRA_BROADCAST_ADDRESS="$CASSANDRA_LISTEN_ADDRESS"}

	if [ "$CASSANDRA_BROADCAST_ADDRESS" = 'auto' ]; then
		CASSANDRA_BROADCAST_ADDRESS="$(hostname --ip-address)"
	fi
	: ${CASSANDRA_BROADCAST_RPC_ADDRESS:=$CASSANDRA_BROADCAST_ADDRESS}

	if [ "$CASSANDRA_SEEDS" ]; then
		CASSANDRA_SEEDS="$CASSANDRA_SEEDS,$CASSANDRA_BROADCAST_ADDRESS"
	else
		CASSANDRA_SEEDS="$CASSANDRA_BROADCAST_ADDRESS"
	fi
	sed -ri 's/(- seeds:) "127.0.0.1"/\1 "'"$CASSANDRA_SEEDS"'"/' "$CASSANDRA_CONFIG/cassandra.yaml"

	sed -i -e "s/^rpc_address:.*/rpc_address: $CASSANDRA_LISTEN_ADDRESS/" "$CASSANDRA_CONFIG/cassandra.yaml"

	for yaml in \
		broadcast_address \
		broadcast_rpc_address \
		cluster_name \
		endpoint_snitch \
		listen_address \
		num_tokens \
	; do
		var="CASSANDRA_${yaml^^}"
		val="${!var}"
		if [ "$val" ]; then
			sed -ri 's/^(# )?('"$yaml"':).*/\2 '"$val"'/' "$CASSANDRA_CONFIG/cassandra.yaml"
		fi
	done

	for rackdc in dc rack; do
		var="CASSANDRA_${rackdc^^}"
		val="${!var}"
		if [ "$val" ]; then
			sed -ri 's/^('"$rackdc"'=).*/\1 '"$val"'/' "$CASSANDRA_CONFIG/cassandra-rackdc.properties"
		fi
	done

	echo "Starting cassandra"
	#start cassandra 1
	nohup cassandra

fi
echo "After Starting cassandra"
#if env variable SPARK_MASTER set, start spark as slave connecting to that master 
if [ "$SPARK_MASTER" ]; then
	echo "Starting spark as slave with ip: $(hostname --ip-address)"
	/usr/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://"$SPARK_MASTER:7077"
else 
	#start master and print 
	echo "Starting spark as master with ip: $(hostname --ip-address)"
	/usr/spark/bin/spark-class org.apache.spark.deploy.master.Master 
fi


