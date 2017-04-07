#!/bin/bash -x
sed -i "s|^#MAX_HEAP_SIZE.*|MAX_HEAP_SIZE='${MAX_HEAP:=256M}'|" /etc/sds/cassandra/cassandra-env.sh
sed -i "s|^#HEAP_NEWSIZE.*|HEAP_NEWSIZE='${MAX_NEW:=64M}'|" /etc/sds/cassandra/cassandra-env.sh
if [ -d "/var/sds/cassandra/data/airlines_cassandra" ]; then
    sed -i "s|^cluster_name.*|cluster_name: 'XXXCLUSTERNAMEXXX'|" /etc/sds/cassandra/cassandra.yaml
    chown -R cassandra:stratio /var/sds/cassandra/data/airlines_cassandra
else
    sed -i "s|^cluster_name.*|cluster_name: '${CLUSTER_NAME:=Stratio cluster}'|" /etc/sds/cassandra/cassandra.yaml
fi

if [ -d "/opt/sds/cassandra/lib/jts" ]; then
    chown cassandra:stratio /opt/sds/cassandra/lib/jts/jts-core-1.14.0.jar
    chmod 755 /opt/sds/cassandra/lib/jts/jts-core-1.14.0.jar
fi

HOST=$(hostname --ip-address)
sed -i "s|^rpc_address.*|rpc_address: 0.0.0.0|"  /etc/sds/cassandra/cassandra.yaml
sed -i "s|^# broadcast_rpc_address.*|broadcast_rpc_address: ${HOST}|" /etc/sds/cassandra/cassandra.yaml
sed -i "s|- seeds.*|- seeds: '${SEEDS:=${HOST}}'|" /etc/sds/cassandra/cassandra.yaml
sed -i "s|^listen_address.*|listen_address: ${HOST}|" /etc/sds/cassandra/cassandra.yaml
sed -i "s|^enable_user_defined_functions.*|enable_user_defined_functions: true|" /etc/sds/cassandra/cassandra.yaml

if [ -v START_JOLOKIA ]; then
    wget -O /opt/sds/cassandra/lib/jolokia-jvm-1.3.4-agent.jar http://search.maven.org/remotecontent?filepath=org/jolokia/jolokia-jvm/1.3.4/jolokia-jvm-1.3.4-agent.jar
    if [ -v JOLOKIA_OPTS ]; then
        sed -i "s|^# add the jamm javaagent|#add the jolokia jvm agent\nJVM_OPTS=\"\$JVM_OPTS -javaagent:\$CASSANDRA_HOME/lib/jolokia-jvm-1.3.4-agent.jar=${JOLOKIA_OPTS}\"\n\n# add the jamm javaagent|" /etc/sds/cassandra/cassandra-env.sh
    else
        sed -i "s|^# add the jamm javaagent|#add the jolokia jvm agent\nJVM_OPTS=\"\$JVM_OPTS -javaagent:\$CASSANDRA_HOME/lib/jolokia-jvm-1.3.4-agent.jar\"\n\n# add the jamm javaagent|" /etc/sds/cassandra/cassandra-env.sh
    fi
fi

/etc/init.d/cassandra start

if [ -d "/var/sds/cassandra/data/airlines_cassandra" ]; then
    for i in {1..60}; do
        sh /opt/sds/cassandra/bin/cqlsh -e "UPDATE system.local SET cluster_name = '${CLUSTER_NAME:=XXXCLUSTERNAMEXXX}' where key='local';" >> /var/log/sds/cassandra/boot.log
        if [ \$? -ne 0 ]; then
            sleep 0.5
        else
            sed -i "s|^cluster_name.*|cluster_name: '${CLUSTER_NAME:=XXXCLUSTERNAMEXXX}'|" /etc/sds/cassandra/cassandra.yaml
            break
        fi
    done
fi
tail -F /var/log/sds/cassandra/boot.log
