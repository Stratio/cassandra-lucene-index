# Supported tags

- 3.9.0
- 3.8.0
- 3.7.3
- 3.7.2
- 3.7.1
- 3.7.0
- 3.5.2
- 3.5.1
- 3.5.0
- 3.0.9.0
- 3.0.8.3
- 3.0.8.2
- 3.0.8.1
- 3.0.8.0
- 3.0.7.3
- 3.0.7.2
- 3.0.7.1
- 3.0.7.0
- 3.0.6.3
- 3.0.6.2
- 3.0.6.1
- 3.0.6.0
- 3.0.5.2
- 3.0.5.1
- 3.0.5.0
- 3.0.4.1
- 3.0.4.0
- 2.2.7.1
- 2.2.7.0
- 2.2.6.2
- 2.2.6.1
- 2.2.6.0
- 2.2.5.4
- 2.2.5.3
- 2.1.15.0
- 2.1.14.0
- 2.1.13.0
- 2.1.11.0

For more information about Stratio's cassandra-lucene-index, please see [GitHub repo](https://github.com/Stratio/cassandra-lucene-index)

# What is Stratio Cassandra Lucene Index?

Stratio’s Cassandra Lucene Index, derived from Stratio Cassandra, is a plugin for Apache Cassandra that extends its index functionality to provide near real time search such as ElasticSearch or Solr, including full text search capabilities and free multivariable, geospatial and bitemporal search. It is achieved through an Apache Lucene based implementation of Cassandra secondary indexes, where each node of the cluster indexes its own data.


# How to use this image

```
docker run --name cassandra-node stratio/cassandra-lucene-index:latest
```
# Environment variables
Start the image using the following environment variables (if needed):


## CLUSTER_NAME
The name of the cluster. This setting prevents nodes in one logical cluster from joining another. All nodes in a cluster must have the same value.
## SEEDS
IPs (separated by commas) of the nodes included in the cassandra cluster


## MAX_HEAP
Sets the maximum heap size for the JVM. The same value is also used for the minimum heap size. This allows the heap to be locked in memory at process start to keep it from being swapped out by the OS
## MAX_NEW
The size of the young generation. The larger this is, the longer GC pause times will be. The shorter it is, the more expensive GC will be (usually). A good guideline is 100 MB per CPU core.

## START_JOLOKIA (Since 3.0.9.1)
Starts jolokia agent with cassandra with user defined opts.

## JOLOKIA_OPTS (Since 3.0.9.1)
All the options jolokia accepts as a formatted string like 'key=value,key2=value2,key3=value3'.

As stated in jolokia [doc](https://jolokia.org/reference/html/agents.html#agents-jvm) all options are included in [Table-3.1](https://jolokia.org/reference/html/agents.html#agent-war-init-params) and [Table 3.6](https://jolokia.org/reference/html/agents.html#agent-jvm-config)


Example:
1 cluster with 3 containers with MAX_HEAP 1G and MAX_NEW 64M, this terminal command starts two nodes, wait for stabilize them and then adds a new node

```
docker run -dit --name node1 --env MAX_HEAP=1G --env MAX_NEW=64M stratio/cassandra-lucene-index &&
export NODE1_IP=$(docker inspect -f  '{{ .NetworkSettings.IPAddress }}' node1) &&
docker run -dit --name node2 --env MAX_HEAP=1G --env MAX_NEW=64M --env SEEDS=$NODE1_IP stratio/cassandra-lucene-index &&
while [ $(docker exec -it node1 sh /opt/sds/cassandra/bin/nodetool -h localhost -p 7199 status | grep -E "^UN" | wc -l) != 2 ]; do sleep 5 ; done &&
docker run -dit --name node3 --env MAX_HEAP=1G --env MAX_NEW=64M --env SEEDS=$NODE1_IP stratio/cassandra-lucene-index
```

