Stratio’s Cassandra Lucene Index Spark Examples 
===============================================

Here you have some Spark examples over cassandra lucene queries



Pre requisites
--------------

To be able to run these examples we have created a debian-based Docker container with java 7.80.15 maven 3.3.3, Spark
 1.4.1 with hadoop 2.6, cassandra 2.1.8 and cassandra-lucene-plugin 2.1.8.1. Once the docker container is builded 
 know every user can deploy a cluster with one machine acting as Spark Master and others with spark Workers and 
 Cassandra. Here we show you all steps you have to run before getting the entire cluster
First step, build the docker container
--------------------------------------

If you don't have docker installed run 

.. code-block:: bash

    sudo apt-get install docker 

Download a fresh version of this project 

.. code-block:: bash

    git clone https://github.com/Stratio/cassandra-lucene-index.git

Compile and package it

.. code-block:: bash

	mvn clean package 

Go to docker containers directory

.. code-block:: bash

    cd spark-examples/resources/docker/cassandra.spark.docker_image/
    
    
Build the docker container, this will take a while, please be patient 

.. code-block:: bash
	
	docker build -t stratio/cassandra_spark
Second step , deploy the cluster 
--------------------------------

As mentioned before there are two types of machienes in our cluster, one is Spark Master, you can run it like this 

.. code-block:: bash

	docker run -i -t --rm --name spark_master stratio/cassandra_spark

The other type of machine contains a spark_worker and cassandra node, this machine needs to know which one is the 
SPARK Master so we proportionate the spark master ip (you get the ip from log output in terminal running spark 
master machine )
Run the first so:

.. code-block:: bash

	docker run -i -t --rm -e SPARK_MASTER=[SPARK_MASTER_IP] --name worker1 stratio/cassandra_spark


The rest of worker machines need almost one cassandra_seeds ip in order to form the ring so we proportionate the 
CASSANDRA_SEEDS_IP with the worker1 ip 

.. code-block:: bash

	docker run -i -t --rm -e SPARK_MASTER=[SPARK_MASTER_IP] -e CASSANDRA_SEEDS=[WORKER1_IP] --name worker2 
	stratio/cassandra_spark



Now you have a cassandra/spark running cluster. You can check the Spark cluster in spark master web
: 
SPARK_MASTER_IP:8080


You will see the N spark workers attached to the Spark master 

or the cassandra ring running in host terminal 

.. code-block:: bash

	docker exec -it worker1 nodetool status 
Third step, Create Table and Populate it 
----------------------------------------

When you have your cluster running you can execute the CreateTable&Populate.cql, this file with the jar containingg 
examples' code is in /home/example in docker containers, so you dont have to copy anything.
 
Open a terminal in any of the workers 

.. code-block:: bash

	docker exec -it worker1 /bin/bash 

Go to /home/example

.. code-block:: bash

	cd /home/example
	
Run CreateTable&Populate.cql script by CQL shell 
	
.. code-block:: bash

	cqlsh WORKER1_IP -f CreateTable&Populate.cql
	

Examples 
--------

Now having the cluster deployed and populated data you can run the examples.  

The examples are based in a table called sensors, his table with its keyspace and custom index is created with file 

.. code-block:: sql

	--create keyspace
	CREATE KEYSPACE spark_example_keyspace with replication = {'class':'SimpleStrategy', 'replication_factor': 1};
	
	USE spark_example_keyspace;
	
	
	--create sensor table 
	CREATE TABLE sensors_table (
		id int PRIMARY KEY,
		latitude float,
		longitude float,
		lucene text,
		sensor_name text,
		sensor_type text,
		temp_value float
	);

	
	--create index 
	CREATE CUSTOM INDEX sensors_index ON spark_example_keyspace.sensors_table (lucene) 
		USING 'com.stratio.cassandra.lucene.Index' 
		WITH OPTIONS = {
			'refresh_seconds' : '0.1',
			'schema' : '{
				fields : {
					sensor_name : {type:"string"},
					sensor_type : {type:"string"},
					temp_value : {type:"float"},
					place : {type:"geo_point", latitude:"latitude", longitude:"longitude"}
				}
			}'
		};


The examples calcules the mean of temp_value based in several CQL lucene queries, every example can be executed via 
spark-submit or in a spark-shell
 
 
Example 1 calculate mean temp of all values 
-------------------------------------------



.. code-block:: bash

 	spark-submit --class com.stratio.cassandra.examples.calcAllMean --master spark://172.17.0.2:7077 --deploy-mode 
 	client ./spark-example-2.1.8.4-SNAPSHOT.jar 
 	
 	
.. code-block:: bash 

	spark-shell
	
 	val KEYSPACE: String = "spark_example_keyspace"
    val TABLE: String = "sensors_table"

    var totalMean = 0.0f

    val sc : SparkContext = new SparkContext(new SparkConf)

    val tempRdd=sc.cassandraTable(KEYSPACE, TABLE).select("temp_value").map[Float]((row)=>row.getFloat("temp_value"))

    val totalNumElems: Long =tempRdd.count()

    if (totalNumElems>0) {
      val pairTempRdd = tempRdd.map(s => (1, s))
      val totalTempPairRdd = pairTempRdd.reduceByKey((a, b) => a + b)
      totalMean = totalTempPairRdd.first()._2 / totalNumElems.asInstanceOf[Float]
    }

    println("Mean calculed on all data mean: %s , numRows: %s", totalMean, totalNumElems)
 	
 	
Example 2 calculate mean temp of only sensors with sensor_type match "plane" 
----------------------------------------------------------------------------



Example 3 calculate mean temp of only sensors whose position in inside [(-10.0, 10.0), (-10.0, 10.0)] 
-----------------------------------------------------------------------------------------------------



Example 4 calculate mean temp of only sensors whose position distance from [0.0, 0.0] is less than 100000km
------------------------------------------------------------------------------------------------------------


Example 5 calculate mean temp of only sensors whose temp >= 30.0 
----------------------------------------------------------------




