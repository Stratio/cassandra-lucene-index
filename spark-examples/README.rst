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
examples' code is in /home/example in docker containers, so you dont have to copy anything
 
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


