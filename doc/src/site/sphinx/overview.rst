Overview
********

`Cassandra <http://cassandra.apache.org/>`__ index functionality has
been extended to provide near real time search such as
`ElasticSearch <http://www.elasticsearch.org/>`__ or
`Solr <https://lucene.apache.org/solr/>`__, including full text search
capabilities and multivariable and geospatial search.

It is also fully compatible with `Apache
Spark <https://spark.apache.org/>`__ and `Apache
Hadoop <https://hadoop.apache.org/>`__, allowing you to filter data at
database level. This speeds up jobs reducing the amount of data to be
collected and processed.

Indexing is achieved through a Lucene based implementation of Cassandra
secondary indexes, where each node of the cluster indexes its own data.
Stratio Cassandra is one of the core modules on which Stratio's BigData
platform (SDS) is based.

Features
========

Lucene search technology integration into Cassandra provides:

Stratio’s Cassandra Lucene Index and its integration with Lucene search technology provides:

-  Full text search
-  Geospatial search
-  Date ranges (durations) search
-  Multidimensional boolean (and, or, not) search
-  Bitemporal search
-  Near real-time search
-  Relevance scoring and sorting
-  General top-k queries
-  Custom analyzers
-  CQL3 support
-  Wide rows support
-  Partition and cluster composite keys support
-  Support for indexing columns part of primary key
-  Third-party drivers compatibility
-  Spark compatibility
-  Hadoop compatibility
-  Paging over non-relevance searches (filters)


Not yet supported:

-  Thrift API
-  Legacy compact storage option
-  Indexing ``counter`` columns
-  Columns with TTL
-  CQL user defined types
-  Static columns
-  Paging over relevance searches (queries and sorts)

Requirements
============

-  Cassandra 2.1.7
-  Java >= 1.7 (OpenJDK and Sun have been tested)
-  Maven >= 3.0

Installation
============

Stratio's Cassandra Lucene Index is distributed as a plugin for Apache
Cassandra. Thus, you just need to build a JAR containing the plugin and
add it to the Cassandra's classpath:

-  Build the plugin with Maven: ``mvn clean package``
-  Copy the generated JAR to the lib folder of your compatible Cassandra
   installation:
   ``cp target/cassandra-lucene-index-2.1.7.0.jar <CASSANDRA_HOME>/lib/``
-  Start/restart Cassandra as usual

Patching can also be done with this Maven profile, specifying the path
of your Cassandra installation:

.. code-block:: bash

    mvn clean package -Ppatch -Dcassandra_home=<CASSANDRA_HOME>

Alternatively, if you don't have an installed version of Cassandra,
there is a profile to let Maven download and patch the proper version of
Apache Cassandra:

.. code-block:: bash

    mvn clean package -Pdownload_and_patch -Dcassandra_home=<CASSANDRA_HOME>

Now you can run Cassandra and do some tests using the Cassandra Query
Language:

.. code-block:: bash

    <CASSANDRA_HOME>/bin/cassandra -f
    <CASSANDRA_HOME>/bin/cqlsh

The Lucene's index files will be stored in the same directories where
the Cassandra's will be. The default data directory is
``/var/lib/cassandra/data``, and each index is placed next to the
SSTables of its indexed column family.

For more details about Apache Cassandra please see its
`documentation <http://cassandra.apache.org/>`__.

Example
=======

We will create the following table to store tweets:

.. code-block:: sql

    CREATE KEYSPACE demo
    WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1};
    USE demo;
    CREATE TABLE tweets (
        id INT PRIMARY KEY,
        user TEXT,
        body TEXT,
        time TIMESTAMP,
        latitude FLOAT,
        longitude FLOAT,
        lucene TEXT
    );

We have created a column called *lucene* to link the index searches. This column will not store data. Now you can create a custom Lucene index on it with the following statement:

.. code-block:: sql

    CREATE CUSTOM INDEX tweets_index ON tweets (lucene)
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                id    : {type : "integer"},
                user  : {type : "string"},
                body  : {type : "text", analyzer : "english"},
                time  : {type : "date", pattern : "yyyy/MM/dd"},
                place : {type : "geo_point", latitude:"latitude", longitude:"longitude"}
            }
        }'
    };

This will index all the columns in the table with the specified types, and it will be refreshed once per second.

Now, to search for tweets within a certain date range:

.. code-block:: sql

    SELECT * FROM tweets WHERE lucene='{
        filter : {type:"range", field:"time", lower:"2014/04/25", upper:"2014/05/1"}
    }' limit 100;

Now, to search the top 100 more relevant tweets where *body* field contains the phrase “big data gives organizations”
within the aforementioned date range:

.. code-block:: sql

    SELECT * FROM tweets WHERE lucene='{
        filter : {type:"range", field:"time", lower:"2014/04/25", upper:"2014/05/1"},
        query  : {type:"phrase", field:"body", value:"big data gives organizations", slop:1}
    }' limit 100;

To refine the search to get only the tweets written by users whose name starts with "a":

.. code-block:: sql

    SELECT * FROM tweets WHERE lucene='{
        filter : {type:"boolean", must:[
                       {type:"range", field:"time", lower:"2014/04/25", upper:"2014/05/1"},
                       {type:"prefix", field:"user", value:"a"} ] },
        query  : {type:"phrase", field:"body", value:"big data gives organizations", slop:1}
    }' limit 100;

To get the 100 more recent filtered results you can use the *sort* option:

.. code-block:: sql

    SELECT * FROM tweets WHERE lucene='{
        filter : {type:"boolean", must:[
                       {type:"range", field:"time", lower:"2014/04/25", upper:"2014/05/1"},
                       {type:"prefix", field:"user", value:"a"} ] },
        query  : {type:"phrase", field:"body", value:"big data gives organizations", slop:1},
        sort   : {fields: [ {field:"time", reverse:true} ] }
    }' limit 100;

The previous search can be restricted to a geographical bounding box:

.. code-block:: sql

    SELECT * FROM tweets WHERE lucene='{
        filter : {type:"boolean", must:[
                       {type:"range", field:"time", lower:"2014/04/25", upper:"2014/05/1"},
                       {type:"prefix", field:"user", value:"a"},
                       {type:"geo_bbox",
                        field:"place",
                        min_latitude:40.225479,
                        max_latitude:40.560174,
                        min_longitude:-3.999278,
                        max_longitude:-3.378550} ] },
        query  : {type:"phrase", field:"body", value:"big data gives organizations", slop:1},
        sort   : {fields: [ {field:"time", reverse:true} ] }
    }' limit 100;

Alternatively, you can restrict the search to retrieve tweets that are within a specific distance from a geographical position:

.. code-block:: sql

    SELECT * FROM tweets WHERE lucene='{
        filter : {type:"boolean", must:[
                       {type:"range", field:"time", lower:"2014/04/25", upper:"2014/05/1"},
                       {type:"prefix", field:"user", value:"a"},
                       {type:"geo_distance",
                        field:"place",
                        latitude:40.393035,
                        longitude:-3.732859,
                        max_distance:"10km",
                        min_distance:"100m"} ] },
        query  : {type:"phrase", field:"body", value:"big data gives organizations", slop:1},
        sort   : {fields: [ {field:"time", reverse:true} ] }
    }' limit 100;
