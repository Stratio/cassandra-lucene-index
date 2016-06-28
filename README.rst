================================
Stratio’s Cassandra Lucene Index
================================

Stratio’s Cassandra Lucene Index, derived from `Stratio Cassandra <https://github.com/Stratio/stratio-cassandra>`__, is
a plugin for `Apache Cassandra <http://cassandra.apache.org/>`__ that extends its index functionality to provide near
real time search such as ElasticSearch or Solr, including `full text search <http://en.wikipedia.org/wiki/Full_text_search>`__
capabilities and free multivariable, geospatial and bitemporal search. It is achieved through an `Apache Lucene <http://lucene.apache.org/>`__
based implementation of Cassandra secondary indexes, where each node of the cluster indexes its own data. Stratio’s
Cassandra indexes are one of the core modules on which `Stratio’s BigData platform <http://www.stratio.com/>`__ is based.

.. image:: /doc/resources/architecture.png
   :width: 100%
   :alt: architecture
   :align: center

Index `relevance searches <http://en.wikipedia.org/wiki/Relevance_(information_retrieval)>`__ allow you to retrieve the
*n* more relevant results satisfying a search. The coordinator node sends the search to each node in the cluster, each node
returns its *n* best results and then the coordinator combines these partial results and gives you the *n* best of them,
avoiding full scan. You can also base the sorting in a combination of fields.

Any cell in the tables can be indexed, including those in the primary key as well as collections. Wide rows are also
supported. You can scan token/key ranges, apply additional CQL3 clauses and page on the filtered results.

Index filtered searches are a powerful help when analyzing the data stored in Cassandra with `MapReduce <http://es.wikipedia.org/wiki/MapReduce>`__
frameworks as `Apache Hadoop <http://hadoop.apache.org/>`__ or, even better, `Apache Spark <http://spark.apache.org/>`__.
Adding Lucene filters in the jobs input can dramatically reduce the amount of data to be processed, avoiding full scan.

.. image:: /doc/resources/spark_architecture.png
   :width: 100%
   :alt: spark_architecture
   :align: center

The following benchmark result can give you an idea about the expected performance when combining Lucene indexes with
Spark. We do successive queries requesting from the 1% to 100% of the stored data. We can see a high performance for the
index for the queries requesting strongly filtered data. However, the performance decays in less restrictive queries.
As the number of records returned by the query increases, we reach a point where the index becomes slower than the full
scan. So, the decision to use indexes in your Spark jobs depends on the query selectivity. The tradeoff between both
approaches depends on the particular use case. Generally, combining Lucene indexes with Spark is recommended for jobs
retrieving no more than the 25% of the stored data.

.. image:: /doc/resources/spark_performance.png
   :width: 100%
   :alt: spark_performance
   :align: center

This project is not intended to replace Apache Cassandra denormalized tables, inverted indexes, and/or secondary
indexes. It is just a tool to perform some kind of queries which are really hard to be addressed using Apache Cassandra
out of the box features, filling the gap between real-time and analytics.

.. image:: /doc/resources/oltp_olap.png
   :width: 100%
   :alt: oltp_olap
   :align: center

More detailed information is available at `Stratio’s Cassandra Lucene Index documentation <doc/documentation.rst>`__.

Features
--------

Lucene search technology integration into Cassandra provides:

Stratio’s Cassandra Lucene Index and its integration with Lucene search technology provides:

-  Full text search (language-aware analysis, wildcard, fuzzy, regexp)
-  Boolean search (and, or, not)
-  Sorting by relevance, column value, and distance)
-  Geospatial indexing (points, lines, polygons and their multiparts)
-  Geospatial transformations (bounding box, buffer, centroid, convex hull, union, difference, intersection)
-  Geospatial operations (intersects, contains, is within)
-  Bitemporal search (valid and transaction time durations)
-  CQL complex types (list, set, map, tuple and UDT)
-  CQL user defined functions (UDF)
-  CQL paging, even with sorted searches
-  Columns with TTL
-  Third-party CQL-based drivers compatibility
-  Spark and Hadoop compatibility

Not yet supported:

-  Thrift API
-  Legacy compact storage option
-  Indexing ``counter`` columns
-  Indexing static columns
-  Other partitioners than Murmur3

Requirements
------------

-  Cassandra (identified by the three first numbers of the plugin version)
-  Java >= 1.7 (OpenJDK and Sun have been tested)
-  Maven >= 3.0

Build and install
-----------------

Stratio’s Cassandra Lucene Index is distributed as a plugin for Apache Cassandra. Thus, you just need to build a JAR
containing the plugin and add it to the Cassandra’s classpath:

-  Clone the project: ``git clone http://github.com/Stratio/cassandra-lucene-index``
-  Change to the downloaded directory: ``cd cassandra-lucene-index``
-  Checkout a plugin version suitable for your Apache Cassandra version: ``git checkout A.B.C.X``
-  Build the plugin with Maven: ``mvn clean package``
-  Copy the generated JAR to the lib folder of your compatible Cassandra installation:
   ``cp plugin/target/cassandra-lucene-index-plugin-*.jar <CASSANDRA_HOME>/lib/``
-  Start/restart Cassandra as usual.

Specific Cassandra Lucene index versions are targeted to specific Apache Cassandra versions. So, cassandra-lucene-index
A.B.C.X is aimed to be used with Apache Cassandra A.B.C, e.g.
`cassandra-lucene-index:3.0.7.1 <http://www.github.com/Stratio/cassandra-lucene-index/tree/3.0.7.1>`__ for
`cassandra:3.0.7 <http://www.github.com/apache/cassandra/tree/cassandra-3.0.7>`__. Please note that production-ready
releases are version tags (e.g. 3.0.6.3), don't use branch-X nor master branches in production.

Alternatively, patching can also be done with this Maven profile, specifying the path of your Cassandra installation,
this task also deletes previous plugin's JAR versions in CASSANDRA_HOME/lib/ directory:

.. code-block:: bash

    mvn clean package -Ppatch -Dcassandra_home=<CASSANDRA_HOME>

If you don’t have an installed version of Cassandra, there is also an alternative profile to let Maven download and
patch the proper version of Apache Cassandra:

.. code-block:: bash

    mvn clean package -Pdownload_and_patch -Dcassandra_home=<CASSANDRA_HOME>

Now you can run Cassandra and do some tests using the Cassandra Query Language:

.. code-block:: bash

    <CASSANDRA_HOME>/bin/cassandra -f
    <CASSANDRA_HOME>/bin/cqlsh

The Lucene’s index files will be stored in the same directories where the Cassandra’s will be. The default data
directory is ``/var/lib/cassandra/data``, and each index is placed next to the SSTables of its indexed column family.

For more details about Apache Cassandra please see its `documentation <http://cassandra.apache.org/>`__.

Examples
--------

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
        longitude FLOAT
    );

Now you can create a custom Lucene index on it with the following statement:

.. code-block:: sql

    CREATE CUSTOM INDEX tweets_index ON tweets ()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                id    : {type : "integer"},
                user  : {type : "string"},
                body  : {type : "text", analyzer : "english"},
                time  : {type : "date", pattern : "yyyy/MM/dd"},
                place : {type : "geo_point", latitude: "latitude", longitude: "longitude"}
            }
        }'
    };

This will index all the columns in the table with the specified types, and it will be refreshed once per second.
Alternatively, you can explicitly refresh all the index shards with an empty search with consistency ``ALL``:

.. code-block:: sql

    CONSISTENCY ALL
    SELECT * FROM tweets WHERE expr(tweets_index, '{refresh:true}');
    CONSISTENCY QUORUM

Now, to search for tweets within a certain date range:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index, '{
        filter : {type: "range", field: "time", lower: "2014/04/25", upper: "2014/05/01"}
    }');

The same search can be performed forcing an explicit refresh of the involved index shards:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index, '{
        filter : {type: "range", field: "time", lower: "2014/04/25", upper: "2014/05/01"},
        refresh : true
    }') limit 100;

Now, to search the top 100 more relevant tweets where *body* field contains the phrase “big data gives organizations”
within the aforementioned date range:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index, '{
        filter : {type: "range", field: "time", lower: "2014/04/25", upper: "2014/05/01"},
        query : {type: "phrase", field: "body", value: "big data gives organizations", slop: 1}
    }') LIMIT 100;

To refine the search to get only the tweets written by users whose names start with "a":

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index, '{
        filter : [ {type: "range", field: "time", lower: "2014/04/25", upper: "2014/05/01"},
                   {type: "prefix", field: "user", value: "a"} ],
        query : {type: "phrase", field: "body", value: "big data gives organizations", slop: 1}
    }') LIMIT 100;

To get the 100 more recent filtered results you can use the *sort* option:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index, '{
        filter : [ {type: "range", field: "time", lower: "2014/04/25", upper: "2014/05/01"},
                   {type: "prefix", field: "user", value: "a"} ],
        query : {type: "phrase", field: "body", value: "big data gives organizations", slop: 1},
        sort : {field: "time", reverse: true}
    }') limit 100;

The previous search can be restricted to tweets created close to a geographical position:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index, '{
        filter : [ {type: "range", field: "time", lower: "2014/04/25", upper: "2014/05/01"},
                   {type: "prefix", field: "user", value: "a"},
                   {type: "geo_distance", field: "place", latitude: 40.3930, longitude: -3.7328, max_distance: "10km"} ],
        query : {type: "phrase", field: "body", value: "big data gives organizations", slop: 1},
        sort : {field: "time", reverse: true}
    }') limit 100;

It is also possible to sort the results by distance to a geographical position:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index, '{
        filter : [ {type: "range", field: "time", lower: "2014/04/25", upper: "2014/05/01"},
                   {type: "prefix", field: "user", value: "a"},
                   {type: "geo_distance", field: "place", latitude: 40.3930, longitude: -3.7328, max_distance: "10km"} ],
        query : {type: "phrase", field: "body", value: "big data gives organizations", slop: 1},
        sort : [ {field: "time", reverse: true},
                 {field: "place", type: "geo_distance", latitude: 40.3930, longitude: -3.7328}]
    }') limit 100;

Last but not least, you can route any search to a certain token range or partition, in such a way that only a
subset of the cluster nodes will be hit, saving precious resources:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index, '{
        filter : [ {type: "range", field: "time", lower: "2014/04/25", upper: "2014/05/01"},
                   {type: "prefix", field: "user", value: "a"},
                   {type: "geo_distance", field: "place", latitude: 40.3930, longitude: -3.7328, max_distance: "10km"} ],
        query : {type: "phrase", field: "body", value: "big data gives organizations", slop: 1},
        sort : [ {field: "time", reverse: true},
                 {field: "place", type: "geo_distance", latitude: 40.3930, longitude: -3.7328}]
    }') AND TOKEN(id) >= TOKEN(0) AND TOKEN(id) < TOKEN(10000000) limit 100;

This last is the basis for `Hadoop, Spark and other MapReduce frameworks support <doc/documentation.rst#spark-and-hadoop>`__.

Please, refer to the comprehensive `Stratio’s Cassandra Lucene Index documentation <doc/documentation.rst>`__.
