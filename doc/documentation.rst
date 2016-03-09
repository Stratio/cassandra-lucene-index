++++++++++++++++++++++++++++++++
Stratio's Cassandra Lucene Index
++++++++++++++++++++++++++++++++

- `Overview <#overview>`__
    - `Features <#features>`__
    - `Architecture <#architecture>`__
    - `Requirements <#requirements>`__
    - `Installation <#installation>`__
    - `Upgrade <#upgrade>`__
    - `Example <#example>`__
- `Indexing <#indexing>`__
    - `Analyzers <#analysis>`__
        - `Classpath analyzer <#classpath-analyzer>`__
        - `Snowball analyzer <#snowball-analyzer>`__
    - `Mappers <#mappers>`__
        - `Big decimal mapper <#big-decimal-mapper>`__
        - `Big integer mapper <#big-integer-mapper>`__
        - `Bitemporal mapper <#bitemporal-mapper>`__
        - `Blob mapper <#blob-mapper>`__
        - `Boolean mapper <#boolean-mapper>`__
        - `Date mapper <#date-mapper>`__
        - `Date range mapper <#daterange-mapper>`__
        - `Double mapper <#double-mapper>`__
        - `Float mapper <#float-mapper>`__
        - `Geo point mapper <#geo-point-mapper>`__
        - `Geo shape mapper <#geo-shape-mapper>`__
        - `Inet mapper <#inet-mapper>`__
        - `Integer mapper <#integer-mapper>`__
        - `Long mapper <#long-mapper>`__
        - `String mapper <#string-mapper>`__
        - `Text mapper <#text-mapper>`__
        - `UUID mapper <#uuid-mapper>`__
    - `Example <#example>`__
- `Searching <#searching>`__
    - `All search <#all-search>`__
    - `Bitemporal search <#bitemporal-search>`__
    - `Boolean search <#boolean-search>`__
    - `Contains search <#contains-search>`__
    - `Date range search <#date-range-search>`__
    - `Fuzzy search <#fuzzy-search>`__
    - `Geo bounding box search <#geo-bbox-search>`__
    - `Geo distance search <#geo-distance-search>`__
    - `Geo shape search <#geo-shape-search>`__
    - `Match search <#match-search>`__
    - `None search <#none-search>`__
    - `Phrase search <#phrase-search>`__
    - `Prefix search <#prefix-search>`__
    - `Range search <#range-search>`__
    - `Regexp search <#regexp-search>`__
    - `Wildcard search <#wildcard-search>`__
- `Geographical elements <#geographical-elements>`__
    - `Distance <#distance>`__
    - `Transformations <#tranformations>`__
        - `Buffer <#buffer>`__
        - `Centroid <#centroid>`__
        - `Difference <#difference>`__
        - `Intersection <#intersection>`__
        - `Union <#intersection>`__
- `Complex data types <#complex-data-types>`__
    - `Tuples <#tuples>`__
    - `User Defined Types <#user-defined-types>`__
    - `Collections <#collections>`__
- `Query builder <#query-builder>`__
- `Spark and Hadoop <#spark-and-hadoop>`__
    - `Token range searches <#token-range-searches>`__
    - `Paging <#paging>`__
    - `Examples <#examples>`__
    - `Performance <#performance>`__
- `JMX interface <#jmx-interface>`__
- `Performance tips <#performance-tips>`__
    - `Choose the right use case <#choose-the-right-use-case>`__
    - `Use the latest version <#use-the-latest-version>`__
    - `Disable virtual nodes <#disable-virtual-nodes>`__
    - `Use a separate disk <#use-a-separate-disk>`__
    - `Disregard the first query <disregard-the-first-query>`__
    - `Index only what you need <#index-only-what-you-need>`__
    - `Use a low refresh rate <#use-a-low-refresh-rate>`__
    - `Prefer filters over queries <#prefer-filters-over-queries>`__
    - `Limit top-k searches <#limit-top-k-searches>`__

Overview
********

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

This project is not intended to replace Apache Cassandra denormalized tables, inverted indexes, and/or secondary
indexes. It is just a tool to perform some kind of queries which are really hard to be addressed using Apache Cassandra
out of the box features, filling the gap between real-time and analytics.

.. image:: /doc/resources/oltp_olap.png
   :width: 100%
   :alt: oltp_olap
   :align: center


IMPORTANT -- cassandra-driver-core does not support new 3.0 'expr' Clauses
yet, so please note all the Using Builder examples in this document are
obsolete until cassandra-driver-core includes this new type of Clauses in QueryBuilder

Features
========

Lucene search technology integration into Cassandra provides:

Stratio’s Cassandra Lucene Index and its integration with Lucene search technology provides:

-  Full text search (language-aware analysis, wildcard, fuzzy, regexp)
-  Geospatial indexing (points, lines, polygons and their multiparts)
-  Geospatial transformations (union, difference, intersection, buffer, centroid)
-  Geospatial operations (intersects, contains, is within)
-  Bitemporal search (valid and transaction time durations)
-  Boolean search (and, or, not)
-  Top-k queries (relevance scoring, sort by value, sort by distance)
-  CQL complex types (list, set, map, tuple and UDT)
-  CQL user defined functions (UDF)
-  Third-party CQL-based drivers compatibility
-  Spark and Hadoop compatibility
-  Paging over filters

Not yet supported:

-  Thrift API
-  Legacy compact storage option
-  Indexing ``counter`` columns
-  Columns with TTL
-  Static columns
-  Other partitioners than Murmur3
-  Paging over top-k searches

Architecture
============

Indexing is achieved through a Lucene based implementation of Apache Cassandra secondary indexes.
Cassandra's secondary indexes are local indexes,
meaning that each node of the cluster indexes it's own data.
As usual in Cassandra, each node can act as search coordinator.
The coordinator node sends the searches to all the involved nodes,
and then it post-processes the returned rows to return the required ones.
This post-processing is particularly important in top-k queries.

Regarding to the Cassandra-Lucene mapping, each node has a single Lucene index per indexed table,
and each logic CQL row is mapped to a Lucene document.
This documents are composed by the user-defined fields, the primary key and the partitioner's token.
Indexing is done in a synchronous fashion at the storage layer, so each row upsert implies a document upsert.
This adds an extra cost for write operations, which is the price of the provided search features.
As long as indexing is done below the distribution layer,
replication has been already achieved when the rows come to the index.

Requirements
============

-  Cassandra (identified by the three first numbers of the plugin version)
-  Java >= 1.8 (OpenJDK and Sun have been tested)
-  Maven >= 3.0

Installation
============

Stratio’s Cassandra Lucene Index is distributed as a plugin for Apache Cassandra. Thus, you just need to build a JAR
containing the plugin and add it to the Cassandra’s classpath:

-  Build the plugin with Maven: ``mvn clean package``
-  Copy the generated JAR to the lib folder of your compatible Cassandra installation:
   ``cp plugin/target/cassandra-lucene-index-plugin-*.jar <CASSANDRA_HOME>/lib/``
-  Start/restart Cassandra as usual.

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

Upgrade
=======

If you want to upgrade your cassandra cluster to a newer version  you must follow the Datastax official `upgrade instructions <https://docs.datastax.com/en/upgrade/doc/upgrade/cassandra/upgradeCassandra_g.html>`__.

The rule for the Lucene secondary indexes is to delete them with older version, upgrade cassandra and lucene index jar
and create them again with running newer version.

If you have huge amount of data in your cluster this could be an expensive task. We have tested it and here you have a
compatibility matrix that states between which versions it is not needed to delete the index:


+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| From\\ To | 2.1.6.2 | 2.1.7.1 | 2.1.8.5 | 2.1.9.0 | 2.1.10.0 | 2.1.11.1 | 2.2.3.2 | 2.2.4.3 | 2.2.4.4 | 2.2.5.0 | 2.2.5.1 | 2.2.5.2 | 3.0.3.0 | 3.0.3.1 | 3.0.4.0 |
+===========+=========+=========+=========+=========+==========+==========+=========+=========+=========+=========+=========+=========+=========+=========+=========+
| 2.1.6.0   |   YES   |   YES   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.6.1   |   YES   |   YES   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.6.2   |    --   |   YES   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.7.0   |    --   |   YES   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.7.1   |    --   |    --   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.8.0   |    --   |    --   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.8.1   |    --   |    --   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.8.2   |    --   |    --   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.8.3   |    --   |    --   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.8.4   |    --   |    --   |   YES   |   YES   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.8.5   |    --   |    --   |    --   |   YES   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.9.0   |    --   |    --   |    --   |    --   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.10.0  |    --   |    --   |    --   |    --   |    --    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.11.0  |    --   |    --   |    --   |    --   |    --    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.11.1  |    --   |    --   |    --   |    --   |    --    |    --    |   YES   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.3.0   |    --   |    --   |    --   |    --   |    --    |    --    |   YES   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.3.1   |    --   |    --   |    --   |    --   |    --    |    --    |   YES   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.3.2   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.4.0   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.4.1   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.4.2   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.4.3   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.4.5   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.5.0   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |   YES   |   YES   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.5.1   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |    --   |   YES   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.5.2   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |    --   |    --   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.3.0   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |   YES   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.3.1   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+

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
                time  : {type : "date", pattern : "yyyy/MM/dd", sorted : true},
                place : {type : "geo_point", latitude:"latitude", longitude:"longitude"}
            }
        }'
    };

This will index all the columns in the table with the specified types, and it will be refreshed once per second.
Alternatively, you can explicitly refresh all the index shards with an empty search with consistency ``ALL``:

.. code-block:: sql

    CONSISTENCY ALL
    SELECT * FROM tweets WHERE expr(tweets_index,'{refresh:true}');
    CONSISTENCY QUORUM

Now, to search for tweets within a certain date range:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index,'{
        filter : {type:"range", field:"time", lower:"2014/04/25", upper:"2014/05/01"}
    }') LIMIT 100;

The same search can be performed forcing an explicit refresh of the involved index shards:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index,'{
        filter : {type:"range", field:"time", lower:"2014/04/25", upper:"2014/05/01"},
        refresh : true
    }') LIMIT 100;

Now, to search the top 100 more relevant tweets where *body* field contains the phrase “big data gives organizations”
within the aforementioned date range:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index,'{
        filter : {type:"range", field:"time", lower:"2014/04/25", upper:"2014/05/01"},
        query  : {type:"phrase", field:"body", value:"big data gives organizations", slop:1}
    }') LIMIT 100;

To refine the search to get only the tweets written by users whose name starts with "a":

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index,'{
        filter : {type:"boolean", must:[
                       {type:"range", field:"time", lower:"2014/04/25", upper:"2014/05/01"},
                       {type:"prefix", field:"user", value:"a"} ] },
        query  : {type:"phrase", field:"body", value:"big data gives organizations", slop:1}
    }') LIMIT 100;

To get the 100 more recent filtered results you can use the *sort* option:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index,'{
        filter : {type:"boolean", must:[
                       {type:"range", field:"time", lower:"2014/04/25", upper:"2014/05/01"},
                       {type:"prefix", field:"user", value:"a"} ] },
        query  : {type:"phrase", field:"body", value:"big data gives organizations", slop:1},
        sort   : {fields: [ {field:"time", reverse:true} ] }
    }') LIMIT 100;

The previous search can be restricted to a geographical bounding box:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index,'{
        filter : {type:"boolean", must:[
                       {type:"range", field:"time", lower:"2014/04/25", upper:"2014/05/01"},
                       {type:"prefix", field:"user", value:"a"},
                       {type:"geo_bbox",
                        field:"place",
                        min_latitude:40.225479,
                        max_latitude:40.560174,
                        min_longitude:-3.999278,
                        max_longitude:-3.378550} ] },
        query  : {type:"phrase", field:"body", value:"big data gives organizations", slop:1},
        sort   : {fields: [ {field:"time", reverse:true} ] }
    }') LIMIT 100;

Alternatively, you can restrict the search to retrieve tweets that are within a specific distance from a geographical position:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index,'{
        filter : {type:"boolean", must:[
                       {type:"range", field:"time", lower:"2014/04/25", upper:"2014/05/01"},
                       {type:"prefix", field:"user", value:"a"},
                       {type:"geo_distance",
                        field:"place",
                        latitude:40.393035,
                        longitude:-3.732859,
                        max_distance:"10km",
                        min_distance:"100m"} ] },
        query  : {type:"phrase", field:"body", value:"big data gives organizations", slop:1},
        sort   : {fields: [ {field:"time", reverse:true} ] }
    }') LIMIT 100;

Indexing
********

**Syntax:**

.. code-block:: sql

    CREATE CUSTOM INDEX (IF NOT EXISTS)? <index_name>
                                      ON <table_name> ()
                                   USING 'com.stratio.cassandra.lucene.Index'
                            WITH OPTIONS = <options>

where <options> is a JSON object:

.. code-block:: sql

    <options> := { ('refresh_seconds'        : '<int_value>',)?
                   ('ram_buffer_mb'          : '<int_value>',)?
                   ('max_merge_mb'           : '<int_value>',)?
                   ('max_cached_mb'          : '<int_value>',)?
                   ('indexing_threads'       : '<int_value>',)?
                   ('indexing_queues_size'   : '<int_value>',)?
                   ('directory_path'         : '<string_value>',)?
                   ('excluded_data_centers'  : '<string_value>',)?
                   ('token_range_cache_size' : '<int_value>',)?
                   ('search_cache_size'      : '<int_value>',)?
                   'schema'                  : '<schema_definition>'};

All options take a value enclosed in single quotes:

-  **refresh\_seconds**: number of seconds before auto-refreshing the
   index reader. It is the max time taken for writes to be searchable
   without forcing an index refresh. Defaults to '60'.
-  **ram\_buffer\_mb**: size of the write buffer. Its content will be
   committed to disk when full. Defaults to '64'.
-  **max\_merge\_mb**: defaults to '5'.
-  **max\_cached\_mb**: defaults to '30'.
-  **indexing\_threads**: number of asynchronous indexing threads. ’0’
   means synchronous indexing. Defaults to ’0’.
-  **indexing\_queues\_size**: max number of queued documents per
   asynchronous indexing thread. Defaults to ’50’.
-  **directory\_path**: The path of the directory where the  Lucene index
   will be stored.
-  **excluded\_data\_centers**: The comma-separated list of the data centers
   to be excluded. The index will be created on this data centers but all the
   write operations will be silently ignored.
-  **token\_range\_cache\_size**: max number of token ranges to be cached. Defaults to ’16’.
-  **search\_cache\_size**: max number of searches to be cached. Defaults to ’16’.
-  **schema**: see below

.. code-block:: sql

    <schema_definition> := {
        (analyzers : { <analyzer_definition> (, <analyzer_definition>)* } ,)?
        (default_analyzer : "<analyzer_name>",)?
        fields : { <field_definition> (, <field_definition>)* }
    }

Where default\_analyzer defaults to
‘org.apache.lucene.analysis.standard.StandardAnalyzer’.

.. code-block:: sql

    <analyzer_definition> := <analyzer_name> : {
        type : "<analyzer_type>" (, <option> : "<value>")*
    }

.. code-block:: sql

    <field_definition> := <column_name> : {
        type : "<field_type>" (, <option> : "<value>")*
    }

Analyzers
=========

Analyzer definition options depend on the analyzer type. Details and
default values are listed in the table below.

+-----------------+-------------+--------------+-----------------+
| Analyzer type   | Option      | Value type   | Default value   |
+=================+=============+==============+=================+
| classpath       | class       | string       | null            |
+-----------------+-------------+--------------+-----------------+
| snowball        | language    | string       | null            |
|                 +-------------+--------------+-----------------+
|                 | stopwords   | string       | null            |
+-----------------+-------------+--------------+-----------------+

Classpath analyzer
__________________

Analyzer which instances a Lucene's `analyzer <https://lucene.apache.org/core/5_3_0/core/org/apache/lucene/analysis/Analyzer.html>`__
present in classpath.

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            analyzers : {
                an_analyzer : {
                    type  : "classpath",
                    class : "org.apache.lucene.analysis.en.EnglishAnalyzer"
                }
            }
        }'
    };

Snowball analyzer
_________________

Analyzer using a `http://snowball.tartarus.org/ <http://snowball.tartarus.org/>`__ snowball filter `SnowballFilter <https://lucene.apache.org/core/5_3_0/analyzers-common/org/apache/lucene/analysis/snowball/SnowballFilter.html>`__

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            analyzers : {
                an_analyzer : {
                    type  : "snowball",
                    language : "English",
                    stopwords : "a,an,the,this,that"
                }
            }
        }'
    };

Supported languages: English, French, Spanish, Portuguese, Italian, Romanian, German, Dutch, Swedish, Norwegian,
Danish, Russian, Finnish, Irish, Hungarian, Turkish, Armenian, Basque and Catalan

Mappers
=======

Field mapping definition options specify how the CQL rows will be mapped to Lucene documents.
Several mappers can be applied to the same CQL column/s.
Details and default values are listed in the table below.

+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| Mapper type                         | Option          | Value type      | Default value                  | Mandatory |
+=====================================+=================+=================+================================+===========+
| `bigdec <#big-decimal-mapper>`__    | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | integer_digits  | integer         | 32                             | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | decimal_digits  | integer         | 32                             | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `bigint <#big-integer-mapper>`__    | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | digits          | integer         | 32                             | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `bitemporal <#bitemporal-mapper>`__ | vt_from         | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | vt_to           | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | tt_from         | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | tt_to           | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | pattern         | string          | yyyy/MM/dd HH:mm:ss.SSS Z      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | now_value       | object          | Long.MAX_VALUE                 | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `blob <#blob-mapper>`__             | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `boolean <#boolean-mapper>`__       | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `date <#date-mapper>`__             | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | pattern         | string          | yyyy/MM/dd HH:mm:ss.SSS Z      | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `date_range <#daterange-mapper>`__  | from            | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | to              | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | pattern         | string          | yyyy/MM/dd HH:mm:ss.SSS Z      | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `double <#double-mapper>`__         | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | boost           | integer         | 0.1f                           | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `float <#float-mapper>`__           | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | boost           | integer         | 0.1f                           | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `geo_point <#geo-point-mapper>`__   | latitude        | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | longitude       | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | max_levels      | integer         | 11                             | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `geo_shape <#geo-shape-mapper>`__   | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | max_levels      | integer         | 11                             | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | transformations | array           |                                | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `inet <#inet-mapper>`__             | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `integer <#integer-mapper>`__       | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | boost           | integer         | 0.1f                           | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `long <#long-mapper>`__             | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | boost           | integer         | 0.1f                           | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `string <#string-mapper>`__         | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `text <#text-mapper>`__             | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | analyzer        | string          | default_analyzer of the schema | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `uuid <#uuid-mapper>`__             | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+

Most mappers have an ``indexed`` option indicating if the field is searchable, it is true by default.
There is also a ``sorted`` option specifying if it's possible to sort rows by the corresponding field, false by default.
List and set columns can't be sorted because they produce multivalued fields.
These options should be set to false when no needed in order to have a smaller and faster index.

All mappers have a ``validated`` option indicating if the mapped column values must be validated at CQL level
before performing the distributed write operation.
If this option is set then the coordinator node will throw an error on writes containing values that can't be mapped,
causing the failure of all the write operation and notifying the client about the failure cause.
If validation is not set, which is the default setting, writes to C* will never fail due to the index.
Instead, each failing column value will be silently discarded,
and the error message will be just logged in the implied nodes.
This option is useful to avoid writes containing values that can't be searched afterwards,
and can also be used as a generic data validation layer.
Note that mappers affecting several columns at a time, such as ``date_range``,``geo_point`` and ``bitemporal``,
need to have all the involved columns to perform validation,
so no partial columns update will be allowed when validation is active.

Cassandra allows only one custom per-row index per table, and it does not allow a modify operation on indexes.
So, to modify an index it needs to be deleted first and created again.

Big decimal mapper
__________________

Maps arbitrary precision decimal values.

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                bigdecimal : {
                    type           : "bigdec",
                    integer_digits : 2,
                    decimal_digits : 2,
                    indexed        : true,
                    sorted         : false,
                    validated      : true,
                    column         : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, decimal, double, float, int, smallint, text, tinyint, varchar, varint

Big integer mapper
__________________

Maps arbitrary precision integer values.

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                biginteger : {
                    type      : "bigint",
                    digits    : 10,
                    indexed   : true,
                    sorted    : false,
                    validated : true,
                    column    : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, int, smallint, text, tinyint, varchar, varint

Bitemporal mapper
_________________

Maps four columns containing the four columns of a bitemporal fact.

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                bitemporal : {
                    type      : "bitemporal",
                    vt_from   : "vt_from",
                    vt_to     : "vt_to",
                    tt_from   : "tt_from",
                    tt_to     : "tt_to",
                    validated : true,
                    pattern   : "yyyy/MM/dd HH:mm:ss.SSS";,
                    now_value : "3000/01/01 00:00:00.000",
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, date, int, text, timestamp, timeuuid, varchar, varint

Blob mapper
___________

Maps a blob value.

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                blob : {
                    type    : "bytes",
                    indexed : true,
                    sorted  : false,
                    column  : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, blob,  text, varchar

Boolean mapper
______________

Maps a boolean value.

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                bool : {
                    type      : "boolean",
                    indexed   : true,
                    sorted    : false,
                    validated : true,
                    column    : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, boolean , text, varchar

Date mapper
___________

Maps dates using a either a pattern, an UNIX timestamp or a time UUID.

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                date : {
                    type      : "date",
                    validated : true,
                    pattern   : "yyyy/MM/dd HH:mm:ss.SSS",
                    indexed   : true,
                    sorted    : false,
                    column    : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, date, int, text, timestamp, timeuuid, varchar, varint

Date range mapper
_________________

Maps a time duration/period defined by a start date and a stop date.

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                date_range : {
                    type      : "date_range",
                    validated : true,
                    from      : "range_from",
                    to        : "range_to",
                    pattern   : "yyyy/MM/dd HH:mm:ss.SSS"
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, date, int, text, timestamp, timeuuid, varchar, varint

Double mapper
_____________

Maps a 64-bit decimal number.

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                double : {
                    type      : "double",
                    boost     : 2.0,
                    indexed   : true,
                    sorted    : false,
                    validated : true,
                    column    : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, decimal, double, float, int, smallint, text, timestamp,  tinyint, varchar, varint

Float mapper
____________

Maps a 32-bit decimal number.

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                float : {
                    type      : "float",
                    boost     : 2.0,
                    indexed   : true,
                    sorted    : false,
                    validated : true,
                    column    : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, decimal, double, float, int, smallint, timestamp, tinyint, varchar, varint

Geo point mapper
________________

Maps a geospatial location (point) defined by two columns containing a latitude and a longitude.

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                geo_point : {
                    type       : "geo_point",
                    validated  : true,
                    latitude   : "lat",
                    longitude  : "long",
                    max_levels : 15
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, decimal, double, float, int, smallint, text, timestamp, varchar, varint

Geo shape mapper
________________

Maps a geographical shape stored in a text column with `Well Known Text (WKT) <http://en.wikipedia.org/wiki/Well-known_text>`__
format. The supported WKT shapes are point, linestring, polygon, multipoint, multilinestring and multipolygon.

It is possible to specify a sequence of `geometrical transformations <#transformations>`__ to be applied to the shape
before indexing it. It could be used for indexing only the centroid of the shape, or a buffer around it, etc.

This mapper depends on `Java Topology Suite (JTS) <http://www.vividsolutions.com/jts>`__.
This library can't be distributed together with this project due to license compatibility problems, but you can add it
by putting `jts-core-1.14.0.jar <http://search.maven.org/remotecontent?filepath=com/vividsolutions/jts-core/1.14.0/jts-core-1.14.0.jar>`__
into your Cassandra installation lib directory.

**Example 1:**

.. code-block:: sql

    CREATE TABLE IF NOT EXISTS test (
        id int,
        shape text,
        lucene text,
        PRIMARY KEY (id)
    );

    INSERT INTO test(id, shape) VALUES (1, 'POINT(-0.13 51.50)');
    INSERT INTO test(id, shape) VALUES (2, 'LINESTRING(-0.25 51.52, -0.08 51.39, -0.02 51.42)');
    INSERT INTO test(id, shape) VALUES (3, 'POLYGON((-0.07 51.63, 0.03 51.54, 0.05 51.65, -0.07 51.63))');
    INSERT INTO test(id, shape) VALUES (4, 'MULTIPOINT(-0.65 52.60, -1.00 51.76, -0.65 52.60)');
    INSERT INTO test(id, shape) VALUES (5, 'MULTILINESTRING((-0.43 51.56, -0.33 51.35, -0.13 51.35),
                                                            (-0.25 51.56, -0.14 51.48))');
    INSERT INTO test(id, shape) VALUES (6, 'MULTIPOLYGON(((-0.51 51.58, -0.18 51.14, 0.49 51.73, -0.51 51.58),
                                                          (-0.25 51.54, -0.12 51.32, 0.16 51.59, -0.25 51.54)))');

    CREATE CUSTOM INDEX test_index on test()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                shape : {
                    type       : "geo_shape",
                    max_levels : 15
                }
            }
        }'
    };

**Example 2:** Index only the centroid of the WKT shape contained in the indexed column:

.. image:: /doc/resources/geo_shape_mapper_example_2.png
    :width: 100%
    :alt: search by shape
    :align: center

.. code-block:: sql

    CREATE TABLE IF NOT EXISTS cities (
        name text,
        shape text,
        lucene text,
        PRIMARY KEY (name)
    );

    INSERT INTO cities(name, shape) VALUES ('birmingham', 'POLYGON((-2.25 52.63, -2.26 52.49, -2.13 52.36, -1.80 52.34, -1.57 52.54, -1.89 52.67, -2.25 52.63))');
    INSERT INTO cities(name, shape) VALUES ('london', 'POLYGON((-0.55 51.50, -0.13 51.19, 0.21 51.35, 0.30 51.62, -0.02 51.75, -0.34 51.69, -0.55 51.50))');

    CREATE CUSTOM INDEX cities_index on cities()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                shape : {
                    type            : "geo_shape",
                    max_levels      : 15,
                    transformations : [{type:"centroid"}]
                }
            }
        }'
    };

**Example 3:** Index a buffer 50 kilometres around the area of a city:

.. image:: /doc/resources/geo_shape_mapper_example_3.png
    :width: 100%
    :alt: search by shape
    :align: center

.. code-block:: sql

    CREATE TABLE IF NOT EXISTS cities (
        name text,
        shape text,
        lucene text,
        PRIMARY KEY (name)
    );

    INSERT INTO cities(name, shape) VALUES ('birmingham', 'POLYGON((-2.25 52.63, -2.26 52.49, -2.13 52.36, -1.80 52.34, -1.57 52.54, -1.89 52.67, -2.25 52.63))');
    INSERT INTO cities(name, shape) VALUES ('london', 'POLYGON((-0.55 51.50, -0.13 51.19, 0.21 51.35, 0.30 51.62, -0.02 51.75, -0.34 51.69, -0.55 51.50))');

    CREATE CUSTOM INDEX cities_index on cities()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                shape : {
                    type            : "geo_shape",
                    max_levels      : 15,
                    transformations : [{type:"buffer", min_distance:"50km"}]
                }
            }
        }'
    };

**Example 4:** Index a buffer 50 kilometres around the borders of a country:

.. image:: /doc/resources/geo_shape_mapper_example_4.png
    :width: 100%
    :alt: search by shape
    :align: center

.. code-block:: sql

    CREATE TABLE IF NOT EXISTS borders (
        country text,
        shape text,
        lucene text,
        PRIMARY KEY (country)
    );

    INSERT INTO borders(country, shape) VALUES ('france', 'LINESTRING(-1.8037198483943 43.463094234466, -1.3642667233943 43.331258296966, -1.3642667233943 43.111531734466, -0.74903234839434 42.979695796966, -0.66114172339434 42.847859859466, -0.17774328589434 42.891805171966, -0.089852660894337 42.759969234466, 0.61327233910569 42.716023921966, 0.61327233910569 42.891805171966, 1.3163973391057 42.759969234466, 1.4482332766057 42.672078609466, 1.4482332766057 42.496297359466, 1.6240145266057 42.496297359466, 1.6679598391057 42.540242671966, 2.0195223391057 42.408406734466, 2.2392489016057 42.496297359466, 2.5908114016057 42.408406734466, 2.8984285891057 42.496297359466, 3.2060457766057 42.408406734466)');
    INSERT INTO borders(country, shape) VALUES ('portugal', 'LINESTRING(-8.8789151608943 41.925008296966, -8.2636807858943 42.100789546966, -8.1318448483943 42.056844234466, -8.1757901608943 41.881062984466, -7.8242276608943 41.793172359466, -7.7802823483943 41.925008296966, -7.1650479733943 41.925008296966, -7.1211026608943 42.012898921966, -6.5498135983943 42.056844234466, -6.5498135983943 41.661336421966, -6.1982510983943 41.661336421966, -6.3740323483943 41.353719234466, -6.9013760983943 41.002156734466, -6.7255948483943 40.738484859466, -6.8134854733943 40.474812984466, -7.0771573483943 40.167195796966, -6.9013760983943 40.123250484466, -6.9892667233943 39.683797359466, -7.4726651608943 39.683797359466, -7.2529385983943 39.464070796966, -7.2529385983943 39.156453609466, -7.0771573483943 39.112508296966, -7.0771573483943 38.936727046966, -7.2529385983943 38.585164546966, -7.1650479733943 38.277547359466, -6.9013760983943 38.277547359466, -7.1211026608943 38.057820796966, -7.4726651608943 37.706258296966, -7.3408292233943 37.178914546966)');

    CREATE CUSTOM INDEX borders_index on borders()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                shape : {
                    type            : "geo_shape",
                    max_levels      : 15,
                    transformations : [{type:"buffer", max_distance:"50km"}]
                }
            }
        }'
    };


Supported CQL types: ascii, text, and varchar

Inet mapper
___________

Maps an IP address. Either IPv4 and IPv6 are supported.

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                inet : {
                    type      : "inet",
                    indexed   : true,
                    sorted    : false,
                    validated : true,
                    column    : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, inet, text, varchar

Integer mapper
______________

Maps a 32-bit integer number.

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                integer : {
                    type      : "integer",
                    indexed   : true,
                    sorted    : false,
                    validated : true,
                    column    : "column_name"
                    boost     : 2.0,
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, decimal, double, float, int, smallint, text, timestamp, tinyint, varchar, varint

Long mapper
___________

Maps a 64-bit integer number.

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                long : {
                    type      : "long",
                    indexed   : true,
                    sorted    : false,
                    validated : true,
                    column    : "column_name"
                    boost     : 2.0,
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, decimal, double, float, int, smallint, text, timestamp, tinyint, varchar, varint

String mapper
_____________

Maps a not-analyzed text value.

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                string : {
                    type           : "string",
                    indexed        : true,
                    sorted         : false,
                    validated      : true,
                    column         : "column_name"
                    case_sensitive : false,
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, blob, boolean, double, float, inet, int, smallint, text, timestamp, timeuuid, tinyint, uuid, varchar, varint

Text mapper
___________

Maps a language-aware text value analyzed according to the specified analyzer.

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            analyzers : {
                my_custom_analyzer : {
                      type      : "snowball",
                      language  : "Spanish",
                      stopwords : "el,la,lo,loas,las,a,ante,bajo,cabe,con,contra"
                }
            },
            fields : {
                text : {
                    type      : "text",
                    indexed   : true,
                    sorted    : false,
                    validated : true,
                    column    : "column_name"
                    analyzer  : "my_custom_analyzer",
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, blob, boolean, double, float, inet, int, smallint, text, timestamp, timeuuid, tinyint, uuid, varchar, varint

UUID mapper
___________

Maps an UUID value.

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                id : {
                    type      : "uuid",
                    indexed   : true,
                    sorted    : false,
                    validated : true,
                    column    : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, text, timeuuid, uuid, varchar


Example
=======

This code below and the one for creating the corresponding keyspace and
table is available in a CQL script that can be sourced from the
Cassandra shell:
`test-users-create.cql </doc/resources/test-users-create.cql>`__.

.. code-block:: sql

    CREATE CUSTOM INDEX IF NOT EXISTS users_index
    ON test.users ()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds'        : '60',
        'ram_buffer_mb'          : '64',
        'max_merge_mb'           : '5',
        'max_cached_mb'          : '30',
        'excluded_data_centers'  : 'dc2,dc3',
        'token_range_cache_size' : '16',
        'search_cache_size'      : '16',
        'schema' : '{
            analyzers : {
                my_custom_analyzer : {
                    type      : "snowball",
                    language  : "Spanish",
                    stopwords : "el,la,lo,loas,las,a,ante,bajo,cabe,con,contra"
                }
            },
            default_analyzer : "english",
            fields : {
                name     : {type : "string"},
                gender   : {type : "string", validated : true},
                animal   : {type : "string"},
                age      : {type : "integer"},
                food     : {type : "string"},
                number   : {type : "integer", indexed : false, sorted : true},
                bool     : {type : "boolean"},
                date     : {type : "date", sorted : true, validated : true, pattern : "yyyy/MM/dd"},
                duration : {type : "date_range", from : "start_date", to : "stop_date", pattern : "yyyy/MM/dd"},
                place    : {type : "geo_point", latitude : "latitude", longitude : "longitude"},
                mapz     : {type : "string", sorted : true},
                setz     : {type : "string"},
                listz    : {type : "string"},
                phrase   : {type : "text", analyzer : "my_custom_analyzer"}
            }
        }'
    };

Searching
*********

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table_name>
    WHERE expr(<index_name>, '{ (   filter  : <filter>  )?
                                ( , query   : <query>   )?
                                ( , sort    : <sort>    )?
                                ( , refresh : ( true | false ) )?
                               }');

where <filter> and <query> are a JSON object:

.. code-block:: sql

    <filter> := { type : <type> (, <option> : ( <value> | <value_list> ) )+ }
    <query>  := { type : <type> (, <option> : ( <value> | <value_list> ) )+ }

and <sort> is another JSON object:

.. code-block:: sql

        <sort> := { fields : <sort_field> (, <sort_field> )* }
        <sort_field> := <simple_sort_field> | <geo_distance_sort_field>
        <simple_sort_field> := {(type: "simple",)? field : <field> (, reverse : <reverse> )? }
        <geo_distance_sort_field> := {  type: "geo_distance",
                                        mapper : <field>,
                                        latitude : <Double>,
                                        longitude: <Double>
                                        (, reverse : <reverse> )? }

When searching by ``filter``, without any ``query`` or ``sort`` defined,
then the results are returned in the Cassandra’s natural order, which is
defined by the partitioner and the column name comparator. When searching
by ``query``, results are returned sorted by descending relevance. Sort option is used
to specify the order in which the indexed rows will be traversed. When
simple_sort_field sorting is used, the query scoring is delayed.

Geo_distance_sort_field is use to sort Rows by min distance to point
indicating the GeoPointMapper to use by mapper field

Relevance queries must touch all the nodes in the ring in order to find
the globally best results, so you should prefer filters over queries
when no relevance nor sorting are needed.

The ``refresh`` boolean option indicates if the search must commit pending
writes and refresh the Lucene IndexSearcher before being performed. This
way a search with ``refresh`` set to true will view the most recent changes
done to the index, independently of the index auto-refresh time.
Please note that it is a costly operation, so you should not use it
unless it is strictly necessary. The default value is false. You can
explicitly refresh all the index shards with an empty search with consistency
``ALL``, and the return to your desired consistency level:

.. code-block:: sql

    CONSISTENCY ALL
    SELECT * FROM <table> WHERE expr(<index_name>, '{refresh:true}');
    CONSISTENCY QUORUM

This way the subsequent searches will view all the writes done before this
operation, without needing to wait for the index auto refresh. It is useful to
perform this operation before searching after a bulk data load.

Types of search and their options are summarized in the table below.
Details for each of them are available in individual sections and the
examples can be downloaded as a CQL script:
`extended-search-examples.cql </doc/resources/extended-search-examples.cql>`__.

In addition to the options described in the table, all search types have
a “\ **boost**\ ” option that acts as a weight on the resulting score.

+-----------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| Search type                             | Option          | Value type      | Default value                  | Mandatory |
+=========================================+=================+=================+================================+===========+
| `All <#all-search>`__                   |                 |                 |                                |           |
+-----------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `Bitemporal <#bitemporal-search>`__     | field           | string          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | vt_from         | string/long     | 0L                             | No        |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | vt_to           | string/long     | Long.MAX_VALUE                 | No        |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | tt_from         | string/long     | 0L                             | No        |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | tt_to           | string/long     | Long.MAX_VALUE                 | No        |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | operation       | string          | intersects                     | No        |
+-----------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `Boolean <#boolean-search>`__           | must            | search          |                                | No        |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | should          | search          |                                | No        |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | not             | search          |                                | No        |
+-----------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `Contains <#contains-search>`__         | field           | string          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | values          | array           |                                | Yes       |
+-----------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `Date range <#date-range-search>`__     | field           | string          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | from            | string/long     | 0                              | No        |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | to              | string/long     | Long.MAX_VALUE                 | No        |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | operation       | string          | is_within                      | No        |
+-----------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `Fuzzy <#fuzzy-search>`__               | field           | string          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | value           | string          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | max_edits       | integer         | 2                              | No        |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | prefix_length   | integer         | 0                              | No        |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | max_expansions  | integer         | 50                             | No        |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | transpositions  | boolean         | true                           | No        |
+-----------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `Geo bounding box <#geo-bbox-search>`__ | field           | string          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | min_latitude    | double          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | max_latitude    | double          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | min_longitude   | double          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | max_longitude   | double          |                                | Yes       |
+-----------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `Geo distance <#geo-distance-search>`__ | field           | string          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | latitude        | double          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | longitude       | double          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | max_distance    | string          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | min_distance    | string          |                                | No        |
+-----------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `Geo shape <#geo-shape-search>`__       | field           | string          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | shape           | string (WKT)    |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | operation       | string          | is_within                      | No        |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | transformations | array           |                                | No        |
+-----------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `Match <#match-search>`__               | field           | string          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | value           | any             |                                | Yes       |
+-----------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `None <#none-search>`__                 |                 |                 |                                |           |
+-----------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `Phrase <#phrase-search>`__             | field           | string          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | value           | string          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | slop            | integer         | 0                              | No        |
+-----------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `Prefix <#prefix-search>`__             | field           | string          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | value           | string          |                                | Yes       |
+-----------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `Range <#range-search>`__               | field           | string          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | lower           | any             |                                | No        |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | upper           | any             |                                | No        |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | include_lower   | boolean         | false                          | No        |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | include_upper   | boolean         | false                          | No        |
+-----------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `Regexp <#regexp-search>`__             | field           | string          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | value           | string          |                                | Yes       |
+-----------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `Wildcard <#wildcard-search>`__         | field           | string          |                                | Yes       |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | value           | string          |                                | Yes       |
+-----------------------------------------+-----------------+-----------------+--------------------------------+-----------+

All search
==========

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE expr(<index_name>, '{ (filter | query) : { type  : "all"} }');

**Example:** search for all the indexed rows

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : { type  : "all" }}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(all()).build());



Bitemporal search
=================

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE expr(<index_name>, '{ (filter | query) : {
                                type       : "bitemporal",
                                (vt_from   : <vt_from> ,)?
                                (vt_to     : <vt_to> ,)?
                                (tt_from   : <tt_from> ,)?
                                (tt_to     : <tt_to> ,)?
                                (operation : <operation> )?
                              }}');

where:

-  **vt\_from** (default = 0L): a string or a number being the beginning of the valid date range.
-  **vt\_to** (default = Long.MAX_VALUE): a string or a number being the end of the valid date range.
-  **tt\_from** (default = 0L): a string or a number being the beginning of the transaction date range.
-  **tt\_to** (default = Long.MAX_VALUE): a string or a number being the end of the transaction date range.
-  **operation** (default = intersects): the spatial operation to be performed, it can be **intersects**,
   **contains** and **is\_within**.

Bitemporal searching is so complex that we want to stay an example.

We want to implement a system for census bureau to track where resides a citizen and when the censyus bureau knows this.

First we create the table where all this data resides:

.. code-block:: sql

    CREATE KEYSPACE test with replication = {'class':'SimpleStrategy', 'replication_factor': 1};
    USE test;

    CREATE TABLE census (
        name text,
        city text,
        vt_from text,
        vt_to text,
        tt_from text,
        tt_to text,
        PRIMARY KEY (name, vt_from, tt_from)
    );


Second, we create the index:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                bitemporal : {
                    type      : "bitemporal",
                    tt_from   : "tt_from",
                    tt_to     : "tt_to",
                    vt_from   : "vt_from",
                    vt_to     : "vt_to",
                    pattern   : "yyyy/MM/dd",
                    now_value : "2200/12/31"}
            }
    }'};

We insert the population of 5 citizens lives in each city from 2015/01/01 until now


.. code-block:: sql

    INSERT INTO census(name, city, vt_from, vt_to, tt_from, tt_to)
    VALUES ('John', 'Madrid', '2015/01/01', '2200/12/31', '2015/01/01', '2200/12/31');

    INSERT INTO census(name, city, vt_from, vt_to, tt_from, tt_to)
    VALUES ('Margaret', 'Barcelona', '2015/01/01', '2200/12/31', '2015/01/01', '2200/12/31');

    INSERT INTO census(name, city, vt_from, vt_to, tt_from, tt_to)
    VALUES ('Cristian', 'Ceuta', '2015/01/01', '2200/12/31', '2015/01/01', '2200/12/31');

    INSERT INTO census(name, city, vt_from, vt_to, tt_from, tt_to)
    VALUES ('Edward', 'New York','2015/01/01', '2200/12/31', '2015/01/01', '2200/12/31');

    INSERT INTO census(name, city, vt_from, vt_to, tt_from, tt_to)
    VALUES ('Johnatan', 'San Francisco', '2015/01/01', '2200/12/31', '2015/01/01', '2200/12/31');


John moves to Amsterdam in '2015/03/05' but he does not comunicate this to census bureau until '2015/06/29' because he need it to apply for taxes reduction.

So, the system need to update last information from John, and insert the new. This is done with batch execution updating the transaction time end of previous data and inserting new.


.. code-block:: sql

    BEGIN BATCH
        -- This update until when the system believed in this false information
        UPDATE census SET tt_to = '2015/06/29' WHERE name = 'John' AND vt_from = '2015/01/01' AND tt_from = '2015/01/01' IF tt_to = '2200/12/31';

        -- Here inserts the new knowledge about the period where john resided in Madrid
        INSERT INTO census(name, city, vt_from, vt_to, tt_from, tt_to) VALUES ('John', 'Madrid', '2015/01/01', '2015/03/04', '2015/06/30', '2200/12/31');

        -- This inserts the new knowledge about the period where john resides in Amsterdam
        INSERT INTO census(name, city, vt_from, vt_to, tt_from, tt_to) VALUES ('John', 'Amsterdam', '2015/03/05', '2200/12/31', '2015/06/30', '2200/12/31');
    APPLY BATCH;

Now , we can see the main difference between valid time and transaction time. The system knows from '2015/01/01' to '2015/06/29' that John resides in Madrid from '2015/01/01' until now, and resides in Amsterdam from '2015/03/05' until now.

There are several types of queries concerning this type of indexing

If its needed to get all the data in the table:

.. code-block:: sql

    SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM census ;


If you want to know what is the last info about where John resides, you perform a query with tt_from and tt_to setted to now_value:

.. code-block:: sql

    SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM census
    WHERE expr(tweets_index, '{
        filter : {
            type    : "bitemporal",
            field   : "bitemporal",
            vt_from : 0,
            vt_to   : "2200/12/31",
            tt_from : "2200/12/31",
            tt_to   : "2200/12/31"
        }
    }')
    AND name='John';

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM test.census WHERE expr(census_index, '%s')",
        search().filter(bitemporal("bitemporal").ttFrom("2200/12/31")
                                                .ttTo("2200/12/31")
                                                .vtFrom(0)
                                                .vtTo("2200/12/31").build());



If you want to know what is the last info about where John resides now, you perform a query with tt_from, tt_to, vt_from, vt_to setted to now_value:

.. code-block:: sql

    SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM census WHERE
    expr(tweets_index,'{
        filter : {
            type    : "bitemporal",
            field   : "bitemporal",
            vt_from : "2200/12/31",
            vt_to   : "2200/12/31",
            tt_from : "2200/12/31",
            tt_to   : "2200/12/31"
        }
    }')
    AND name='John';

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM test.census WHERE expr(census_index, '%s')",
        search().filter(bitemporal("bitemporal").ttFrom("2200/12/31")
                                                .ttTo("2200/12/31")
                                                .vtFrom("2200/12/31")
                                                .vtTo("2200/12/31")).build());


If the test case needs to know what the system was thinking at '2015/03/01' about where John resides in "2015/03/01".

.. code-block:: sql

    SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM census
    WHERE expr(tweets_index, '{
        filter : {
            type    : "bitemporal",
            field   : "bitemporal",
            vt_from : "2015/03/01",
            vt_to   : "2015/03/01",
            tt_from : "2015/03/01",
            tt_to   : "2015/03/01"
        }
    }')
    AND name = 'John';

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM test.census WHERE expr(census_index, '%s')",
        search().filter(bitemporal("bitemporal").ttFrom("2015/03/01")
                                                .ttTo("2015/03/01")
                                                .vtFrom("2015/03/01")
                                                .vtTo("2015/03/01")).build());

If the test case needs to know what the system was thinking at '2015/07/05' about where John resides:

.. code-block:: sql

    SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM census WHERE
    expr(tweets_index,'{
        filter : {
            type    : "bitemporal",
            field   : "bitemporal",
            tt_from : "2015/07/05",
            tt_to   : "2015/07/05"
        }
    }')
    AND name='John';

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM test.census WHERE expr(census_index, '%s')",
        search().filter(bitemporal("bitemporal").ttFrom("2015/07/05").ttTo("2015/07/05").build());


This code is available in CQL script here: `example_bitemporal.cql </doc/resources/example_bitemporal.cql>`__.

Boolean search
==============

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE expr(<index_name>, '{ (filter | query) : {
                               type     : "boolean",
                               ( must   : [(search,)?] , )?
                               ( should : [(search,)?] , )?
                               ( not    : [(search,)?] , )? } }');

where:

-  **must**: represents the conjunction of searches: search_1 AND search_2
   AND … AND search_n
-  **should**: represents the disjunction of searches: search_1 OR search_2
   OR … OR search_n
-  **not**: represents the negation of the disjunction of searches:
   NOT(search_1 OR search_2 OR … OR search_n)

Since "not" will be applied to the results of a "must" or "should"
condition, it can not be used in isolation.

**Example 1:** search for rows where name ends with “a” AND food starts
with “tu”:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : {
                            type : "boolean",
                            must : [{type : "wildcard", field : "name", value : "*a"},
                                    {type : "wildcard", field : "food", value : "tu*"}]}}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(bool().must(wildcard("name", "*a"), wildcard("food", "tu*"))).build());



**Example 2:** search for rows where food starts with “tu” but name does not end with “a”:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : {
                            type : "boolean",
                            not  : [{type : "wildcard", field : "name", value : "*a"}],
                            must : [{type : "wildcard", field : "food", value : "tu*"}]}}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(bool().not(wildcard("name", "*a")).must(wildcard("food", "tu*"))).build());


**Example 3:** search for rows where name ends with “a” or food starts with
“tu”:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : {
                            type   : "boolean",
                            should : [{type : "wildcard", field : "name", value : "*a"},
                                      {type : "wildcard", field : "food", value : "tu*"}]}}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(bool().should(wildcard("name", "*a"), wildcard("food", "tu*"))).build());


**Example 4:** will return zero rows independently of the index contents:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : { type   : "boolean"}}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    SResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(bool()).build());


**Example 5:** search for rows where name does not end with “a”, which is
a resource-intensive pure negation search:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : {
                            not  : [{type : "wildcard", field : "name", value : "*a"}]}}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(bool().not(wildcard("name", "*a"))).build());


Contains search
===============

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE expr(<index_name>, '{ (filter | query) : {
                                type   : "contains",
                                field  : <field_name> ,
                                values : <value_list> }}');

**Example 1:** search for rows where name matches “Alicia” or “mancha”:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : {
                            type   : "contains",
                            field  : "name",
                            values : ["Alicia", "mancha"] }}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(contains("name", "Alicia", "mancha").build());


**Example 2:** search for rows where date matches “2014/01/01″,
“2014/01/02″ or “2014/01/03″:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : {
                              type   : "contains",
                              field  : "date",
                              values : ["2014/01/01", "2014/01/02", "2014/01/03"] }}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(contains("date", "2014/01/01", "2014/01/02", "2014/01/03")).build());


Date range search
=================

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE expr(<index_name>, '{ (filter | query) : {
                                type  : "date_range",
                                (from : <from> ,)?
                                (to   : <to> ,)?
                                (operation: <operation> )?
                              }}');

where:

-  **from**: a string or a number being the beginning of the date
   range.
-  **to**: a string or a number being the end of the date range.
-  **operation**: the spatial operation to be performed, it can be
   **intersects**, **contains** and **is\_within**.

**Example 1:** will return rows where duration intersects "2014/01/01" and
"2014/12/31":

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{ filter : {
                        type      : "date_range",
                        field     : "duration",
                        from      : "2014/01/01",
                        to        : "2014/12/31",
                        operation : "intersects"}}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(dateRange("duration").from("2014/01/01").to("2014/12/31").operation("intersects")).build());


**Example 2:** search for rows where duration contains "2014/06/01" and
"2014/06/02":

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{ filter : {
                        type      : "date_range",
                        field     : "duration",
                        from      : "2014/06/01",
                        to        : "2014/06/02",
                        operation : "contains"}}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(dateRange("duration").from("2014/06/01").to("2014/06/02").operation("contains")).build());


**Example 3:** search for rows where duration is within "2014/01/01" and
"2014/12/31":

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{ filter : {
                        type      : "date_range",
                        field     : "duration",
                        from      : "2014/01/01",
                        to        : "2014/12/31",
                        operation : "is_within"}}');


Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(dateRange("duration").from("2014/01/01").to("2014/12/31").operation("is_within")).build());


Fuzzy search
============

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE expr(<index_name>, '{ (filter | query) : {
                                type  : "fuzzy",
                                field : <field_name> ,
                                value : <value>
                                (, max_edits      : <max_edits> )?
                                (, prefix_length  : <prefix_length> )?
                                (, max_expansions : <max_expansion> )?
                                (, transpositions : <transposition> )?
                              }}');

where:

-  **max\_edits** (default = 2): a integer value between 0 and 2. Will
   return rows which distance from <value> to <field> content has a
   distance of at most <max\_edits>. Distance will be interpreted
   according to the value of “transpositions”.
-  **prefix\_length** (default = 0): an integer value being the length
   of the common non-fuzzy prefix
-  **max\_expansions** (default = 50): an integer for the maximum number
   of terms to match
-  **transpositions** (default = true): if transpositions should be
   treated as a primitive edit operation (`Damerau-Levenshtein
   distance <http://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance>`__).
   When false, comparisons will implement the classic `Levenshtein
   distance <http://en.wikipedia.org/wiki/Levenshtein_distance>`__.

**Example 1:** search for any rows where “phrase” contains a word that
differs in one edit operation from “puma”, such as “pumas”:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : { type      : "fuzzy",
                                         field     : "phrase",
                                         value     : "puma",
                                         max_edits : 1 }}');


Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(fuzzy("phrase", "puma").maxEdits(1)).build());


**Example 2:** same as example 1 but will limit the results to rows where
phrase contains a word that starts with “pu”:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : { type          : "fuzzy",
                                         field         : "phrase",
                                         value         : "puma",
                                         max_edits     : 1,
                                         prefix_length : 2 }}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(fuzzy("phrase", "puma").maxEdits(1).prefixLength(2)).build());


Geo bbox search
===============

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE expr(<index_name>, '{ (filter | query) : {
                                type          : "geo_bbox",
                                field         : <field_name>,
                                min_latitude  : <min_latitude> ,
                                max_latitude  : <max_latitude> ,
                                min_longitude : <min_longitude> ,
                                max_longitude : <max_longitude>
                              }}');

where:

-  **min\_latitude** : a double value between -90 and 90 being the min
   allowed latitude.
-  **max\_latitude** : a double value between -90 and 90 being the max
   allowed latitude.
-  **min\_longitude** : a double value between -180 and 180 being the
   min allowed longitude.
-  **max\_longitude** : a double value between -180 and 180 being the
   max allowed longitude.

**Example 1:** search for any rows where “place” is formed by a latitude
between -90.0 and 90.0, and a longitude between -180.0 and
180.0:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : { type          : "geo_bbox",
                                         field         : "place",
                                         min_latitude  : -90.0,
                                         max_latitude  : 90.0,
                                         min_longitude : -180.0,
                                         max_longitude : 180.0 }}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(geoBBox("place", -180.0, 180.0, -90.0, 90.0)).build());


**Example 2:** search for any rows where “place” is formed by a latitude
between -90.0 and 90.0, and a longitude between 0.0 and
10.0:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : { type          : "geo_bbox",
                                         field         : "place",
                                         min_latitude  : -90.0,
                                         max_latitude  : 90.0,
                                         min_longitude : 0.0,
                                         max_longitude : 10.0 }}');


Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(geoBBox("place",0.0,10.0,-90.0,90.0)).build());


**Example 3:** search for any rows where “place” is formed by a latitude
between 0.0 and 10.0, and a longitude between -180.0 and
180.0 sorted by min distance to point [0.0, 0.0]:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{  filter : { type          : "geo_bbox",
                                           field         : "place",
                                           min_latitude  : 0.0,
                                           max_latitude  : 10.0,
                                           min_longitude : -180.0,
                                           max_longitude : 180.0
                                         },
                                sort   : { fields: [
                                          { type      : "geo_distance",
    					 	                mapper    : "geo_point",
    					 	                reverse   : false,
                                            latitude  : 0.0,
    					 	                longitude : 0.0
    					 	              }]
                                         }
                              }');


Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?) LIMIT 100",
        search().filter(geoBBox("place", -180.0, 180.0, 0.0, 10.0))
                .sort(geoDistanceSortField("geo_point", 0.0, 0.0).reverse(false)
                .build());

Geo distance search
===================

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE expr(<index_name>, '{ (filter | query) : {
                                type            : "geo_distance",
                                field           : <field_name> ,
                                latitude        : <latitude> ,
                                longitude       : <longitude> ,
                                max_distance    : <max_distance>
                                (, min_distance : <min_distance> )?
                              }}');

where:

-  **latitude** : a double value between -90 and 90 being the latitude
   of the reference point.
-  **longitude** : a double value between -180 and 180 being the
   longitude of the reference point.
-  **max\_distance** : a string value being the max allowed `distance <#distance>`__ from the reference point.
-  **min\_distance** : a string value being the min allowed `distance <#distance>`__ from the reference point.

**Example 1:** search for any rows where “place” is within one kilometer from the geo point (40.225479, -3.999278):

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : { type         : "geo_distance",
                                         field        : "place",
                                         latitude     : 40.225479,
                                         longitude    : -3.999278,
                                         max_distance : "1km"}}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(geoDistance("place", -3.999278d, 40.225479d, "1km").build());


**Example 2:** search for any rows where “place” is within one yard and ten
yards from the geo point (40.225479, -3.999278) sorted by min distance to point (40.225479, -3.999278):

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : { type         : "geo_distance",
                                         field        : "place",
                                         latitude     : 40.225479,
                                         longitude    : -3.999278,
                                         max_distance : "10yd" ,
                                         min_distance : "1yd" },
                              sort   : { fields: [
                                        { type      : "geo_distance",
    					 	              mapper    : "geo_point",
    					 	              reverse   : false,
                                          latitude  : 40.225479,
    					 	              longitude : -3.999278}
    					 	              ]
                                        }
                        }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?) LIMIT 100",
        search().filter(geoDistance("place", -3.999278d, 40.225479d, "10yd").minDistance("1yd"))
                .sort(geoDistanceSortField("geo_point", -3.999278, 40.225479).reverse(false))
                .build());

Geo shape search
================

Searches for `geographical points <#geo-point-mapper>`__ or `geographical shapes <#geo-shape-mapper>`__ related to a
specified shape with `Well Known Text (WKT) <http://en.wikipedia.org/wiki/Well-known_text>`__ format.
The supported WKT shapes are point, linestring, polygon, multipoint, multilinestring and multipolygon.

This search type depends on `Java Topology Suite (JTS) <http://www.vividsolutions.com/jts>`__.
This library can't be distributed together with this project due to license compatibility problems, but you can add it
by putting `jts-core-1.14.0.jar <http://search.maven.org/remotecontent?filepath=com/vividsolutions/jts-core/1.14.0/jts-core-1.14.0.jar>`__
into your Cassandra installation lib directory.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE expr(<index_name, '{ (filter | query) : {
                                type               : "geo_shape",
                                field              : <fieldname> ,
                                shape              : <shape>
                                (, operation       : <operation>)?
                                (, transformations : [(<transformation>,)?])?
                              }}');

where:

-  **shape** : a double value between -90 and 90 being the latitude
   of the reference point.
-  **operation** : the type of spatial operation to be performed. The possible values are "intersects", "is_within" and
"contains". Defaults to "is_within".
-  **transformation** : a list of `geometrical transformations <#transformations>`__ to be applied to the shape before using it for searching.

**Example 1:** search for shapes within a polygon:

.. image:: /doc/resources/geo_shape_condition_example_1.png
    :width: 100%
    :alt: search by shape
    :align: center

.. code-block:: sql

    SELECT * FROM test
    WHERE expr(test_index, '{filter : {
                               type : "geo_shape",
                              field : "place",
                              shape : "POLYGON((-0.07 51.63, 0.03 51.54, 0.05 51.65, -0.07 51.63))" }}';

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
      "SELECT * FROM TABLE test WHERE expr(test_index, ?)",
      search().filter(geoShape("place", "POLYGON((-0.07 51.63, 0.03 51.54, 0.05 51.65, -0.07 51.63))")).build());

**Example 2:** search for shapes intersecting with a shape defined by a buffer 10 kilometers around a segment of the
Florida's coastline:

.. image:: /doc/resources/geo_shape_condition_example_2.png
    :width: 100%
    :alt: buffer transformation
    :align: center

.. code-block:: sql

    SELECT * FROM test
    WHERE expr(test_index, '{filter : {
                  type : "geo_shape",
                 field : "place",
              relation : "intersects",
                 shape : "LINESTRING(-80.90 29.05, -80.51 28.47, -80.60 28.12, -80.00 26.85, -80.05 26.37)",
       transformations : [{type:"buffer", max_distance:"10km"}] }}';

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM TABLE test WHERE expr(test_index, ?)",
        search().filter(geoShape("place", "POLYGON((-0.07 51.63, 0.03 51.54, 0.05 51.65, -0.07 51.63))")
             .operation("intersects").transform(bufferGeoTransformation().maxDistance("10km"))).build());


Match search
============

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE expr(<index_name>, '{ (filter | query) : {
                                  type  : "match",
                                  field : <field_name> ,
                                  value : <value> }}');

**Example 1:** search for rows where name matches “Alicia”:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : {
                           type  : "match",
                           field : "name",
                           value : "Alicia" }}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(match("name", "Alicia")).build());


**Example 2:** search for any rows where phrase contains “mancha”:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : {
                           type  : "match",
                           field : "phrase",
                           value : "mancha" }}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(match("phrase", "mancha").build());


**Example 3:** search for rows where date matches “2014/01/01″:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : {
                           type  : "match",
                           field : "date",
                           value : "2014/01/01" }}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(match("date", "2014/01/01")).build());


None search
===========

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE expr(<index_name>, '{ (filter | query) : { type  : "none"} }');

**Example:** will return no one of the indexed rows:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : { type  : "none" }}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(none()).build());

Phrase search
=============

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE expr(<index_name>, '{ (filter | query) : {
                                type  :"phrase",
                                field : <field_name> ,
                                value : <value>
                                (, slop : <slop> )?
                            }}');

where:

-  **values**: an ordered list of values.
-  **slop** (default = 0): number of words permitted between words.

**Example 1:** search for rows where “phrase” contains the word “camisa”
followed by the word “manchada”:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : {
                          type   : "phrase",
                          field  : "phrase",
                          values : "camisa manchada" }}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(phrase("phrase", "camisa manchada")).build());

**Example 2:** search for rows where “phrase” contains the word “mancha”
followed by the word “camisa” having 0 to 2 words in between:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : {
                          type   : "phrase",
                          field  : "phrase",
                          values : "mancha camisa",
                          slop   : 2 }}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(phrase("phrase", "camisa manchada").slop(2)).build());

Prefix search
=============

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE expr(<index_name>, '{ (filter | query) : {
                                type  : "prefix",
                                field : <field_name> ,
                                value : <value> }}');

**Example:** search for rows where “phrase” contains a word starting with
“lu”. If the column is indexed as “text” and uses an analyzer, words
ignored by the analyzer will not be retrieved:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : {
                           type  : "prefix",
                           field : "phrase",
                           value : "lu" }}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(prefix("phrase", "lu")).build());

Range search
============

**Syntax:**

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{(filter | query) : {
                            type     : "range",
                            field    : <field_name>
                            (, lower : <min> , include_lower : <min_included> )?
                            (, upper : <max> , include_upper : <max_included> )?
                         }}');

where:

-  **lower**: lower bound of the range.
-  **include\_lower** (default = false): if the lower bound is included
   (left-closed range).
-  **upper**: upper bound of the range.
-  **include\_upper** (default = false): if the upper bound is included
   (right-closed range).

Lower and upper will default to :math:`-/+\\infty` for number. In the
case of byte and string like data (bytes, inet, string, text), all
values from lower up to upper will be returned if both are specified. If
only “lower” is specified, all rows with values from “lower” will be
returned. If only “upper” is specified then all rows with field values
up to “upper” will be returned. If both are omitted than all rows will
be returned.

**Example 1:** search for rows where *age* is in [1, ∞):

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : {
                            type          : "range",
                            field         : "age",
                            lower         : 1,
                            include_lower : true }}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(range("age").lower(1).includeLower(true)).build());

**Example 2:** search for rows where *age* is in (-∞, 0]:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : {
                            type          : "range",
                            field         : "age",
                            upper         : 0,
                            include_upper : true }}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(range("age").upper(0).includeUpper(true)).build());

**Example 3:** search for rows where *age* is in [-1, 1]:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : {
                            type          : "range",
                            field         : "age",
                            lower         : -1,
                            upper         : 1,
                            include_lower : true,
                            include_upper : true }}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(range("age").lower(-1).upper(1)
                                    .includeLower(true)
                                    .includeUpper(true)).build());

**Example 4:** search for rows where *date* is in [2014/01/01, 2014/01/02]:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : {
                            type          : "range",
                            field         : "date",
                            lower         : "2014/01/01",
                            upper         : "2014/01/02",
                            include_lower : true,
                            include_upper : true }}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(range("date").lower("2014/01/01")
                                     .upper( "2014/01/02")
                                     .includeLower(true)
                                     .includeUpper(true)).build());

Regexp search
=============

**Syntax:**

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{(filter | query) : {
                            type  : "regexp",
                            field : <field_name>,
                            value : <regexp>
                         }}');

where:

-  **value**: a regular expression. See
   `org.apache.lucene.util.automaton.RegExp <http://lucene.apache.org/core/4_6_1/core/org/apache/lucene/util/automaton/RegExp.html>`__
   for syntax reference.

**Example:** search for rows where name contains a word that starts with
“p” and a vowel repeated twice (e.g. “pape”):

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : {
                           type  : "regexp",
                           field : "name",
                           value : "[J][aeiou]{2}.*" }}');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(regexp("name", "[J][aeiou]{2}.*")).build());

Wildcard search
===============

**Syntax:**

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{(filter | query) : {
                            type  : "wildcard" ,
                            field : <field_name> ,
                            value : <wildcard_exp>
                         }}');

where:

-  **value**: a wildcard expression. Supported wildcards are \*, which
   matches any character sequence (including the empty one), and ?,
   which matches any single character. ” is the escape character.

**Example:** search for rows where food starts with or is “tu”:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE expr(users_index, '{filter : {
                           type  : "wildcard",
                           field : "food",
                           value : "tu*" }}');


Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM test.users WHERE expr(users_index, ?)",
        search().filter(wildcard("food", "tu*")).build());

Geographical elements
*********************

Geographical indexing and search make use of some common elements that are described in this section.

Distance
========

Both `geo distance search <#geo-distance-search>`__ and `buffer transformation <#buffer>`__ take a spatial distance as
argument. This distance is just a string with the form "1km", "1000m", etc. The following table shows the available
options for distance units. The default distance unit is metre.

+----------------------------+---------------+
|            Values          |      Unit     |
+============================+===============+
|            mm, millimetres |    millimetre |
+----------------------------+---------------+
|            cm, centimetres |    centimetre |
+----------------------------+---------------+
|             dm, decimetres |     decimetre |
+----------------------------+---------------+
|                  m, metres |         metre |
+----------------------------+---------------+
|            dam, decametres |     decametre |
+----------------------------+---------------+
|            hm, hectometres |    hectometre |
+----------------------------+---------------+
|             km, kilometres |     kilometre |
+----------------------------+---------------+
|                  ft, foots |          foot |
+----------------------------+---------------+
|                  yd, yards |          yard |
+----------------------------+---------------+
|                 in, inches |          inch |
+----------------------------+---------------+
|                  mi, miles |          mile |
+----------------------------+---------------+
| M, NM, mil, nautical_miles | nautical mile |
+----------------------------+---------------+

**Example:** the following `geo distance search <#geo-distance-search>`__ search for any rows where “place” is within
one kilometer from the geo point (40.225479, -3.999278). The distance is expressed in kilometers.

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type         : "geo_distance",
                                     field        : "place",
                                     latitude     : 40.225479,
                                     longitude    : -3.999278,
                                     max_distance : "1km"}}';


Transformations
===============

Both `geo shape mapper <#geo-shape-mapper>`__ and `geo shape search <#geo-shape-search>`__ take a  list of geometrical
transformations as argument. These transformations are sequentially applied to the shape that is going to be indexed or
searched.

Buffer
______

Buffer transformation returns a buffer around a shape.

**Syntax:**

.. code-block:: sql

    { type : "buffer"
      (, min_distance : <distance> )?
      (, max_distance : <distance> )?
    }

where:

-  **min_distance**: the inside buffer `distance <#distance>`__. Optional.
-  **max_distance**: the outside buffer `distance <#distance>`__. Optional.

**Example:** the following `geo shape search <#geo-shape-search>`__ will retrieve shapes intersecting with a shape
defined by a buffer 10 kilometers around a segment of the Florida's coastline:

.. code-block:: sql

    SELECT * FROM test
    WHERE expr(test_idx,'{filter : {
                            type : "geo_shape",
                           field : "place",
                        relation : "intersects",
                           shape : "LINESTRING(-80.90 29.05, -80.51 28.47, -80.60 28.12, -80.00 26.85, -80.05 26.37)",
                 transformations : [{type:"buffer", max_distance:"10km"}] }}');

Centroid
________

Centroid transformation returns the geometric center of a shape.

**Syntax:**

.. code-block:: sql

    { type : "centroid" }

**Example:** The following `geo shape mapper <#geo-shape-mapper>`__ will index only the centroid of the WKT shape
contained in the indexed column:

.. code-block:: sql

    CREATE CUSTOM INDEX cities_index on cities()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                shape : {
                    type            : "geo_shape",
                    max_levels      : 15,
                    transformations : [{type:"centroid"}]
                }
            }
        }'
    };

Difference
__________

Difference transformation subtracts the specified shape.

**Syntax:**

.. code-block:: sql

    {
      type : "difference",
      shape : "<shape>"
    }

where:

-  **shape**: The shape to be subtracted as a `Well Known Text (WKT) <http://en.wikipedia.org/wiki/Well-known_text>`__ string. Mandatory.

Intersection
____________

Intersection transformation intersects the specified shape.

**Syntax:**

.. code-block:: sql

    {
      type : "intersection",
      shape : "<shape>"
    }

where:

-  **shape**: The shape to be intersected as a `Well Known Text (WKT) <http://en.wikipedia.org/wiki/Well-known_text>`__ string. Mandatory.

Union
_____

Union transformation adds the specified shape.

**Syntax:**

.. code-block:: sql

    {
      type : "union",
      shape : "<shape>"
    }

where:

-  **shape**: The shape to be added as a `Well Known Text (WKT) <http://en.wikipedia.org/wiki/Well-known_text>`__ string. Mandatory.


Complex data types
******************

Tuples
======

Cassandra 2.1.x introduces the `tuple type <http://docs.datastax.com/en/cql/3.1/cql/cql_reference/tupleType.html>`__.
You can index, search and sort tuples this way:

.. code-block:: sql

    CREATE TABLE collect_things (
      k int PRIMARY KEY,
      v tuple<int, text, float>
    );

    INSERT INTO collect_things (k, v) VALUES(0, (1, 'bar', 2.1));
    INSERT INTO collect_things (k, v) VALUES(1, (2, 'bar', 2.1));
    INSERT INTO collect_things (k, v) VALUES(2, (3, 'foo', 2.1));


    CREATE CUSTOM INDEX idx ON  collect_things () USING 'com.stratio.cassandra.lucene.Index' WITH OPTIONS = {
    'refresh_seconds':'1',
    'schema':'{
        fields:{
            "v.0":{type:"integer"},
            "v.1":{type:"string"},
            "v.2":{type:"float"}
        }
     }'};

    SELECT * FROM collect_things WHERE expr(tweets_index, '{
        filter : {
            type  : "match",
            field : "v.0",
            value : 1
        }
    }');

    SELECT * FROM collect_things WHERE expr(tweets_index, '{
        filter : {
            type  : "match",
            field : "v.1",
            value : "bar"
        }
    }');

    SELECT * FROM collect_things WHERE expr(tweets_index, '{
        sort : {
            fields : [ {field : "v.2"} ]
        }
    }');


User Defined Types
==================

Since Cassandra 2.1.X users can declare `User Defined Types <http://docs.datastax.com/en/developer/java-driver/2.1/java-driver/reference/userDefinedTypes.html>`__ as follows:

.. code-block:: sql

    CREATE TYPE address_udt (
        street text,
        city text,
        zip int
    );

    CREATE TABLE user_profiles (
        login text PRIMARY KEY,
        first_name text,
        last_name text,
        address address_udt
    );

The components of UDTs can be indexed, searched and sorted this way :

.. code-block:: sql

    CREATE CUSTOM INDEX test_index ON test.user_profiles()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                "address.city" : { type : "string"},
                "address.zip"  : { type : "integer"}
            }
        }'
    };

    SELECT * FROM user_profiles
    WHERE expr(tweets_index,'{
        filter : {
            type  : "match",
            field : "address.city",
            value : "San Fransisco"
        }
    }');

    SELECT * FROM user_profiles
    WHERE expr(tweets_index,'{
        filter : {
            type  : "range",
            field : "address.zip",
            lower : 0,
            upper : 10
        }
    }');

Collections
===========

CQL `collections <http://docs.datastax.com/en/cql/3.0/cql/cql_using/use_collections_c.html>`__ (lists, sets and maps) can be indexed.

List ans sets are indexed in the same way as regular columns, using their base type:

.. code-block:: sql

    CREATE TABLE user_profiles (
        login text PRIMARY KEY,
        first_name text,
        last_name text,
        cities list<text>
    );

    CREATE CUSTOM INDEX test_index ON test.user_profiles()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                cities : { type : "string"}
            }
        }'
    };

Searches are also done in the same way as with regular columns:

.. code-block:: sql

    SELECT * FROM user_profiles
    WHERE expr(tweets_index,'{
        filter : {
            type  : "match",
            field : "cities",
            value : "San Francisco"
        }
    }');

Maps are indexed associating values to their keys:

.. code-block:: sql

    CREATE TABLE user_profiles (
        login text PRIMARY KEY,
        first_name text,
        last_name text,
        addresses map<text,text>
    );

    CREATE CUSTOM INDEX test_index ON test.user_profiles()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                addresses : { type : "string"}
            }
        }'
    };

For searching map values under a certain key you should use '$' as field-key separator:

.. code-block:: sql

    INSERT INTO user_profiles (login, first_name, last_name, addresses)
        VALUES('user','Peter','Handsome',
                {'San Francisco':'Market street 2', 'Madrid': 'Calle Velazquez' })

    SELECT * FROM user_profiles
    WHERE expr(tweets_index,'{
        filter : {
            type  : "match",
            field : "cities$Madrid",
            value : "San Francisco"
        }
    }');

Please don't use map keys containing the separator chars, which are '.' and '$'.

UDTs can be indexed even while being inside collections. It is done so using '.' as name separator:

.. code-block:: sql

    CREATE TYPE address (
        street text,
        city text,
        zip int
    );

    CREATE TABLE user_profiles (
        login text PRIMARY KEY,
        first_name text,
        last_name text,
        addresses list<frozen<address>>
    );

    CREATE CUSTOM INDEX test_index ON test.user_profiles()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                "addresses.city" : { type : "string"},
                "addresses.zip"  : { type : "integer"}
            }
        }'
    };


Query Builder
*************

There is a separate module named "builder" that can be included in client applications
to ease the building of the JSON statements used by the index.
If you are using Maven you can use it by adding this dependency to your pom.xml:

.. code-block:: xml

    <dependency>
        <groupId>com.stratio.cassandra</groupId>
        <artifactId>cassandra-lucene-index-builder</artifactId>
        <version>PLUGIN_VERSION</version>
    </dependency>

Then you can build an index creation statement this way:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    session.execute(index("keyspace_name", "table_name", "index_name")
           .refreshSeconds(10)
           .defaultAnalyzer("english")
           .analyzer("danish", snowballAnalyzer("danish"))
           .mapper("id", uuidMapper())
           .mapper("user", stringMapper().caseSensitive(false))
           .mapper("message", textMapper().analyzer("danish"))
           .mapper("date", dateMapper().pattern("yyyyMMdd"))
           .build());

And you can also build searches in a similar fashion:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM table WHERE expr(users_index, ?)",
        search().filter(match("user", "adelapena"))
                .query(phrase("message", "cassandra rules"))
                .sort(field("date").reverse(true))
                .refresh(true)
                .build());


Spark and Hadoop
****************

Spark and Hadoop integrations are fully supported because Lucene searches
can be combined with token range restrictions and paging, which are the
basis of MapReduce frameworks support.

Token Range Searches
====================

The token function allows computing the token for a given partition key.
The primary key of the example table “users” is ((name, gender), animal,
age) where (name, gender) is the partition key. When combining the token
function and a Lucene-based filter in a where clause, the filter on
tokens is applied first and then the condition of the filter clause.

**Example:** search for rows which tokens are greater than (‘Alicia’,
‘female’) and then test them against the match condition:

.. code-block:: sql

    SELECT name, gender
      FROM test.users
     WHERE expr(users_index, '{filter : {type : "match", field : "food", value : "chips"}}')
       AND token(name, gender) > token('Alicia', 'female');

Paging
======

Paging filtered results is fully supported. You can retrieve
the rows starting from a certain key. For example, if the primary key is
(userid, createdAt), you can search:

.. code-block:: sql

    SELECT *
      FROM tweets
      WHERE expr(users_index, ‘{ filter : {type:”match",  field:”text", value:”cassandra”}}’)
        AND userid = 3543534
        AND createdAt > 2011-02-03 04:05+0000
      LIMIT 5000;

Examples
========

There is a `Spark examples project <https://github.com/Stratio/cassandra-lucene-index-examples>`_ where you can find Spark usage examples.

Performance
===========

Lucene indexes should be combined with Spark only with searches requesting a relatively small fraction of the total
data. Generally, reading *n* rows from an index is slower that reading the same *n* rows in a token range query.
However, the relevance of the index stems from the efficiency to collect only the required data.

The following benchmark compares the performance of Spark using full scan or using a Lucene index to filter the data.
We do successive queries requesting from the 1% to 100% of the stored data. We can see a high performance for the
index for the queries requesting strongly filtered data. However, the performance decays in less restrictive queries.
As the number of records returned by the query increases, we reach a point where the index becomes slower than the full
scan. So, the decision to use indexes in your Spark jobs depends on the query selectivity. The tradeoff between both
approaches depends on the particular use case. Generally, combining Lucene indexes with Spark is recommended for jobs
retrieving no more than the 25% of the stored data.

.. image:: /doc/resources/spark_performance.png
   :width: 100%
   :alt: spark_performance
   :align: center

JMX Interface
*************

The existing Lucene indexes expose some attributes and operations
through JMX, using the same MBean server as Apache Cassandra. The MBeans
provided by Stratio are under the domain **com.stratio.cassandra.lucene**.

Please note that all the JMX attributes and operations refer to the
index shard living inside the local JVM, and not to the globally
distributed index.


+-------------------+-----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Name              | Type      | Notes                                                                                                                                                                                 |
+===================+===========+=======================================================================================================================================================================================+
| NumDeletedDocs    | Attribute | Total number of documents in the index.                                                                                                                                               |
+-------------------+-----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| NumDocs           | Attribute | Total number of documents in the index.                                                                                                                                               |
+-------------------+-----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Commit            | Operation | Commits all the pending index changes to disk.                                                                                                                                        |
+-------------------+-----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Refresh           | Operation | Reopens all the readers and searchers to provide a recent view of the index.                                                                                                          |
+-------------------+-----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| forceMerge        | Operation | Optimizes the index forcing merge segments leaving the specified number of segments. It also includes a boolean parameter to block until all merging completes.                       |
+-------------------+-----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| forceMergeDeletes | Operation | Optimizes the index forcing merge segments containing deletions, leaving the specified number of segments. It also includes a boolean parameter to block until all merging completes. |
+-------------------+-----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Performance tips
****************

Lucene index plugin performance varies depending upon several factors
regarding to the use case and you should probably do some tuning work.
However, there is some general advice.

Choose the right use case
=========================

Lucene searches are much more time and resource consuming than their Cassandra counterparts,
not being an alternative to Apache Cassandra denormalized tables, inverted indexes, and/or
secondary indexes.
In most cases, it is a bad idea to model a system with simple skinny rows and try to satisfy
all queries with Lucene.
For example, the following search could be more efficiently addressed using a denormalized table:

.. code-block:: sql

    SELECT * FROM users
    WHERE expr(tweets_index, '{filter : {
                      type  : "match",
                      field : "name",
                      value : "Alice" }}');

However, this search could be a good use case for Lucene just because there is no easy counterpart:

.. code-block:: sql

    SELECT * FROM users
    WHERE expr(tweets_index, '{filter : {
                       type : "boolean",
                       must : [{type  : "regexp", field : "name", value : "[J][aeiou]{2}.*"},
                               {type  : "range",
                                field : "birthday",
                                lower : "2014/04/25",
                                upper : "2014/05/01"}]}}');

Lucene indexes are intended to be used in those cases that can't be efficiently addressed
with Apache Cassandra common techniques, such as full-text queries, multidimensional queries,
geospatial search and bitemporal data models.

Use the latest version
======================

Each new version might be as fast or faster than the previous one,
so please try to use the latest version if possible.
You can find the list of changes and performance improvements at `changelog file </CHANGELOG.md>`__.

Disable virtual nodes
=====================

Although virtual nodes are fully supported, we recommend turning them off.
In the same way as virtual nodes use to be problematic with analytical tools as Spark, Hadoop and Solr,
Lucene indexes performance goes down because each node query is split into several data range sub-queries.

Use a separate disk
===================

You will get better performance using a separate disk for the Lucene index files.
You can set the place where the index will be stored using the `directory_path` option:

.. code-block:: sql

    CREATE CUSTOM INDEX tweets_index ON tweets ()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'directory_path' : '<lucene_disk>',
        ...
    };

Disregard the first query
=========================

Lucene makes a huge use of caching,
so the first query done to an index will be specially slow dou to the cost of initializing caches.
Thus, you should disregard the first query when measuring performance.


Index only what you need
========================

The more fields you index, the more resources will be consumed.
So you should carefully study which kind of queries are you going to use before creating the schema.
You should also be careful choosing the ``indexed`` and ``sorted`` options of the mappers,
because each of them creates at least on field per Cassandra column, doing your index larger and slower.

Use a low refresh rate
======================

You can choose any index refresh rate you need,
and you can expect a good behaviour even with a refresh rate of just one second.
The default refresh rate is 60 seconds, which is a pretty conservative value.
However, high refresh rates imply a higher general resources consumption.
So you should use a refresh rate as low as your use case allows.
You can set the refresh rate using the `refresh` option:

.. code-block:: sql

    CREATE CUSTOM INDEX tweets_index ON tweets ()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh' : '<refresh_rate>',
        ...
    };

Prefer filters over queries
===========================

Query searches involve relevance so they should be sent to all nodes in the
cluster in order to find the globally best results.
However, filters have a chance to find the results in a subset of the nodes.
So if you are not interested in relevance sorting then you should prefer filters over queries.

Limit top-k searches
====================

Top-k searches are those containing a `query` or a `sort` condition.
These searches return results sorted in a different order than the provided by Cassandra.
They are meant to retrieve the k best results according to a certain criterion,
not to sort all the contents in the database. For this reason, the search engine disables
paging and forces to specify a `LIMIT` clause limiting the number of results to be collected.
High `LIMIT` clauses (more than a few thousands) are risky because they can produce a memory
issues in the coordinator node.

