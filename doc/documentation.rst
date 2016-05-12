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
- `Two ways of Indexing <#two-ways-of-indexing>`__
- `Indexing <#indexing>`__
    - `Analyzers <#analyzers>`__
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
        - `Convex hull <#convex-hull>`__
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

Features
========

Lucene search technology integration into Cassandra provides:

Stratio’s Cassandra Lucene Index and its integration with Lucene search technology provides:

-  Full text search (language-aware analysis, wildcard, fuzzy, regexp)
-  Geospatial indexing (points, lines, polygons and their multiparts)
-  Geospatial transformations (union, difference, intersection, buffer, centroid, convex hull)
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


+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| From\\ To | 2.1.6.2 | 2.1.7.1 | 2.1.8.5 | 2.1.9.0 | 2.1.10.0 | 2.1.11.1 | 2.2.3.2 | 2.2.4.3 | 2.2.4.4 | 2.2.5.0 | 2.2.5.1 | 2.2.5.2 | 3.0.3.0 | 3.0.3.1 | 3.0.4.0 | 3.0.4.1 | 3.0.5.0 |
+===========+=========+=========+=========+=========+==========+==========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+
| 2.1.6.0   |   YES   |   YES   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.6.1   |   YES   |   YES   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.6.2   |    --   |   YES   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.7.0   |    --   |   YES   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.7.1   |    --   |    --   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.8.0   |    --   |    --   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.8.1   |    --   |    --   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.8.2   |    --   |    --   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.8.3   |    --   |    --   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.8.4   |    --   |    --   |   YES   |   YES   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.8.5   |    --   |    --   |    --   |   YES   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.9.0   |    --   |    --   |    --   |    --   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.10.0  |    --   |    --   |    --   |    --   |    --    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.11.0  |    --   |    --   |    --   |    --   |    --    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.11.1  |    --   |    --   |    --   |    --   |    --    |    --    |   YES   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.3.0   |    --   |    --   |    --   |    --   |    --    |    --    |   YES   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.3.1   |    --   |    --   |    --   |    --   |    --    |    --    |   YES   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.3.2   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.4.0   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.4.1   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.4.2   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.4.3   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.4.5   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.5.0   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.5.1   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |    --   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.5.2   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |    --   |    --   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.3.0   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |   YES   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.3.1   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.4.0   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.4.1   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
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



Two ways of Indexing
********************

Currently, there is two ways of indexing and searching.

The old one way of indexing is the fake-column approach

.. code-block:: sql

    CREATE CUSTOM INDEX (IF NOT EXISTS)? <index_name>
                                      ON <table_name> (<fake_column>)
                                   USING 'com.stratio.cassandra.lucene.Index'
                            WITH OPTIONS = <options>

where you need to use an additional column where you build the index on.

The new way does not need the fake column.

.. code-block:: sql

    CREATE CUSTOM INDEX (IF NOT EXISTS)? <index_name>
                                      ON <table_name> ()
                                   USING 'com.stratio.cassandra.lucene.Index'
                            WITH OPTIONS = <options>

There is also two ways of searching:

The old fake_column approach

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table_name> WHERE <fake_column> = <query>;

and the new 3.0 prettiest fake_column-free way

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table_name> WHERE expr(<index_name>,<query>);

We would like to only support the new way but if you want use cassandra 3.x connected though cassandra-spark-connector to spark and cassandra-lucene-index  you need to use the old approach because the cassandra-spark-conector has not uploaded to new secondary index search way (expr).

The old way of indexing is useful too if you want to get the per-row score in top-k queries retuned in fake-column.

Every example in this document is written in new 'expr' way but they could be executed in old fake-column way too.


Indexing
********

Lucene indexes are an extension of the Cassandra secondary indexes. As such, they are created through CQL
`CREATE CUSTOM INDEX statement <https://cassandra.apache.org/doc/cql3/CQL.html#createIndexStmt>`__, specifying the full
qualified class name and a list of configuration options that are specified in this section.


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

**Example 5:** Index the convex hull of the WKT shape contained in the indexed column:

.. image:: /doc/resources/geo_shape_mapper_example_5.png
    :width: 100%
    :alt: search by shape
    :align: center

.. code-block:: sql

    CREATE TABLE IF NOT EXISTS blocks (
        id bigint PRIMARY KEY,
        shape text
    );

    INSERT INTO blocks(name, shape) VALUES (341, 'MULTIPOLYGON(((-86.69327962999995 32.39069119000004, -86.69318559999994 32.39149454000005, -86.69159042999996 32.39136299000006, -86.69162177999993 32.39109520000005, -86.69130274999998 32.39106889000004, -86.69124004999998 32.39160446000005, -86.69155907999999 32.39163077000006, -86.69152772999996 32.39189856000007, -86.69057062999997 32.39181961000003, -86.69060197999994 32.391551830000026, -86.68964487999995 32.39147287000003, -86.68961351999997 32.39174066000004, -86.68929448999995 32.39171434000008, -86.68920041999996 32.392517690000034, -86.68983848999994 32.39257033000007, -86.68977577999999 32.39310589000007, -86.69009481999996 32.39313221000003, -86.69003210999995 32.393667780000044, -86.69067018999993 32.39372041000007, -86.69070154999997 32.39345263000007, -86.69165866999998 32.39353157000005, -86.69162731999995 32.39379936000006, -86.69194635999997 32.39382567000007, -86.69188365999997 32.39436124000002, -86.69124556999998 32.39430861000005, -86.69121421999995 32.394576390000054, -86.69089517999998 32.394550080000045, -86.69076975999997 32.39562121000006, -86.69045070999994 32.39559489000004, -86.69041935999996 32.39586267000004, -86.68978125999996 32.39581004000007, -86.68981261999994 32.39554226000007, -86.68949356999997 32.39551594000005, -86.68955627999998 32.39498037000004, -86.68987532999995 32.39500669000006, -86.68990668999999 32.39473891000006, -86.69022572999995 32.394765230000075, -86.69025708999999 32.394497450000074, -86.68993803999996 32.394471130000056, -86.69000074999997 32.393935560000045, -86.68936266999998 32.39388292000007, -86.68942537999999 32.393347360000064, -86.68846825999998 32.39326840000007, -86.68840553999996 32.39380396000007, -86.68872457999998 32.39383028000003, -86.68866185999997 32.39436585000004, -86.68834281999995 32.394339530000025, -86.68828009999999 32.39487509000003, -86.68764200999999 32.39482244000004, -86.68767336999997 32.39455466000004, -86.68703527999998 32.39450201000005, -86.68700391999994 32.394769790000055, -86.68668486999997 32.39474347000004, -86.68671623999995 32.39447568000003, -86.68575910999994 32.39439670000007, -86.68579047999998 32.39412892000007, -86.68547143999996 32.394102590000045, -86.68540868999997 32.39463815000005, -86.68508964999995 32.39461182000008, -86.68506681999997 32.39480670000006, -86.68508399999996 32.39455000000004, -86.68511099999995 32.39426000000003, -86.68514199999998 32.39309300000008, -86.68513999999999 32.39282000000003, -86.68513099999996 32.39269100000007, -86.68511499999994 32.39261400000004, -86.68508499999996 32.39250500000003, -86.68478399999998 32.39256700000004, -86.68402499999996 32.39273900000006, -86.68369699999994 32.39282300000008, -86.68352999999996 32.39286200000004, -86.68336099999999 32.392892000000074, -86.68318899999997 32.39290900000003, -86.68301699999995 32.392918000000066, -86.68183499999998 32.39293800000007, -86.68139799999994 32.39295200000004, -86.67883675999997 32.39299371000004, -86.67886577999997 32.392746110000076, -86.67918481999999 32.39277246000006, -86.67921620999994 32.39250468000006, -86.67953523999995 32.392531020000035, -86.67956662999995 32.392263240000034, -86.67924759999994 32.39223690000006, -86.67927898999994 32.39196911000005, -86.67959801999996 32.39199546000003, -86.67962940999996 32.39172768000003, -86.67994843999998 32.39175403000007, -86.68010537999999 32.39041512000006, -86.68074342999995 32.390467810000075, -86.68086896999995 32.38939668000006, -86.68118798999996 32.389423030000046, -86.68121937999996 32.389155240000036, -86.68090035999995 32.38912890000006, -86.68099450999995 32.38832555000005, -86.68035646999994 32.38827286000003, -86.68038785999994 32.38800508000003, -86.67911179999999 32.38789969000004, -86.67908040999998 32.38816747000004, -86.67876138999998 32.388141130000065, -86.67872999999997 32.38840891000007, -86.67841097999997 32.388382560000025, -86.67834819999996 32.38891812000003, -86.67802917999995 32.38889177000004, -86.67787220999998 32.39023068000006, -86.67755317999996 32.390204330000074, -86.67742759999999 32.39127545000002, -86.67710856999997 32.39124909000003, -86.67704577999996 32.39178466000004, -86.67672674999994 32.39175830000005, -86.67669534999999 32.39202608000005, -86.67701437999995 32.39205244000004, -86.67695157999998 32.392588000000046, -86.67663254999997 32.392561640000054, -86.67660114999995 32.392829420000055, -86.67723920999998 32.39288213000003, -86.67727060999994 32.39261435000003, -86.67758964999996 32.39264070000007, -86.67755824999995 32.392908480000074, -86.67860415999996 32.39299487000005, -86.67709599999995 32.39300100000003, -86.67623899999995 32.393016000000046, -86.67518699999994 32.39302800000007, -86.67511199999996 32.39298500000007, -86.67504599999995 32.39293200000003, -86.67499999999995 32.39287800000005, -86.67495899999994 32.39281900000003, -86.67500199999995 32.39262000000008, -86.67506299999997 32.391886000000056, -86.67507899999998 32.391741000000025, -86.67510499999997 32.39159600000005, -86.67514199999994 32.39145300000007, -86.67518999999999 32.39131400000008, -86.67525099999995 32.39117700000003, -86.67532499999999 32.39104500000008, -86.67541099999994 32.390918000000056, -86.67550799999998 32.39079700000008, -86.67572699999994 32.39057200000008, -86.67583199999996 32.390458000000024, -86.67592399999995 32.39033500000005, -86.67599299999995 32.39020300000004, -86.67604299999994 32.39006200000006, -86.67611499999998 32.38977500000004, -86.67615799999999 32.38963400000006, -86.67621199999996 32.38949400000007, -86.67627699999997 32.38935900000007, -86.67635199999995 32.38922700000006, -86.67652199999998 32.38897300000008, -86.67670299999997 32.38872300000003, -86.67698499999995 32.38835700000004, -86.67706999999996 32.38823300000007, -86.67714499999994 32.388104000000055, -86.67720299999996 32.387969000000055, -86.67727199999996 32.38775700000008, -86.67732499999994 32.38761800000003, -86.67739699999998 32.38748300000003, -86.67749699999996 32.387361000000055, -86.67761099999996 32.38724700000006, -86.67780099999999 32.38709600000004, -86.67811999999998 32.386851000000036, -86.67848999999995 32.38655000000006, -86.67906499999998 32.38611100000003, -86.67934616999997 32.38590026000003, -86.67930012999994 32.38629299000007, -86.67898111999995 32.38626665000004, -86.67894972999994 32.38653443000004, -86.67863072999995 32.386508080000056, -86.67853654999999 32.38731143000007, -86.67821753999993 32.38728508000003, -86.67818614999999 32.38755286000003, -86.67882416999998 32.387605560000054, -86.67885555999999 32.38733778000005, -86.67949358999994 32.38739047000007, -86.67952497999994 32.38712269000007, -86.67984398999994 32.387149040000054, -86.67987537999994 32.38688126000005, -86.68019438999994 32.38690760000003, -86.68022576999994 32.38663982000003, -86.68150181999994 32.386745190000056, -86.68140766999994 32.38754854000007, -86.68204569999995 32.38760123000003, -86.68217121999999 32.386530090000065, -86.68185220999999 32.38650375000003, -86.68191496999998 32.38596818000008, -86.68287198999997 32.386047200000064, -86.68284061999998 32.38631499000007, -86.68315962999998 32.38634132000004, -86.68337924999997 32.384466830000065, -86.68401725999996 32.38451950000007, -86.68398587999997 32.38478729000008, -86.68366687999998 32.38476095000004, -86.68360412999994 32.385296520000054, -86.68424214999999 32.385349190000056, -86.68421076999994 32.385616980000066, -86.68452977999993 32.385643310000034, -86.68462388999995 32.38483996000008, -86.68494289999995 32.38486629000005, -86.68488015999998 32.38540186000006, -86.68519916999998 32.38542819000003, -86.68510505999996 32.38623155000005, -86.68542406999995 32.38625788000007, -86.68536132999998 32.38679345000003, -86.68472330999998 32.38674079000003, -86.68469193999994 32.387008570000035, -86.68501094999993 32.38703490000006, -86.68491683999997 32.387838260000024, -86.68395978999996 32.387759250000045, -86.68392841999997 32.388027040000054, -86.68424743999998 32.38805337000008, -86.68418468999994 32.388588940000034, -86.68386566999999 32.388562610000065, -86.68383429999994 32.38883039000007, -86.68319625999999 32.388777720000064, -86.68303936999996 32.39011663000008, -86.68367741999998 32.39016931000003, -86.68370879999998 32.38990152000002, -86.68402781999998 32.38992786000006, -86.68409056999997 32.389392290000046, -86.68440958999997 32.38941863000008, -86.68444096999997 32.38915084000007, -86.68507900999998 32.38920351000007, -86.68514174999996 32.38866794000006, -86.68546076999996 32.38869427000003, -86.68549213999995 32.38842649000003, -86.68581115999996 32.388452820000055, -86.68587388999998 32.387917250000044, -86.68619290999999 32.38794358000007, -86.68622427999998 32.38767579000006, -86.68654328999997 32.38770212000003, -86.68651192999994 32.38796991000004, -86.68683094999994 32.387996230000056, -86.68692503999995 32.38719288000004, -86.68756306999995 32.38724553000003, -86.68762578999997 32.386709960000076, -86.68730677999997 32.38668363000005, -86.68740085999997 32.38588028000004, -86.68708184999997 32.38585395000007, -86.68739545999995 32.38317609000006, -86.68707645999996 32.38314976000004, -86.68710782999995 32.382881980000036, -86.68742681999998 32.382908300000054, -86.68745817999996 32.382640510000044, -86.68777717999996 32.38266684000007, -86.68774581999998 32.38293463000008, -86.68806481999997 32.38296095000004, -86.68803345999999 32.38322874000005, -86.68835245999998 32.38325506000007, -86.68838381999996 32.38298727000006, -86.68870281999995 32.38301360000003, -86.68873416999998 32.382745810000074, -86.68841516999998 32.382719490000056, -86.68844652999996 32.38245170000005, -86.68876552999996 32.382478020000065, -86.68879687999998 32.382210230000055, -86.68911587999997 32.38223655000007, -86.68930399999994 32.380629820000024, -86.68898500999995 32.38060350000006, -86.68895364999997 32.38087129000007, -86.68863465999993 32.380844970000055, -86.68872872999998 32.380041610000035, -86.68840973999994 32.380015280000066, -86.68844109999998 32.37974749000006, -86.68812210999994 32.37972117000004, -86.68815346999997 32.37945338000003, -86.68783447999994 32.37942706000007, -86.68786583999997 32.37915927000006, -86.68754685999994 32.379132940000034, -86.68760956999995 32.37859737000008, -86.68792854999998 32.37862369000004, -86.68795990999996 32.37835590000003, -86.69019276999995 32.37854015000005, -86.69022411999998 32.37827236000004, -86.69086207999999 32.37832499000007, -86.69089342999996 32.37805720000006, -86.69153138999997 32.37810983000003, -86.69156272999999 32.377842040000075, -86.69283864999994 32.377947290000066, -86.69280730999998 32.378215080000075, -86.69312628999995 32.37824139000003, -86.69306360999997 32.37877697000005, -86.69338259999995 32.37880328000006, -86.69331991999996 32.379338860000075, -86.69395788999998 32.37939148000004, -86.69389521999994 32.37992706000006, -86.69357622999996 32.37990075000005, -86.69341953999998 32.38123970000004, -86.69310053999999 32.38121339000003, -86.69297517999996 32.382284550000065, -86.69329417999995 32.382310860000075, -86.69323149999997 32.38284644000004, -86.69291249999998 32.38282013000003, -86.69281846999996 32.38362349000005, -86.69249946999997 32.38359718000004, -86.69240543999996 32.384400550000066, -86.69208642999996 32.38437424000006, -86.69202373999997 32.38490981000007, -86.69138572999998 32.38485718000004, -86.69126033999999 32.38592833000007, -86.69094132999999 32.38590202000006, -86.69087862999999 32.38643759000007, -86.69024059999998 32.38638496000004, -86.69027195999996 32.386117170000034, -86.68995293999996 32.38609085000007, -86.68989023999995 32.386626430000035, -86.69020924999995 32.386652740000045, -86.69017789999998 32.386920530000054, -86.69145395999999 32.38702579000005, -86.69151665999999 32.38649022000004, -86.69183566999999 32.38651653000005, -86.69186701999996 32.38624875000005, -86.69218602999996 32.38627506000006, -86.69221736999998 32.38600727000005, -86.69285539999998 32.38605989000007, -86.69288673999995 32.38579211000007, -86.69352475999995 32.38584472000008, -86.69374413999998 32.38397021000003, -86.69406314999998 32.38399652000004, -86.69409447999999 32.38372873000003, -86.69441348999999 32.38375503000003, -86.69444481999994 32.38348725000003, -86.69476382999994 32.38351355000003, -86.69479515999996 32.38324576000008, -86.69511415999995 32.38327207000003, -86.69517682999998 32.38273649000007, -86.69549582999997 32.38276279000007, -86.69558982999996 32.381959420000044, -86.69590882999995 32.381985720000046, -86.69594015999996 32.381717930000036, -86.69562115999997 32.381691630000034, -86.69565248999999 32.381423840000025, -86.69597148999998 32.38145014000003, -86.69600281999999 32.381182350000074, -86.69632180999997 32.381208650000076, -86.69625914999995 32.381744240000046, -86.69657814999994 32.38177054000005, -86.69610818999996 32.38578737000006, -86.69578917999996 32.38576107000006, -86.69566384999996 32.38683222000003, -86.69502581999996 32.38677962000003, -86.69505714999997 32.386511830000074, -86.69473813999997 32.38648553000007, -86.69464412999997 32.387288890000036, -86.69432510999997 32.387262580000026, -86.69429376999994 32.387530370000036, -86.69397474999994 32.387504060000026, -86.69394340999997 32.387771850000036, -86.69362438999997 32.387745540000026, -86.69359304999995 32.388013330000035, -86.69327402999994 32.387987020000025, -86.69324268999998 32.388254810000035, -86.69292366999997 32.388228500000025, -86.69289231999994 32.38849628000003, -86.69225427999999 32.388443660000064, -86.69222292999996 32.38871145000007, -86.69190390999995 32.388685140000064, -86.69187255999998 32.388952920000065, -86.69123451999997 32.38890029000004, -86.69120316999994 32.38916808000005, -86.69088413999998 32.38914177000004, -86.69085278999995 32.38940955000004, -86.69212889999994 32.38951481000004, -86.69216023999996 32.38924702000003, -86.69311731999994 32.38932595000006, -86.69308596999997 32.389593740000066, -86.69276694999996 32.389567430000056, -86.69270425999997 32.39010300000007, -86.69238522999996 32.39007669000006, -86.69235387999998 32.39034447000006, -86.69299193999996 32.39039709000008, -86.69296059999994 32.39066488000003, -86.69327962999995 32.39069119000004), (-86.67669534999999 32.39202608000005, -86.67637631999997 32.391999730000066, -86.67634491999996 32.39226751000007, -86.67666394999998 32.39229386000005, -86.67669534999999 32.39202608000005), (-86.68304461999998 32.39282079000003, -86.68313875999996 32.392017440000075, -86.68281972999995 32.39199111000005, -86.68278834999995 32.39225889000005, -86.68246930999999 32.392232550000074, -86.68240654999994 32.39276811000008, -86.68304461999998 32.39282079000003), (-86.68409585999996 32.392096450000054, -86.68412722999994 32.39182867000005, -86.68444625999996 32.39185500000008, -86.68454037999999 32.39105166000007, -86.68390232999997 32.39099899000007, -86.68383957999998 32.391534550000074, -86.68352053999996 32.39150822000005, -86.68345778999998 32.39204378000005, -86.68409585999996 32.392096450000054)), ((-86.69391768999998 32.39074380000005, -86.69327962999995 32.39069119000004, -86.69331096999997 32.39042340000003, -86.69362999999998 32.39044971000004, -86.69366133999995 32.39018193000004, -86.69398036999996 32.39020823000004, -86.69391768999998 32.39074380000005)), ((-86.68812210999994 32.37972117000004, -86.68802803999995 32.38052454000007, -86.68770904999997 32.38049821000004, -86.68780312999996 32.37969485000008, -86.68812210999994 32.37972117000004)), ((-86.68462388999995 32.38483996000008, -86.68430488999996 32.384813620000045, -86.68433625999995 32.384545840000044, -86.68465525999994 32.38457217000007, -86.68462388999995 32.38483996000008)), ((-86.68433625999995 32.384545840000044, -86.68401725999996 32.38451950000007, -86.68404862999995 32.384251720000066, -86.68436762999994 32.384278050000034, -86.68433625999995 32.384545840000044)), ((-86.67701437999995 32.39205244000004, -86.67704577999996 32.39178466000004, -86.67736480999997 32.391811010000026, -86.67733340999996 32.39207879000003, -86.67701437999995 32.39205244000004)), ((-86.67927898999994 32.39196911000005, -86.67895995999999 32.39194277000007, -86.67899134999999 32.39167499000007, -86.67931037999995 32.391701330000046, -86.67927898999994 32.39196911000005)), ((-86.68265226999995 32.35493512000005, -86.68424668999995 32.35506681000004, -86.68427804999999 32.35479901000008, -86.68523470999997 32.35487801000005, -86.68520334999994 32.35514581000007, -86.68552222999995 32.355172140000036, -86.68549087999997 32.35543994000005, -86.68517198999996 32.35541361000003, -86.68510927999995 32.35594921000006, -86.68542816999997 32.35597554000003, -86.68539680999999 32.35624334000005, -86.68571569999995 32.356269670000074, -86.68574705999998 32.35600187000006, -86.68606594999994 32.356028200000026, -86.68603458999996 32.35629600000004, -86.68667237999995 32.35634866000004, -86.68664101999997 32.356616460000055, -86.68727880999995 32.35666912000005, -86.68724745999998 32.35693691000006, -86.68756634999994 32.35696324000003, -86.68753499999997 32.357231040000045, -86.68785389999994 32.35725737000007, -86.68782254999996 32.35752516000008, -86.68846033999995 32.357577810000066, -86.68842899999999 32.35784561000003, -86.68874788999995 32.35787194000005, -86.68877923999997 32.357604140000035, -86.68909813999994 32.35763046000005, -86.68906678999997 32.35789826000007, -86.68938568999994 32.35792458000003, -86.68932299999994 32.358460180000066, -86.68836629999998 32.35838121000006, -86.68830360999993 32.35891681000004, -86.68926030999995 32.358995770000035, -86.68922896999999 32.35926357000005, -86.68891006999996 32.359237250000035, -86.68887871999999 32.35950505000005, -86.68760309999999 32.35939975000008, -86.68763444999996 32.35913195000006, -86.68603993999994 32.359000310000056, -86.68607129999998 32.358732520000046, -86.68575239999996 32.35870618000007, -86.68584645999994 32.357902790000026, -86.68552756999998 32.35787646000006, -86.68540213999995 32.35894765000006, -86.68508323999998 32.358921320000036, -86.68505187999995 32.359189110000045, -86.68345736999999 32.35905744000007, -86.68342600999995 32.359325230000024, -86.68406380999994 32.359377910000035, -86.68400108999998 32.35991350000006, -86.68463889999998 32.359966170000064, -86.68467025999996 32.35969837000005, -86.68530806999996 32.35975104000005, -86.68527670999998 32.360018840000066, -86.68559560999995 32.360045170000035, -86.68540746999997 32.361651950000066, -86.68508855999994 32.36162562000004, -86.68505719999996 32.36189341000005, -86.68441936999994 32.36184074000005, -86.68438800999996 32.362108540000065, -86.68375017999995 32.36205587000006, -86.68371881999997 32.36232366000007, -86.68339989999998 32.36229733000005, -86.68336853999995 32.362565120000056, -86.68304961999996 32.36253878000008, -86.68301825999998 32.36280658000004, -86.68238042999997 32.36275390000003, -86.68234905999998 32.36302169000004, -86.68266796999995 32.36304803000007, -86.68263659999997 32.36331583000003, -86.68231768999999 32.36328949000006, -86.68228631999995 32.363557280000066, -86.68260523999999 32.36358362000004, -86.68254249999995 32.36411921000007, -86.68286141999994 32.364145550000046, -86.68283004999995 32.36441335000006, -86.68314896999999 32.36443969000004, -86.68311760999995 32.36470748000005, -86.68375544999998 32.36476015000005, -86.68356725999996 32.366366920000075, -86.68261046999999 32.36628790000003, -86.68248498999998 32.36735907000008, -86.68280391999997 32.367385410000054, -86.68269795999998 32.36828993000006, -86.68238199999996 32.36692000000005, -86.68232499999993 32.36665300000004, -86.68229999999994 32.36641100000003, -86.68229599999995 32.366370000000074, -86.68229999999994 32.36621900000006, -86.68233799999996 32.365924000000064, -86.68234099999995 32.36569900000006, -86.68233099999998 32.36555000000004, -86.68226499999997 32.36533900000006, -86.68218899999994 32.36520600000006, -86.68214799999998 32.36514400000004, -86.68171499999994 32.36459300000007, -86.68156599999998 32.364415000000065, -86.68147599999998 32.36428900000004, -86.68139799999994 32.36415800000003, -86.68133899999998 32.36402100000004, -86.68129899999997 32.36387800000006, -86.68124799999998 32.363608000000056, -86.68119199999995 32.36330600000008, -86.68115299999994 32.36316300000004, -86.68112899999994 32.36309300000005, -86.68109899999996 32.363027000000045, -86.68101899999994 32.362902000000076, -86.68091999999996 32.36278600000003, -86.68086099999994 32.36273200000005, -86.68079699999998 32.362685000000056, -86.68065699999994 32.362603000000036, -86.68043699999998 32.36249300000003, -86.68025799999998 32.36242600000003, -86.68003299999998 32.362370000000055, -86.67986899999994 32.36233500000003, -86.67928399999994 32.36220000000003, -86.67912099999995 32.362153000000035, -86.67895899999996 32.36210200000005, -86.67864199999997 32.36198100000007, -86.67860866999996 32.36196656000004, -86.67861621999998 32.36190216000006, -86.67842319999994 32.36188621000008, -86.67833499999995 32.361848000000066, -86.67818799999998 32.361770000000035, -86.67804399999994 32.36168800000007, -86.67790899999994 32.361597000000074, -86.67779599999994 32.36150300000003, -86.67729099999997 32.36108500000006, -86.67720299999996 32.36102400000004, -86.67716099999996 32.360993000000065, -86.67653799999994 32.360614000000055, -86.67635599999994 32.360498000000064, -86.67621520999995 32.36040926000004, -86.67622187999996 32.36035235000003, -86.67611362999997 32.36034340000003, -86.67608399999995 32.36032300000005, -86.67602299999999 32.36027500000006, -86.67596799999995 32.360221000000024, -86.67588299999994 32.36008900000007, -86.67586099999994 32.36001600000003, -86.67585299999996 32.35994000000005, -86.67586699999998 32.359848000000056, -86.67589499999997 32.35976400000004, -86.67595599999999 32.35967100000005, -86.67579499999994 32.35973000000007, -86.67571899999996 32.35976900000003, -86.67565099999996 32.35981300000003, -86.67563099999995 32.35982800000005, -86.67534565999995 32.35958670000008, -86.67535100999999 32.35954104000007, -86.67543501999995 32.35947614000003, -86.67567824999998 32.35949625000006, -86.67570393999995 32.35927710000004, -86.67576496999999 32.35923302000003, -86.67602853999995 32.35925481000004, -86.67605633999995 32.35901764000005, -86.67609378999998 32.35898982000003, -86.67637881999997 32.35901338000008, -86.67641020999997 32.35874558000006, -86.67672910999994 32.358771940000054, -86.67676049999994 32.358504140000036, -86.67707938999996 32.35853050000003, -86.67711077999996 32.358262710000076, -86.67742966999998 32.35828906000006, -86.67748308999995 32.35783327000007, -86.67748699999999 32.357830000000035, -86.67753599999998 32.35781600000007, -86.67764999999997 32.357804000000044, -86.67770437999997 32.357770980000055, -86.67781132999994 32.357779820000076, -86.67782415999994 32.357670330000076, -86.67793599999999 32.35757600000005, -86.67798242999999 32.35752357000007, -86.67816160999996 32.35753838000005, -86.67819298999996 32.35727059000004, -86.67851187999997 32.357296940000026, -86.67854325999997 32.357029140000066, -86.67886214999999 32.35705549000005, -86.67892490999998 32.35651990000002, -86.67956268999995 32.35657260000005, -86.67959406999995 32.35630480000003, -86.67991294999996 32.356331150000074, -86.67994432999996 32.356063350000056, -86.67962543999994 32.35603701000008, -86.67963574999999 32.355949020000025, -86.67971499999999 32.35589600000003, -86.67980899999998 32.35585200000003, -86.67990299999997 32.35580100000004, -86.67992554999995 32.35579141000005, -86.68029458999996 32.355821900000024, -86.68026321999997 32.35608970000004, -86.68090098999994 32.35614239000006, -86.68093236999994 32.35587460000005, -86.68125124999995 32.355900940000026, -86.68131398999998 32.35536534000005, -86.68163287999994 32.35539169000003, -86.68165105999998 32.35523645000006, -86.68174199999999 32.35524600000008, -86.68179099999998 32.355233000000055, -86.68183199999999 32.35520600000007, -86.68190362999997 32.355143670000075, -86.68262089999996 32.35520292000007, -86.68265226999995 32.35493512000005), (-86.68228631999995 32.363557280000066, -86.68196739999996 32.36353094000003, -86.68193602999997 32.36379874000005, -86.68225494999996 32.363825080000026, -86.68228631999995 32.363557280000066)), ((-86.68268362999999 32.35466732000003, -86.68265226999995 32.35493512000005, -86.68233337999999 32.35490878000007, -86.68235971999997 32.354683960000045, -86.68247799999995 32.35466100000008, -86.68249694999997 32.35465190000008, -86.68268362999999 32.35466732000003)), ((-86.67970329999997 32.38560131000003, -86.67968191999995 32.385783770000046, -86.67951948999996 32.38577036000004, -86.67957599999994 32.38572800000003, -86.67970329999997 32.38560131000003)), ((-86.68009507999994 32.385006770000075, -86.68003230999994 32.38554234000003, -86.67978322999994 32.385521770000025, -86.67979099999997 32.38551400000006, -86.67987999999997 32.385392000000024, -86.67995799999994 32.38525800000008, -86.68009899999998 32.38498400000003, -86.68028899999996 32.38463800000005, -86.68050353999996 32.38426988000003, -86.68041407999993 32.38503312000006, -86.68009507999994 32.385006770000075)), ((-86.68090986999994 32.38355104000004, -86.68082723999999 32.384256110000024, -86.68052605999998 32.38423123000007, -86.68083099999996 32.38370800000007, -86.68090986999994 32.38355104000004)), ((-86.68142865999994 32.38187238000006, -86.68139727999994 32.38214017000007, -86.68115824999995 32.38212043000004, -86.68118971999996 32.38185265000004, -86.68142865999994 32.38187238000006)), ((-86.68149141999999 32.38133681000005, -86.68146003999999 32.38160460000006, -86.68122118999997 32.38158487000004, -86.68125264999998 32.38131709000004, -86.68149141999999 32.38133681000005)), ((-86.68142346999997 32.37916817000007, -86.68130445999998 32.38018375000007, -86.68128199999995 32.380040000000065, -86.68127099999998 32.379894000000036, -86.68127299999998 32.37982100000005, -86.68130399999995 32.37953100000004, -86.68133399999994 32.37938900000006, -86.68135199999995 32.379327000000046, -86.68139599999995 32.37917900000008, -86.68140499999998 32.37915300000003, -86.68145199999998 32.37901300000004, -86.68153699999993 32.378757000000064, -86.68157799999994 32.378611000000035, -86.68161699999996 32.37847300000004, -86.68164999999999 32.37833100000006, -86.68227199999995 32.374547000000064, -86.68231875999999 32.374275760000046, -86.68227583999999 32.374642110000025, -86.68578443999996 32.37493180000007, -86.68575307999998 32.37519959000008, -86.68543410999996 32.375173260000054, -86.68537138999994 32.37570884000007, -86.68505241999998 32.37568251000005, -86.68502104999999 32.375950300000056, -86.68470207999997 32.37592397000003, -86.68467071999999 32.37619176000004, -86.68435174999996 32.37616542000006, -86.68432037999997 32.37643321000007, -86.68400140999995 32.37640688000005, -86.68393866999997 32.376942460000066, -86.68361969999995 32.37691612000003, -86.68358832999996 32.37718391000004, -86.68326935999994 32.377157570000065, -86.68320660999996 32.37769315000003, -86.68288763999999 32.37766681000005, -86.68298174999995 32.37686344000008, -86.68266277999999 32.376837110000054, -86.68269415999998 32.376569320000044, -86.68237518999996 32.37654298000007, -86.68231243999998 32.37707855000008, -86.68199345999994 32.377052210000045, -86.68196208999996 32.377320000000054, -86.68228105999998 32.37734634000003, -86.68224968999994 32.37761413000004, -86.68193070999996 32.377587790000064, -86.68186795999998 32.378123370000026, -86.68250590999997 32.378176050000036, -86.68247453999999 32.378443840000045, -86.68279351999996 32.37847017000007, -86.68273076999998 32.37900575000003, -86.68241178999995 32.378979410000056, -86.68238040999995 32.379247200000066, -86.68142346999997 32.37916817000007)), ((-86.68299737999996 32.368482920000076, -86.68291152999996 32.36921588000007, -86.68273752999994 32.36846146000005, -86.68299737999996 32.368482920000076)), ((-86.68319084999996 32.36958043000004, -86.68293988999994 32.37172276000007, -86.68276140999996 32.37170802000003, -86.68279399999994 32.371519000000035, -86.68285299999997 32.37108500000005, -86.68296599999996 32.37006400000007, -86.68298099999998 32.36977100000007, -86.68297699999994 32.36962500000004, -86.68297089999999 32.369562270000074, -86.68319084999996 32.36958043000004)), ((-86.68847912999996 32.39867667000004, -86.68752194999996 32.39859770000004, -86.68755331999995 32.39832992000004, -86.68723425999997 32.39830360000008, -86.68726561999995 32.398035820000075, -86.68694656999998 32.39800949000005, -86.68700929999994 32.39747393000005, -86.68637118999999 32.39742128000006, -86.68640255999998 32.39715350000006, -86.68608349999994 32.39712717000003, -86.68605520999995 32.39736865000003, -86.68589899999995 32.39724300000006, -86.68573201999999 32.39709816000004, -86.68576444999997 32.397100840000064, -86.68582718999994 32.39656528000006, -86.68518908999994 32.39651262000007, -86.68522045999998 32.396244840000065, -86.68490140999995 32.39621850000003, -86.68484027999995 32.39674022000003, -86.68470984999999 32.39674344000008, -86.68451960999994 32.396727730000066, -86.68451846999994 32.396737450000046, -86.68443199999996 32.39671500000003, -86.68434999999994 32.396668000000034, -86.68459699999994 32.39630900000003, -86.68479699999995 32.395980000000066, -86.68486899999994 32.39584700000006, -86.68493199999995 32.39571200000006, -86.68498399999999 32.39557200000007, -86.68501699999996 32.39542600000004, -86.68503499999997 32.395282000000066, -86.68504391999994 32.395148780000056, -86.68534594999994 32.39517371000005, -86.68525182999997 32.395977060000064, -86.68716612999998 32.39613503000004, -86.68719749999997 32.39586725000004, -86.68783559999997 32.39591989000007, -86.68780423999993 32.396187680000025, -86.68812328999996 32.39621400000004, -86.68799782999997 32.39728512000005, -86.68895499999996 32.39736409000005, -86.68892363999998 32.397631870000055, -86.68860457999995 32.39760555000004, -86.68854185999999 32.39814111000004, -86.68886091999997 32.39816743000006, -86.68882955999999 32.39843521000006, -86.68851049999995 32.39840889000004, -86.68847912999996 32.39867667000004)), ((-86.68694656999998 32.39800949000005, -86.68691817999996 32.39825183000005, -86.68674499999997 32.398055000000056, -86.68669063999994 32.397988370000064, -86.68694656999998 32.39800949000005)), ((-86.68637118999999 32.39742128000006, -86.68634868999999 32.397613310000054, -86.68626699999999 32.39753900000005, -86.68609199999997 32.39739824000003, -86.68637118999999 32.39742128000006)), ((-86.68547676999998 32.39680673000004, -86.68546897999994 32.396873190000065, -86.68541999999997 32.39683300000007, -86.68534396999996 32.39679577000004, -86.68547676999998 32.39680673000004)), ((-86.68708531999994 32.35557159000007, -86.68705396999997 32.35583939000003, -86.68577840999995 32.35573407000004, -86.68584111999996 32.35519847000006, -86.68743555999998 32.355330120000076, -86.68744510999994 32.35524853000004, -86.68749099999997 32.355260000000044, -86.68761199999994 32.355323000000055, -86.68766099999993 32.35533700000008, -86.68771699999996 32.355372000000045, -86.68786999999998 32.355421000000035, -86.68791799999997 32.35544900000008, -86.68804699999998 32.355485000000044, -86.68806052999997 32.35549208000003, -86.68804197999998 32.35565057000008, -86.68708531999994 32.35557159000007)), ((-86.71422337999996 32.38511647000007, -86.71390435999996 32.38509022000005, -86.71393562999998 32.38482243000004, -86.71361661999998 32.38479618000002, -86.71367915999997 32.38426059000005, -86.71336014999997 32.384234340000035, -86.71339141999994 32.383966540000074, -86.71371042999994 32.383992800000044, -86.71374170999997 32.38372500000003, -86.71278467999997 32.383646240000076, -86.71284722999997 32.38311066000006, -86.71316623999996 32.383136910000076, -86.71322877999995 32.38260132000005, -86.71386678999994 32.38265382000003, -86.71392932999998 32.38211823000006, -86.71361032999994 32.38209198000004, -86.71364159999996 32.38182419000003, -86.71300359999998 32.38177168000004, -86.71294104999998 32.38230727000007, -86.71262204999994 32.38228102000005, -86.71265331999996 32.38201322000003, -86.71233431999997 32.38198697000007, -86.71227176999997 32.38252256000004, -86.71195275999997 32.38249630000007, -86.71198403999995 32.38222851000006, -86.71145735999994 32.382185150000055, -86.71145499999994 32.38215100000008, -86.71148599999998 32.38164100000006, -86.71147195999998 32.381097890000035, -86.71159660999996 32.38003062000007, -86.71160499999996 32.380007000000035, -86.71165599999995 32.37976200000003, -86.71168799999998 32.37954800000006, -86.71171399999997 32.37906400000003, -86.71171383999996 32.37902698000005, -86.71175264999994 32.378694660000065, -86.71168445999996 32.37868905000005, -86.71166299999999 32.378577000000064, -86.71163399999995 32.37839400000007, -86.71153199999998 32.37812300000007, -86.71144999999996 32.377958000000035, -86.71129399999995 32.37761500000005, -86.71121999999997 32.37747100000007, -86.71119899999997 32.37739800000003, -86.71117199999998 32.37737500000003, -86.71100399999995 32.37710800000008, -86.71097588999999 32.377073050000035, -86.71098335999994 32.377009110000074, -86.71091738999996 32.37700368000003, -86.71077199999996 32.37688000000003, -86.71070699999996 32.37684000000007, -86.71068177999996 32.37683389000006, -86.71075821999995 32.37617946000006, -86.71043923999997 32.37615320000003, -86.71047051999994 32.37588540000007, -86.70951358999997 32.37580661000004, -86.70954486999995 32.37553882000003, -86.70922588999997 32.37551255000005, -86.70919460999994 32.37578035000007, -86.70887562999997 32.375754090000044, -86.70881305999995 32.37628968000007, -86.70849407999998 32.376263410000035, -86.70852536999996 32.375995620000026, -86.70820638999999 32.37596935000005, -86.70811252999994 32.376772740000035, -86.70843150999997 32.37679900000006, -86.70846279999995 32.37653121000005, -86.70910075999996 32.37658374000006, -86.70897561999999 32.377654920000055, -86.70865662999995 32.37762866000003, -86.70868791999999 32.37736086000007, -86.70773096999994 32.37728206000003, -86.70776225999998 32.37701426000007, -86.70744327999995 32.37698799000003, -86.70738069999999 32.37752359000007, -86.70706171999996 32.37749732000003, -86.70693654999997 32.37856849000008, -86.70725552999994 32.378594760000055, -86.70722423999996 32.37886256000007, -86.70754322999994 32.37888883000005, -86.70751193999996 32.37915662000006, -86.70783092999994 32.37918289000004, -86.70776834999998 32.379718480000065, -86.70808733999996 32.37974475000004, -86.70802475999994 32.38028034000007, -86.70898173999996 32.38035914000005, -86.70895045999998 32.38062693000006, -86.70926944999997 32.38065319000003, -86.70920687999995 32.38118878000006, -86.70952587999994 32.38121505000004, -86.70943201999995 32.38201843000007, -86.71038902999999 32.38209721000004, -86.71035774999996 32.38236501000006, -86.71067674999995 32.38239127000003, -86.71061418999994 32.382926860000055, -86.71029517999995 32.38290060000003, -86.71023260999993 32.38343618000005, -86.71055161999999 32.38346244000007, -86.71042648999997 32.38453361000006, -86.71138352999998 32.38461239000003, -86.71135224999995 32.38488019000005, -86.71167125999995 32.384906440000066, -86.71163997999997 32.385174240000026, -86.71227800999998 32.38522675000007, -86.71221544999997 32.385762330000034, -86.71253446999998 32.38578859000006, -86.71244063999995 32.38659197000004, -86.71275965999996 32.38661822000006, -86.71263454999996 32.38768939000005, -86.71295357999998 32.38771564000007, -86.71282846999998 32.388786810000056, -86.71314749999999 32.388813060000075, -86.71311622999997 32.389080860000035, -86.71279719999995 32.389054600000065, -86.71273463999995 32.389590190000035, -86.71241560999994 32.389563930000065, -86.71247816999994 32.389028350000046, -86.71215912999997 32.389002090000076, -86.71219040999995 32.38873430000007, -86.71187137999999 32.38870805000005, -86.71190265999996 32.38844025000003, -86.71158363999996 32.38841400000007, -86.71161491999999 32.38814621000006, -86.71129588999997 32.38811995000003, -86.71132716999995 32.38785216000008, -86.71164619999996 32.38787842000005, -86.71170875999996 32.38734283000008, -86.71138973999996 32.38731657000005, -86.71145229999996 32.386780990000034, -86.71177131999997 32.38680725000006, -86.71183387999997 32.386271660000034, -86.71119583999996 32.386219150000045, -86.71122711999999 32.385951360000035, -86.71090809999998 32.385925100000065, -86.71093938999996 32.38565730000005, -86.71157741999997 32.385709820000045, -86.71160869999994 32.385442030000036, -86.71128967999994 32.385415770000066, -86.71132095999997 32.385147980000056, -86.71100194999997 32.38512172000003, -86.71103322999994 32.38485393000008, -86.71039520999994 32.38480141000008, -86.71036391999996 32.38506920000003, -86.71004490999997 32.38504294000006, -86.71001361999998 32.38531073000007, -86.71033263999999 32.38533699000004, -86.71023877999994 32.38614037000008, -86.71055779999995 32.38616663000005, -86.71049523999994 32.38670221000007, -86.71081425999995 32.38672847000004, -86.71078296999997 32.386996270000054, -86.71110199999998 32.38702252000007, -86.71100814999994 32.38782590000005, -86.71068911999998 32.387799640000026, -86.71059526999994 32.388603010000054, -86.71027623999998 32.38857675000003, -86.71033880999994 32.388041170000065, -86.71001978999999 32.38801491000004, -86.71008235999994 32.387479330000076, -86.70976333999994 32.38745307000005, -86.70982590999995 32.38691748000008, -86.70950688999994 32.38689122000005, -86.70953817999998 32.38662343000004, -86.70929924999996 32.38660376000007, -86.70928199999997 32.386577000000045, -86.70924899999994 32.386536000000035, -86.70923219999997 32.38648559000006, -86.70931301999997 32.38579379000004, -86.70907291999998 32.38577403000005, -86.70901799999996 32.38564300000007, -86.70901019999997 32.38562890000003, -86.70902528999994 32.38549973000005, -86.70891382999997 32.38549055000004, -86.70883799999996 32.38539900000006, -86.70861899999994 32.38521000000003, -86.70855299999994 32.38516800000008, -86.70853199999999 32.38516400000003, -86.70848899999999 32.38514200000003, -86.70830999999998 32.38509400000004, -86.70826099999994 32.38506700000005, -86.70817199999999 32.38504700000004, -86.70811560999994 32.385015570000064, -86.70813082999996 32.38488535000005, -86.70799007999995 32.38487376000006, -86.70796799999994 32.38483300000007, -86.70796799999994 32.38480700000002, -86.70795099999998 32.38477100000006, -86.70793599999996 32.38471800000008, -86.70790299999999 32.38467600000007, -86.70783943999999 32.38462269000007, -86.70787439999998 32.38432350000005, -86.70755538999998 32.38429723000007, -86.70758667999996 32.38402944000006, -86.70726767999997 32.38400317000003, -86.70729896999995 32.383735380000076, -86.70666094999996 32.38368284000006, -86.70669224999995 32.38341505000005, -86.70733025999994 32.383467590000066, -86.70742413999994 32.38266421000003, -86.70710513999995 32.38263794000005, -86.70713642999993 32.38237015000004, -86.70681742999994 32.382343880000064, -86.70688001999997 32.38180829000004, -86.70656101999998 32.38178202000006, -86.70643583999998 32.38285319000005, -86.70611682999998 32.38282692000007, -86.70608553999995 32.38309471000008, -86.70576652999995 32.383068440000045, -86.70570392999997 32.383604020000064, -86.70474690999998 32.38352519000006, -86.70471560999994 32.383792990000074, -86.70439660999995 32.38376671000003, -86.70436529999995 32.38403450000004, -86.70404628999995 32.384008220000055, -86.70407759999995 32.383740430000046, -86.70248256999997 32.38360903000006, -86.70251387999997 32.38334123000004, -86.70219486999997 32.383314950000056, -86.70222617999997 32.38304716000005, -86.70095016999994 32.38294202000003, -86.70091885999994 32.38320981000004, -86.70059985999995 32.383183520000046, -86.70063116999995 32.382915730000036, -86.70031216999996 32.38288944000004, -86.70028084999996 32.38315723000005, -86.69996184999997 32.38313094000006, -86.69989920999996 32.38366652000008, -86.69958020999997 32.383640230000026, -86.69948624999995 32.384443600000054, -86.69884822999995 32.384391020000066, -86.69894219999998 32.38358765000004, -86.69862319999999 32.383561350000036, -86.69881112999997 32.38195461000004, -86.69849212999998 32.38192832000004, -86.69852344999998 32.38166053000003, -86.69820445999994 32.38163423000003, -86.69826709999995 32.38109865000007, -86.69794810999997 32.381072360000076, -86.69797942999998 32.38080456000006, -86.69766043999994 32.380778270000064, -86.69769175999994 32.380510480000055, -86.69737276999996 32.38048418000005, -86.69740409999997 32.380216390000044, -86.69708510999999 32.38019009000004, -86.69711642999994 32.37992230000003, -86.69615946999994 32.37984340000003, -86.69622212999997 32.379307820000065, -86.69558414999995 32.37925522000006, -86.69561548999997 32.378987420000044, -86.69529649999998 32.37896112000004, -86.69535915999995 32.37842554000002, -86.69504017999998 32.37839923000007, -86.69513417999997 32.37759586000004, -86.69481519999994 32.37756956000004, -86.69484652999995 32.37730176000008, -86.69452754999998 32.37727546000008, -86.69455888999994 32.37700767000007, -86.69392093999994 32.37695505000005, -86.69395226999995 32.37668726000004, -86.69363329999999 32.37666095000003, -86.69366462999994 32.37639316000008, -86.69334565999998 32.37636685000007, -86.69337699999994 32.37609906000006, -86.69273904999994 32.37604644000004, -86.69277038999996 32.37577865000003, -86.69245141999994 32.375752340000076, -86.69248275999996 32.37548455000007, -86.69216378999994 32.37545824000006, -86.69222646999998 32.37492265000003, -86.69158853999994 32.37487002000006, -86.69161987999996 32.37460223000005, -86.69130091999995 32.37457592000004, -86.69133225999997 32.37430813000003, -86.69165121999998 32.37433444000004, -86.69177658999996 32.37326327000005, -86.69145762999995 32.37323696000004, -86.69161433999994 32.371897990000036, -86.69129538999994 32.37187168000003, -86.69120135999998 32.37267506000006, -86.69088239999996 32.372648740000045, -86.69085105999994 32.372916530000055, -86.68925625999998 32.37278494000003, -86.68928760999995 32.37251715000008, -86.68896865999994 32.37249083000006, -86.68893730999997 32.37275862000007, -86.68861834999996 32.37273230000005, -86.68855563999995 32.37326788000007, -86.68791772999998 32.37321523000003, -86.68794907999995 32.372947440000075, -86.68763011999994 32.37292112000006, -86.68766147999997 32.37265332000004, -86.68734251999996 32.37262700000008, -86.68737387999994 32.37235921000007, -86.68801178999996 32.37241186000006, -86.68804313999999 32.37214407000005, -86.68836209999995 32.372170390000065, -86.68839344999998 32.371902600000055, -86.68871239999999 32.37192892000007, -86.68874374999996 32.371661130000064, -86.68970061999994 32.37174009000006, -86.68973195999996 32.37147230000005, -86.69005091999998 32.37149862000007, -86.69008225999994 32.37123082000005, -86.69072016999996 32.37128346000003, -86.69078284999995 32.37074787000006, -86.69046389999994 32.37072156000005, -86.69043255999998 32.37098935000006, -86.69011360999997 32.37096303000004, -86.69017629999996 32.37042744000007, -86.68953839999995 32.370374810000044, -86.68950705999998 32.37064260000005, -86.68918810999998 32.370616280000036, -86.68915675999995 32.370884070000045, -86.68851885999999 32.37083143000007, -86.68848750999996 32.371099220000076, -86.68816854999994 32.37107290000006, -86.68819990999998 32.37080510000004, -86.68692410999995 32.37069980000007, -86.68689274999997 32.37096759000008, -86.68625484999995 32.370914940000034, -86.68634892999995 32.370111560000055, -86.68602997999994 32.37008523000003, -86.68606133999998 32.36981744000008, -86.68638028999999 32.369843770000045, -86.68650571999996 32.36877259000005, -86.68586783999996 32.36871994000006, -86.68615006999994 32.36630979000006, -86.68678792999998 32.366362450000054, -86.68672521999997 32.366898040000024, -86.68704414999996 32.36692437000005, -86.68710685999997 32.36638878000008, -86.68774471999996 32.366441430000066, -86.68777607999994 32.366173640000056, -86.68809500999998 32.366199960000074, -86.68812635999996 32.365932170000065, -86.68876421999994 32.36598481000004, -86.68879556999997 32.365717020000034, -86.68911449999996 32.36574334000005, -86.68908314999999 32.36601113000006, -86.69003993999996 32.366090100000065, -86.69007128999993 32.36582230000005, -86.69070914999998 32.365874940000026, -86.69067780999995 32.366142730000035, -86.69099673999995 32.36616905000005, -86.69102807999997 32.365901250000036, -86.69134700999996 32.36592757000005, -86.69131566999994 32.36619536000006, -86.69291032999996 32.36632693000007, -86.69287899999995 32.36659473000003, -86.69256006999996 32.36656841000007, -86.69252872999994 32.36683621000003, -86.69284765999998 32.36686252000004, -86.69281632999997 32.36713032000006, -86.69249738999997 32.36710401000005, -86.69234070999994 32.368442980000054, -86.69265964999994 32.368469290000064, -86.69262830999998 32.368737090000025, -86.69390407999998 32.368842330000064, -86.69387274999997 32.36911012000007, -86.69419169999998 32.369136430000026, -86.69422302999999 32.36886864000007, -86.69454196999999 32.368894940000075, -86.69451063999998 32.369162740000036, -86.69514852999998 32.36921535000005, -86.69505453999994 32.37001873000003, -86.69537348999995 32.370045040000036, -86.69534215999994 32.370312830000046, -86.69470426999999 32.370260220000034, -86.69467292999997 32.37052802000005, -86.69339712999994 32.37042279000008, -86.69336579999998 32.37069058000003, -86.69272789999997 32.37063796000007, -86.69269655999994 32.37090576000003, -86.69429131999999 32.37103730000007, -86.69425998999998 32.37130510000003, -86.69457893999999 32.37133140000003, -86.69461026999994 32.37106361000008, -86.69524817999996 32.37111622000003, -86.69527950999998 32.37084842000007, -86.69591740999994 32.37090103000003, -86.69582342999996 32.37170441000006, -86.69614238999998 32.371730710000065, -86.69617370999998 32.371462920000056, -86.69649266999994 32.37148922000006, -86.69652398999995 32.37122143000005, -86.69620503999994 32.37119512000004, -86.69626768999996 32.37065953000007, -86.69658663999996 32.37068584000008, -86.69655531999996 32.37095363000003, -86.69783112999994 32.37105882000003, -86.69786244999995 32.37079103000008, -86.69690558999997 32.37071214000002, -86.69693691999998 32.37044434000006, -86.69661796999998 32.37041804000006, -86.69664928999998 32.37015025000005, -86.69633033999997 32.37012394000004, -86.69639298999994 32.36958835000007, -86.69607403999998 32.36956205000007, -86.69626198999998 32.36795528000005, -86.69721881999999 32.36803418000005, -86.69715616999997 32.36856977000008, -86.69747511999998 32.368596070000024, -86.69744379999997 32.36886386000003, -86.69935746999994 32.36902164000003, -86.69932615999994 32.36928943000004, -86.69964509999994 32.36931572000003, -86.69961378999994 32.36958352000005, -86.69993273999995 32.36960981000004, -86.69990142999995 32.36987761000006, -86.70053932999997 32.36993019000005, -86.70050800999996 32.370197990000065, -86.70082696999998 32.37022427000005, -86.70076434999999 32.37075987000003, -86.70172120999996 32.37083873000006, -86.70168989999996 32.37110652000007, -86.70200884999997 32.37113281000006, -86.70197754999998 32.37140061000002, -86.70229650999994 32.371426890000066, -86.70226519999994 32.37169469000003, -86.70258415999996 32.37172097000007, -86.70255284999996 32.37198877000003, -86.70287180999998 32.37201505000007, -86.70284050999999 32.37228285000003, -86.70315946999995 32.372309130000076, -86.70309685999996 32.372844720000046, -86.70341581999998 32.37287100000003, -86.70338451999999 32.37313880000005, -86.70370348999995 32.373165080000035, -86.70367217999996 32.37343288000005, -86.70431011999995 32.37348543000007, -86.70427881999996 32.373753230000034, -86.70459777999997 32.37377951000008, -86.70456647999998 32.37404730000003, -86.70488544999995 32.37407358000007, -86.70482285999998 32.37460917000004, -86.70514182999995 32.37463545000003, -86.70511052999996 32.37490324000004, -86.70542949999998 32.37492952000002, -86.70536690999995 32.37546511000005, -86.70600485999995 32.37551766000007, -86.70597355999996 32.37578546000003, -86.70724945999996 32.37589054000006, -86.70728074999994 32.37562275000005, -86.70696177999997 32.37559648000007, -86.70699306999995 32.37532868000005, -86.70731203999998 32.37535495000003, -86.70734332999996 32.37508715000007, -86.70702435999993 32.375060880000035, -86.70705564999997 32.374793090000026, -86.70737461999994 32.37481936000006, -86.70749977999998 32.373748170000056, -86.70781874999994 32.373774440000034, -86.70785003999998 32.37350664000007, -86.70753106999996 32.373480380000046, -86.70756235999994 32.37321258000003, -86.70788132999996 32.373238850000064, -86.70791260999994 32.37297105000005, -86.70759364999998 32.37294478000007, -86.70762493999996 32.37267698000005, -86.70730596999994 32.37265071000007, -86.70736854999996 32.372115120000046, -86.70641166999997 32.372036310000055, -86.70650553999997 32.37123291000006, -86.70618658999996 32.371206640000025, -86.70621787999994 32.370938840000065, -86.70589891999998 32.37091257000003, -86.70583633999996 32.371448170000065, -86.70519841999999 32.371395610000036, -86.70522971999998 32.37112782000003, -86.70491075999996 32.37110154000004, -86.70494205999995 32.37083374000002, -86.70462309999994 32.370807470000045, -86.70465439999998 32.37053967000003, -86.70433543999997 32.37051339000004, -86.70439803999994 32.36997780000007, -86.70407908999994 32.36995152000003, -86.70404778999995 32.37021932000005, -86.70372883999994 32.37019304000006, -86.70382273999996 32.369389640000065, -86.70286588999994 32.36931080000005, -86.70289718999999 32.36904300000003, -86.70257824999999 32.36901672000005, -86.70254693999999 32.369284520000065, -86.70190904999998 32.36923195000003, -86.70194034999997 32.36896415000007, -86.70162140999997 32.368937860000074, -86.70165270999996 32.368670070000064, -86.70133376999996 32.36864378000007, -86.70136506999995 32.36837598000005, -86.70104612999995 32.36834970000007, -86.70107743999995 32.36808190000005, -86.70012060999994 32.36800303000007, -86.70018322999994 32.36746744000004, -86.69954534999994 32.367414850000046, -86.69951403999994 32.36768265000006, -86.69887615999994 32.36763006000007, -86.69890746999994 32.36736227000006, -86.69795065999995 32.36728338000006, -86.69798197999995 32.36701558000004, -86.69702515999995 32.366936690000045, -86.69708780999997 32.366401100000076, -86.69644993999998 32.36634850000007, -86.69641860999997 32.366616290000024, -86.69578073999998 32.36656369000008, -86.69581206999999 32.36629589000006, -86.69517419999994 32.36624329000006, -86.69520552999995 32.36597549000004, -86.69488658999995 32.36594918000003, -86.69491791999997 32.36568139000008, -86.69459898999997 32.36565508000007, -86.69463031999999 32.36538728000005, -86.69431138999994 32.36536098000005, -86.69434271999995 32.36509318000003, -86.69402378999996 32.36506687000008, -86.69399245999995 32.36533467000004, -86.69367352999996 32.36530836000003, -86.69370485999997 32.365040570000076, -86.69338592999998 32.365014260000066, -86.69341726999994 32.36474646000005, -86.69277940999996 32.36469384000003, -86.69284207999993 32.36415825000006, -86.69252315999995 32.36413193000004, -86.69255448999996 32.36386414000003, -86.69223556999998 32.36383782000007, -86.69226690999994 32.36357003000006, -86.69162905999997 32.363517400000035, -86.69169173999995 32.362981810000065, -86.69137281999997 32.36295549000005, -86.69143549999995 32.36241990000008, -86.69111657999997 32.36239358000006, -86.69117925999996 32.36185799000003, -86.69022251999996 32.36177903000004, -86.69019116999993 32.362046830000054, -86.68955333999997 32.361994190000075, -86.68949064999998 32.36252979000005, -86.68917173999995 32.36250346000003, -86.68914038999998 32.362771260000045, -86.69009713999998 32.36285022000004, -86.69003445999994 32.363385820000076, -86.69035337999998 32.36341213000003, -86.69032203999996 32.363679930000046, -86.69000310999996 32.36365361000003, -86.68997176999994 32.363921410000046, -86.68965284999996 32.36389509000003, -86.68971553999995 32.36335950000006, -86.68939661999997 32.36333318000004, -86.68936526999994 32.36360097000005, -86.68904634999996 32.36357465000003, -86.68910903999995 32.36303906000006, -86.68879012999997 32.363012740000045, -86.68882146999994 32.36274494000003, -86.68754579999995 32.362639640000054, -86.68757715999999 32.362371850000045, -86.68693931999996 32.36231919000005, -86.68697067999994 32.36205140000004, -86.68665175999996 32.36202507000007, -86.68668311999994 32.361757270000055, -86.68636419999996 32.36173094000003, -86.68639555999994 32.36146315000008, -86.68607664999996 32.36143682000005, -86.68617071999995 32.360633430000064, -86.68648961999997 32.36065976000003, -86.68655232999998 32.360124160000055, -86.68687123999996 32.360150490000024, -86.68690258999999 32.35988269000006, -86.68817821999994 32.359988000000044, -86.68820955999996 32.35972020000003, -86.68852846999994 32.359746520000044, -86.68849711999997 32.36001432000006, -86.68977274999997 32.36011961000003, -86.68980408999994 32.359851810000066, -86.69012299999997 32.35987813000003, -86.69021701999998 32.35907473000003, -86.69117372999995 32.35915369000003, -86.69120506999997 32.35888589000007, -86.69152397999994 32.35891220000008, -86.69146129999996 32.359447800000055, -86.69178020999999 32.35947412000007, -86.69181153999995 32.359206320000055, -86.69213044999998 32.359232630000065, -86.69209910999996 32.359500430000026, -86.69241801999999 32.359526740000035, -86.69244934999995 32.359258940000075, -86.69276825999998 32.359285260000036, -86.69292491999994 32.35794626000006, -86.69388161999996 32.358025190000035, -86.69385028999994 32.35829299000005, -86.69416918999997 32.35831930000006, -86.69413785999996 32.35858710000008, -86.69541347999996 32.358692320000046, -86.69544479999996 32.35842452000003, -86.69576369999999 32.35845082000003, -86.69563840999996 32.35952202000004, -86.69627622999997 32.359574630000054, -86.69624489999995 32.35984243000007, -86.69847726999996 32.360026520000076, -86.69844595999996 32.36029432000004, -86.69908377999997 32.36034691000003, -86.69905246999997 32.36061471000005, -86.69937137999995 32.360641000000044, -86.69934006999995 32.36090880000006, -86.69806440999997 32.36080362000007, -86.69809572999998 32.36053582000005, -86.69522552999996 32.360299120000036, -86.69516287999994 32.36083472000007, -86.69580070999996 32.360887320000074, -86.69573805999994 32.36142292000005, -86.69446239999996 32.36131770000003, -86.69439974999995 32.361853300000064, -86.69503757999996 32.361905910000075, -86.69500624999995 32.362173710000036, -86.69564408999997 32.36222632000005, -86.69561275999996 32.362494120000065, -86.69688843999995 32.36259932000007, -86.69691975999996 32.36233153000006, -86.69787651999997 32.36241042000006, -86.69778256999996 32.363213820000055, -86.69810148999994 32.36324011000005, -86.69807016999994 32.36350791000007, -86.69838909999999 32.36353421000007, -86.69832645999998 32.364069810000046, -86.69864538999997 32.36409610000004, -86.69858275999997 32.364631700000075, -86.69890168999996 32.36465799000007, -86.69887036999995 32.36492579000003, -86.69982715999998 32.365004670000076, -86.70014026999996 32.362326680000024, -86.70205378999998 32.362484410000036, -86.70202247999998 32.36275221000005, -86.70266032999996 32.362804770000025, -86.70262902999997 32.36307257000004, -86.70326687999994 32.36312514000008, -86.70323557999996 32.36339294000004, -86.70451127999996 32.36349806000004, -86.70447997999997 32.36376586000006, -86.70479890999997 32.363792140000044, -86.70473631999994 32.36432774000008, -86.70537417999998 32.36438029000004, -86.70534288999994 32.36464809000006, -86.70566181999999 32.364674370000046, -86.70563052999995 32.36494217000006, -86.70690625999998 32.36504726000004, -86.70687496999994 32.36531506000006, -86.70846963999998 32.36544641000006, -86.70843835999995 32.36571421000008, -86.70875728999994 32.365740480000056, -86.70872600999996 32.366008280000074, -86.70936388999996 32.36606081000008, -86.70933260999999 32.36632861000004, -86.70997047999998 32.366381140000044, -86.70993920999996 32.36664894000006, -86.71025814999996 32.36667520000003, -86.71022686999999 32.36694300000005, -86.71150263999994 32.36704804000004, -86.71147135999996 32.36731584000006, -86.71274713999998 32.367420870000046, -86.71271586999995 32.367688670000064, -86.71335375999996 32.36774118000005, -86.71332248999994 32.36800898000007, -86.71364143999995 32.36803523000003, -86.71357890999997 32.36857083000007, -86.71389785999997 32.36859709000004, -86.71386658999995 32.368864890000054, -86.71418553999996 32.36889114000007, -86.71415427999995 32.369158940000034, -86.71447322999995 32.36918519000005, -86.71444195999999 32.36945299000007, -86.71476091999995 32.36947924000003, -86.71472964999998 32.36974704000005, -86.71504860999994 32.36977329000007, -86.71501734999998 32.37004109000003, -86.71533629999999 32.37006734000005, -86.71530503999998 32.370335140000066, -86.71562399999993 32.37036138000008, -86.71559273999998 32.37062918000004, -86.71591169999994 32.370655430000056, -86.71588043999998 32.37092323000007, -86.71619938999999 32.370949480000036, -86.71616813999998 32.37121728000005, -86.71648709999994 32.371243520000064, -86.71642457999997 32.37177912000004, -86.71674353999998 32.37180537000006, -86.71671228999998 32.37207317000008, -86.71703124999999 32.37209941000003, -86.71699998999998 32.37236721000005, -86.71731895999994 32.37239345000006, -86.71728769999999 32.37266125000008, -86.71760666999995 32.37268749000003, -86.71757541999995 32.37295529000005, -86.71789437999996 32.37298153000006, -86.71783187999995 32.37351713000004, -86.71815084999997 32.37354337000005, -86.71811958999996 32.37381117000007, -86.71843855999998 32.37383741000008, -86.71818853999997 32.37597981000005, -86.71786955999994 32.375953570000036, -86.71790080999995 32.375685770000075, -86.71758183999998 32.375659530000064, -86.71755057999997 32.375927330000025, -86.71723159999999 32.37590109000007, -86.71741913999995 32.37429429000008, -86.71678118999995 32.37424181000006, -86.71634355999998 32.37799098000005, -86.71666254999997 32.37801723000007, -86.71663128999995 32.37828502000008, -86.71631229999997 32.37825878000007, -86.71628103999996 32.37852658000003, -86.71468608999999 32.378395340000054, -86.71465482999997 32.37866314000007, -86.71433583999999 32.37863689000005, -86.71430456999997 32.37890469000007, -86.71366658999995 32.37885219000003, -86.71360404999996 32.37938778000006, -86.71328504999997 32.37936153000004, -86.71331632999994 32.37909373000008, -86.71299732999995 32.37906748000006, -86.71293478999996 32.37960307000003, -86.71261579999998 32.37957681000006, -86.71245941999996 32.380915790000074, -86.71277841999995 32.380942040000036, -86.71271586999995 32.38147763000006, -86.71303486999994 32.38150389000003, -86.71306614999997 32.38123609000007, -86.71338514999997 32.38126235000004, -86.71341641999999 32.380994550000025, -86.71373541999998 32.381020800000044, -86.71370414999996 32.38128860000006, -86.71402314999995 32.381314850000024, -86.71396060999996 32.38185044000005, -86.71427960999995 32.38187669000007, -86.71421706999996 32.38241228000004, -86.71453606999995 32.38243853000006, -86.71450479999999 32.38270632000007, -86.71482380999998 32.38273257000003, -86.71476126999994 32.38326816000006, -86.71444226999995 32.38324191000004, -86.71437972999996 32.383777500000065, -86.71469873999996 32.38380375000003, -86.71466746999994 32.384071550000044, -86.71434845999994 32.384045300000025, -86.71422337999996 32.38511647000007), (-86.71316623999996 32.383136910000076, -86.71313495999999 32.38340470000003, -86.71345396999999 32.383430960000055, -86.71348523999995 32.38316316000004, -86.71316623999996 32.383136910000076), (-86.71233431999997 32.38198697000007, -86.71236559999994 32.38171917000005, -86.71204659999995 32.381692920000035, -86.71201531999998 32.381960710000044, -86.71233431999997 32.38198697000007), (-86.70954486999995 32.37553882000003, -86.70986383999997 32.37556508000006, -86.70989512999995 32.37529728000004, -86.70957614999998 32.37527102000007, -86.70954486999995 32.37553882000003), (-86.70887562999997 32.375754090000044, -86.70890691999995 32.375486290000026, -86.70858793999997 32.37546002000005, -86.70855665999994 32.375727820000066, -86.70887562999997 32.375754090000044), (-86.70744327999995 32.37698799000003, -86.70750585999997 32.37645240000006, -86.70654892999994 32.37637359000007, -86.70651762999995 32.37664139000003, -86.70715558999996 32.376693930000044, -86.70712429999998 32.376961720000054, -86.70744327999995 32.37698799000003), (-86.70576652999995 32.383068440000045, -86.70582912999998 32.382532850000075, -86.70487211999995 32.382454030000076, -86.70484081999996 32.38272182000003, -86.70547882999995 32.38277437000005, -86.70544752999996 32.38304216000006, -86.70576652999995 32.383068440000045), (-86.70487211999995 32.382454030000076, -86.70490341999994 32.38218623000006, -86.70458441999995 32.382159960000024, -86.70455311999996 32.38242775000003, -86.70487211999995 32.382454030000076), (-86.70490341999994 32.38218623000006, -86.70554141999997 32.38223878000008, -86.70557271999996 32.38197099000007, -86.70493471999998 32.38191844000005, -86.70490341999994 32.38218623000006), (-86.70544752999996 32.38304216000006, -86.70512851999996 32.383015890000024, -86.70509721999997 32.383283680000034, -86.70541622999997 32.38330995000007, -86.70544752999996 32.38304216000006), (-86.69582342999996 32.37170441000006, -86.69518551999994 32.37165181000006, -86.69515418999998 32.37191960000007, -86.69579209999995 32.371972210000024, -86.69582342999996 32.37170441000006), (-86.69579209999995 32.371972210000024, -86.69576077999994 32.37224000000003, -86.69607972999995 32.372266300000035, -86.69611105999996 32.371998510000026, -86.69579209999995 32.371972210000024), (-86.69786244999995 32.37079103000008, -86.69850035999997 32.37084362000007, -86.69853167999997 32.370575830000064, -86.69789376999995 32.37052323000006, -86.69786244999995 32.37079103000008), (-86.70781874999994 32.373774440000034, -86.70778745999996 32.37404224000005, -86.70810642999999 32.37406851000003, -86.70813771999997 32.37380071000007, -86.70781874999994 32.373774440000034), (-86.70810642999999 32.37406851000003, -86.70807513999995 32.37433630000004, -86.70839410999997 32.37436257000007, -86.70842539999995 32.374094770000056, -86.70810642999999 32.37406851000003), (-86.70807513999995 32.37433630000004, -86.70775616999998 32.37431003000006, -86.70772488999995 32.37457783000008, -86.70804385999998 32.374604100000056, -86.70807513999995 32.37433630000004), (-86.70804385999998 32.374604100000056, -86.70801256999994 32.37487190000007, -86.70833153999996 32.37489816000004, -86.70836282999994 32.374630370000034, -86.70804385999998 32.374604100000056), (-86.69581206999999 32.36629589000006, -86.69613099999998 32.36632220000007, -86.69616232999994 32.36605440000005, -86.69584338999994 32.36602810000005, -86.69581206999999 32.36629589000006), (-86.69022251999996 32.36177903000004, -86.69025385999998 32.36151124000003, -86.68897820999996 32.36140595000006, -86.68894685999999 32.36167375000008, -86.69022251999996 32.36177903000004), (-86.68917173999995 32.36250346000003, -86.68920307999997 32.362235670000075, -86.68888416999994 32.36220935000006, -86.68885281999997 32.36247714000007, -86.68917173999995 32.36250346000003), (-86.69012299999997 32.35987813000003, -86.69009165999995 32.360145930000044, -86.69072946999995 32.36019856000007, -86.69076080999997 32.35993077000006, -86.69012299999997 32.35987813000003), (-86.69522552999996 32.360299120000036, -86.69525685999997 32.360031320000076, -86.69493794999994 32.360005010000066, -86.69490661999998 32.360272810000026, -86.69522552999996 32.360299120000036), (-86.68736847999998 32.36965496000005, -86.68743118999998 32.36911937000008, -86.68838801999999 32.369198340000025, -86.68841936999996 32.36893055000007, -86.68714359999996 32.36882525000004, -86.68711224999998 32.36909304000005, -86.68679330999998 32.369066720000035, -86.68673058999997 32.369602300000054, -86.68736847999998 32.36965496000005), (-86.69691702999995 32.376120610000044, -86.69694835999996 32.375852820000034, -86.69535349999995 32.375721320000025, -86.69532216999994 32.375989110000035, -86.69691702999995 32.376120610000044), (-86.69704232999999 32.375049440000055, -86.69707365999994 32.374781650000045, -86.69675468999998 32.374755350000044, -86.69678600999998 32.374487560000034, -86.69646704999997 32.37446126000003, -86.69649836999997 32.37419346000007, -86.69617940999996 32.37416716000007, -86.69608542999998 32.37497054000005, -86.69704232999999 32.375049440000055), (-86.69889351999996 32.37574281000008, -86.69892483999996 32.37547501000006, -86.69924380999998 32.37550131000006, -86.69930643999999 32.374965720000034, -86.69866849999994 32.37491313000004, -86.69863717999993 32.375180930000056, -86.69799923999994 32.37512833000005, -86.69793659999993 32.37566392000008, -86.69889351999996 32.37574281000008), (-86.69866849999994 32.37491313000004, -86.69869981999994 32.37464534000003, -86.69838084999998 32.37461904000003, -86.69834952999997 32.374886840000045, -86.69866849999994 32.37491313000004), (-86.69838084999998 32.37461904000003, -86.69844348999999 32.37408345000006, -86.69812452999997 32.37405716000006, -86.69818716999998 32.37352157000004, -86.69754923999994 32.37346898000004, -86.69758055999995 32.373201180000024, -86.69726159999999 32.37317489000003, -86.69729291999994 32.37290709000007, -86.69697395999998 32.37288079000007, -86.69703660999994 32.37234520000004, -86.69639868999997 32.37229260000004, -86.69633603999995 32.372828190000064, -86.69665499999996 32.372854490000066, -86.69662367999996 32.373122290000026, -86.69694263999997 32.37314859000003, -86.69687998999996 32.373684180000055, -86.69719894999997 32.37371047000005, -86.69716762999997 32.373978270000066, -86.69748658999998 32.37400457000007, -86.69745526999998 32.37427236000008, -86.69777423999994 32.37429866000008, -86.69774291999994 32.37456645000003, -86.69838084999998 32.37461904000003), (-86.70272711999996 32.37876249000004, -86.70275842999996 32.378494690000025, -86.70594828999998 32.37875747000004, -86.70601087999995 32.37822188000007, -86.70569189999998 32.378195610000034, -86.70566059999999 32.378463400000044, -86.70534161999996 32.378437130000066, -86.70537290999994 32.378169340000056, -86.70473493999998 32.37811678000003, -86.70470363999993 32.378384580000045, -86.70438465999996 32.37835830000006, -86.70441595999995 32.37809051000005, -86.70377798999994 32.37803795000008, -86.70374668999995 32.37830574000003, -86.70247074999997 32.37820062000003, -86.70240813999999 32.37873620000005, -86.70272711999996 32.37876249000004)), ((-86.71483013999995 32.38543676000006, -86.71419209999993 32.38538426000002, -86.71422337999996 32.38511647000007, -86.71486140999997 32.38516897000005, -86.71483013999995 32.38543676000006)), ((-86.71284722999997 32.38311066000006, -86.71252821999997 32.38308440000003, -86.71255949999994 32.38281661000008, -86.71224048999994 32.38279035000005, -86.71227176999997 32.38252256000004, -86.71290977999996 32.38257507000003, -86.71284722999997 32.38311066000006)), ((-86.71224048999994 32.38279035000005, -86.71220921999998 32.38305815000007, -86.71142757999996 32.38299381000007, -86.71146399999998 32.382889000000034, -86.71151999999995 32.38283100000007, -86.71156799999994 32.382772000000045, -86.71158459999998 32.38273637000003, -86.71160247999995 32.38273784000006, -86.71160868999993 32.382684690000076, -86.71161599999994 32.38266900000008, -86.71161259999997 32.38265117000003, -86.71163375999998 32.382470040000044, -86.71195275999997 32.38249630000007, -86.71192148999995 32.38276410000003, -86.71224048999994 32.38279035000005)), ((-86.71145229999996 32.386780990000034, -86.71081425999995 32.38672847000004, -86.71084553999998 32.38646068000003, -86.71148357999994 32.386513200000024, -86.71145229999996 32.386780990000034)), ((-86.71033263999999 32.38533699000004, -86.71036391999996 32.38506920000003, -86.71100194999997 32.38512172000003, -86.71093938999996 32.38565730000005, -86.71062036999996 32.38563105000003, -86.71065164999999 32.38536325000007, -86.71033263999999 32.38533699000004)), ((-86.70669224999995 32.38341505000005, -86.70605423999996 32.38336250000003, -86.70608553999995 32.38309471000008, -86.70640453999994 32.38312098000006, -86.70643583999998 32.38285319000005, -86.70675483999997 32.38287946000003, -86.70669224999995 32.38341505000005)), ((-86.69145762999995 32.37323696000004, -86.69139494999996 32.37377254000006, -86.69107597999994 32.37374623000005, -86.69104463999997 32.37401402000006, -86.69072567999996 32.37398770000004, -86.69075701999998 32.373719910000034, -86.68948116999997 32.37361464000003, -86.68951251999994 32.373346850000075, -86.69015043999997 32.373399480000046, -86.69018178999994 32.37313169000004, -86.69145762999995 32.37323696000004)), ((-86.70756235999994 32.37321258000003, -86.70692442999996 32.37316004000007, -86.70698700999998 32.37262444000004, -86.70730596999994 32.37265071000007, -86.70727467999995 32.372918510000034, -86.70759364999998 32.37294478000007, -86.70756235999994 32.37321258000003)), ((-86.70698700999998 32.37262444000004, -86.70666804999996 32.37259817000006, -86.70669933999994 32.37233038000005, -86.70701829999996 32.37235665000003, -86.70698700999998 32.37262444000004)), ((-86.69488658999995 32.36594918000003, -86.69482393999994 32.366484780000064, -86.69450499999994 32.366458470000055, -86.69456765999996 32.36592288000003, -86.69488658999995 32.36594918000003)), ((-86.69456765999996 32.36592288000003, -86.69392979999998 32.365870260000065, -86.69396112999999 32.365602470000056, -86.69459898999997 32.36565508000007, -86.69456765999996 32.36592288000003)), ((-86.69367352999996 32.36530836000003, -86.69364219999994 32.365576160000046, -86.69332326999995 32.365549850000036, -86.69335459999996 32.365282050000076, -86.69367352999996 32.36530836000003)), ((-86.69335459999996 32.365282050000076, -86.69303566999997 32.365255740000066, -86.69306700999994 32.36498795000006, -86.69338592999998 32.365014260000066, -86.69335459999996 32.365282050000076)), ((-86.69019116999993 32.362046830000054, -86.69082899999995 32.36209947000003, -86.69076631999997 32.36263506000006, -86.68980956999997 32.36255611000007, -86.68984090999999 32.362288310000054, -86.69015982999997 32.36231463000007, -86.69019116999993 32.362046830000054)), ((-86.71294104999998 32.38230727000007, -86.71326005999998 32.38233353000004, -86.71322877999995 32.38260132000005, -86.71290977999996 32.38257507000003, -86.71294104999998 32.38230727000007)), ((-86.71361032999994 32.38209198000004, -86.71357905999997 32.38235978000006, -86.71326005999998 32.38233353000004, -86.71329132999995 32.38206573000008, -86.71361032999994 32.38209198000004)), ((-86.68665887999998 32.39771538000008, -86.68663489999994 32.39792006000005, -86.68654999999995 32.397816000000034, -86.68644399999994 32.39770000000004, -86.68644115999996 32.39769742000004, -86.68665887999998 32.39771538000008)), ((-86.68835367999998 32.39974779000005, -86.68867274999997 32.39977411000007, -86.68864195999998 32.40003696000008, -86.68858099999994 32.39997200000005, -86.68835346999998 32.39974954000007, -86.68835367999998 32.39974779000005)), ((-86.68835367999998 32.39974779000005, -86.68835149999995 32.39974761000008, -86.68813199999994 32.399533000000076, -86.68806471999994 32.399464470000055, -86.68806597999998 32.39945369000003, -86.68838504999997 32.39948001000005, -86.68835367999998 32.39974779000005)), ((-86.68806597999998 32.39945369000003, -86.68805308999998 32.39945263000004, -86.68791599999997 32.39931300000006, -86.68780999999996 32.39919900000007, -86.68777806999998 32.39916142000004, -86.68777827999997 32.39915959000007, -86.68809734999996 32.39918591000003, -86.68806597999998 32.39945369000003)), ((-86.68982357999994 32.40095051000003, -86.68976842999996 32.401421440000036, -86.68964499999998 32.40127400000006, -86.68944399999998 32.40104800000006, -86.68934999999999 32.400933000000066, -86.68916699999994 32.40069100000005, -86.68898699999994 32.400442000000055, -86.68889299999995 32.400319000000025, -86.68879399999997 32.40019900000004, -86.68864701999996 32.400042350000035, -86.68896044999997 32.40006821000003, -86.68892908999999 32.40033599000003, -86.68924815999998 32.40036231000005, -86.68921679999994 32.40063009000005, -86.68953586999999 32.40065641000007, -86.68950450999995 32.40092419000007, -86.68982357999994 32.40095051000003)), ((-86.69161798999994 32.35810881000003, -86.69066128999998 32.358029860000045, -86.69069262999994 32.35776206000003, -86.69005482999995 32.35770942000005, -86.69002348999999 32.357977220000066, -86.68970458999996 32.35795090000005, -86.68973592999998 32.35768310000003, -86.68941702999996 32.35765678000007, -86.68944837999999 32.35738898000005, -86.68912947999996 32.357362660000035, -86.68919216999996 32.35682706000006, -86.68982995999994 32.356879700000036, -86.68979861999998 32.35714750000005, -86.69043640999996 32.35720014000003, -86.69046774999998 32.35693234000007, -86.69110553999997 32.35698498000005, -86.69104285999998 32.35752058000003, -86.69136175999995 32.35754689000004, -86.69133041999999 32.357814690000055, -86.69164931999995 32.35784101000007, -86.69161798999994 32.35810881000003)), ((-86.69066128999998 32.358029860000045, -86.69062994999996 32.35829766000006, -86.69051399999995 32.35828809000003, -86.69037299999997 32.358213000000035, -86.69031877999998 32.358205250000026, -86.69034238999996 32.35800354000003, -86.69066128999998 32.358029860000045)), ((-86.68991322999995 32.40149864000006, -86.68986599999994 32.40153800000007, -86.68982709999995 32.401491530000044, -86.68991322999995 32.40149864000006)), ((-86.70897625999999 32.36386587000004, -86.70929518999998 32.36389213000007, -86.70926390999995 32.364159940000036, -86.70894497999996 32.36413367000006, -86.70897625999999 32.36386587000004)), ((-86.70897625999999 32.36386587000004, -86.70840363999997 32.36381871000003, -86.70834999999994 32.36378000000008, -86.70834279999997 32.36377564000003, -86.70836967999998 32.36354553000007, -86.70783828999998 32.36350177000003, -86.70779899999997 32.36348000000004, -86.70773829999996 32.36343761000006, -86.70776310999997 32.363225200000045, -86.70744418999999 32.36319893000007, -86.70747546999996 32.36293112000004, -86.70811331999994 32.362983660000054, -86.70808203999997 32.36325146000007, -86.70871988999994 32.36330400000003, -86.70868860999997 32.363571800000045, -86.70900753999996 32.36359807000002, -86.70897625999999 32.36386587000004)), ((-86.70747546999996 32.36293112000004, -86.70683761999999 32.36287858000003, -86.70686890999997 32.36261078000007, -86.70623106999994 32.362558240000055, -86.70626234999997 32.36229043000003, -86.70530558999997 32.36221161000003, -86.70527429999999 32.36247941000005, -86.70463645999996 32.36242686000003, -86.70466774999994 32.36215906000007, -86.70402990999997 32.36210650000004, -86.70406120999996 32.36183870000008, -86.70310444999996 32.361759850000055, -86.70313574999994 32.36149205000004, -86.70281682999996 32.36146577000005, -86.70278552999997 32.36173357000007, -86.70246660999999 32.36170729000003, -86.70249790999998 32.361439490000066, -86.70186007999996 32.36138692000003, -86.70182877999997 32.36165472000005, -86.70087202999997 32.36157586000007, -86.70090332999996 32.361308060000056, -86.70026549999994 32.36125548000007, -86.70042203999998 32.35991647000003, -86.70169767999994 32.36002163000006, -86.70166637999995 32.36028943000008, -86.70230419999996 32.36034200000006, -86.70227289999997 32.36060980000008, -86.70291072999999 32.36066237000006, -86.70294202999997 32.36039457000004, -86.70357984999998 32.36044713000007, -86.70354855999994 32.36071493000003, -86.70418638999996 32.36076749000006, -86.70415508999997 32.361035290000075, -86.70479291999999 32.36108785000005, -86.70476162999995 32.361355650000064, -86.70539946999997 32.361408210000036, -86.70536816999999 32.36167601000005, -86.70600600999995 32.361728560000074, -86.70597471999997 32.361996360000035, -86.70661255999994 32.362048910000055, -86.70658127999997 32.36231671000007, -86.70721911999999 32.36236925000003, -86.70718782999995 32.362637050000046, -86.70750675999994 32.362663320000024, -86.70747546999996 32.36293112000004)), ((-86.71562449999999 32.384150290000036, -86.71549943999997 32.385221460000025, -86.71518041999997 32.38519522000007, -86.71521168999999 32.384927420000054, -86.71489267999993 32.38490118000004, -86.71492393999995 32.384633380000025, -86.71524295999996 32.384659630000044, -86.71527421999997 32.384391840000035, -86.71495520999997 32.38436559000007, -86.71504900999997 32.38356220000003, -86.71536801999997 32.38358845000005, -86.71533675999996 32.383856250000065, -86.71565576999996 32.383882490000076, -86.71568702999997 32.38361470000007, -86.71632504999997 32.38366719000004, -86.71629378999995 32.38393498000005, -86.71597477999995 32.38390874000004, -86.71594351999994 32.38417653000005, -86.71562449999999 32.384150290000036)))');

    CREATE CUSTOM INDEX blocks_index on cities()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                shape : {
                    type            : "geo_shape",
                    max_levels      : 15,
                    transformations : [{type:"convex_hull"}]
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
        'refresh_seconds'       : '60',
        'ram_buffer_mb'         : '64',
        'max_merge_mb'          : '5',
        'max_cached_mb'         : '30',
        'excluded_data_centers' : 'dc2,dc3',
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

Lucene indexes are queried using a custom JSON syntax defining the kind of search to be done.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table_name> WHERE expr(<index_name>, '{
        (   filter  : <filter>  )?
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

Search for all the indexed rows.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table> WHERE expr(<index_name>, '{
        (filter | query) : { type  : "all"}
    }');

**Example:** search for all the indexed rows:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '
        {filter : { type  : "all" }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(all()).build());



Bitemporal search
=================

Search for `bitemporally-indexed <https://en.wikipedia.org/wiki/Temporal_database>`__ rows according to the specified
transaction time and valid time ranges.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table> WHERE expr(<index_name>, '{
        (filter | query) : {
            type       : "bitemporal",
            (vt_from   : <vt_from> ,)?
            (vt_to     : <vt_to> ,)?
            (tt_from   : <tt_from> ,)?
            (tt_to     : <tt_to> ,)?
            (operation : <operation> )? }
    }');

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
    VALUES ('Jonathan', 'San Francisco', '2015/01/01', '2200/12/31', '2015/01/01', '2200/12/31');


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

    SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM census WHERE expr(tweets_index, '{
        filter : {
            type    : "bitemporal",
            field   : "bitemporal",
            vt_from : 0,
            vt_to   : "2200/12/31",
            tt_from : "2200/12/31",
            tt_to   : "2200/12/31"
        }
    }') AND name='John';

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

    SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM census WHERE expr(census_index, '{
        filter : {
            type    : "bitemporal",
            field   : "bitemporal",
            vt_from : "2200/12/31",
            vt_to   : "2200/12/31",
            tt_from : "2200/12/31",
            tt_to   : "2200/12/31"
        }
    }') AND name='John';

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

    SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM census WHERE expr(census_index, '{
        filter : {
            type    : "bitemporal",
            field   : "bitemporal",
            vt_from : "2015/03/01",
            vt_to   : "2015/03/01",
            tt_from : "2015/03/01",
            tt_to   : "2015/03/01"
        }
    }') AND name = 'John';

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

    SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM census WHERE expr(census_index,'{
        filter : {
            type    : "bitemporal",
            field   : "bitemporal",
            tt_from : "2015/07/05",
            tt_to   : "2015/07/05"
        }
    }') AND name='John';

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

Searches for rows matching boolean combinations of other searches.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table> WHERE expr(<index_name>, '{
        (filter | query) : {
            type     : "boolean",
            ( must   : [(search,)?] , )?
            ( should : [(search,)?] , )?
            ( not    : [(search,)?] , )? }
    }');

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

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type : "boolean",
            must : [ {type : "wildcard", field : "name", value : "*a"},
                     {type : "wildcard", field : "food", value : "tu*"} ]}
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(bool().must(wildcard("name", "*a"), wildcard("food", "tu*"))).build());



**Example 2:** search for rows where food starts with “tu” but name does not end with “a”:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type : "boolean",
            not  : [ {type : "wildcard", field : "name", value : "*a" } ],
            must : [ {type : "wildcard", field : "food", value : "tu*" } ] }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(bool().not(wildcard("name", "*a")).must(wildcard("food", "tu*"))).build());


**Example 3:** search for rows where name ends with “a” or food starts with
“tu”:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type   : "boolean",
            should : [ { type : "wildcard", field : "name", value : "*a" },
                       { type : "wildcard", field : "food", value : "tu*" } ] }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(bool().should(wildcard("name", "*a"), wildcard("food", "tu*"))).build());


**Example 4:** will return zero rows independently of the index contents:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : { type : "boolean" }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(bool()).build());


**Example 5:** search for rows where name does not end with “a”, which is
a resource-intensive pure negation search:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            not  : [ {
                type : "wildcard", field : "name", value : "*a" } ] }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(bool().not(wildcard("name", "*a"))).build());


Contains search
===============

Searches for rows matching one or more of the specified terms.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table> WHERE expr(<index_name>, '{
        ( filter | query ) : {
            type   : "contains",
            field  : <field_name> ,
            values : <value_list> }
    }');

**Example 1:** search for rows where name matches “Alicia” or “mancha”:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type   : "contains",
            field  : "name",
            values : [ "Alicia", "mancha" ] }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(contains("name", "Alicia", "mancha").build());


**Example 2:** search for rows where date matches “2014/01/01″,
“2014/01/02″ or “2014/01/03″:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type   : "contains",
            field  : "date",
            values : [ "2014/01/01", "2014/01/02", "2014/01/03" ] }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(contains("date", "2014/01/01", "2014/01/02", "2014/01/03")).build());


Date range search
=================

Searches for rows within a specified date range.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table> WHERE expr(<index_name>, '{
        (filter | query) : {
            type  : "date_range",
            (from : <from> ,)?
            (to   : <to> ,)?
            (operation: <operation> )? }
    }');

where:

-  **from**: a string or a number being the beginning of the date
   range.
-  **to**: a string or a number being the end of the date range.
-  **operation**: the spatial operation to be performed, it can be
   **intersects**, **contains** and **is\_within**.

**Example 1:** will return rows where duration intersects "2014/01/01" and
"2014/12/31":

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type      : "date_range",
            field     : "duration",
            from      : "2014/01/01",
            to        : "2014/12/31",
            operation : "intersects" }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(dateRange("duration").from("2014/01/01").to("2014/12/31").operation("intersects")).build());


**Example 2:** search for rows where duration contains "2014/06/01" and
"2014/06/02":

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type      : "date_range",
            field     : "duration",
            from      : "2014/06/01",
            to        : "2014/06/02",
            operation : "contains" }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(dateRange("duration").from("2014/06/01").to("2014/06/02").operation("contains")).build());


**Example 3:** search for rows where duration is within "2014/01/01" and
"2014/12/31":

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type      : "date_range",
            field     : "duration",
            from      : "2014/01/01",
            to        : "2014/12/31",
            operation : "is_within" }
    }');


Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(dateRange("duration").from("2014/01/01").to("2014/12/31").operation("is_within")).build());


Fuzzy search
============

Searches for rows matching a term using similarity based on
`Damerau-Levenshtein distance <http://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance>`__ edit distance.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table> WHERE expr(<index_name>, '{
        (filter | query) : {
            type  : "fuzzy",
            field : <field_name> ,
            value : <value>
            (, max_edits      : <max_edits> )?
            (, prefix_length  : <prefix_length> )?
            (, max_expansions : <max_expansion> )?
            (, transpositions : <transposition> )? }
    }');

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

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type      : "fuzzy",
            field     : "phrase",
            value     : "puma",
            max_edits : 1 }
    }');


Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(fuzzy("phrase", "puma").maxEdits(1)).build());


**Example 2:** same as example 1 but will limit the results to rows where
phrase contains a word that starts with “pu”:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type          : "fuzzy",
            field         : "phrase",
            value         : "puma",
            max_edits     : 1,
            prefix_length : 2 }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(fuzzy("phrase", "puma").maxEdits(1).prefixLength(2)).build());


Geo bbox search
===============

Searches for rows with `geographical points <#geo-point-mapper>`__ or `geographical shapes <#geo-shape-mapper>`__
contained in the specified bounding box.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table> WHERE expr(<index_name>, '{
        (filter | query) : {
            type          : "geo_bbox",
            field         : <field_name>,
            min_latitude  : <min_latitude> ,
            max_latitude  : <max_latitude> ,
            min_longitude : <min_longitude> ,
            max_longitude : <max_longitude> }
    }');

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

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type          : "geo_bbox",
            field         : "place",
            min_latitude  : -90.0,
            max_latitude  : 90.0,
            min_longitude : -180.0,
            max_longitude : 180.0 }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(geoBBox("place", -180.0, 180.0, -90.0, 90.0)).build());


**Example 2:** search for any rows where “place” is formed by a latitude
between -90.0 and 90.0, and a longitude between 0.0 and
10.0:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type          : "geo_bbox",
            field         : "place",
            min_latitude  : -90.0,
            max_latitude  : 90.0,
            min_longitude : 0.0,
            max_longitude : 10.0 }
    }');


Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(geoBBox("place",0.0,10.0,-90.0,90.0)).build());


**Example 3:** search for any rows where “place” is formed by a latitude
between 0.0 and 10.0, and a longitude between -180.0 and
180.0 sorted by min distance to point [0.0, 0.0]:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type          : "geo_bbox",
            field         : "place",
            min_latitude  : 0.0,
            max_latitude  : 10.0,
            min_longitude : -180.0,
            max_longitude : 180.0 },
        sort : {
            fields: [ {
                type      : "geo_distance",
                mapper    : "geo_point",
                reverse   : false,
                latitude  : 0.0,
                longitude : 0.0 }]
    }');


Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?) LIMIT 100",
        search().filter(geoBBox("place", -180.0, 180.0, 0.0, 10.0))
                .sort(geoDistanceSortField("geo_point", 0.0, 0.0).reverse(false)
                .build());

Geo distance search
===================

Searches for rows with `geographical points <#geo-point-mapper>`__ or `geographical shapes <#geo-shape-mapper>`__
within a distance range from a specified point.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table> WHERE expr(<index_name>, '{
        (filter | query) : {
            type            : "geo_distance",
            field           : <field_name> ,
            latitude        : <latitude> ,
            longitude       : <longitude> ,
            max_distance    : <max_distance>
            (, min_distance : <min_distance> )? }
    }');

where:

-  **latitude** : a double value between -90 and 90 being the latitude
   of the reference point.
-  **longitude** : a double value between -180 and 180 being the
   longitude of the reference point.
-  **max\_distance** : a string value being the max allowed `distance <#distance>`__ from the reference point.
-  **min\_distance** : a string value being the min allowed `distance <#distance>`__ from the reference point.

**Example 1:** search for any rows where “place” is within one kilometer from the geo point (40.225479, -3.999278):

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type         : "geo_distance",
            field        : "place",
            latitude     : 40.225479,
            longitude    : -3.999278,
            max_distance : "1km" }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(geoDistance("place", -3.999278d, 40.225479d, "1km").build());


**Example 2:** search for any rows where “place” is within one yard and ten
yards from the geo point (40.225479, -3.999278) sorted by min distance to point (40.225479, -3.999278):

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type          : "geo_distance",
            field         : "place",
            latitude      : 40.225479,
            longitude     : -3.999278,
            max_distance  : "10yd" ,
            min_distance  : "1yd" },
        sort   : {
            fields: [ {
                type      : "geo_distance",
                mapper    : "geo_point",
                reverse   : false,
                latitude  : 40.225479,
                longitude : -3.999278} ] }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?) LIMIT 100",
        search().filter(geoDistance("place", -3.999278d, 40.225479d, "10yd").minDistance("1yd"))
                .sort(geoDistanceSortField("geo_point", -3.999278, 40.225479).reverse(false))
                .build());

Geo shape search
================

Searches for rows with `geographical points <#geo-point-mapper>`__ or `geographical shapes <#geo-shape-mapper>`__
related to a specified shape with `Well Known Text (WKT) <http://en.wikipedia.org/wiki/Well-known_text>`__ format.
The supported WKT shapes are point, linestring, polygon, multipoint, multilinestring and multipolygon.

This search type depends on `Java Topology Suite (JTS) <http://www.vividsolutions.com/jts>`__.
This library can't be distributed together with this project due to license compatibility problems, but you can add it
by putting `jts-core-1.14.0.jar <http://search.maven.org/remotecontent?filepath=com/vividsolutions/jts-core/1.14.0/jts-core-1.14.0.jar>`__
into your Cassandra installation lib directory.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table> WHERE expr(<index_name, '{
        (filter | query) : {
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

    SELECT * FROM test WHERE expr(test_index, '{
        filter : {
            type  : "geo_shape",
            field : "place",
            shape : "POLYGON((-0.07 51.63, 0.03 51.54, 0.05 51.65, -0.07 51.63))" }
    }';

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

    SELECT * FROM test WHERE expr(test_index, '{
        filter : {
            type            : "geo_shape",
            field           : "place",
            relation        : "intersects",
            shape           : "LINESTRING(-80.90 29.05, -80.51 28.47, -80.60 28.12, -80.00 26.85, -80.05 26.37)",
            transformations : [{type:"buffer", max_distance:"10km"}] }
    }';

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

Searches for rows with columns containing the specified term. The matching depends on the used analyzer.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table> WHERE expr(<index_name>, '{
        (filter | query) : {
            type  : "match",
            field : <field_name> ,
            value : <value> }
    }');

**Example 1:** search for rows where name matches “Alicia”:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type  : "match",
            field : "name",
            value : "Alicia" }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(match("name", "Alicia")).build());


**Example 2:** search for any rows where phrase contains “mancha”:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type  : "match",
            field : "phrase",
            value : "mancha" }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(match("phrase", "mancha").build());


**Example 3:** search for rows where date matches “2014/01/01″:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type  : "match",
            field : "date",
            value : "2014/01/01" }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(match("date", "2014/01/01")).build());


None search
===========

Returns no results.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table> WHERE expr(<index_name>, '{
        (filter | query) : { type  : "none"}
    }');

**Example:** will return no one of the indexed rows:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : { type  : "none" }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(none()).build());

Phrase search
=============

Searches for rows with columns containing a particular sequence of terms.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table> WHERE expr(<index_name>, '{
        (filter | query) : {
            type    : "phrase",
            field   : <field_name> ,
            value   : <value>
            (, slop : <slop> )? }
    }');

where:

-  **values**: an ordered list of values.
-  **slop** (default = 0): number of words permitted between words.

**Example 1:** search for rows where “phrase” contains the word “camisa”
followed by the word “manchada”:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
        type   : "phrase",
        field  : "phrase",
        values : "camisa manchada" }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(phrase("phrase", "camisa manchada")).build());

**Example 2:** search for rows where “phrase” contains the word “mancha”
followed by the word “camisa” having 0 to 2 words in between:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type   : "phrase",
            field  : "phrase",
            values : "mancha camisa",
            slop   : 2 }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(phrase("phrase", "camisa manchada").slop(2)).build());

Prefix search
=============

Searches for rows with columns with terms starting with the specified prefix.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table> WHERE expr(<index_name>, '{
        (filter | query) : {
            type  : "prefix",
            field : <field_name> ,
            value : <value> }
    }');

**Example:** search for rows where “phrase” contains a word starting with
“lu”. If the column is indexed as “text” and uses an analyzer, words
ignored by the analyzer will not be retrieved:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type  : "prefix",
            field : "phrase",
            value : "lu" }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(prefix("phrase", "lu")).build());

Range search
============

Searches for rows with columns with terms within the specified term range.

**Syntax:**

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        (filter | query) : {
            type     : "range",
            field    : <field_name>
            (, lower : <min> , include_lower : <min_included> )?
            (, upper : <max> , include_upper : <max_included> )? }
    }');

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

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type          : "range",
            field         : "age",
            lower         : 1,
            include_lower : true }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(range("age").lower(1).includeLower(true)).build());

**Example 2:** search for rows where *age* is in (-∞, 0]:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type          : "range",
            field         : "age",
            upper         : 0,
            include_upper : true }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(range("age").upper(0).includeUpper(true)).build());

**Example 3:** search for rows where *age* is in [-1, 1]:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type          : "range",
            field         : "age",
            lower         : -1,
            upper         : 1,
            include_lower : true,
            include_upper : true }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(range("age").lower(-1).upper(1)
                                    .includeLower(true)
                                    .includeUpper(true)).build());

**Example 4:** search for rows where *date* is in [2014/01/01, 2014/01/02]:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type          : "range",
            field         : "date",
            lower         : "2014/01/01",
            upper         : "2014/01/02",
            include_lower : true,
            include_upper : true }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(range("date").lower("2014/01/01")
                                     .upper( "2014/01/02")
                                     .includeLower(true)
                                     .includeUpper(true)).build());

Regexp search
=============

Searches for rows with columns with terms satisfying the specified regular expression.

**Syntax:**

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        (filter | query) : {
            type  : "regexp",
            field : <field_name>,
            value : <regexp> }
    }');

where:

-  **value**: a regular expression. See
   `org.apache.lucene.util.automaton.RegExp <http://lucene.apache.org/core/4_6_1/core/org/apache/lucene/util/automaton/RegExp.html>`__
   for syntax reference.

**Example:** search for rows where name contains a word that starts with
“p” and a vowel repeated twice (e.g. “pape”):

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type  : "regexp",
            field : "name",
            value : "[J][aeiou]{2}.*" }
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(regexp("name", "[J][aeiou]{2}.*")).build());

Wildcard search
===============

Searches for rows with columns with terms satisfying the specified wildcard pattern.

**Syntax:**

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        (filter | query) : {
            type  : "wildcard" ,
            field : <field_name> ,
            value : <wildcard_exp> }
    }');

where:

-  **value**: a wildcard expression. Supported wildcards are \*, which
   matches any character sequence (including the empty one), and ?,
   which matches any single character. ” is the escape character.

**Example:** search for rows where food starts with or is “tu”:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type  : "wildcard",
            field : "food",
            value : "tu*" }
    }');


Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
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

    SELECT * FROM test.users WHERE stratio_col = '{
        filter : {
            type         : "geo_distance",
            field        : "place",
            latitude     : 40.225479,
            longitude    : -3.999278,
            max_distance : "1km" }
    }';


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

    SELECT * FROM test WHERE expr(test_idx,'{
        filter : {
            type            : "geo_shape",
            field           : "place",
            relation        : "intersects",
            shape           : "LINESTRING(-80.90 29.05, -80.51 28.47, -80.60 28.12, -80.00 26.85, -80.05 26.37)",
            transformations : [{type:"buffer", max_distance:"10km"}] }
    }');

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

Convex hull
___________

Convex hull transformation returns the `convex envelope <https://en.wikipedia.org/wiki/Convex_hull>`__ of a shape.

**Syntax:**

.. code-block:: sql

    { type : "convex_hull" }

**Example:** The following `geo shape mapper <#geo-shape-mapper>`__ will index only the convex hull of the WKT shape
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
                    transformations : [{type:"convex_hull"}]
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
            "v.2":{type:"float"} }
     }'};

    SELECT * FROM collect_things WHERE expr(tweets_index, '{
        filter : {
            type  : "match",
            field : "v.0",
            value : 1 }
    }');

    SELECT * FROM collect_things WHERE expr(tweets_index, '{
        filter : {
            type  : "match",
            field : "v.1",
            value : "bar" }
    }');

    SELECT * FROM collect_things WHERE expr(tweets_index, '{
        sort : {
            fields : [ {field : "v.2"} ] }
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

    SELECT * FROM user_profiles WHERE expr(tweets_index,'{
        filter : {
            type  : "match",
            field : "address.city",
            value : "San Fransisco"
        }
    }');

    SELECT * FROM user_profiles WHERE expr(tweets_index,'{
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

    SELECT * FROM user_profiles WHERE expr(tweets_index,'{
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

    SELECT * FROM user_profiles WHERE expr(tweets_index,'{
        filter : {
            type  : "match",
            field : "cities$Madrid",
            value : "San Francisco" }
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

    SELECT name, gender FROM test.users WHERE expr(users_index, '{
        filter : {type : "match", field : "food", value : "chips"}}
    ') AND token(name, gender) > token('Alicia', 'female');

Paging
======

Paging filtered results is fully supported. You can retrieve
the rows starting from a certain key. For example, if the primary key is
(userid, createdAt), you can search:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(users_index, ‘{
        filter : {type:”match",  field:”text", value:”cassandra”}}
    ') AND userid = 3543534 AND createdAt > 2011-02-03 04:05+0000 LIMIT 5000;

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

    SELECT * FROM users WHERE expr(tweets_index, '{
        filter : {
            type  : "match",
            field : "name",
            value : "Alice" }
    }');

However, this search could be a good use case for Lucene just because there is no easy counterpart:

.. code-block:: sql

    SELECT * FROM users WHERE expr(tweets_index, '{
        filter : {
            type : "boolean",
            must : [
                { type  : "regexp", field : "name", value : "[J][aeiou]{2}.*"},
                { type  : "range", field : "birthday", lower : "2014/04/25"}]},
         sort : {
            fields: [ { field : "name" } ] }
    }') LIMIT 20;

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

