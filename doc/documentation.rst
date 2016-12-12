================================
Stratio's Cassandra Lucene Index
================================

- `Overview <#overview>`__
    - `Features <#features>`__
    - `Architecture <#architecture>`__
    - `Requirements <#requirements>`__
    - `Installation <#installation>`__
    - `Upgrade <#upgrade>`__
    - `Example <#example>`__
    - `Alternative syntaxes <#alternative-syntaxes>`__
- `Indexing <#indexing>`__
    - `Partitioners <#partitioners>`__
        - `None partitioner <#none-partitioner>`__
        - `Token partitioner <#token-partitioner>`__
        - `Column partitioner <#column-partitioner>`__
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
        - `Date range mapper <#date-range-mapper>`__
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
    - `Transformations <#transformations>`__
        - `Bounding box transformation <#bounding-box-transformation>`__
        - `Buffer transformation <#buffer-transformation>`__
        - `Centroid transformation <#centroid-transformation>`__
        - `Convex hull transformation <#convex-hull-transformation>`__
    - `Shapes <#shapes>`__
        - `WKT shape <#wkt-shape>`__
        - `Bounding box shape <#bounding-box-shape>`__
        - `Buffer shape <#buffer-shape>`__
        - `Centroid shape <#centroid-shape>`__
        - `Convex hull shape <#convex-hull-shape>`__
        - `Difference shape <#difference-shape>`__
        - `Intersection shape <#intersection-shape>`__
        - `Union shape <#intersection-shape>`__
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
    - `Try doc values <#try-doc-values>`__
    - `Force segments merge <#force-segments-merge>`__

--------
Overview
--------

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
-  Boolean search (and, or, not)
-  Sorting by relevance, column value, and distance
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

Architecture
============

Indexing is achieved through a Lucene based implementation of Apache Cassandra secondary indexes.
Cassandra's secondary indexes are local indexes,
meaning that each node of the cluster indexes it's own data.
As usual in Cassandra, each node can act as search coordinator.
The coordinator node sends the searches to all the involved nodes,
and then it post-processes the returned rows to return the required ones.
This post-processing is particularly important in sorted searches.

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

Upgrade
=======

If you want to upgrade your cassandra cluster to a newer version  you must follow the Datastax official `upgrade instructions <https://docs.datastax.com/en/upgrade/doc/upgrade/cassandra/upgradeCassandra_g.html>`__.

The rule for the Lucene secondary indexes is to delete them with older version, upgrade cassandra and lucene index jar
and create them again with running newer version.

If you have huge amount of data in your cluster this could be an expensive task. We have tested it and here you have a
compatibility matrix that states between which versions it is not needed to delete the index:

+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| From\\ To | 3.0.3.0 | 3.0.3.1 | 3.0.4.0 | 3.0.4.1 | 3.0.5.0 | 3.0.5.1 | 3.0.5.2 | 3.0.6.0 | 3.0.6.1 | 3.0.6.2 | 3.0.7.0 | 3.0.7.1 | 3.0.7.2 | 3.0.8.0 | 3.0.8.1 | 3.0.8.2 | 3.0.8.3 | 3.0.9.0 | 3.0.9.1 | 3.0.9.2 | 3.0.10.0| 3.0.10.1|
+===========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+
| 2.x       |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.3.0   |    --   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.3.1   |    --   |    --   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.4.0   |    --   |    --   |    --   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.4.1   |    --   |    --   |    --   |    --   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.5.0   |    --   |    --   |    --   |    --   |    --   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.5.1   |    --   |    --   |    --   |    --   |    --   |    --   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.5.2   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |   YES   |   YES   |   YES   |   YES   |   YES   |   YES   |  \(1\)  |  \(1\)  |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.6.0   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |   YES   |   YES   |   YES   |   YES   |   YES   |  \(1\)  |  \(1\)  |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.6.1   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |   YES   |   YES   |   YES   |   YES   |  \(1\)  |  \(1\)  |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.6.2   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |   YES   |   YES   |   YES   |  \(1\)  |  \(1\)  |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.7.0   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |   YES   |   YES   |  \(1\)  |  \(1\)  |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.7.1   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |   YES   |  \(1\)  |  \(1\)  |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.7.2   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |  \(1\)  |  \(1\)  |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.8.0   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |  \(1\)  |  \(1\)  |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.8.1   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.8.2   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.8.3   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |   YES   |   YES   |   YES   |   YES   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.9.0   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |   YES   |   YES   |   YES   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.9.1   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |   YES   |   YES   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.9.2   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |   YES   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.10.0  |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |
+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+

**(1):** Compatible only if you are not using geospatial mappers.

Alternative syntaxes
====================

There are two alternative syntaxes for managing indexes. Prior to Cassandra 3.0, indexes had to be linked to a dummy
column due to CQL syntax limitations:

.. code-block:: sql

    CREATE TABLE test(pk int PRIMARY KEY, rc text);
    ALTER TABLE test ADD lucene text; -- Dummy column

    CREATE CUSTOM INDEX idx ON test(lucene) -- Index is linked to the dummy column
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {'schema': '{fields: {rc: {type: "text"}}}'};

This column wasn't intended to store anything, it was just a trick to embed Lucene syntax into CQL syntax, so custom
search predicates could be directed to this dummy column:

.. code-block:: sql

    SELECT * FROM test WHERE lucene = '{...}';

As a collateral benefit, this column was used to return the score assigned by the Lucene query to each of the rows.

However, Cassandra 3.0 introduced `a secondary index API redesign <https://issues.apache.org/jira/browse/CASSANDRA-9459>`__
including explicit syntactical support for custom per-row indexes using their own query language.
This new syntax didn't require the dummy column anymore:

.. code-block:: sql

    CREATE TABLE test(pk int PRIMARY KEY, rc text);

    CREATE CUSTOM INDEX idx ON test() -- Index is directly linked to the table, without dummy column
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {'schema': '{fields: {rc: {type: "text"}}}'};

Instead, we can address custom search expressions directly to the index using the new 'expr' operator:

.. code-block:: sql

    SELECT * FROM test WHERE expr(idx, '{...}');

As you can see, this new syntax is far clearer than the previous one.
However, the old syntax is still supported for compatibility reasons, given that several client applications do not
support the new syntax yet.
The most remarkable case is `DataStax's connector for Apache Spark <https://github.com/datastax/spark-cassandra-connector>`__,
which `doesn't allow 'expr' queries <https://datastax-oss.atlassian.net/browse/SPARKC-332>`__  and
`fails managing tables with new-style indexes <https://datastax-oss.atlassian.net/browse/SPARKC-361>`__ even if the
Spark operation doesn't use the index at all.
So, unfortunately, you must continue using the old dummy column approach if you are going to use the Spark connector or
any other incompatible software.

Additionally, another possible reason for using the old syntax is that it uses the fake column to show the scores assigned
by the Lucene's scoring formula to each one of the matched rows. This score is internally used for sorting and selecting
the matched rows according to some user-defined search criteria. Although it is more intended for internal use, showing
this value could be useful in some specific cases.

Last but not least, it is important to note that you can address searches with the new syntax to indexes created with
the old fake column approach:

.. code-block:: sql

    CREATE TABLE test(pk int PRIMARY KEY, rc text);
    ALTER TABLE test ADD lucene text; -- Dummy column

    CREATE CUSTOM INDEX idx ON test(lucene) -- Index is linked to the dummy column
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {'schema': '{fields: {rc: {type: "text"}}}'};

    SELECT * FROM test WHERE expr(idx,'{...}');

This offers a good balance between the advantages of both syntaxes.

Cassandra only allows one per-row index per table,
whereas there is no limit for the number of per-column indexes that a table can have.
So, an additional benefit of creating indexes over dummy columns is that you can have multiple Lucene indexes per table,
as long as they are considered per-column indexes.

All the examples in this document use the new syntax, but all of them can be written in the old way.

Example
=======

We will create the following table to store tweets:

.. code-block:: sql

    CREATE KEYSPACE demo
    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
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
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             id: {type: "integer"},
             user: {type: "string"},
             body: {type: "text", analyzer: "english"},
             time: {type: "date", pattern: "yyyy/MM/dd"},
             place: {type: "geo_point", latitude: "latitude", longitude: "longitude"}
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
       filter: {type: "range", field: "time", lower: "2014/04/25", upper: "2014/05/01"}
    }');

The same search can be performed forcing an explicit refresh of the involved index shards:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index, '{
       filter: {type: "range", field: "time", lower: "2014/04/25", upper: "2014/05/01"},
       refresh: true
    }') limit 100;

Now, to search the top 100 more relevant tweets where *body* field contains the phrase “big data gives organizations”
within the aforementioned date range:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index, '{
       filter: {type: "range", field: "time", lower: "2014/04/25", upper: "2014/05/01"},
       query: {type: "phrase", field: "body", value: "big data gives organizations", slop: 1}
    }') LIMIT 100;

To refine the search to get only the tweets written by users whose names start with "a":

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index, '{
       filter: [
          {type: "range", field: "time", lower: "2014/04/25", upper: "2014/05/01"},
          {type: "prefix", field: "user", value: "a"}
       ],
       query: {type: "phrase", field: "body", value: "big data gives organizations", slop: 1}
    }') LIMIT 100;

To get the 100 more recent filtered results you can use the *sort* option:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index, '{
       filter: [
          {type: "range", field: "time", lower: "2014/04/25", upper: "2014/05/01"},
          {type: "prefix", field: "user", value: "a"}
       ],
       query: {type: "phrase", field: "body", value: "big data gives organizations", slop: 1},
       sort: {field: "time", reverse: true}
    }') limit 100;

The previous search can be restricted to tweets created close to a geographical position:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index, '{
       filter: [
          {type: "range", field: "time", lower: "2014/04/25", upper: "2014/05/01"},
          {type: "prefix", field: "user", value: "a"},
          {type: "geo_distance", field: "place", latitude: 40.3930, longitude: -3.7328, max_distance: "1km"}
       ],
       query: {type: "phrase", field: "body", value: "big data gives organizations", slop: 1},
       sort: {field: "time", reverse: true}
    }') limit 100;

It is also possible to sort the results by distance to a geographical position:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index, '{
       filter: [
          {type: "range", field: "time", lower: "2014/04/25", upper: "2014/05/01"},
          {type: "prefix", field: "user", value: "a"},
          {type: "geo_distance", field: "place", latitude: 40.3930, longitude: -3.7328, max_distance: "1km"}
       ],
       query: {type: "phrase", field: "body", value: "big data gives organizations", slop: 1},
       sort: [
          {field: "time", reverse: true},
          {field: "place", type: "geo_distance", latitude: 40.3930, longitude: -3.7328}
       ]
    }') limit 100;

Last but not least, you can route any search to a certain token range or partition, in such a way that only a
subset of the cluster nodes will be hit, saving precious resources:

.. code-block:: sql

    SELECT * FROM tweets WHERE expr(tweets_index, '{
       filter: [
          {type: "range", field: "time", lower: "2014/04/25", upper: "2014/05/01"},
          {type: "prefix", field: "user", value: "a"},
          {type: "geo_distance", field: "place", latitude: 40.3930, longitude: -3.7328, max_distance: "1km"}
       ],
       query: {type: "phrase", field: "body", value: "big data gives organizations", slop: 1},
       sort: [
          {field: "time", reverse: true},
          {field: "place", type: "geo_distance", latitude: 40.3930, longitude: -3.7328}
       ]
    }') AND TOKEN(id) >= TOKEN(0) AND TOKEN(id) < TOKEN(10000000) limit 100;

--------
Indexing
--------

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

    <options>:= {
       ('refresh_seconds': '<int_value>',)?
       ('ram_buffer_mb': '<int_value>',)?
       ('max_merge_mb': '<int_value>',)?
       ('max_cached_mb': '<int_value>',)?
       ('indexing_threads': '<int_value>',)?
       ('indexing_queues_size': '<int_value>',)?
       ('directory_path': '<string_value>',)?
       ('excluded_data_centers': '<string_value>',)?
       ('partitioner': '<partitioner_definition>',)?
       'schema': '<schema_definition>'
    };

All options take a value enclosed in single quotes:

-  **refresh\_seconds**: number of seconds before auto-refreshing the
   index reader. It is the max time taken for writes to be searchable
   without forcing an index refresh. Defaults to '60'.
-  **ram\_buffer\_mb**: size of the write buffer. Its content will be
   committed to disk when full. Defaults to '64'.
-  **max\_merge\_mb**: defaults to '5'.
-  **max\_cached\_mb**: defaults to '30'.
-  **indexing\_threads**: number of asynchronous indexing threads. ’0’
   means synchronous indexing. Defaults to number of processors available to the JVM.
-  **indexing\_queues\_size**: max number of queued documents per
   asynchronous indexing thread. Defaults to ’50’.
-  **directory\_path**: The path of the directory where the  Lucene index
   will be stored.
-  **excluded\_data\_centers**: The comma-separated list of the data centers
   to be excluded. The index will be created on this data centers but all the
   write operations will be silently ignored.
-  **partitioner**: The optional index `partitioner <#partitioners>`__. Index partitioning is useful
   to speed up some searches to the detriment of others, depending on the implementation. It is also
   useful to overcome the Lucene's hard limit of 2147483519 documents per index.
-  **schema**: see below

.. code-block:: sql

    <schema_definition>:= {
       (analyzers: { <analyzer_definition> (, <analyzer_definition>)* } ,)?
       (default_analyzer: "<analyzer_name>",)?
       fields: { <mapper_definition> (, <mapper_definition>)* }
    }

Where default\_analyzer defaults to ‘org.apache.lucene.analysis.standard.StandardAnalyzer’.

.. code-block:: sql

    <analyzer_definition>:= <analyzer_name>: {
       type: "<analyzer_type>" (, <option>: "<value>")*
    }

.. code-block:: sql

    <mapper_definition>:= <mapper_name>: {
       type: "<mapper_type>" (, <option>: "<value>")*
    }

Partitioners
============

Lucene indexes can be partitioned on a per-node basis. This means that the local index in each node
can be split in multiple smaller fragments. Index partitioning is useful to speed up some searches
to the detriment of others, depending on the implementation. It is also useful to overcome the
Lucene's hard limit of 2147483519 documents per local index.

Partitioning is disabled by default, and it can be activated specifying a partitioner implementation
in the index creation statement.

Please note that the index creation statement specifies the values of several Lucene memory-related
attributes, such as *max_merge_mb* or *ram_buffer_mb*. These attributes are applied to each local
Lucene index or partition, so the amount of memory should be multiplied by the number of partitions.

None partitioner
________________

A partitioner with no action, equivalent to not defining a partitioner. This is the default
implementation.

.. code-block:: sql

    CREATE CUSTOM INDEX test_idx ON test()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'schema': '{...}',
       'partitioner': '{type: "none"}',
    };

Token partitioner
_________________

A partitioner based on the partition key token. Partitioning on token guarantees a good load
balancing between partitions while speeding up partition-directed searches to the detriment of any
other searches. The number of partitions per node should be specified.

.. code-block:: sql

    CREATE TABLE tweets (
       user TEXT,
       month INT,
       date TIMESTAMP,
       id INT,
       body TEXT
       PRIMARY KEY ((user, month), date, id)
    );

    CREATE CUSTOM INDEX idx ON tweets()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'schema': '{...}',
       'partitioner': '{type: "token", partitions: 4}',
    };

    SELECT * FROM tweets WHERE expr(idx, '{...}') AND user = 'jsmith' AND month = 5; -- Fetches 1 node, 1 partition

    SELECT * FROM tweets WHERE expr(idx, '{...}') AND user = 'jsmith' ALLOW FILTERING; -- Fetches all nodes, all partitions

    SELECT * FROM tweets WHERE expr(idx, '{...}')'; -- Fetches all nodes, all partitions

Column partitioner
__________________

A partitioner based on a column of the partition key. Rows will be stored in an index partition determined by the hash
of the specified partition key column. Both partition-directed and token range searches containing an CQL equality
filter over the selected partition key column will be routed to a single partition, increasing performance. However,
token range searches without filters over the partitioning column will be routed to all the partitions, with a slightly
lower performance.

Load balancing depends on the cardinality and distribution of the values of the partitioning column. Both high
cardinalities and uniform distributions will provide better load balancing between partitions.

.. code-block:: sql

    CREATE TABLE tweets (
       user TEXT,
       month INT,
       date TIMESTAMP,
       id INT,
       body TEXT
       PRIMARY KEY ((user, month), date, id)
    );

    CREATE CUSTOM INDEX idx ON tweets()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'schema': '{...}',
       'partitioner': '{type: "column", partitions: 4, column:"user"}',
    };

    SELECT * FROM tweets WHERE expr(idx, '{...}') AND user = 'jsmith' AND month = 5; -- Fetches 1 node, 1 partition

    SELECT * FROM tweets WHERE expr(idx, '{...}') AND user = 'jsmith' ALLOW FILTERING; -- Fetches all nodes, 1 partition

    SELECT * FROM tweets WHERE expr(idx, '{...}')'; -- Fetches all nodes, all partitions

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

The analyzers defined in this section can by referenced by mappers. Additionally, there are prebuilt analyzers for
Arabic, Bulgarian, Brazilian, Catalan, Sorani, Czech, Danish, German, Greek, English, Spanish, Basque, Persian, Finnish,
French, Irish, Galician, Hindi, Hungarian, Armenian, Indonesian, Italian, Latvian, Dutch, Norwegian, Portuguese,
Romanian, Russian, Swedish, Thai and Turkish.

Classpath analyzer
__________________

Analyzer which instances a Lucene's `analyzer <https://lucene.apache.org/core/5_3_0/core/org/apache/lucene/analysis/Analyzer.html>`__
present in classpath.

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          analyzers: {
             an_analyzer: {
                type: "classpath",
                class: "org.apache.lucene.analysis.en.EnglishAnalyzer"
             }
          }
       }'
    };

Snowball analyzer
_________________

Analyzer using a `http://snowball.tartarus.org/ <http://snowball.tartarus.org/>`__ snowball filter
`SnowballFilter <https://lucene.apache.org/core/5_3_0/analyzers-common/org/apache/lucene/analysis/snowball/SnowballFilter.html>`__

Example:
~~~~~~~~
.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          analyzers: {
             an_analyzer: {
                type: "snowball",
                language: "English",
                stopwords: "a,an,the,this,that"
             }
          }
       }'
    };

Supported languages: English, French, Spanish, Portuguese, Italian, Romanian, German, Dutch, Swedish, Norwegian, Danish,
Russian, Finnish, Hungarian and Turkish.

Mappers
=======

Field mapping definition options specify how the CQL rows will be mapped to Lucene documents.
Several mappers can be applied to the same CQL column/s.
Details and default values are listed in the table below.

+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| Mapper type                         | Option          | Value type      | Default value                  | Mandatory |
+=====================================+=================+=================+================================+===========+
| `bigdec <#big-decimal-mapper>`__    | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | integer_digits  | integer         | 32                             | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | decimal_digits  | integer         | 32                             | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `bigint <#big-integer-mapper>`__    | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | digits          | integer         | 32                             | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `bitemporal <#bitemporal-mapper>`__ | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | vt_from         | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | vt_to           | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | tt_from         | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | tt_to           | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | pattern         | string          | yyyy/MM/dd HH:mm:ss.SSS Z      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | now_value       | object          | Long.MAX_VALUE                 | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `blob <#blob-mapper>`__             | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | column          | string          | mapper_name of the schema      | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `boolean <#boolean-mapper>`__       | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | column          | string          | mapper_name of the schema      | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `date <#date-mapper>`__             | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | pattern         | string          | yyyy/MM/dd HH:mm:ss.SSS Z      | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `date_range <#daterange-mapper>`__  | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | from            | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | to              | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | pattern         | string          | yyyy/MM/dd HH:mm:ss.SSS Z      | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `double <#double-mapper>`__         | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | boost           | integer         | 0.1f                           | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `float <#float-mapper>`__           | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | boost           | integer         | 0.1f                           | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `geo_point <#geo-point-mapper>`__   | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | latitude        | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | longitude       | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | max_levels      | integer         | 11                             | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `geo_shape <#geo-shape-mapper>`__   | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | max_levels      | integer         | 5                              | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | transformations | array           |                                | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `inet <#inet-mapper>`__             | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | column          | string          | mapper_name of the schema      | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `integer <#integer-mapper>`__       | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | boost           | integer         | 0.1f                           | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `long <#long-mapper>`__             | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | boost           | integer         | 0.1f                           | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `string <#string-mapper>`__         | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | case_sensitive  | boolean         | true                           | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `text <#text-mapper>`__             | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | analyzer        | string          | default_analyzer of the schema | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `uuid <#uuid-mapper>`__             | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | column          | string          | mapper_name of the schema      | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+

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

Cassandra allows only one custom per-row index per table, and it does not allow any modify operation on indexes.
So, to modify an index it needs to be deleted first and created again.
Alternatively, if you are using the `classic dummy-column syntax <#alternative-syntaxes>`__,
the index will be considered per-column, so you would be able to create a second index with the new schema,
wait until the new index is completely built, and then delete the old index.

Big decimal mapper
__________________

Maps arbitrary precision signed decimal values.

**Parameters:**

-  **validated** (default = false): if mapping errors should make CQL writes fail, instead of just logging the error.
-  **column** (default = name of the mapper): the name of the column storing the big decimal to be indexed.
-  **integer\_digits** (default = 32): the max number of decimal digits for the integer part.
-  **decimal\_digits** (mandatory): the max number of decimal digits for the decimal part.

**Supported CQL types:**

-  ascii, bigint, decimal, double, float, int, smallint, text, tinyint, varchar, varint

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             bigdecimal: {
                type: "bigdec",
                integer_digits: 2,
                 decimal_digits: 2,
                 validated: true,
                 column: "column_name"
             }
          }
       }'
    };

Big integer mapper
__________________

Maps arbitrary precision signed integer values.

**Parameters:**

-  **validated** (default = false): if mapping errors should make CQL writes fail, instead of just logging the error.
-  **column** (default = name of the mapper): the name of the column storing the big integer to be indexed.
-  **digits** (default = 32): the max number of decimal digits.

**Supported CQL types:**

-  ascii, bigint, int, smallint, text, tinyint, varchar, varint

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX test_idx ON test()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             biginteger: {
                type: "bigint",
                 digits: 10,
                 validated: true,
                 column: "column_name"
             }
          }
       }'
    };


Bitemporal mapper
_________________

Maps four columns containing the four dates defining a bitemporal fact.

**Parameters:**

-  **validated** (default = false): if mapping errors should make CQL writes fail, instead of just logging the error.
-  **vt\_from** (mandatory): the name of the column storing the beginning of the valid date range.
-  **vt\_to** (mandatory): the name of the column storing the end of the valid date range.
-  **tt\_from** (mandatory): the name of the column storing the beginning of the transaction date range.
-  **tt\_to** (mandatory): the name of the column storing the end of the transaction date range.
-  **now\_value** (default = Long.MAX_VALUE): a date representing now.
-  **pattern** (default = yyyy/MM/dd HH:mm:ss.SSS Z): the date pattern for parsing Cassandra not-date columns and
   creating Lucene fields. Note that it can be used to index dates with reduced precision.

**Supported CQL types:**

-  ascii, bigint, date, int, text, timestamp, timeuuid, varchar, varint

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             bitemporal: {
                type: "bitemporal",
                vt_from: "vt_from",
                vt_to: "vt_to",
                tt_from: "tt_from",
                tt_to: "tt_to",
                validated: true,
                pattern: "yyyy/MM/dd HH:mm:ss.SSS",
                now_value: "3000/01/01 00:00:00.000",
             }
          }
       }'
    };


Blob mapper
___________

Maps a blob value.

**Parameters:**

-  **validated** (default = false): if mapping errors should make CQL writes fail, instead of just logging the error.
-  **column** (default = name of the mapper): the name of the column storing blob to be indexed.

**Supported CQL types:**

-  ascii, blob,  text, varchar

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX test_idx ON test()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             blob: {
                type: "bytes",
                column: "column_name"
             }
          }
       }'
    };


Boolean mapper
______________

Maps a boolean value.

**Parameters:**

-  **validated** (default = false): if mapping errors should make CQL writes fail, instead of just logging the error.
-  **column** (default = name of the mapper): the name of the column storing boolean value to be indexed.

**Supported CQL types:**

-  ascii, boolean , text, varchar

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX test_idx ON test()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             bool: {
                type: "boolean",
                 validated: true,
                 column: "column_name"
             }
          }
       }'
    };


Date mapper
___________

Maps dates using a either a pattern, an UNIX timestamp or a time UUID.

**Parameters:**

-  **validated** (default = false): if mapping errors should make CQL writes fail, instead of just logging the error.
-  **column** (default = name of the mapper): the name of the column storing the date to be indexed.
-  **pattern** (default = yyyy/MM/dd HH:mm:ss.SSS Z): the date pattern for parsing Cassandra not-date columns and
   creating Lucene fields. Note that it can be used to index dates with reduced precision.

**Supported CQL types:**

-  ascii, bigint, date, int, text, timestamp, timeuuid, varchar, varint

**Example:** Index the column *creation* with a precision of minutes using the date format pattern *yyyy/MM/dd HH:mm*:

.. code-block:: sql

    CREATE CUSTOM INDEX test_idx ON test()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             creation: {
                type: "date",
                pattern: "yyyy/MM/dd HH:mm",
             }
          }
       }'
    };


Date range mapper
_________________

Maps a time duration/period defined by a start date and a stop date.

**Parameters:**

-  **validated** (default = false): if mapping errors should make CQL writes fail, instead of just logging the error.
-  **from** (mandatory): the name of the column storing the start date of the time duration to be indexed.
-  **to** (mandatory): the name of the column storing the stop date of the time duration to be indexed.
-  **pattern** (default = yyyy/MM/dd HH:mm:ss.SSS Z): the date pattern for parsing Cassandra not-date columns and
   creating Lucene fields. Note that it can be used to index dates with reduced precision.

**Supported CQL types:**

-  ascii, bigint, date, int, text, timestamp, timeuuid, varchar, varint

**Example 1:** Index the column time period defined by the columns *start* and *stop*, using the default date pattern:

.. code-block:: sql

    CREATE CUSTOM INDEX test_idx ON test()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             duration: {
                type: "date_range",
                from: "start",
                to: "stop"
             }
          }
       }'
    };

**Example 2:** Index the column time period defined by the columns *start* and *stop*, validating values, and using a
precision of minutes:

.. code-block:: sql

    CREATE CUSTOM INDEX test_idx ON test()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             duration: {
                type: "date_range",
                validated: true,
                from: "start",
                to: "stop",
                pattern: "yyyy/MM/dd HH:mm"
             }
          }
       }'
    };


Double mapper
_____________

Maps a 64-bit decimal number.

**Parameters:**

-  **validated** (default = false): if mapping errors should make CQL writes fail, instead of just logging the error.
-  **column** (default = name of the mapper): the name of the column storing the double to be indexed.
-  **boost** (default = 0.1f): the Lucene's index-time boosting factor.

**Supported CQL types:**

-  ascii, bigint, decimal, double, float, int, smallint, text, tinyint, varchar, varint

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX test_idx ON test()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             double: {
                type: "double",
                 boost: 2.0,
                 validated: true,
                 column: "column_name"
             }
          }
       }'
    };


Float mapper
____________

Maps a 32-bit decimal number.

**Parameters:**

-  **validated** (default = false): if mapping errors should make CQL writes fail, instead of just logging the error.
-  **column** (default = name of the mapper): the name of the column storing the float to be indexed.
-  **boost** (default = 0.1f): the Lucene's index-time boosting factor.

**Supported CQL types:**

-  ascii, bigint, decimal, double, float, int, smallint, tinyint, varchar, varint

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX test_idx ON test()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             float: {
                type: "float",
                boost: 2.0,
                validated: true,
                column: "column_name"
             }
          }
       }'
    };


Geo point mapper
________________

Maps a geospatial location (point) defined by two columns containing a latitude and a longitude.
Indexing is based on a `composite spatial strategy <https://eng.climate.com/2014/04/16/polygons-in-lucene/>`__ that
stores points in a doc values field and also indexes them into a geohash recursive prefix tree with a certain precision
level. The low-accuracy prefix tree is used to quickly find results, maybe producing some false positives,
and the doc values field is used to discard these false positives.

**Parameters:**

-  **validated** (default = false): if mapping errors should make CQL writes fail, instead of just logging the error.
-  **latitude** (mandatory): the name of the column storing the latitude of the point to be indexed.
-  **longitude** (mandatory): the name of the column storing the longitude of the point to be indexed.
-  **max_levels** (default = 11): the maximum number of levels in the underlying geohash search tree. False positives
   will be discarded using stored doc values, so this doesn't mean precision lost. Higher values will produce few false
   positives to be post-filtered, at the expense of creating more terms in the search index.

**Supported CQL types:**

-  ascii, bigint, decimal, double, float, int, smallint, text, timestamp, varchar, varint

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX test_idx ON test()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             geo_point: {
                type: "geo_point",
                validated: true,
                latitude: "lat",
                longitude: "long",
                max_levels: 15
             }
          }
       }'
    };


Geo shape mapper
________________

Maps a geographical shape stored in a text column with `Well Known Text (WKT) <http://en.wikipedia.org/wiki/Well-known_text>`__
format. The supported WKT shapes are point, linestring, polygon, multipoint, multilinestring and multipolygon.

It is possible to specify a sequence of `geometrical transformations <#transformations>`__ to be applied to the shape
before indexing it. It could be used for indexing only the centroid of the shape, or a buffer around it, etc.

Indexing is based on a `composite spatial strategy <https://eng.climate.com/2014/04/16/polygons-in-lucene/>`__ that
stores shapes in a doc values field and also indexes them into a geohash recursive prefix tree with a certain precision
level. The low-accuracy prefix tree is used to quickly find results, maybe producing some false positives,
and the doc values field is used to discard these false positives.

This mapper depends on `Java Topology Suite (JTS) <http://www.vividsolutions.com/jts>`__.
This library can't be distributed together with this project due to license compatibility problems, but you can add it
by putting `jts-core-1.14.0.jar <http://search.maven.org/remotecontent?filepath=com/vividsolutions/jts-core/1.14.0/jts-core-1.14.0.jar>`__
into your Cassandra installation lib directory.

**Parameters:**

-  **validated** (default = false): if mapping errors should make CQL writes fail, instead of just logging the error.
-  **column** (default = name of the mapper): the name of the column storing the shape to be indexed in `WKT format <http://en.wikipedia.org/wiki/Well-known_text>`__.
-  **max_levels** (default = 5): the maximum number of levels in the underlying geohash search tree. False positives
   will be discarded using stored doc values, so this doesn't mean precision lost. Higher values will produce few false
   positives to be post-filtered, at the expense of creating more terms in the search index.
-  **transformations** (optional): sequence of `geometrical transformations <#transformations>`__ to be applied to each
   shape before indexing it.

**Supported CQL types:**

-  ascii, text, varchar

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

    CREATE CUSTOM INDEX test_idx ON test()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             shape: {
                type: "geo_shape",
                max_levels: 15
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
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             shape: {
                type: "geo_shape",
                max_levels: 15,
                transformations: [{type: "centroid"}]
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
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             shape: {
                type: "geo_shape",
                max_levels: 15,
                transformations: [{type: "buffer", min_distance: "50km"}]
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

    INSERT INTO borders(country, shape) VALUES ('france', 'LINESTRING(-1.8037198483943 43.463094234466, -1.3642667233943 43.331258296966 ... )');
    INSERT INTO borders(country, shape) VALUES ('portugal', 'LINESTRING(-8.8789151608943 41.925008296966, -8.2636807858943 42.100789546966 ... )');

    CREATE CUSTOM INDEX borders_index on borders()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             shape: {
                type: "geo_shape",
                max_levels: 15,
                transformations: [{type: "buffer", max_distance: "50km"}]
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

    INSERT INTO blocks(name, shape) VALUES (341, 'MULTIPOLYGON(((-86.693279 32.390691, -86.693185 32.391494, -86.691590 32.391362, -86.691621 32.391095 ... )))');

    CREATE CUSTOM INDEX blocks_index on cities()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             shape: {
                type: "geo_shape",
                max_levels: 15,
                transformations: [{type: "convex_hull"}]
             }
          }
       }'
    };

**Example 6:** Index the bounding box of the WKT shape contained in the indexed column:

.. image:: /doc/resources/geo_shape_mapper_example_6.png
   :width: 100%
   :alt: search by shape
   :align: center

.. code-block:: sql

    CREATE TABLE IF NOT EXISTS blocks (
       id bigint PRIMARY KEY,
       shape text
    );

    INSERT INTO blocks(name, shape) VALUES (341, 'MULTIPOLYGON(((-86.693279 32.390691, -86.693185 32.391494, -86.691590 32.391362 ... )))');

    CREATE CUSTOM INDEX blocks_index on cities()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             shape: {
                type: "geo_shape",
                max_levels: 15,
                transformations: [{type: "bbox"}]
             }
          }
       }'
    };


Inet mapper
___________

Maps an IP address. Either IPv4 and IPv6 are supported.

**Parameters:**

-  **validated** (default = false): if mapping errors should make CQL writes fail, instead of just logging the error.
-  **column** (default = name of the mapper): the name of the column storing the IP address to be indexed.

**Supported CQL types:**

-  ascii, inet, text, varchar

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX test_idx ON test()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             inet: {
                type: "inet",
                validated: true,
                column: "column_name"
             }
          }
       }'
    };


Integer mapper
______________

Maps a 32-bit integer number.

**Parameters:**

-  **validated** (default = false): if mapping errors should make CQL writes fail, instead of just logging the error.
-  **column** (default = name of the mapper): the name of the column storing the integer to be indexed.
-  **boost** (default = 0.1f): the Lucene's index-time boosting factor.

**Supported CQL types:**

-  ascii, bigint, date, decimal, double, float, int, smallint, text, timestamp, tinyint, varchar, varint

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX test_idx ON test()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             integer: {
                type: "integer",
                validated: true,
                column: "column_name"
                boost: 2.0,
             }
          }
       }'
    };


Long mapper
___________

Maps a 64-bit integer number.

**Parameters:**

-  **validated** (default = false): if mapping errors should make CQL writes fail, instead of just logging the error.
-  **column** (default = name of the mapper): the name of the column storing the double to be indexed.
-  **boost** (default = 0.1f): the Lucene's index-time boosting factor.

**Supported CQL types:**

-  ascii, bigint, date, decimal, double, float, int, smallint, text, timestamp, tinyint, varchar, varint

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX test_idx ON test()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             long: {
                type: "long",
                validated: true,
                column: "column_name"
                 boost: 2.0
             }
          }
       }'
    };


String mapper
_____________

Maps a not-analyzed text value.

**Parameters:**

-  **validated** (default = false): if mapping errors should make CQL writes fail, instead of just logging the error.
-  **column** (default = name of the mapper): the name of the column storing the IP address to be indexed.
-  **case_sensitive** (default = true): if the text will be indexed preserving its casing.

**Supported CQL types:**

-  ascii, bigint, blob, boolean, double, float, inet, int, smallint, text, timestamp, timeuuid, tinyint, uuid, varchar, varint

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX test_idx ON test()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             string: {
                type: "string",
                validated: true,
                column: "column_name"
                case_sensitive: false
             }
          }
       }'
    };


Text mapper
___________

Maps a language-aware text value analyzed according to the specified analyzer.

**Parameters:**

-  **validated** (default = false): if mapping errors should make CQL writes fail, instead of just logging the error.
-  **column** (default = name of the mapper): the name of the column storing the IP address to be indexed.
-  **analyzer** (default = default_analyzer): the name of the `text analyzer <https://lucene.apache.org/core/5_5_1/core/org/apache/lucene/analysis/Analyzer.html>`__ to be used.
   Additionally to references to those analyzers defined in the `analyzers section <#analyzers>`__ of the schema,
   there are prebuilt analyzers for Arabic, Bulgarian, Brazilian, Catalan, Sorani, Czech, Danish, German, Greek,
   English, Spanish, Basque, Persian, Finnish, French, Irish, Galician, Hindi, Hungarian, Armenian, Indonesian, Italian,
   Latvian, Dutch, Norwegian, Portuguese, Romanian, Russian, Swedish, Thai and Turkish.

**Supported CQL types:**

-  ascii, bigint, blob, boolean, double, float, inet, int, smallint, text, timestamp, timeuuid, tinyint, uuid, varchar, varint

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX test_idx ON test()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          analyzers: {
             my_custom_analyzer: {
                 type: "snowball",
                 language: "Spanish",
                 stopwords: "el,la,lo,los,las,a,ante,bajo,cabe,con,contra"
             }
          },
          fields: {
             spanish_text: {
                 type: "text",
                 validated: true,
                 column: "message_body",
                 analyzer: "my_custom_analyzer"
             },
             english_text: {
                 type: "text",
                 column: "message_body",
                 analyzer: "English"
             }
         }
       }'
    };


UUID mapper
___________

Maps an UUID value.

**Parameters:**

-  **validated** (default = false): if mapping errors should make CQL writes fail, instead of just logging the error.
-  **column** (default = name of the mapper): the name of the column storing the IP address to be indexed.

**Supported CQL types:**

-  ascii, text, timeuuid, uuid, varchar

**Example:**

.. code-block:: sql

    CREATE CUSTOM INDEX test_idx ON test()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             id: {
                type: "uuid",
                validated: true,
                column: "column_name"
             }
          }
       }'
    };

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
       'refresh_seconds': '60',
       'ram_buffer_mb': '64',
       'max_merge_mb': '5',
       'max_cached_mb': '30',
       'excluded_data_centers': 'dc2,dc3',
       'partitioner': '{type: "token", partitions: 4}',
       'schema': '{
          analyzers: {
             my_custom_analyzer: {
                type: "snowball",
                language: "Spanish",
                stopwords: "el,la,lo,los,las,a,ante,bajo,cabe,con,contra"
             }
         },
         default_analyzer: "english",
         fields: {
            name: {type: "string"},
            gender: {type: "string", validated: true},
            animal: {type: "string"},
            age: {type: "integer"},
            food: {type: "string"},
            number: {type: "integer"},
            bool: {type: "boolean"},
            date: {type: "date", validated: true, pattern: "yyyy/MM/dd"},
            duration: {type: "date_range", from: "start_date", to: "stop_date"},
            place: {type: "geo_point", latitude: "latitude", longitude: "longitude"},
            mapz: {type: "string"},
            setz: {type: "string"},
            listz: {type: "string"},
            phrase: {type: "text", analyzer: "my_custom_analyzer"}
         }
      }'
    };

---------
Searching
---------

Lucene indexes are queried using a custom JSON syntax defining the kind of search to be done.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table_name> WHERE expr(<index_name>, '{
       (  filter: ( <filter> )* )?
       (, query: ( <query>  )* )?
       (, sort: ( <sort>   )* )?
       (, refresh: ( true | false ) )?
    }');

where <filter> and <query> are a JSON object:

.. code-block:: sql

    <filter>:= {type: <type> (, <option>: ( <value> | <value_list> ) )* }
    <query>:= {type: <type> (, <option>: ( <value> | <value_list> ) )* }

and <sort> is another JSON object:

.. code-block:: sql

        <sort>:= <simple_sort_field> | <geo_distance_sort_field>
        <simple_sort_field>:= {
           (type: "simple",)?
           field: <field>
           (, reverse: <reverse> )?
        }
        <geo_distance_sort_field>:= {
           type: "geo_distance",
           field: <field>,
           latitude: <Double>,
           longitude: <Double>
           (, reverse: <reverse> )?
        }

When searching by ``filter``, without any ``query`` or ``sort`` defined,
then the results are returned in the Cassandra’s natural order, which is
defined by the partitioner and the column name comparator. When searching
by ``query``, results are returned sorted by descending
`relevance <https://lucene.apache.org/core/3_6_0/scoring.html>`_. ``sort``
option is used to specify the order in which the indexed rows will be traversed.
Field-based sorting has preference over query's relevance sorting.

``geo_distance`` sort field is used to sort the matched rows by ascending distance
to a certain geographical point, indicating the `geo point field <#geo-point-mapper>`__
to be used.

Relevance queries must touch all the nodes in the ring in order to find
the globally best results, so you should prefer filters over queries
when no relevance nor sorting are needed. The only exception to this are
searches directed to a single very wide partition (hundreds of thousands of rows),
where query's particular pagination technique may sometimes have a better performance.

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
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | doc_values      | boolean         | false                          | No        |
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
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | doc_values      | boolean         | false                          | No        |
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
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | doc_values      | boolean         | false                          | No        |
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
       (filter | query): {type: "all"}
    }');

**Example:** search for all the indexed rows:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '
       {filter: {type: "all"}
    }');

Using the `Java query builder <#query-builder>`__:

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
       (filter | query): {
          type: "bitemporal",
          (vt_from: <vt_from> ,)?
          (vt_to: <vt_to> ,)?
          (tt_from: <tt_from> ,)?
          (tt_to: <tt_to> ,)?
          (operation: <operation> )?
       }
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
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             bitemporal: {
                type: "bitemporal",
                tt_from: "tt_from",
                tt_to: "tt_to",
                vt_from: "vt_from",
                vt_to: "vt_to",
                pattern: "yyyy/MM/dd",
                now_value: "2200/12/31"
             }
          }
       }
    '};

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


If you want to know what is the last info about where John resides, you perform a query with tt_from and tt_to set to now_value:

.. code-block:: sql

    SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM census WHERE expr(tweets_index, '{
       filter: {
          type: "bitemporal",
          field: "bitemporal",
          vt_from: 0,
          vt_to: "2200/12/31",
          tt_from: "2200/12/31",
          tt_to: "2200/12/31"
       }
    }') AND name='John';

Using the `Java query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
       "SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM test.census WHERE expr(census_index, '%s')",
       search().filter(bitemporal("bitemporal").ttFrom("2200/12/31")
                                               .ttTo("2200/12/31")
                                               .vtFrom(0)
                                               .vtTo("2200/12/31").build());



If you want to know what is the last info about where John resides now, you perform a query with tt_from, tt_to, vt_from, vt_to set to now_value:

.. code-block:: sql

    SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM census WHERE expr(census_index, '{
       filter: {
          type: "bitemporal",
          field: "bitemporal",
          vt_from: "2200/12/31",
          vt_to: "2200/12/31",
          tt_from: "2200/12/31",
          tt_to: "2200/12/31"
       }
    }') AND name='John';

Using the `Java query builder <#query-builder>`__:

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
       filter: {
          type: "bitemporal",
          field: "bitemporal",
          vt_from: "2015/03/01",
          vt_to: "2015/03/01",
          tt_from: "2015/03/01",
          tt_to: "2015/03/01"
       }
    }') AND name = 'John';

Using the `Java query builder <#query-builder>`__:

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
       filter: {
          type: "bitemporal",
          field: "bitemporal",
          tt_from: "2015/07/05",
          tt_to: "2015/07/05"
       }
    }') AND name='John';

Using the `Java query builder <#query-builder>`__:

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
       (filter | query): {
         ( type: "boolean" , )?
         ( must: [(search,)?] , )?
         ( should: [(search,)?] , )?
         ( not: [(search,)?] , )?
       }
    }');

where:

-  **must**: represents the conjunction of searches: search_1 AND search_2
   AND … AND search_n
-  **should**: represents the disjunction of searches: search_1 OR search_2
   OR … OR search_n
-  **not**: represents the negation of the disjunction of searches:
   NOT(search_1 OR search_2 OR … OR search_n)


**Example 1:** search for rows where name ends with “a” AND food starts
with “tu”:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          type: "boolean",
          must: [
             {type: "wildcard", field: "name", value: "*a"},
             {type: "wildcard", field: "food", value: "tu*"}
          ]
       }
    }');

You can also write this search without the ``type`` attribute:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          must: [
             {type: "wildcard", field: "name", value: "*a"},
             {type: "wildcard", field: "food", value: "tu*"}
          ]
       }
    }');

Or inside the base filter path:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: [
          {type: "wildcard", field: "name", value: "*a"},
          {type: "wildcard", field: "food", value: "tu*"}
       ]
    }');

Using the `Java query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs1 = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(bool().must(wildcard("name", "*a"), wildcard("food", "tu*"))).build());

    ResultSet rs2 = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(must(wildcard("name", "*a"), wildcard("food", "tu*"))).build());

    ResultSet rs3 = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(wildcard("name", "*a"), wildcard("food", "tu*")).build());


**Example 2:** search for rows where food starts with “tu” but name does not end with “a”:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          type: "boolean",
          not: [{type: "wildcard", field: "name", value: "*a"}],
          must: [{type: "wildcard", field: "food", value: "tu*"}]
       }
    }');

You can also write this search without the ``type`` attribute:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          not: [{type: "wildcard", field: "name", value: "*a"}],
          must: [{type: "wildcard", field: "food", value: "tu*"}]
       }
    }');

It is also possible to write the search this way:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: [
          {type: "wildcard", field: "food", value: "tu*"},
          {not: {type: "wildcard", field: "name", value: "*a"}}
       ]
    }');

Using the `Java query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs1 = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(bool().must(wildcard("food", "tu*")).not(wildcard("name", "*a"))).build());

    ResultSet rs2 = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(must(wildcard("food", "tu*")).not(wildcard("name", "*a"))).build());

    ResultSet rs3 = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(wildcard("food", "tu*"), not(wildcard("name", "*a"))).build());


**Example 3:** search for rows where name ends with “a” or food starts with
“tu”:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          type: "boolean",
          should: [
             {type: "wildcard", field: "name", value: "*a"},
             {type: "wildcard", field: "food", value: "tu*"}
          ]
       }
    }');

You can also write this search without the ``type`` attribute:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          should: [
             {type: "wildcard", field: "name", value: "*a"},
             {type: "wildcard", field: "food", value: "tu*"}
          ]
       }
    }');

Using the `Java query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs1 = session.execute(
       "SELECT * FROM users WHERE expr(users_index, ?)",
       search().filter(bool().should(wildcard("name", "*a"), wildcard("food", "tu*"))).build());

    ResultSet rs2 = session.execute(
       "SELECT * FROM users WHERE expr(users_index, ?)",
       search().filter(should(wildcard("name", "*a"), wildcard("food", "tu*"))).build());


**Example 4:** will return zero rows independently of the index contents:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {type: "boolean"}
    }');

Using the `Java query builder <#query-builder>`__:

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
       filter: {not: [{type: "wildcard", field: "name", value: "*a"}]}
    }');

Using the `Java query builder <#query-builder>`__:

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
       ( filter | query ): {
          type: "contains",
          field: <field_name> ,
          values: <value_list> }
          (, doc_values: <doc_values> )?
       }
    }');

where:

-  **doc\_values** (default = false): if the generated Lucene query should use doc values instead of inverted index.
   Doc values searches are typically slower, but they can be faster in the dense case where most rows match the search.

**Example 1:** search for rows where name matches “Alicia” or “mancha”:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          type: "contains",
          field: "name",
          values: ["Alicia", "mancha"]
       }
    }');

Using the `Java query builder <#query-builder>`__:

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
       filter: {
          type: "contains",
          field: "date",
          values: ["2014/01/01", "2014/01/02", "2014/01/03"]
       }
    }');

Using the `Java query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
       "SELECT * FROM users WHERE expr(users_index, ?)",
       search().filter(contains("date", "2014/01/01", "2014/01/02", "2014/01/03")).build());


Date range search
=================

Searches for date ranges/durations indexed by a `date range mapper <#date-range-mapper>`__, using a spatial approach.
This allows you to use spatial operators such as *intersects*, *contains* and *is\_within*.
If you just want to search for single-column dates (points in time) within a certain time range, you should index them
use a `range search <#range-search>`__.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table> WHERE expr(<index_name>, '{
       (filter | query): {
          type: "date_range",
          (from: <from> ,)?
          (to: <to> ,)?
          (operation: <operation> )?
       }
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
       filter: {
          type: "date_range",
          field: "duration",
          from: "2014/01/01",
          to: "2014/12/31",
          operation: "intersects"
       }
    }');

Using the `Java query builder <#query-builder>`__:

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
       filter: {
          type: "date_range",
          field: "duration",
          from: "2014/06/01",
          to: "2014/06/02",
          operation: "contains"
       }
    }');

Using the `Java query builder <#query-builder>`__:

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
       filter: {
          type: "date_range",
          field: "duration",
          from: "2014/01/01",
          to: "2014/12/31",
          operation: "is_within"
       }
    }');


Using the `Java query builder <#query-builder>`__:

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
       (filter | query): {
          type: "fuzzy",
          field: <field_name> ,
          value: <value>
          (, max_edits: <max_edits> )?
          (, prefix_length: <prefix_length> )?
          (, max_expansions: <max_expansion> )?
          (, transpositions: <transposition> )?
       }
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
       filter: {
          type: "fuzzy",
          field: "phrase",
          value: "puma",
          max_edits: 1
       }
    }');


Using the `Java query builder <#query-builder>`__:

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
       filter: {
          type: "fuzzy",
          field: "phrase",
          value: "puma",
          max_edits: 1,
          prefix_length: 2
       }
    }');

Using the `Java query builder <#query-builder>`__:

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
       (filter | query): {
          type: "geo_bbox",
          field: <field_name>,
          min_latitude: <min_latitude> ,
          max_latitude: <max_latitude> ,
          min_longitude: <min_longitude> ,
          max_longitude: <max_longitude>
       }
    }');

where:

-  **min\_latitude**: a double value between -90 and 90 being the min
   allowed latitude.
-  **max\_latitude**: a double value between -90 and 90 being the max
   allowed latitude.
-  **min\_longitude**: a double value between -180 and 180 being the
   min allowed longitude.
-  **max\_longitude**: a double value between -180 and 180 being the
   max allowed longitude.

**Example 1:** search for any rows where “place” is formed by a latitude
between -90.0 and 90.0, and a longitude between -180.0 and
180.0:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          type: "geo_bbox",
          field: "place",
          min_latitude: -90.0,
          max_latitude: 90.0,
          min_longitude: -180.0,
          max_longitude: 180.0
       }
    }');

Using the `Java query builder <#query-builder>`__:

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
       filter: {
          type: "geo_bbox",
          field: "place",
          min_latitude: -90.0,
          max_latitude: 90.0,
          min_longitude: 0.0,
          max_longitude: 10.0 }
    }');


Using the `Java query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
       "SELECT * FROM users WHERE expr(users_index, ?)",
       search().filter(geoBBox("place", 0.0, 10.0, -90.0, 90.0)).build());


**Example 3:** search for any rows where “place” is formed by a latitude
between 0.0 and 10.0, and a longitude between -180.0 and
180.0 sorted by min distance to point [0.0, 0.0]:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          type: "geo_bbox",
          field: "place",
          min_latitude: 0.0,
          max_latitude: 10.0,
          min_longitude: -180.0,
          max_longitude: 180.0 },
       sort: {
          type: "geo_distance",
          field: "geo_point",
          reverse: false,
          latitude: 0.0,
          longitude: 0.0 }
    }');


Using the `Java query builder <#query-builder>`__:

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
        (filter | query): {
            type: "geo_distance",
            field: <field_name> ,
            latitude: <latitude> ,
            longitude: <longitude> ,
            max_distance: <max_distance>
            (, min_distance: <min_distance> )? }
    }');

where:

-  **latitude**: a double value between -90 and 90 being the latitude
   of the reference point.
-  **longitude**: a double value between -180 and 180 being the
   longitude of the reference point.
-  **max\_distance**: a string value being the max allowed `distance <#distance>`__ from the reference point.
-  **min\_distance**: a string value being the min allowed `distance <#distance>`__ from the reference point.

**Example 1:** search for any rows where “place” is within one kilometer from the geo point (40.225479, -3.999278):

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          type: "geo_distance",
          field: "place",
          latitude: 40.225479,
          longitude: -3.999278,
          max_distance: "1km"
       }
    }');

Using the `Java query builder <#query-builder>`__:

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
       filter: {
          type: "geo_distance",
          field: "place",
          latitude: 40.225479,
          longitude: -3.999278,
          max_distance: "10yd" ,
          min_distance: "1yd"
       },
       sort: {
          fields: [ {
             type: "geo_distance",
             field: "geo_point",
             reverse: false,
             latitude: 40.225479,
             longitude: -3.999278
          } ]
       }
    }');

Using the `Java query builder <#query-builder>`__:

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
related to a specified `shape <#shapes>`__. Search shapes can be either shapes with
`Well Known Text (WKT) <http://en.wikipedia.org/wiki/Well-known_text>`__ format or transformations over WKT shapes.
The supported WKT shapes are point, linestring, polygon, multipoint, multilinestring and multipolygon.

This search type depends on `Java Topology Suite (JTS) <http://www.vividsolutions.com/jts>`__.
This library can't be distributed together with this project due to license compatibility problems, but you can add it
by putting `jts-core-1.14.0.jar <http://search.maven.org/remotecontent?filepath=com/vividsolutions/jts-core/1.14.0/jts-core-1.14.0.jar>`__
into your Cassandra installation lib directory.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table> WHERE expr(<index_name, '{
       (filter | query): {
          type : "geo_shape",
          field: <fieldname> ,
          shape: <shape>
          (, operation: <operation>)?
       }
    }');

where:

-  **shape**: a geospatial `shape <#shapes>`__.
-  **operation**: the type of spatial operation to be performed. The possible values are "intersects", "is_within" and
"contains". Defaults to "is_within".

**Example 1:** search for shapes within a polygon:

.. image:: /doc/resources/geo_shape_condition_example_1.png
   :width: 100%
   :alt: search by shape
   :align: center

.. code-block:: sql

    SELECT * FROM test WHERE expr(test_index, '{
        filter: {
            type: "geo_shape",
            field: "place",
            shape: {
               type: "wkt",
               value: "POLYGON((-0.07 51.63, 0.03 51.54, 0.05 51.65, -0.07 51.63))"
            }
        }
    }');

Using the `Java query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String shape = "POLYGON((-0.07 51.63, 0.03 51.54, 0.05 51.65, -0.07 51.63))";
    ResultSet rs = session.execute(
      "SELECT * FROM TABLE test WHERE expr(test_index, ?)",
      search().filter(geoShape("place", wkt(shape))).build());

**Example 2:** search for shapes intersecting with a shape defined by a buffer 10 kilometers around a segment of the
Florida's coastline:

.. image:: /doc/resources/geo_shape_condition_example_2.png
   :width: 100%
   :alt: buffer transformation
   :align: center

.. code-block:: sql

    SELECT * FROM test WHERE expr(test_index, '{
        filter: {
            type: "geo_shape",
            field: "place",
            operation: "intersects",
            shape: {
               type: "buffer",
               max_distance: "10km",
               shape: {
                  type: "wkt",
                  value: "LINESTRING(-80.90 29.05, -80.51 28.47, -80.60 28.12, -80.00 26.85, -80.05 26.37)"
               }
            }
        }
    }');

Using the `Java query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String shape = "POLYGON((-0.07 51.63, 0.03 51.54, 0.05 51.65, -0.07 51.63))";
    ResultSet rs = session.execute(
        "SELECT * FROM TABLE test WHERE expr(test_index, ?)",
        search().filter(geoShape("place", buffer(shape).maxDistance("10km")).operation("intersects")).build());


Match search
============

Searches for rows with columns containing the specified term. The matching depends on the used analyzer.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table> WHERE expr(<index_name>, '{
       (filter | query): {
          type: "match",
          field: <field_name>,
          value: <value>,
          (, doc_values: <doc_values> )?
       }
    }');

where:

-  **doc\_values** (default = false): if the generated Lucene query should use doc values instead of inverted index.
   Doc values searches are typically slower, but they can be faster in the dense case where most rows match the search.

**Example 1:** search for rows where name matches “Alicia”:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          type: "match",
          field: "name",
          value: "Alicia"
       }
    }');

Using the `Java query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
       "SELECT * FROM users WHERE expr(users_index, ?)",
       search().filter(match("name", "Alicia")).build());


**Example 2:** search for any rows where phrase contains “mancha”:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          type: "match",
          field: "phrase",
          value: "mancha"
       }
    }');

Using the `Java query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
       "SELECT * FROM users WHERE expr(users_index, ?)",
       search().filter(match("phrase", "mancha").build());


**Example 3:** search for rows where date matches “2014/01/01″:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          type: "match",
          field: "date",
          value: "2014/01/01",
          doc_values: true
       }
    }');

Using the `Java query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
       "SELECT * FROM users WHERE expr(users_index, ?)",
       search().filter(match("date", "2014/01/01").docValues(true)).build());


None search
===========

Returns no results.

**Syntax:**

.. code-block:: sql

    SELECT ( <fields> | * ) FROM <table> WHERE expr(<index_name>, '{
       (filter | query): {type: "none"}
    }');

**Example:** will return no one of the indexed rows:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {type: "none"}
    }');

Using the `Java query builder <#query-builder>`__:

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
       (filter | query): {
          type: "phrase",
          field: <field_name> ,
          value: <value>
          (, slop: <slop> )? }
    }');

where:

-  **values**: an ordered list of values.
-  **slop** (default = 0): number of words permitted between words.

**Example 1:** search for rows where “phrase” contains the word “camisa”
followed by the word “manchada”:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          type: "phrase",
          field: "phrase",
          values: "camisa manchada"
       }
    }');

Using the `Java query builder <#query-builder>`__:

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
       filter: {
          type: "phrase",
          field: "phrase",
          values: "mancha camisa",
          slop: 2
       }
    }');

Using the `Java query builder <#query-builder>`__:

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
       (filter | query): {
           type: "prefix",
          field: <field_name> ,
          value: <value>
       }
    }');

**Example:** search for rows where “phrase” contains a word starting with
“lu”. If the column is indexed as “text” and uses an analyzer, words
ignored by the analyzer will not be retrieved:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          type: "prefix",
          field: "phrase",
          value: "lu"
       }
    }');

Using the `Java query builder <#query-builder>`__:

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
       (filter | query): {
          type: "range",
          field: <field_name>
          (, lower: <lower>)?
          (, upper: <upper>)?
          (, include_lower: <include_lower> )?
          (, include_upper: <include_upper> )?
          (, doc_values: <doc_values> )?
       }
    }');

where:

-  **lower**: lower bound of the range.
-  **upper**: upper bound of the range.
-  **include\_lower** (default = false): if the lower bound is included
   (left-closed range).
-  **include\_upper** (default = false): if the upper bound is included
   (right-closed range).
-  **doc\_values** (default = false): if the generated Lucene query should use doc values instead of inverted index.
   Doc values searches are typically slower, but they can be faster in the dense case where most rows match the search.

Lower and upper will default to:math:`-/+\\infty` for number. In the
case of byte and string like data (bytes, inet, string, text), all
values from lower up to upper will be returned if both are specified. If
only “lower” is specified, all rows with values from “lower” will be
returned. If only “upper” is specified then all rows with field values
up to “upper” will be returned. If both are omitted than all rows will
be returned.

**Example 1:** search for rows where *age* is in [65, ∞):

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          type: "range",
          field: "age",
          lower: 65,
          include_lower: true
       }
    }');

Using the `Java query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
       "SELECT * FROM users WHERE expr(users_index, ?)",
       search().filter(range("age").lower(1).includeLower(true)).build());

**Example 2:** search for rows where *age* is in (-∞, 0]:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          type: "range",
          field: "age",
          upper: 0,
          include_upper: true,
          doc_values: true
       }
    }');

Using the `Java query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
       "SELECT * FROM users WHERE expr(users_index, ?)",
       search().filter(range("age").upper(0).includeUpper(true).docValues(true)).build());

**Example 3:** search for rows where *age* is in [-1, 1]:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          type: "range",
          field: "age",
          lower: -1,
          upper: 1,
          include_lower: true,
          include_upper: true,
          doc_values: false
       }
    }');

Using the `Java query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
       "SELECT * FROM users WHERE expr(users_index, ?)",
       search().filter(range("age").lower(-1).upper(1)
                                   .includeLower(true)
                                   .includeUpper(true)
                                   .docValues(true)).build());

**Example 4:** search for rows where *date* is in [2014/01/01, 2014/01/02]:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          type: "range",
          field: "date",
          lower: "2014/01/01",
          upper: "2014/01/02",
          include_lower: true,
          include_upper: true
       }
    }');

Using the `Java query builder <#query-builder>`__:

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
       (filter | query): {
          type: "regexp",
          field: <field_name>,
          value: <regexp>
       }
    }');

where:

-  **value**: a regular expression. See
   `org.apache.lucene.util.automaton.RegExp <http://lucene.apache.org/core/4_6_1/core/org/apache/lucene/util/automaton/RegExp.html>`__
   for syntax reference.

**Example:** search for rows where name contains a word that starts with
“p” and a vowel repeated twice (e.g. “pape”):

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          type: "regexp",
          field: "name",
          value: "[J][aeiou]{2}.*"
       }
    }');

Using the `Java query builder <#query-builder>`__:

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
       (filter | query): {
          type: "wildcard" ,
          field: <field_name> ,
          value: <wildcard_exp>
       }
    }');

where:

-  **value**: a wildcard expression. Supported wildcards are \*, which
   matches any character sequence (including the empty one), and ?,
   which matches any single character. ” is the escape character.

**Example:** search for rows where food starts with or is “tu”:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
       filter: {
          type: "wildcard",
          field: "food",
          value: "tu*"
       }
    }');


Using the `Java query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
       "SELECT * FROM users WHERE expr(users_index, ?)",
       search().filter(wildcard("food", "tu*")).build());

---------------------
Geographical elements
---------------------

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
       filter: {
          type: "geo_distance",
          field: "place",
          latitude: 40.225479,
          longitude: -3.999278,
          max_distance: "1km"
       }
    }';


Transformations
===============

`Geo shape mapper <#geo-shape-mapper>`__ takes a list of geometrical transformations as argument. These transformations
are sequentially applied to the shape that is going to be indexed or searched.

Bounding box transformation
___________________________

Buffer transformation returns the `minimum bounding box <https://en.wikipedia.org/wiki/Minimum_bounding_box>`__ of a
shape, that is, the minimum rectangle containing the shape.

**Syntax:**

.. code-block:: sql

    {type: "bbox"}

**Example:** The following `geo shape mapper <#geo-shape-mapper>`__ will index only the bounding box of the WKT shape
contained in the indexed column:

.. code-block:: sql

    CREATE CUSTOM INDEX places_index on places()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             shape: {
                type: "geo_shape",
                max_levels: 8,
                 transformations: [{type: "bbox"}]
             }
          }
       }'
    };

Buffer transformation
_____________________

Buffer transformation returns a buffer around a shape.

**Syntax:**

.. code-block:: sql

    {type: "buffer"
      (, min_distance: <distance> )?
      (, max_distance: <distance> )?
    }

where:

-  **min_distance**: the inside buffer `distance <#distance>`__. Optional.
-  **max_distance**: the outside buffer `distance <#distance>`__. Optional.

**Example:** the following `geo shape mapper <#geo-shape-mapper>`__ will index a buffer 10 kilometers around the WKT
shape contained in the indexed column:

.. code-block:: sql

    CREATE CUSTOM INDEX places_index on places()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             shape: {
                type: "geo_shape",
                max_levels: 8,
                 transformations: [{type: "buffer", max_distance: "10km"}]
             }
          }
       }'
    };

Centroid transformation
_______________________

Centroid transformation returns the geometric center of a shape.

**Syntax:**

.. code-block:: sql

    {type: "centroid"}

**Example:** The following `geo shape mapper <#geo-shape-mapper>`__ will index only the centroid of the WKT shape
contained in the indexed column:

.. code-block:: sql

    CREATE CUSTOM INDEX places_index on places()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             shape: {
                type: "geo_shape",
                max_levels: 8,
                transformations: [{type: "centroid"}]
             }
          }
       }'
    };

Convex hull transformation
__________________________

Convex hull transformation returns the `convex envelope <https://en.wikipedia.org/wiki/Convex_hull>`__ of a shape.

**Syntax:**

.. code-block:: sql

    {type: "convex_hull"}

**Example:** The following `geo shape mapper <#geo-shape-mapper>`__ will index only the convex hull of the WKT shape
contained in the indexed column:

.. code-block:: sql

    CREATE CUSTOM INDEX places_index on places()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             shape: {
                type: "geo_shape",
                max_levels: 8,
                transformations: [{type: "convex_hull"}]
             }
          }
       }'
    };


Shapes
======

`Geo shape search <#geo-shape-search>`__ allows the recursive definition of the search shape as a group of
transformations over other shapes.

WKT shape
_________

A shape defined in `Well Known Text (WKT) <http://en.wikipedia.org/wiki/Well-known_text>`__ format.

**Syntax:**

.. code-block:: sql

    {type: "wkt", value: <value>}

where:

-  **value**: A string containing the WKT shape. Mandatory.

**Example:** The following `geo shape search <#geo-shape-search>`__ will retrieve shapes intersecting a WKT shape:

.. code-block:: sql

    SELECT * FROM places WHERE expr(places_idx,'{
       filter: {
          type: "geo_shape",
          field: "place",
          operation: "intersects",
          shape: {
             type: "wkt",
             value: "LINESTRING(-80.90 29.05, -80.51 28.47, -80.60 28.12, -80.00 26.85, -80.05 26.37)"
          }
       }
    }');

Bounding box shape
__________________

Buffer transformation returns the `minimum bounding box <https://en.wikipedia.org/wiki/Minimum_bounding_box>`__ a shape,
that is, the minimum rectangle containing the shape.

**Syntax:**

.. code-block:: sql

    {type: "bbox", shape: <shape>}

where:

-  **shape**: the `shape <#shapes>`__ to be transformed. Mandatory.

**Example:** The following `geo shape search <#geo-shape-search>`__ will retrieve shapes intersecting the bounding box
of a WKT shape:

.. code-block:: sql

    SELECT * FROM places WHERE expr(places_idx,'{
       filter: {
          type: "geo_shape",
          field: "place",
          operation: "intersects",
          shape: {
             type: "bbox",
             shape: {
                type: "wkt",
                value: "LINESTRING(-80.90 29.05, -80.51 28.47, -80.60 28.12, -80.00 26.85, -80.05 26.37)"
             }
          }
       }
    }');

Buffer shape
____________

Buffer transformation returns a buffer around a shape.

**Syntax:**

.. code-block:: sql

    {
       type: "buffer"
       shape: <shape>
       (, min_distance: <distance> )?
       (, max_distance: <distance> )?
    }

where:

-  **shape**: the `shape <#shapes>`__ to be transformed. Mandatory.
-  **min_distance**: the inside buffer `distance <#distance>`__. Optional.
-  **max_distance**: the outside buffer `distance <#distance>`__. Optional.

**Example:** the following `geo shape search <#geo-shape-search>`__ will retrieve shapes intersecting with a shape
defined by a buffer 10 kilometers around a segment of the Florida's coastline:

.. code-block:: sql

    SELECT * FROM test WHERE expr(test_idx,'{
       filter: {
          type: "geo_shape",
          field: "place",
          operation: "intersects",
          shape: {
             type: "buffer",
             max_distance: "10km",
             shape: {
                type: "wkt",
                value: "LINESTRING(-80.90 29.05, -80.51 28.47, -80.60 28.12, -80.00 26.85, -80.05 26.37)"
             }
          }
       }
    }');

Centroid shape
______________

Centroid transformation returns the geometric center of a shape.

**Syntax:**

.. code-block:: sql

    {type: "centroid", shape: <shape>}

where:

-  **shape**: the `shape <#shapes>`__ to be transformed. Mandatory.

**Example:** The following `geo shape search <#geo-shape-search>`__ will retrieve shapes intersecting the centroid of a
WKT shape:

.. code-block:: sql

    SELECT * FROM places WHERE expr(places_idx,'{
       filter: {
          type: "geo_shape",
          field: "place",
          operation: "intersects",
          shape: {
             type: "centroid",
             shape: {
                type: "wkt",
                value: "LINESTRING(-80.90 29.05, -80.51 28.47, -80.60 28.12, -80.00 26.85, -80.05 26.37)"
             }
          }
       }
    }');

Convex hull shape
_________________

Convex hull transformation returns the `convex envelope <https://en.wikipedia.org/wiki/Convex_hull>`__ of a shape.

**Syntax:**

.. code-block:: sql

    {type: "convex_hull", shape: <shape>}

where:

-  **shape**: the `shape <#shapes>`__ to be transformed. Mandatory.

**Example:** The following `geo shape search <#geo-shape-search>`__ will retrieve shapes intersecting the convex hull of
a WKT shape:

.. code-block:: sql

    SELECT * FROM places WHERE expr(places_idx,'{
       filter: {
          type: "geo_shape",
          field: "place",
          operation: "intersects",
          shape: {
             type: "convex_hull",
             shape: {
                type: "wkt",
                value: "LINESTRING(-80.90 29.05, -80.51 28.47, -80.60 28.12, -80.00 26.85, -80.05 26.37)"
             }
          }
       }
    }');

Difference shape
________________

Difference transformation subtracts the specified shapes.

**Syntax:**

.. code-block:: sql

    {type: "difference", shapes: [ <shape> (, <shape>)* ] }

where:

-  **shapes**: the `shapes <#shapes>`__ to be subtracted. Mandatory.

Intersection shape
__________________

Intersection transformation intersects the specified shapes.

**Syntax:**

.. code-block:: sql

    {type: "intersection", shapes: [ <shape> (, <shape>)* ] }

where:

-  **shapes**: the `shapes <#shapes>`__ to be intersected. Mandatory.

Union shape
___________

Union transformation adds the specified shapes.

**Syntax:**

.. code-block:: sql

    {type: "union", shapes: [ <shape> (, <shape>)* ] }

where:

-  **shapes**: the `shapes <#shapes>`__ to be added. Mandatory.

------------------
Complex data types
------------------

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


    CREATE CUSTOM INDEX idx ON  collect_things() USING 'com.stratio.cassandra.lucene.Index' WITH OPTIONS = {
    'refresh_seconds':'1',
    'schema':'{
        fields:{
            "v.0": {type: "integer"},
            "v.1": {type: "string"},
            "v.2": {type: "float"} }
     }'};

    SELECT * FROM collect_things WHERE expr(idx, '{
       filter: {
          type: "match",
          field: "v.0",
          value: 1
       }
    }');

    SELECT * FROM collect_things WHERE expr(idx, '{
       filter: {
          type: "match",
          field: "v.1",
          value: "bar"
        }
    }');

    SELECT * FROM collect_things WHERE expr(idx, '{
        sort: {field: "v.2"}
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
        address frozen<address_udt>
    );

The components of UDTs can be indexed, searched and sorted this way:

.. code-block:: sql

    CREATE CUSTOM INDEX user_profiles_idx ON test.user_profiles()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             "address.city": {type: "string"},
             "address.zip": {type: "integer"}
          }
       }'
    };

    INSERT INTO user_profiles (login, first_name, last_name, address)
    VALUES('jsmith', 'John', 'Smith', {street: 'S Ellis Ave', city: 'Chicago', zip: 60601});

    SELECT * FROM user_profiles WHERE expr(user_profiles_idx, '{
       filter: {
          type: "match",
          field: "address.city",
          value: "Chicago"
       }
    }');

    SELECT * FROM user_profiles WHERE expr(user_profiles_idx, '{
       filter: {
          type: "match",
          field: "address.zip",
          value: 60601
       }
    }');

    SELECT * FROM user_profiles WHERE expr(user_profiles_idx, '{
       sort: {
          field: "address.zip"
       }
    }');

Collections
===========

CQL `collections <http://docs.datastax.com/en/cql/3.0/cql/cql_using/use_collections_c.html>`__ (lists, sets and maps) can be indexed.

Lists ans sets are indexed in the same way as regular columns, using their base type:

.. code-block:: sql

    CREATE TABLE user_profiles (
       login text PRIMARY KEY,
       first_name text,
       last_name text,
       cities list<text>
    );

    CREATE CUSTOM INDEX user_profiles_idx ON user_profiles()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
           fields: {
              cities: {type: "string"}
           }
       }'
    };

Searches are also done in the same way as with regular columns:

.. code-block:: sql

    INSERT INTO user_profiles (login, first_name, last_name, cities)
    VALUES('jsmith', 'John', 'Smith', ['London', 'Madrid']);

    SELECT * FROM user_profiles WHERE expr(user_profiles_idx, '{
       filter: {
          type: "match",
          field: "cities",
          value: "London"
       }
    }');

Maps values are indexed using their keys as field name suffixes:

.. code-block:: sql

    CREATE TABLE user_profiles (
       login text PRIMARY KEY,
       first_name text,
       last_name text,
       addresses map<text,text>
    );

    CREATE CUSTOM INDEX user_profiles_idx ON user_profiles()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             addresses: {type: "string"}
          }
       }'
    };

For searching map values under a certain key you should use '$' as field-key separator:

.. code-block:: sql

    INSERT INTO user_profiles (login, first_name, last_name, addresses)
    VALUES('jsmith', 'John', 'Smith', {'London': 'Camden Road', 'Madrid': 'Buenavista'});

    SELECT * FROM user_profiles WHERE expr(user_profiles_idx, '{
       filter: {
          type: "match",
          field: "addresses$London",
          value: "Camden Road"
       }
    }');

Please don't use map keys containing the separator chars, which are '.' and '$'.

UDTs can be indexed even while being inside collections:

.. code-block:: sql

    CREATE TYPE address (
       city text,
       zip int
    );

    CREATE TABLE user_profiles (
       login text PRIMARY KEY,
       first_name text,
       last_name text,
       addresses map<text, frozen<address>>
    );

    CREATE CUSTOM INDEX user_profiles_idx ON user_profiles()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
       'refresh_seconds': '1',
       'schema': '{
          fields: {
             "addresses.city" : {type: "string"},
             "addresses.zip"  : {type: "integer"}
          }
       }'
    };

    INSERT INTO user_profiles (login, first_name, last_name, addresses)
    VALUES('jsmith', 'John', 'Smith',
       {'Illinois':{city: 'Chicago', zip: 60601}, 'Colorado':{city: 'Denver', zip: 80012}});

    SELECT * FROM user_profiles WHERE expr(user_profiles_idx, '{
       filter: {
          type: "match",
          field: "addresses.city$Illinois",
          value: "Chicago"
       }
    }');

    SELECT * FROM user_profiles WHERE expr(user_profiles_idx, '{
       filter: {
          type: "match",
          field: "addresses.zip$Illinois",
          value: 60601
       }
    }');

-------------
Query Builder
-------------

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

----------------
Spark and Hadoop
----------------

Spark and Hadoop integrations are fully supported because Lucene searches
can be combined with token range restrictions and paging, which are the
basis of MapReduce frameworks support.

Please note that `Cassandra connector for Apache Spark <https://github.com/datastax/spark-cassandra-connector>`__ is not
compatible with new secondary index syntax yet, so you should use `the old dummy column approach <#alternative-syntaxes>`__.

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

    SELECT name, gender FROM test.users
    WHERE lucene = '{filter: {type: "match", field: "food", value: "chips"}}')
    AND token(name, gender) > token('Alicia', 'female');

Paging
======

Paging filtered results is fully supported. You can retrieve
the rows starting from a certain key. For example, if the primary key is
(userid, createdAt), you can search:

.. code-block:: sql

    SELECT * FROM tweets
    WHERE lucene = ‘{filter: {type:”match",  field:”text", value:”cassandra”}}'
    AND userid = 3543534 AND createdAt > 2011-02-03 04:05+0000 LIMIT 5000;

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

-------------
JMX Interface
-------------

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

----------------
Performance tips
----------------

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
        filter: {
           type: "match",
           field: "name",
           value: "Alice"
        }
    }');

However, this search could be a good use case for Lucene just because there is no easy counterpart:

.. code-block:: sql

    SELECT * FROM users WHERE expr(tweets_index, '{
        filter: [
            {type: "regexp", field: "name", value: "[J][aeiou]{2}.*"},
            {type: "range", field: "birthday", lower: "2014/04/25"}
        ],
        sort: [
            {field: "birthday", reverse: true },
            {field: "name"}
        ]
    }') LIMIT 20;

Lucene indexes are intended to be used in those cases that can't be efficiently addressed
with Apache Cassandra common techniques, such as full-text queries, multidimensional queries,
geospatial search and bitemporal data models.

Use the latest version
======================

Each new version might be as fast or faster than the previous one,
so please try to use the latest version if possible.
You can find the list of changes and performance improvements at `changelog file </CHANGELOG.md>`__.

It is also preferable to use last patch version of Cassandra (e.g. 2.1.14 instead of 2.1.4) because it includes the most recent bug fixes.


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
        'directory_path': '<lucene_disk>',
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
       'refresh': '<refresh_rate>',
       ...
    };

Prefer filters over queries
===========================

Query searches involve relevance so they should be sent to all nodes in the
cluster in order to find the globally best results.
However, filters have a chance to find the results in a subset of the nodes.
So if you are not interested in relevance sorting then you should prefer filters over queries.

The only exception to this are searches directed to a single very wide partition (hundreds of
thousands of rows), where query's particular pagination technique may sometimes have a better
performance.

Try doc values
==============

`Match <#match-search>`__, `range <#range-search>`__ and `contains <#contains-search>`__ searches have a property named
`doc_values` that can be used with single-column not-analyzed fields. When enabled, these Lucene will use doc values
instead of the inverted index. Doc values searches are typically slower, but they can be faster in the dense case where
most rows match the search. So, if you suspect that your search is going to match most rows in the table, try to enable
`doc_values`, because it could dramatically improve performance in some cases.

Force segments merge
====================

`JMX interface <#jmx-interface>`__ allows you to force a complete index segments merge. This is a very heavy operation
similar to C* compaction that can significantly improve search performance.
Although this operation is not mandatory at all,
you should consider using it if your system has off-peak hours that can be used for optimization tasks.
The ideal scenario is to have all the index in a single segment.