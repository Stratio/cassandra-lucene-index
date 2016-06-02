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
    - `Alternative syntaxes <#alternative-syntaxes>`__
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
        - `Bounding box <#bounding-box>`__
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
    - `Try doc values <#try-doc-values>`__
    - `Force segments merge <#force-segments-merge>`__

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


+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| From\\ To | 2.1.6.2 | 2.1.7.1 | 2.1.8.5 | 2.1.9.0 | 2.1.10.0 | 2.1.11.1 | 2.2.3.2 | 2.2.4.3 | 2.2.4.4 | 2.2.5.0 | 2.2.5.1 | 2.2.5.2 | 3.0.3.0 | 3.0.3.1 | 3.0.4.0 | 3.0.4.1 | 3.0.5.0 | 3.0.5.1 | 3.0.5.2 | 3.0.6.0 |
+===========+=========+=========+=========+=========+==========+==========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+=========+
| 2.1.6.0   |   YES   |   YES   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.6.1   |   YES   |   YES   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.6.2   |    --   |   YES   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.7.0   |    --   |   YES   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.7.1   |    --   |    --   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.8.0   |    --   |    --   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.8.1   |    --   |    --   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.8.2   |    --   |    --   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.8.3   |    --   |    --   |    NO   |    NO   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.8.4   |    --   |    --   |   YES   |   YES   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.8.5   |    --   |    --   |    --   |   YES   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.9.0   |    --   |    --   |    --   |    --   |    NO    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.10.0  |    --   |    --   |    --   |    --   |    --    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.11.0  |    --   |    --   |    --   |    --   |    --    |    NO    |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.1.11.1  |    --   |    --   |    --   |    --   |    --    |    --    |   YES   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.3.0   |    --   |    --   |    --   |    --   |    --    |    --    |   YES   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.3.1   |    --   |    --   |    --   |    --   |    --    |    --    |   YES   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.3.2   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.4.0   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.4.1   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.4.2   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |   YES   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.4.3   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |   YES   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.4.5   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |   YES   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.5.0   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.5.1   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |    --   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 2.2.5.2   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |    --   |    --   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.3.0   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.3.1   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.4.0   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    NO   |    NO   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.4.1   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.5.0   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    NO   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.5.1   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    NO   |    NO   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+
| 3.0.5.2   |    --   |    --   |    --   |    --   |    --    |    --    |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |    --   |   YES   |
+-----------+---------+---------+---------+---------+----------+----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+

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
|                                     | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | integer_digits  | integer         | 32                             | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | decimal_digits  | integer         | 32                             | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `bigint <#big-integer-mapper>`__    | column          | string          | mapper_name of the schema      | No        |
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
|                                     | validated       | boolean         | false                          | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `boolean <#boolean-mapper>`__       | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `date <#date-mapper>`__             | column          | string          | mapper_name of the schema      | No        |
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
|                                     | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | boost           | integer         | 0.1f                           | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `float <#float-mapper>`__           | column          | string          | mapper_name of the schema      | No        |
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
|                                     | validated       | boolean         | false                          | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `integer <#integer-mapper>`__       | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | boost           | integer         | 0.1f                           | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `long <#long-mapper>`__             | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | boost           | integer         | 0.1f                           | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `string <#string-mapper>`__         | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `text <#text-mapper>`__             | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | analyzer        | string          | default_analyzer of the schema | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `uuid <#uuid-mapper>`__             | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | validated       | boolean         | false                          | No        |
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

    INSERT INTO blocks(name, shape) VALUES (341, 'MULTIPOLYGON(((-86.693279 32.390691, -86.693185 32.391494, -86.691590 32.391362, -86.691621 32.391095, -86.691302 32.391068, -86.691240 32.391604, -86.691559 32.391630, -86.691527 32.391898, -86.690570 32.391819, -86.690601 32.391551, -86.689644 32.391472, -86.689613 32.391740, -86.689294 32.391714, -86.689200 32.392517, -86.689838 32.392570, -86.689775 32.393105, -86.690094 32.393132, -86.690032 32.393667, -86.690670 32.393720, -86.690701 32.393452, -86.691658 32.393531, -86.691627 32.393799, -86.691946 32.393825, -86.691883 32.394361, -86.691245 32.394308, -86.691214 32.394576, -86.690895 32.394550, -86.690769 32.395621, -86.690450 32.395594, -86.690419 32.395862, -86.689781 32.395810, -86.689812 32.395542, -86.689493 32.395515, -86.689556 32.394980, -86.689875 32.395006, -86.689906 32.394738, -86.690225 32.394765, -86.690257 32.394497, -86.689938 32.394471, -86.690000 32.393935, -86.689362 32.393882, -86.689425 32.393347, -86.688468 32.393268, -86.688405 32.393803, -86.688724 32.393830, -86.688661 32.394365, -86.688342 32.394339, -86.688280 32.394875, -86.687642 32.394822, -86.687673 32.394554, -86.687035 32.394502, -86.687003 32.394769, -86.686684 32.394743, -86.686716 32.394475, -86.685759 32.394396, -86.685790 32.394128, -86.685471 32.394102, -86.685408 32.394638, -86.685089 32.394611, -86.685066 32.394806, -86.685083 32.394550, -86.685110 32.394260, -86.685141 32.393093, -86.685139 32.392820, -86.685130 32.392691, -86.685114 32.392614, -86.685084 32.392505, -86.684783 32.392567, -86.684024 32.392739, -86.683696 32.392823, -86.683529 32.392862, -86.683360 32.392892, -86.683188 32.392909, -86.683016 32.392918, -86.681834 32.392938, -86.681397 32.392952, -86.678836 32.392993, -86.678865 32.392746, -86.679184 32.392772, -86.679216 32.392504, -86.679535 32.392531, -86.679566 32.392263, -86.679247 32.392236, -86.679278 32.391969, -86.679598 32.391995, -86.679629 32.391727, -86.679948 32.391754, -86.680105 32.390415, -86.680743 32.390467, -86.680868 32.389396, -86.681187 32.389423, -86.681219 32.389155, -86.680900 32.389128, -86.680994 32.388325, -86.680356 32.388272, -86.680387 32.388005, -86.679111 32.387899, -86.679080 32.388167, -86.678761 32.388141, -86.678729 32.388408, -86.678410 32.388382, -86.678348 32.388918, -86.678029 32.388891, -86.677872 32.390230, -86.677553 32.390204, -86.677427 32.391275, -86.677108 32.391249, -86.677045 32.391784, -86.676726 32.391758, -86.676695 32.392026, -86.677014 32.392052, -86.676951 32.392588, -86.676632 32.392561, -86.676601 32.392829, -86.677239 32.392882, -86.677270 32.392614, -86.677589 32.392640, -86.677558 32.392908, -86.678604 32.392994, -86.677095 32.393001, -86.676238 32.393016, -86.675186 32.393028, -86.675111 32.392985, -86.675045 32.392932, -86.674999 32.392878, -86.674958 32.392819, -86.675001 32.392620, -86.675062 32.391886, -86.675078 32.391741, -86.675104 32.391596, -86.675141 32.391453, -86.675189 32.391314, -86.675250 32.391177, -86.675324 32.391045, -86.675410 32.390918, -86.675507 32.390797, -86.675726 32.390572, -86.675831 32.390458, -86.675923 32.390335, -86.675992 32.390203, -86.676042 32.390062, -86.676114 32.389775, -86.676157 32.389634, -86.676211 32.389494, -86.676276 32.389359, -86.676351 32.389227, -86.676521 32.388973, -86.676702 32.388723, -86.676984 32.388357, -86.677069 32.388233, -86.677144 32.388104, -86.677202 32.387969, -86.677271 32.387757, -86.677324 32.387618, -86.677396 32.387483, -86.677496 32.387361, -86.677610 32.387247, -86.677800 32.387096, -86.678119 32.386851, -86.678489 32.386550, -86.679064 32.386111, -86.679346 32.385900, -86.679300 32.386292, -86.678981 32.386266, -86.678949 32.386534, -86.678630 32.386508, -86.678536 32.387311, -86.678217 32.387285, -86.678186 32.387552, -86.678824 32.387605, -86.678855 32.387337, -86.679493 32.387390, -86.679524 32.387122, -86.679843 32.387149, -86.679875 32.386881, -86.680194 32.386907, -86.680225 32.386639, -86.681501 32.386745, -86.681407 32.387548, -86.682045 32.387601, -86.682171 32.386530, -86.681852 32.386503, -86.681914 32.385968, -86.682871 32.386047, -86.682840 32.386314, -86.683159 32.386341, -86.683379 32.384466, -86.684017 32.384519, -86.683985 32.384787, -86.683666 32.384760, -86.683604 32.385296, -86.684242 32.385349, -86.684210 32.385616, -86.684529 32.385643, -86.684623 32.384839, -86.684942 32.384866, -86.684880 32.385401, -86.685199 32.385428, -86.685105 32.386231, -86.685424 32.386257, -86.685361 32.386793, -86.684723 32.386740, -86.684691 32.387008, -86.685010 32.387034, -86.684916 32.387838, -86.683959 32.387759, -86.683928 32.388027, -86.684247 32.388053, -86.684184 32.388588, -86.683865 32.388562, -86.683834 32.388830, -86.683196 32.388777, -86.683039 32.390116, -86.683677 32.390169, -86.683708 32.389901, -86.684027 32.389927, -86.684090 32.389392, -86.684409 32.389418, -86.684440 32.389150, -86.685079 32.389203, -86.685141 32.388667, -86.685460 32.388694, -86.685492 32.388426, -86.685811 32.388452, -86.685873 32.387917, -86.686192 32.387943, -86.686224 32.387675, -86.686543 32.387702, -86.686511 32.387969, -86.686830 32.387996, -86.686925 32.387192, -86.687563 32.387245, -86.687625 32.386709, -86.687306 32.386683, -86.687400 32.385880, -86.687081 32.385853, -86.687395 32.383176, -86.687076 32.383149, -86.687107 32.382881, -86.687426 32.382908, -86.687458 32.382640, -86.687777 32.382666, -86.687745 32.382934, -86.688064 32.382960, -86.688033 32.383228, -86.688352 32.383255, -86.688383 32.382987, -86.688702 32.383013, -86.688734 32.382745, -86.688415 32.382719, -86.688446 32.382451, -86.688765 32.382478, -86.688796 32.382210, -86.689115 32.382236, -86.689303 32.380629, -86.688985 32.380603, -86.688953 32.380871, -86.688634 32.380844, -86.688728 32.380041, -86.688409 32.380015, -86.688441 32.379747, -86.688122 32.379721, -86.688153 32.379453, -86.687834 32.379427, -86.687865 32.379159, -86.687546 32.379132, -86.687609 32.378597, -86.687928 32.378623, -86.687959 32.378355, -86.690192 32.378540, -86.690224 32.378272, -86.690862 32.378324, -86.690893 32.378057, -86.691531 32.378109, -86.691562 32.377842, -86.692838 32.377947, -86.692807 32.378215, -86.693126 32.378241, -86.693063 32.378776, -86.693382 32.378803, -86.693319 32.379338, -86.693957 32.379391, -86.693895 32.379927, -86.693576 32.379900, -86.693419 32.381239, -86.693100 32.381213, -86.692975 32.382284, -86.693294 32.382310, -86.693231 32.382846, -86.692912 32.382820, -86.692818 32.383623, -86.692499 32.383597, -86.692405 32.384400, -86.692086 32.384374, -86.692023 32.384909, -86.691385 32.384857, -86.691260 32.385928, -86.690941 32.385902, -86.690878 32.386437, -86.690240 32.386384, -86.690271 32.386117, -86.689952 32.386090, -86.689890 32.386626, -86.690209 32.386652, -86.690177 32.386920, -86.691453 32.387025, -86.691516 32.386490, -86.691835 32.386516, -86.691867 32.386248, -86.692186 32.386275, -86.692217 32.386007, -86.692855 32.386059, -86.692886 32.385792, -86.693524 32.385844, -86.693744 32.383970, -86.694063 32.383996, -86.694094 32.383728, -86.694413 32.383755, -86.694444 32.383487, -86.694763 32.383513, -86.694795 32.383245, -86.695114 32.383272, -86.695176 32.382736, -86.695495 32.382762, -86.695589 32.381959, -86.695908 32.381985, -86.695940 32.381717, -86.695621 32.381691, -86.695652 32.381423, -86.695971 32.381450, -86.696002 32.381182, -86.696321 32.381208, -86.696259 32.381744, -86.696578 32.381770, -86.696108 32.385787, -86.695789 32.385761, -86.695663 32.386832, -86.695025 32.386779, -86.695057 32.386511, -86.694738 32.386485, -86.694644 32.387288, -86.694325 32.387262, -86.694293 32.387530, -86.693974 32.387504, -86.693943 32.387771, -86.693624 32.387745, -86.693593 32.388013, -86.693274 32.387987, -86.693242 32.388254, -86.692923 32.388228, -86.692892 32.388496, -86.692254 32.388443, -86.692222 32.388711, -86.691903 32.388685, -86.691872 32.388952, -86.691234 32.388900, -86.691203 32.389168, -86.690884 32.389141, -86.690852 32.389409, -86.692128 32.389514, -86.692160 32.389247, -86.693117 32.389325, -86.693085 32.389593, -86.692766 32.389567, -86.692704 32.390103, -86.692385 32.390076, -86.692353 32.390344, -86.692991 32.390397, -86.692960 32.390664, -86.693279 32.390691), (-86.676695 32.392026, -86.676376 32.391999, -86.676344 32.392267, -86.676663 32.392293, -86.676695 32.392026), (-86.683044 32.392820, -86.683138 32.392017, -86.682819 32.391991, -86.682788 32.392258, -86.682469 32.392232, -86.682406 32.392768, -86.683044 32.392820), (-86.684095 32.392096, -86.684127 32.391828, -86.684446 32.391855, -86.684540 32.391051, -86.683902 32.390998, -86.683839 32.391534, -86.683520 32.391508, -86.683457 32.392043, -86.684095 32.392096)), ((-86.693917 32.390743, -86.693279 32.390691, -86.693310 32.390423, -86.693629 32.390449, -86.693661 32.390181, -86.693980 32.390208, -86.693917 32.390743)), ((-86.688122 32.379721, -86.688028 32.380524, -86.687709 32.380498, -86.687803 32.379694, -86.688122 32.379721)), ((-86.684623 32.384839, -86.684304 32.384813, -86.684336 32.384545, -86.684655 32.384572, -86.684623 32.384839)), ((-86.684336 32.384545, -86.684017 32.384519, -86.684048 32.384251, -86.684367 32.384278, -86.684336 32.384545)), ((-86.677014 32.392052, -86.677045 32.391784, -86.677364 32.391811, -86.677333 32.392078, -86.677014 32.392052)), ((-86.679278 32.391969, -86.678959 32.391942, -86.678991 32.391674, -86.679310 32.391701, -86.679278 32.391969)), ((-86.682652 32.354935, -86.684246 32.355066, -86.684278 32.354799, -86.685234 32.354878, -86.685203 32.355145, -86.685522 32.355172, -86.685490 32.355439, -86.685171 32.355413, -86.685109 32.355949, -86.685428 32.355975, -86.685396 32.356243, -86.685715 32.356269, -86.685747 32.356001, -86.686065 32.356028, -86.686034 32.356296, -86.686672 32.356348, -86.686641 32.356616, -86.687278 32.356669, -86.687247 32.356936, -86.687566 32.356963, -86.687534 32.357231, -86.687853 32.357257, -86.687822 32.357525, -86.688460 32.357577, -86.688428 32.357845, -86.688747 32.357871, -86.688779 32.357604, -86.689098 32.357630, -86.689066 32.357898, -86.689385 32.357924, -86.689322 32.358460, -86.688366 32.358381, -86.688303 32.358916, -86.689260 32.358995, -86.689228 32.359263, -86.688910 32.359237, -86.688878 32.359505, -86.687603 32.359399, -86.687634 32.359131, -86.686039 32.359000, -86.686071 32.358732, -86.685752 32.358706, -86.685846 32.357902, -86.685527 32.357876, -86.685402 32.358947, -86.685083 32.358921, -86.685051 32.359189, -86.683457 32.359057, -86.683426 32.359325, -86.684063 32.359377, -86.684001 32.359913, -86.684638 32.359966, -86.684670 32.359698, -86.685308 32.359751, -86.685276 32.360018, -86.685595 32.360045, -86.685407 32.361651, -86.685088 32.361625, -86.685057 32.361893, -86.684419 32.361840, -86.684388 32.362108, -86.683750 32.362055, -86.683718 32.362323, -86.683399 32.362297, -86.683368 32.362565, -86.683049 32.362538, -86.683018 32.362806, -86.682380 32.362753, -86.682349 32.363021, -86.682667 32.363048, -86.682636 32.363315, -86.682317 32.363289, -86.682286 32.363557, -86.682605 32.363583, -86.682542 32.364119, -86.682861 32.364145, -86.682830 32.364413, -86.683148 32.364439, -86.683117 32.364707, -86.683755 32.364760, -86.683567 32.366366, -86.682610 32.366287, -86.682484 32.367359, -86.682803 32.367385, -86.682697 32.368289, -86.682381 32.366920, -86.682324 32.366653, -86.682299 32.366411, -86.682295 32.366370, -86.682299 32.366219, -86.682337 32.365924, -86.682340 32.365699, -86.682330 32.365550, -86.682264 32.365339, -86.682188 32.365206, -86.682147 32.365144, -86.681714 32.364593, -86.681565 32.364415, -86.681475 32.364289, -86.681397 32.364158, -86.681338 32.364021, -86.681298 32.363878, -86.681247 32.363608, -86.681191 32.363306, -86.681152 32.363163, -86.681128 32.363093, -86.681098 32.363027, -86.681018 32.362902, -86.680919 32.362786, -86.680860 32.362732, -86.680796 32.362685, -86.680656 32.362603, -86.680436 32.362493, -86.680257 32.362426, -86.680032 32.362370, -86.679868 32.362335, -86.679283 32.362200, -86.679120 32.362153, -86.678958 32.362102, -86.678641 32.361981, -86.678608 32.361966, -86.678616 32.361902, -86.678423 32.361886, -86.678334 32.361848, -86.678187 32.361770, -86.678043 32.361688, -86.677908 32.361597, -86.677795 32.361503, -86.677290 32.361085, -86.677202 32.361024, -86.677160 32.360993, -86.676537 32.360614, -86.676355 32.360498, -86.676215 32.360409, -86.676221 32.360352, -86.676113 32.360343, -86.676083 32.360323, -86.676022 32.360275, -86.675967 32.360221, -86.675882 32.360089, -86.675860 32.360016, -86.675852 32.359940, -86.675866 32.359848, -86.675894 32.359764, -86.675955 32.359671, -86.675794 32.359730, -86.675718 32.359769, -86.675650 32.359813, -86.675630 32.359828, -86.675345 32.359586, -86.675351 32.359541, -86.675435 32.359476, -86.675678 32.359496, -86.675703 32.359277, -86.675764 32.359233, -86.676028 32.359254, -86.676056 32.359017, -86.676093 32.358989, -86.676378 32.359013, -86.676410 32.358745, -86.676729 32.358771, -86.676760 32.358504, -86.677079 32.358530, -86.677110 32.358262, -86.677429 32.358289, -86.677483 32.357833, -86.677486 32.357830, -86.677535 32.357816, -86.677649 32.357804, -86.677704 32.357770, -86.677811 32.357779, -86.677824 32.357670, -86.677935 32.357576, -86.677982 32.357523, -86.678161 32.357538, -86.678192 32.357270, -86.678511 32.357296, -86.678543 32.357029, -86.678862 32.357055, -86.678924 32.356519, -86.679562 32.356572, -86.679594 32.356304, -86.679912 32.356331, -86.679944 32.356063, -86.679625 32.356037, -86.679635 32.355949, -86.679714 32.355896, -86.679808 32.355852, -86.679902 32.355801, -86.679925 32.355791, -86.680294 32.355821, -86.680263 32.356089, -86.680900 32.356142, -86.680932 32.355874, -86.681251 32.355900, -86.681313 32.355365, -86.681632 32.355391, -86.681651 32.355236, -86.681741 32.355246, -86.681790 32.355233, -86.681831 32.355206, -86.681903 32.355143, -86.682620 32.355202, -86.682652 32.354935), (-86.682286 32.363557, -86.681967 32.363530, -86.681936 32.363798, -86.682254 32.363825, -86.682286 32.363557)), ((-86.682683 32.354667, -86.682652 32.354935, -86.682333 32.354908, -86.682359 32.354683, -86.682477 32.354661, -86.682496 32.354651, -86.682683 32.354667)), ((-86.679703 32.385601, -86.679681 32.385783, -86.679519 32.385770, -86.679575 32.385728, -86.679703 32.385601)), ((-86.680095 32.385006, -86.680032 32.385542, -86.679783 32.385521, -86.679790 32.385514, -86.679879 32.385392, -86.679957 32.385258, -86.680098 32.384984, -86.680288 32.384638, -86.680503 32.384269, -86.680414 32.385033, -86.680095 32.385006)), ((-86.680909 32.383551, -86.680827 32.384256, -86.680526 32.384231, -86.680830 32.383708, -86.680909 32.383551)), ((-86.681428 32.381872, -86.681397 32.382140, -86.681158 32.382120, -86.681189 32.381852, -86.681428 32.381872)), ((-86.681491 32.381336, -86.681460 32.381604, -86.681221 32.381584, -86.681252 32.381317, -86.681491 32.381336)), ((-86.681423 32.379168, -86.681304 32.380183, -86.681281 32.380040, -86.681270 32.379894, -86.681272 32.379821, -86.681303 32.379531, -86.681333 32.379389, -86.681351 32.379327, -86.681395 32.379179, -86.681404 32.379153, -86.681451 32.379013, -86.681536 32.378757, -86.681577 32.378611, -86.681616 32.378473, -86.681649 32.378331, -86.682271 32.374547, -86.682318 32.374275, -86.682275 32.374642, -86.685784 32.374931, -86.685753 32.375199, -86.685434 32.375173, -86.685371 32.375708, -86.685052 32.375682, -86.685021 32.375950, -86.684702 32.375923, -86.684670 32.376191, -86.684351 32.376165, -86.684320 32.376433, -86.684001 32.376406, -86.683938 32.376942, -86.683619 32.376916, -86.683588 32.377183, -86.683269 32.377157, -86.683206 32.377693, -86.682887 32.377666, -86.682981 32.376863, -86.682662 32.376837, -86.682694 32.376569, -86.682375 32.376542, -86.682312 32.377078, -86.681993 32.377052, -86.681962 32.377320, -86.682281 32.377346, -86.682249 32.377614, -86.681930 32.377587, -86.681867 32.378123, -86.682505 32.378176, -86.682474 32.378443, -86.682793 32.378470, -86.682730 32.379005, -86.682411 32.378979, -86.682380 32.379247, -86.681423 32.379168)), ((-86.682997 32.368482, -86.682911 32.369215, -86.682737 32.368461, -86.682997 32.368482)), ((-86.683190 32.369580, -86.682939 32.371722, -86.682761 32.371708, -86.682793 32.371519, -86.682852 32.371085, -86.682965 32.370064, -86.682980 32.369771, -86.682976 32.369625, -86.682970 32.369562, -86.683190 32.369580)), ((-86.688479 32.398676, -86.687521 32.398597, -86.687553 32.398329, -86.687234 32.398303, -86.687265 32.398035, -86.686946 32.398009, -86.687009 32.397473, -86.686371 32.397421, -86.686402 32.397153, -86.686083 32.397127, -86.686055 32.397368, -86.685898 32.397243, -86.685732 32.397098, -86.685764 32.397100, -86.685827 32.396565, -86.685189 32.396512, -86.685220 32.396244, -86.684901 32.396218, -86.684840 32.396740, -86.684709 32.396743, -86.684519 32.396727, -86.684518 32.396737, -86.684431 32.396715, -86.684349 32.396668, -86.684596 32.396309, -86.684796 32.395980, -86.684868 32.395847, -86.684931 32.395712, -86.684983 32.395572, -86.685016 32.395426, -86.685034 32.395282, -86.685043 32.395148, -86.685345 32.395173, -86.685251 32.395977, -86.687166 32.396135, -86.687197 32.395867, -86.687835 32.395919, -86.687804 32.396187, -86.688123 32.396214, -86.687997 32.397285, -86.688954 32.397364, -86.688923 32.397631, -86.688604 32.397605, -86.688541 32.398141, -86.688860 32.398167, -86.688829 32.398435, -86.688510 32.398408, -86.688479 32.398676)), ((-86.686946 32.398009, -86.686918 32.398251, -86.686744 32.398055, -86.686690 32.397988, -86.686946 32.398009)), ((-86.686371 32.397421, -86.686348 32.397613, -86.686266 32.397539, -86.686091 32.397398, -86.686371 32.397421)), ((-86.685476 32.396806, -86.685468 32.396873, -86.685419 32.396833, -86.685343 32.396795, -86.685476 32.396806)), ((-86.687085 32.355571, -86.687053 32.355839, -86.685778 32.355734, -86.685841 32.355198, -86.687435 32.355330, -86.687445 32.355248, -86.687490 32.355260, -86.687611 32.355323, -86.687660 32.355337, -86.687716 32.355372, -86.687869 32.355421, -86.687917 32.355449, -86.688046 32.355485, -86.688060 32.355492, -86.688041 32.355650, -86.687085 32.355571)), ((-86.714223 32.385116, -86.713904 32.385090, -86.713935 32.384822, -86.713616 32.384796, -86.713679 32.384260, -86.713360 32.384234, -86.713391 32.383966, -86.713710 32.383992, -86.713741 32.383725, -86.712784 32.383646, -86.712847 32.383110, -86.713166 32.383136, -86.713228 32.382601, -86.713866 32.382653, -86.713929 32.382118, -86.713610 32.382091, -86.713641 32.381824, -86.713003 32.381771, -86.712941 32.382307, -86.712622 32.382281, -86.712653 32.382013, -86.712334 32.381986, -86.712271 32.382522, -86.711952 32.382496, -86.711984 32.382228, -86.711457 32.382185, -86.711454 32.382151, -86.711485 32.381641, -86.711471 32.381097, -86.711596 32.380030, -86.711604 32.380007, -86.711655 32.379762, -86.711687 32.379548, -86.711713 32.379064, -86.711713 32.379026, -86.711752 32.378694, -86.711684 32.378689, -86.711662 32.378577, -86.711633 32.378394, -86.711531 32.378123, -86.711449 32.377958, -86.711293 32.377615, -86.711219 32.377471, -86.711198 32.377398, -86.711171 32.377375, -86.711003 32.377108, -86.710975 32.377073, -86.710983 32.377009, -86.710917 32.377003, -86.710771 32.376880, -86.710706 32.376840, -86.710681 32.376833, -86.710758 32.376179, -86.710439 32.376153, -86.710470 32.375885, -86.709513 32.375806, -86.709544 32.375538, -86.709225 32.375512, -86.709194 32.375780, -86.708875 32.375754, -86.708813 32.376289, -86.708494 32.376263, -86.708525 32.375995, -86.708206 32.375969, -86.708112 32.376772, -86.708431 32.376799, -86.708462 32.376531, -86.709100 32.376583, -86.708975 32.377654, -86.708656 32.377628, -86.708687 32.377360, -86.707730 32.377282, -86.707762 32.377014, -86.707443 32.376987, -86.707380 32.377523, -86.707061 32.377497, -86.706936 32.378568, -86.707255 32.378594, -86.707224 32.378862, -86.707543 32.378888, -86.707511 32.379156, -86.707830 32.379182, -86.707768 32.379718, -86.708087 32.379744, -86.708024 32.380280, -86.708981 32.380359, -86.708950 32.380626, -86.709269 32.380653, -86.709206 32.381188, -86.709525 32.381215, -86.709432 32.382018, -86.710389 32.382097, -86.710357 32.382365, -86.710676 32.382391, -86.710614 32.382926, -86.710295 32.382900, -86.710232 32.383436, -86.710551 32.383462, -86.710426 32.384533, -86.711383 32.384612, -86.711352 32.384880, -86.711671 32.384906, -86.711639 32.385174, -86.712278 32.385226, -86.712215 32.385762, -86.712534 32.385788, -86.712440 32.386591, -86.712759 32.386618, -86.712634 32.387689, -86.712953 32.387715, -86.712828 32.388786, -86.713147 32.388813, -86.713116 32.389080, -86.712797 32.389054, -86.712734 32.389590, -86.712415 32.389563, -86.712478 32.389028, -86.712159 32.389002, -86.712190 32.388734, -86.711871 32.388708, -86.711902 32.388440, -86.711583 32.388414, -86.711614 32.388146, -86.711295 32.388119, -86.711327 32.387852, -86.711646 32.387878, -86.711708 32.387342, -86.711389 32.387316, -86.711452 32.386780, -86.711771 32.386807, -86.711833 32.386271, -86.711195 32.386219, -86.711227 32.385951, -86.710908 32.385925, -86.710939 32.385657, -86.711577 32.385709, -86.711608 32.385442, -86.711289 32.385415, -86.711320 32.385147, -86.711001 32.385121, -86.711033 32.384853, -86.710395 32.384801, -86.710363 32.385069, -86.710044 32.385042, -86.710013 32.385310, -86.710332 32.385336, -86.710238 32.386140, -86.710557 32.386166, -86.710495 32.386702, -86.710814 32.386728, -86.710782 32.386996, -86.711101 32.387022, -86.711008 32.387825, -86.710689 32.387799, -86.710595 32.388603, -86.710276 32.388576, -86.710338 32.388041, -86.710019 32.388014, -86.710082 32.387479, -86.709763 32.387453, -86.709825 32.386917, -86.709506 32.386891, -86.709538 32.386623, -86.709299 32.386603, -86.709281 32.386577, -86.709248 32.386536, -86.709232 32.386485, -86.709313 32.385793, -86.709072 32.385774, -86.709017 32.385643, -86.709010 32.385628, -86.709025 32.385499, -86.708913 32.385490, -86.708837 32.385399, -86.708618 32.385210, -86.708552 32.385168, -86.708531 32.385164, -86.708488 32.385142, -86.708309 32.385094, -86.708260 32.385067, -86.708171 32.385047, -86.708115 32.385015, -86.708130 32.384885, -86.707990 32.384873, -86.707967 32.384833, -86.707967 32.384807, -86.707950 32.384771, -86.707935 32.384718, -86.707902 32.384676, -86.707839 32.384622, -86.707874 32.384323, -86.707555 32.384297, -86.707586 32.384029, -86.707267 32.384003, -86.707298 32.383735, -86.706660 32.383682, -86.706692 32.383415, -86.707330 32.383467, -86.707424 32.382664, -86.707105 32.382637, -86.707136 32.382370, -86.706817 32.382343, -86.706880 32.381808, -86.706561 32.381782, -86.706435 32.382853, -86.706116 32.382826, -86.706085 32.383094, -86.705766 32.383068, -86.705703 32.383604, -86.704746 32.383525, -86.704715 32.383792, -86.704396 32.383766, -86.704365 32.384034, -86.704046 32.384008, -86.704077 32.383740, -86.702482 32.383609, -86.702513 32.383341, -86.702194 32.383314, -86.702226 32.383047, -86.700950 32.382942, -86.700918 32.383209, -86.700599 32.383183, -86.700631 32.382915, -86.700312 32.382889, -86.700280 32.383157, -86.699961 32.383130, -86.699899 32.383666, -86.699580 32.383640, -86.699486 32.384443, -86.698848 32.384391, -86.698942 32.383587, -86.698623 32.383561, -86.698811 32.381954, -86.698492 32.381928, -86.698523 32.381660, -86.698204 32.381634, -86.698267 32.381098, -86.697948 32.381072, -86.697979 32.380804, -86.697660 32.380778, -86.697691 32.380510, -86.697372 32.380484, -86.697404 32.380216, -86.697085 32.380190, -86.697116 32.379922, -86.696159 32.379843, -86.696222 32.379307, -86.695584 32.379255, -86.695615 32.378987, -86.695296 32.378961, -86.695359 32.378425, -86.695040 32.378399, -86.695134 32.377595, -86.694815 32.377569, -86.694846 32.377301, -86.694527 32.377275, -86.694558 32.377007, -86.693920 32.376955, -86.693952 32.376687, -86.693633 32.376660, -86.693664 32.376393, -86.693345 32.376366, -86.693376 32.376099, -86.692739 32.376046, -86.692770 32.375778, -86.692451 32.375752, -86.692482 32.375484, -86.692163 32.375458, -86.692226 32.374922, -86.691588 32.374870, -86.691619 32.374602, -86.691300 32.374575, -86.691332 32.374308, -86.691651 32.374334, -86.691776 32.373263, -86.691457 32.373236, -86.691614 32.371897, -86.691295 32.371871, -86.691201 32.372675, -86.690882 32.372648, -86.690851 32.372916, -86.689256 32.372784, -86.689287 32.372517, -86.688968 32.372490, -86.688937 32.372758, -86.688618 32.372732, -86.688555 32.373267, -86.687917 32.373215, -86.687949 32.372947, -86.687630 32.372921, -86.687661 32.372653, -86.687342 32.372627, -86.687373 32.372359, -86.688011 32.372411, -86.688043 32.372144, -86.688362 32.372170, -86.688393 32.371902, -86.688712 32.371928, -86.688743 32.371661, -86.689700 32.371740, -86.689731 32.371472, -86.690050 32.371498, -86.690082 32.371230, -86.690720 32.371283, -86.690782 32.370747, -86.690463 32.370721, -86.690432 32.370989, -86.690113 32.370963, -86.690176 32.370427, -86.689538 32.370374, -86.689507 32.370642, -86.689188 32.370616, -86.689156 32.370884, -86.688518 32.370831, -86.688487 32.371099, -86.688168 32.371072, -86.688199 32.370805, -86.686924 32.370699, -86.686892 32.370967, -86.686254 32.370914, -86.686348 32.370111, -86.686029 32.370085, -86.686061 32.369817, -86.686380 32.369843, -86.686505 32.368772, -86.685867 32.368719, -86.686150 32.366309, -86.686787 32.366362, -86.686725 32.366898, -86.687044 32.366924, -86.687106 32.366388, -86.687744 32.366441, -86.687776 32.366173, -86.688095 32.366199, -86.688126 32.365932, -86.688764 32.365984, -86.688795 32.365717, -86.689114 32.365743, -86.689083 32.366011, -86.690039 32.366090, -86.690071 32.365822, -86.690709 32.365874, -86.690677 32.366142, -86.690996 32.366169, -86.691028 32.365901, -86.691347 32.365927, -86.691315 32.366195, -86.692910 32.366326, -86.692878 32.366594, -86.692560 32.366568, -86.692528 32.366836, -86.692847 32.366862, -86.692816 32.367130, -86.692497 32.367104, -86.692340 32.368442, -86.692659 32.368469, -86.692628 32.368737, -86.693904 32.368842, -86.693872 32.369110, -86.694191 32.369136, -86.694223 32.368868, -86.694541 32.368894, -86.694510 32.369162, -86.695148 32.369215, -86.695054 32.370018, -86.695373 32.370045, -86.695342 32.370312, -86.694704 32.370260, -86.694672 32.370528, -86.693397 32.370422, -86.693365 32.370690, -86.692727 32.370637, -86.692696 32.370905, -86.694291 32.371037, -86.694259 32.371305, -86.694578 32.371331, -86.694610 32.371063, -86.695248 32.371116, -86.695279 32.370848, -86.695917 32.370901, -86.695823 32.371704, -86.696142 32.371730, -86.696173 32.371462, -86.696492 32.371489, -86.696523 32.371221, -86.696205 32.371195, -86.696267 32.370659, -86.696586 32.370685, -86.696555 32.370953, -86.697831 32.371058, -86.697862 32.370791, -86.696905 32.370712, -86.696936 32.370444, -86.696617 32.370418, -86.696649 32.370150, -86.696330 32.370123, -86.696392 32.369588, -86.696074 32.369562, -86.696261 32.367955, -86.697218 32.368034, -86.697156 32.368569, -86.697475 32.368596, -86.697443 32.368863, -86.699357 32.369021, -86.699326 32.369289, -86.699645 32.369315, -86.699613 32.369583, -86.699932 32.369609, -86.699901 32.369877, -86.700539 32.369930, -86.700508 32.370197, -86.700826 32.370224, -86.700764 32.370759, -86.701721 32.370838, -86.701689 32.371106, -86.702008 32.371132, -86.701977 32.371400, -86.702296 32.371426, -86.702265 32.371694, -86.702584 32.371720, -86.702552 32.371988, -86.702871 32.372015, -86.702840 32.372282, -86.703159 32.372309, -86.703096 32.372844, -86.703415 32.372871, -86.703384 32.373138, -86.703703 32.373165, -86.703672 32.373432, -86.704310 32.373485, -86.704278 32.373753, -86.704597 32.373779, -86.704566 32.374047, -86.704885 32.374073, -86.704822 32.374609, -86.705141 32.374635, -86.705110 32.374903, -86.705429 32.374929, -86.705366 32.375465, -86.706004 32.375517, -86.705973 32.375785, -86.707249 32.375890, -86.707280 32.375622, -86.706961 32.375596, -86.706993 32.375328, -86.707312 32.375354, -86.707343 32.375087, -86.707024 32.375060, -86.707055 32.374793, -86.707374 32.374819, -86.707499 32.373748, -86.707818 32.373774, -86.707850 32.373506, -86.707531 32.373480, -86.707562 32.373212, -86.707881 32.373238, -86.707912 32.372971, -86.707593 32.372944, -86.707624 32.372676, -86.707305 32.372650, -86.707368 32.372115, -86.706411 32.372036, -86.706505 32.371232, -86.706186 32.371206, -86.706217 32.370938, -86.705898 32.370912, -86.705836 32.371448, -86.705198 32.371395, -86.705229 32.371127, -86.704910 32.371101, -86.704942 32.370833, -86.704623 32.370807, -86.704654 32.370539, -86.704335 32.370513, -86.704398 32.369977, -86.704079 32.369951, -86.704047 32.370219, -86.703728 32.370193, -86.703822 32.369389, -86.702865 32.369310, -86.702897 32.369043, -86.702578 32.369016, -86.702546 32.369284, -86.701909 32.369231, -86.701940 32.368964, -86.701621 32.368937, -86.701652 32.368670, -86.701333 32.368643, -86.701365 32.368375, -86.701046 32.368349, -86.701077 32.368081, -86.700120 32.368003, -86.700183 32.367467, -86.699545 32.367414, -86.699514 32.367682, -86.698876 32.367630, -86.698907 32.367362, -86.697950 32.367283, -86.697981 32.367015, -86.697025 32.366936, -86.697087 32.366401, -86.696449 32.366348, -86.696418 32.366616, -86.695780 32.366563, -86.695812 32.366295, -86.695174 32.366243, -86.695205 32.365975, -86.694886 32.365949, -86.694917 32.365681, -86.694598 32.365655, -86.694630 32.365387, -86.694311 32.365360, -86.694342 32.365093, -86.694023 32.365066, -86.693992 32.365334, -86.693673 32.365308, -86.693704 32.365040, -86.693385 32.365014, -86.693417 32.364746, -86.692779 32.364693, -86.692842 32.364158, -86.692523 32.364131, -86.692554 32.363864, -86.692235 32.363837, -86.692266 32.363570, -86.691629 32.363517, -86.691691 32.362981, -86.691372 32.362955, -86.691435 32.362419, -86.691116 32.362393, -86.691179 32.361857, -86.690222 32.361779, -86.690191 32.362046, -86.689553 32.361994, -86.689490 32.362529, -86.689171 32.362503, -86.689140 32.362771, -86.690097 32.362850, -86.690034 32.363385, -86.690353 32.363412, -86.690322 32.363679, -86.690003 32.363653, -86.689971 32.363921, -86.689652 32.363895, -86.689715 32.363359, -86.689396 32.363333, -86.689365 32.363600, -86.689046 32.363574, -86.689109 32.363039, -86.688790 32.363012, -86.688821 32.362744, -86.687545 32.362639, -86.687577 32.362371, -86.686939 32.362319, -86.686970 32.362051, -86.686651 32.362025, -86.686683 32.361757, -86.686364 32.361730, -86.686395 32.361463, -86.686076 32.361436, -86.686170 32.360633, -86.686489 32.360659, -86.686552 32.360124, -86.686871 32.360150, -86.686902 32.359882, -86.688178 32.359988, -86.688209 32.359720, -86.688528 32.359746, -86.688497 32.360014, -86.689772 32.360119, -86.689804 32.359851, -86.690122 32.359878, -86.690217 32.359074, -86.691173 32.359153, -86.691205 32.358885, -86.691523 32.358912, -86.691461 32.359447, -86.691780 32.359474, -86.691811 32.359206, -86.692130 32.359232, -86.692099 32.359500, -86.692418 32.359526, -86.692449 32.359258, -86.692768 32.359285, -86.692924 32.357946, -86.693881 32.358025, -86.693850 32.358292, -86.694169 32.358319, -86.694137 32.358587, -86.695413 32.358692, -86.695444 32.358424, -86.695763 32.358450, -86.695638 32.359522, -86.696276 32.359574, -86.696244 32.359842, -86.698477 32.360026, -86.698445 32.360294, -86.699083 32.360346, -86.699052 32.360614, -86.699371 32.360641, -86.699340 32.360908, -86.698064 32.360803, -86.698095 32.360535, -86.695225 32.360299, -86.695162 32.360834, -86.695800 32.360887, -86.695738 32.361422, -86.694462 32.361317, -86.694399 32.361853, -86.695037 32.361905, -86.695006 32.362173, -86.695644 32.362226, -86.695612 32.362494, -86.696888 32.362599, -86.696919 32.362331, -86.697876 32.362410, -86.697782 32.363213, -86.698101 32.363240, -86.698070 32.363507, -86.698389 32.363534, -86.698326 32.364069, -86.698645 32.364096, -86.698582 32.364631, -86.698901 32.364657, -86.698870 32.364925, -86.699827 32.365004, -86.700140 32.362326, -86.702053 32.362484, -86.702022 32.362752, -86.702660 32.362804, -86.702629 32.363072, -86.703266 32.363125, -86.703235 32.363392, -86.704511 32.363498, -86.704479 32.363765, -86.704798 32.363792, -86.704736 32.364327, -86.705374 32.364380, -86.705342 32.364648, -86.705661 32.364674, -86.705630 32.364942, -86.706906 32.365047, -86.706874 32.365315, -86.708469 32.365446, -86.708438 32.365714, -86.708757 32.365740, -86.708726 32.366008, -86.709363 32.366060, -86.709332 32.366328, -86.709970 32.366381, -86.709939 32.366648, -86.710258 32.366675, -86.710226 32.366943, -86.711502 32.367048, -86.711471 32.367315, -86.712747 32.367420, -86.712715 32.367688, -86.713353 32.367741, -86.713322 32.368008, -86.713641 32.368035, -86.713578 32.368570, -86.713897 32.368597, -86.713866 32.368864, -86.714185 32.368891, -86.714154 32.369158, -86.714473 32.369185, -86.714441 32.369452, -86.714760 32.369479, -86.714729 32.369747, -86.715048 32.369773, -86.715017 32.370041, -86.715336 32.370067, -86.715305 32.370335, -86.715623 32.370361, -86.715592 32.370629, -86.715911 32.370655, -86.715880 32.370923, -86.716199 32.370949, -86.716168 32.371217, -86.716487 32.371243, -86.716424 32.371779, -86.716743 32.371805, -86.716712 32.372073, -86.717031 32.372099, -86.716999 32.372367, -86.717318 32.372393, -86.717287 32.372661, -86.717606 32.372687, -86.717575 32.372955, -86.717894 32.372981, -86.717831 32.373517, -86.718150 32.373543, -86.718119 32.373811, -86.718438 32.373837, -86.718188 32.375979, -86.717869 32.375953, -86.717900 32.375685, -86.717581 32.375659, -86.717550 32.375927, -86.717231 32.375901, -86.717419 32.374294, -86.716781 32.374241, -86.716343 32.377990, -86.716662 32.378017, -86.716631 32.378285, -86.716312 32.378258, -86.716281 32.378526, -86.714686 32.378395, -86.714654 32.378663, -86.714335 32.378636, -86.714304 32.378904, -86.713666 32.378852, -86.713604 32.379387, -86.713285 32.379361, -86.713316 32.379093, -86.712997 32.379067, -86.712934 32.379603, -86.712615 32.379576, -86.712459 32.380915, -86.712778 32.380942, -86.712715 32.381477, -86.713034 32.381503, -86.713066 32.381236, -86.713385 32.381262, -86.713416 32.380994, -86.713735 32.381020, -86.713704 32.381288, -86.714023 32.381314, -86.713960 32.381850, -86.714279 32.381876, -86.714217 32.382412, -86.714536 32.382438, -86.714504 32.382706, -86.714823 32.382732, -86.714761 32.383268, -86.714442 32.383241, -86.714379 32.383777, -86.714698 32.383803, -86.714667 32.384071, -86.714348 32.384045, -86.714223 32.385116), (-86.713166 32.383136, -86.713134 32.383404, -86.713453 32.383430, -86.713485 32.383163, -86.713166 32.383136), (-86.712334 32.381986, -86.712365 32.381719, -86.712046 32.381692, -86.712015 32.381960, -86.712334 32.381986), (-86.709544 32.375538, -86.709863 32.375565, -86.709895 32.375297, -86.709576 32.375271, -86.709544 32.375538), (-86.708875 32.375754, -86.708906 32.375486, -86.708587 32.375460, -86.708556 32.375727, -86.708875 32.375754), (-86.707443 32.376987, -86.707505 32.376452, -86.706548 32.376373, -86.706517 32.376641, -86.707155 32.376693, -86.707124 32.376961, -86.707443 32.376987), (-86.705766 32.383068, -86.705829 32.382532, -86.704872 32.382454, -86.704840 32.382721, -86.705478 32.382774, -86.705447 32.383042, -86.705766 32.383068), (-86.704872 32.382454, -86.704903 32.382186, -86.704584 32.382159, -86.704553 32.382427, -86.704872 32.382454), (-86.704903 32.382186, -86.705541 32.382238, -86.705572 32.381970, -86.704934 32.381918, -86.704903 32.382186), (-86.705447 32.383042, -86.705128 32.383015, -86.705097 32.383283, -86.705416 32.383309, -86.705447 32.383042), (-86.695823 32.371704, -86.695185 32.371651, -86.695154 32.371919, -86.695792 32.371972, -86.695823 32.371704), (-86.695792 32.371972, -86.695760 32.372240, -86.696079 32.372266, -86.696111 32.371998, -86.695792 32.371972), (-86.697862 32.370791, -86.698500 32.370843, -86.698531 32.370575, -86.697893 32.370523, -86.697862 32.370791), (-86.707818 32.373774, -86.707787 32.374042, -86.708106 32.374068, -86.708137 32.373800, -86.707818 32.373774), (-86.708106 32.374068, -86.708075 32.374336, -86.708394 32.374362, -86.708425 32.374094, -86.708106 32.374068), (-86.708075 32.374336, -86.707756 32.374310, -86.707724 32.374577, -86.708043 32.374604, -86.708075 32.374336), (-86.708043 32.374604, -86.708012 32.374871, -86.708331 32.374898, -86.708362 32.374630, -86.708043 32.374604), (-86.695812 32.366295, -86.696130 32.366322, -86.696162 32.366054, -86.695843 32.366028, -86.695812 32.366295), (-86.690222 32.361779, -86.690253 32.361511, -86.688978 32.361405, -86.688946 32.361673, -86.690222 32.361779), (-86.689171 32.362503, -86.689203 32.362235, -86.688884 32.362209, -86.688852 32.362477, -86.689171 32.362503), (-86.690122 32.359878, -86.690091 32.360145, -86.690729 32.360198, -86.690760 32.359930, -86.690122 32.359878), (-86.695225 32.360299, -86.695256 32.360031, -86.694937 32.360005, -86.694906 32.360272, -86.695225 32.360299), (-86.687368 32.369654, -86.687431 32.369119, -86.688388 32.369198, -86.688419 32.368930, -86.687143 32.368825, -86.687112 32.369093, -86.686793 32.369066, -86.686730 32.369602, -86.687368 32.369654), (-86.696917 32.376120, -86.696948 32.375852, -86.695353 32.375721, -86.695322 32.375989, -86.696917 32.376120), (-86.697042 32.375049, -86.697073 32.374781, -86.696754 32.374755, -86.696786 32.374487, -86.696467 32.374461, -86.696498 32.374193, -86.696179 32.374167, -86.696085 32.374970, -86.697042 32.375049), (-86.698893 32.375742, -86.698924 32.375475, -86.699243 32.375501, -86.699306 32.374965, -86.698668 32.374913, -86.698637 32.375180, -86.697999 32.375128, -86.697936 32.375663, -86.698893 32.375742), (-86.698668 32.374913, -86.698699 32.374645, -86.698380 32.374619, -86.698349 32.374886, -86.698668 32.374913), (-86.698380 32.374619, -86.698443 32.374083, -86.698124 32.374057, -86.698187 32.373521, -86.697549 32.373468, -86.697580 32.373201, -86.697261 32.373174, -86.697292 32.372907, -86.696973 32.372880, -86.697036 32.372345, -86.696398 32.372292, -86.696336 32.372828, -86.696654 32.372854, -86.696623 32.373122, -86.696942 32.373148, -86.696879 32.373684, -86.697198 32.373710, -86.697167 32.373978, -86.697486 32.374004, -86.697455 32.374272, -86.697774 32.374298, -86.697742 32.374566, -86.698380 32.374619), (-86.702727 32.378762, -86.702758 32.378494, -86.705948 32.378757, -86.706010 32.378221, -86.705691 32.378195, -86.705660 32.378463, -86.705341 32.378437, -86.705372 32.378169, -86.704734 32.378116, -86.704703 32.378384, -86.704384 32.378358, -86.704415 32.378090, -86.703777 32.378037, -86.703746 32.378305, -86.702470 32.378200, -86.702408 32.378736, -86.702727 32.378762)), ((-86.714830 32.385436, -86.714192 32.385384, -86.714223 32.385116, -86.714861 32.385168, -86.714830 32.385436)), ((-86.712847 32.383110, -86.712528 32.383084, -86.712559 32.382816, -86.712240 32.382790, -86.712271 32.382522, -86.712909 32.382575, -86.712847 32.383110)), ((-86.712240 32.382790, -86.712209 32.383058, -86.711427 32.382993, -86.711463 32.382889, -86.711519 32.382831, -86.711567 32.382772, -86.711584 32.382736, -86.711602 32.382737, -86.711608 32.382684, -86.711615 32.382669, -86.711612 32.382651, -86.711633 32.382470, -86.711952 32.382496, -86.711921 32.382764, -86.712240 32.382790)), ((-86.711452 32.386780, -86.710814 32.386728, -86.710845 32.386460, -86.711483 32.386513, -86.711452 32.386780)), ((-86.710332 32.385336, -86.710363 32.385069, -86.711001 32.385121, -86.710939 32.385657, -86.710620 32.385631, -86.710651 32.385363, -86.710332 32.385336)), ((-86.706692 32.383415, -86.706054 32.383362, -86.706085 32.383094, -86.706404 32.383120, -86.706435 32.382853, -86.706754 32.382879, -86.706692 32.383415)), ((-86.691457 32.373236, -86.691394 32.373772, -86.691075 32.373746, -86.691044 32.374014, -86.690725 32.373987, -86.690757 32.373719, -86.689481 32.373614, -86.689512 32.373346, -86.690150 32.373399, -86.690181 32.373131, -86.691457 32.373236)), ((-86.707562 32.373212, -86.706924 32.373160, -86.706987 32.372624, -86.707305 32.372650, -86.707274 32.372918, -86.707593 32.372944, -86.707562 32.373212)), ((-86.706987 32.372624, -86.706668 32.372598, -86.706699 32.372330, -86.707018 32.372356, -86.706987 32.372624)), ((-86.694886 32.365949, -86.694823 32.366484, -86.694504 32.366458, -86.694567 32.365922, -86.694886 32.365949)), ((-86.694567 32.365922, -86.693929 32.365870, -86.693961 32.365602, -86.694598 32.365655, -86.694567 32.365922)), ((-86.693673 32.365308, -86.693642 32.365576, -86.693323 32.365549, -86.693354 32.365282, -86.693673 32.365308)), ((-86.693354 32.365282, -86.693035 32.365255, -86.693067 32.364987, -86.693385 32.365014, -86.693354 32.365282)), ((-86.690191 32.362046, -86.690828 32.362099, -86.690766 32.362635, -86.689809 32.362556, -86.689840 32.362288, -86.690159 32.362314, -86.690191 32.362046)), ((-86.712941 32.382307, -86.713260 32.382333, -86.713228 32.382601, -86.712909 32.382575, -86.712941 32.382307)), ((-86.713610 32.382091, -86.713579 32.382359, -86.713260 32.382333, -86.713291 32.382065, -86.713610 32.382091)), ((-86.686658 32.397715, -86.686634 32.397920, -86.686549 32.397816, -86.686443 32.397700, -86.686441 32.397697, -86.686658 32.397715)), ((-86.688353 32.399747, -86.688672 32.399774, -86.688641 32.400036, -86.688580 32.399972, -86.688353 32.399749, -86.688353 32.399747)), ((-86.688353 32.399747, -86.688351 32.399747, -86.688131 32.399533, -86.688064 32.399464, -86.688065 32.399453, -86.688385 32.399480, -86.688353 32.399747)), ((-86.688065 32.399453, -86.688053 32.399452, -86.687915 32.399313, -86.687809 32.399199, -86.687778 32.399161, -86.687778 32.399159, -86.688097 32.399185, -86.688065 32.399453)), ((-86.689823 32.400950, -86.689768 32.401421, -86.689644 32.401274, -86.689443 32.401048, -86.689349 32.400933, -86.689166 32.400691, -86.688986 32.400442, -86.688892 32.400319, -86.688793 32.400199, -86.688647 32.400042, -86.688960 32.400068, -86.688929 32.400335, -86.689248 32.400362, -86.689216 32.400630, -86.689535 32.400656, -86.689504 32.400924, -86.689823 32.400950)), ((-86.691617 32.358108, -86.690661 32.358029, -86.690692 32.357762, -86.690054 32.357709, -86.690023 32.357977, -86.689704 32.357950, -86.689735 32.357683, -86.689417 32.357656, -86.689448 32.357388, -86.689129 32.357362, -86.689192 32.356827, -86.689829 32.356879, -86.689798 32.357147, -86.690436 32.357200, -86.690467 32.356932, -86.691105 32.356984, -86.691042 32.357520, -86.691361 32.357546, -86.691330 32.357814, -86.691649 32.357841, -86.691617 32.358108)), ((-86.690661 32.358029, -86.690629 32.358297, -86.690513 32.358288, -86.690372 32.358213, -86.690318 32.358205, -86.690342 32.358003, -86.690661 32.358029)), ((-86.689913 32.401498, -86.689865 32.401538, -86.689827 32.401491, -86.689913 32.401498)), ((-86.708976 32.363865, -86.709295 32.363892, -86.709263 32.364159, -86.708944 32.364133, -86.708976 32.363865)), ((-86.708976 32.363865, -86.708403 32.363818, -86.708349 32.363780, -86.708342 32.363775, -86.708369 32.363545, -86.707838 32.363501, -86.707798 32.363480, -86.707738 32.363437, -86.707763 32.363225, -86.707444 32.363198, -86.707475 32.362931, -86.708113 32.362983, -86.708082 32.363251, -86.708719 32.363304, -86.708688 32.363571, -86.709007 32.363598, -86.708976 32.363865)), ((-86.707475 32.362931, -86.706837 32.362878, -86.706868 32.362610, -86.706231 32.362558, -86.706262 32.362290, -86.705305 32.362211, -86.705274 32.362479, -86.704636 32.362426, -86.704667 32.362159, -86.704029 32.362106, -86.704061 32.361838, -86.703104 32.361759, -86.703135 32.361492, -86.702816 32.361465, -86.702785 32.361733, -86.702466 32.361707, -86.702497 32.361439, -86.701860 32.361386, -86.701828 32.361654, -86.700872 32.361575, -86.700903 32.361308, -86.700265 32.361255, -86.700422 32.359916, -86.701697 32.360021, -86.701666 32.360289, -86.702304 32.360342, -86.702272 32.360609, -86.702910 32.360662, -86.702942 32.360394, -86.703579 32.360447, -86.703548 32.360714, -86.704186 32.360767, -86.704155 32.361035, -86.704792 32.361087, -86.704761 32.361355, -86.705399 32.361408, -86.705368 32.361676, -86.706006 32.361728, -86.705974 32.361996, -86.706612 32.362048, -86.706581 32.362316, -86.707219 32.362369, -86.707187 32.362637, -86.707506 32.362663, -86.707475 32.362931)), ((-86.715624 32.384150, -86.715499 32.385221, -86.715180 32.385195, -86.715211 32.384927, -86.714892 32.384901, -86.714923 32.384633, -86.715242 32.384659, -86.715274 32.384391, -86.714955 32.384365, -86.715049 32.383562, -86.715368 32.383588, -86.715336 32.383856, -86.715655 32.383882, -86.715687 32.383614, -86.716325 32.383667, -86.716293 32.383934, -86.715974 32.383908, -86.715943 32.384176, -86.715624 32.384150)))');

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

    INSERT INTO blocks(name, shape) VALUES (341, 'MULTIPOLYGON(((-86.693279 32.390691, -86.693185 32.391494, -86.691590 32.391362, -86.691621 32.391095, -86.691302 32.391068, -86.691240 32.391604, -86.691559 32.391630, -86.691527 32.391898, -86.690570 32.391819, -86.690601 32.391551, -86.689644 32.391472, -86.689613 32.391740, -86.689294 32.391714, -86.689200 32.392517, -86.689838 32.392570, -86.689775 32.393105, -86.690094 32.393132, -86.690032 32.393667, -86.690670 32.393720, -86.690701 32.393452, -86.691658 32.393531, -86.691627 32.393799, -86.691946 32.393825, -86.691883 32.394361, -86.691245 32.394308, -86.691214 32.394576, -86.690895 32.394550, -86.690769 32.395621, -86.690450 32.395594, -86.690419 32.395862, -86.689781 32.395810, -86.689812 32.395542, -86.689493 32.395515, -86.689556 32.394980, -86.689875 32.395006, -86.689906 32.394738, -86.690225 32.394765, -86.690257 32.394497, -86.689938 32.394471, -86.690000 32.393935, -86.689362 32.393882, -86.689425 32.393347, -86.688468 32.393268, -86.688405 32.393803, -86.688724 32.393830, -86.688661 32.394365, -86.688342 32.394339, -86.688280 32.394875, -86.687642 32.394822, -86.687673 32.394554, -86.687035 32.394502, -86.687003 32.394769, -86.686684 32.394743, -86.686716 32.394475, -86.685759 32.394396, -86.685790 32.394128, -86.685471 32.394102, -86.685408 32.394638, -86.685089 32.394611, -86.685066 32.394806, -86.685083 32.394550, -86.685110 32.394260, -86.685141 32.393093, -86.685139 32.392820, -86.685130 32.392691, -86.685114 32.392614, -86.685084 32.392505, -86.684783 32.392567, -86.684024 32.392739, -86.683696 32.392823, -86.683529 32.392862, -86.683360 32.392892, -86.683188 32.392909, -86.683016 32.392918, -86.681834 32.392938, -86.681397 32.392952, -86.678836 32.392993, -86.678865 32.392746, -86.679184 32.392772, -86.679216 32.392504, -86.679535 32.392531, -86.679566 32.392263, -86.679247 32.392236, -86.679278 32.391969, -86.679598 32.391995, -86.679629 32.391727, -86.679948 32.391754, -86.680105 32.390415, -86.680743 32.390467, -86.680868 32.389396, -86.681187 32.389423, -86.681219 32.389155, -86.680900 32.389128, -86.680994 32.388325, -86.680356 32.388272, -86.680387 32.388005, -86.679111 32.387899, -86.679080 32.388167, -86.678761 32.388141, -86.678729 32.388408, -86.678410 32.388382, -86.678348 32.388918, -86.678029 32.388891, -86.677872 32.390230, -86.677553 32.390204, -86.677427 32.391275, -86.677108 32.391249, -86.677045 32.391784, -86.676726 32.391758, -86.676695 32.392026, -86.677014 32.392052, -86.676951 32.392588, -86.676632 32.392561, -86.676601 32.392829, -86.677239 32.392882, -86.677270 32.392614, -86.677589 32.392640, -86.677558 32.392908, -86.678604 32.392994, -86.677095 32.393001, -86.676238 32.393016, -86.675186 32.393028, -86.675111 32.392985, -86.675045 32.392932, -86.674999 32.392878, -86.674958 32.392819, -86.675001 32.392620, -86.675062 32.391886, -86.675078 32.391741, -86.675104 32.391596, -86.675141 32.391453, -86.675189 32.391314, -86.675250 32.391177, -86.675324 32.391045, -86.675410 32.390918, -86.675507 32.390797, -86.675726 32.390572, -86.675831 32.390458, -86.675923 32.390335, -86.675992 32.390203, -86.676042 32.390062, -86.676114 32.389775, -86.676157 32.389634, -86.676211 32.389494, -86.676276 32.389359, -86.676351 32.389227, -86.676521 32.388973, -86.676702 32.388723, -86.676984 32.388357, -86.677069 32.388233, -86.677144 32.388104, -86.677202 32.387969, -86.677271 32.387757, -86.677324 32.387618, -86.677396 32.387483, -86.677496 32.387361, -86.677610 32.387247, -86.677800 32.387096, -86.678119 32.386851, -86.678489 32.386550, -86.679064 32.386111, -86.679346 32.385900, -86.679300 32.386292, -86.678981 32.386266, -86.678949 32.386534, -86.678630 32.386508, -86.678536 32.387311, -86.678217 32.387285, -86.678186 32.387552, -86.678824 32.387605, -86.678855 32.387337, -86.679493 32.387390, -86.679524 32.387122, -86.679843 32.387149, -86.679875 32.386881, -86.680194 32.386907, -86.680225 32.386639, -86.681501 32.386745, -86.681407 32.387548, -86.682045 32.387601, -86.682171 32.386530, -86.681852 32.386503, -86.681914 32.385968, -86.682871 32.386047, -86.682840 32.386314, -86.683159 32.386341, -86.683379 32.384466, -86.684017 32.384519, -86.683985 32.384787, -86.683666 32.384760, -86.683604 32.385296, -86.684242 32.385349, -86.684210 32.385616, -86.684529 32.385643, -86.684623 32.384839, -86.684942 32.384866, -86.684880 32.385401, -86.685199 32.385428, -86.685105 32.386231, -86.685424 32.386257, -86.685361 32.386793, -86.684723 32.386740, -86.684691 32.387008, -86.685010 32.387034, -86.684916 32.387838, -86.683959 32.387759, -86.683928 32.388027, -86.684247 32.388053, -86.684184 32.388588, -86.683865 32.388562, -86.683834 32.388830, -86.683196 32.388777, -86.683039 32.390116, -86.683677 32.390169, -86.683708 32.389901, -86.684027 32.389927, -86.684090 32.389392, -86.684409 32.389418, -86.684440 32.389150, -86.685079 32.389203, -86.685141 32.388667, -86.685460 32.388694, -86.685492 32.388426, -86.685811 32.388452, -86.685873 32.387917, -86.686192 32.387943, -86.686224 32.387675, -86.686543 32.387702, -86.686511 32.387969, -86.686830 32.387996, -86.686925 32.387192, -86.687563 32.387245, -86.687625 32.386709, -86.687306 32.386683, -86.687400 32.385880, -86.687081 32.385853, -86.687395 32.383176, -86.687076 32.383149, -86.687107 32.382881, -86.687426 32.382908, -86.687458 32.382640, -86.687777 32.382666, -86.687745 32.382934, -86.688064 32.382960, -86.688033 32.383228, -86.688352 32.383255, -86.688383 32.382987, -86.688702 32.383013, -86.688734 32.382745, -86.688415 32.382719, -86.688446 32.382451, -86.688765 32.382478, -86.688796 32.382210, -86.689115 32.382236, -86.689303 32.380629, -86.688985 32.380603, -86.688953 32.380871, -86.688634 32.380844, -86.688728 32.380041, -86.688409 32.380015, -86.688441 32.379747, -86.688122 32.379721, -86.688153 32.379453, -86.687834 32.379427, -86.687865 32.379159, -86.687546 32.379132, -86.687609 32.378597, -86.687928 32.378623, -86.687959 32.378355, -86.690192 32.378540, -86.690224 32.378272, -86.690862 32.378324, -86.690893 32.378057, -86.691531 32.378109, -86.691562 32.377842, -86.692838 32.377947, -86.692807 32.378215, -86.693126 32.378241, -86.693063 32.378776, -86.693382 32.378803, -86.693319 32.379338, -86.693957 32.379391, -86.693895 32.379927, -86.693576 32.379900, -86.693419 32.381239, -86.693100 32.381213, -86.692975 32.382284, -86.693294 32.382310, -86.693231 32.382846, -86.692912 32.382820, -86.692818 32.383623, -86.692499 32.383597, -86.692405 32.384400, -86.692086 32.384374, -86.692023 32.384909, -86.691385 32.384857, -86.691260 32.385928, -86.690941 32.385902, -86.690878 32.386437, -86.690240 32.386384, -86.690271 32.386117, -86.689952 32.386090, -86.689890 32.386626, -86.690209 32.386652, -86.690177 32.386920, -86.691453 32.387025, -86.691516 32.386490, -86.691835 32.386516, -86.691867 32.386248, -86.692186 32.386275, -86.692217 32.386007, -86.692855 32.386059, -86.692886 32.385792, -86.693524 32.385844, -86.693744 32.383970, -86.694063 32.383996, -86.694094 32.383728, -86.694413 32.383755, -86.694444 32.383487, -86.694763 32.383513, -86.694795 32.383245, -86.695114 32.383272, -86.695176 32.382736, -86.695495 32.382762, -86.695589 32.381959, -86.695908 32.381985, -86.695940 32.381717, -86.695621 32.381691, -86.695652 32.381423, -86.695971 32.381450, -86.696002 32.381182, -86.696321 32.381208, -86.696259 32.381744, -86.696578 32.381770, -86.696108 32.385787, -86.695789 32.385761, -86.695663 32.386832, -86.695025 32.386779, -86.695057 32.386511, -86.694738 32.386485, -86.694644 32.387288, -86.694325 32.387262, -86.694293 32.387530, -86.693974 32.387504, -86.693943 32.387771, -86.693624 32.387745, -86.693593 32.388013, -86.693274 32.387987, -86.693242 32.388254, -86.692923 32.388228, -86.692892 32.388496, -86.692254 32.388443, -86.692222 32.388711, -86.691903 32.388685, -86.691872 32.388952, -86.691234 32.388900, -86.691203 32.389168, -86.690884 32.389141, -86.690852 32.389409, -86.692128 32.389514, -86.692160 32.389247, -86.693117 32.389325, -86.693085 32.389593, -86.692766 32.389567, -86.692704 32.390103, -86.692385 32.390076, -86.692353 32.390344, -86.692991 32.390397, -86.692960 32.390664, -86.693279 32.390691), (-86.676695 32.392026, -86.676376 32.391999, -86.676344 32.392267, -86.676663 32.392293, -86.676695 32.392026), (-86.683044 32.392820, -86.683138 32.392017, -86.682819 32.391991, -86.682788 32.392258, -86.682469 32.392232, -86.682406 32.392768, -86.683044 32.392820), (-86.684095 32.392096, -86.684127 32.391828, -86.684446 32.391855, -86.684540 32.391051, -86.683902 32.390998, -86.683839 32.391534, -86.683520 32.391508, -86.683457 32.392043, -86.684095 32.392096)), ((-86.693917 32.390743, -86.693279 32.390691, -86.693310 32.390423, -86.693629 32.390449, -86.693661 32.390181, -86.693980 32.390208, -86.693917 32.390743)), ((-86.688122 32.379721, -86.688028 32.380524, -86.687709 32.380498, -86.687803 32.379694, -86.688122 32.379721)), ((-86.684623 32.384839, -86.684304 32.384813, -86.684336 32.384545, -86.684655 32.384572, -86.684623 32.384839)), ((-86.684336 32.384545, -86.684017 32.384519, -86.684048 32.384251, -86.684367 32.384278, -86.684336 32.384545)), ((-86.677014 32.392052, -86.677045 32.391784, -86.677364 32.391811, -86.677333 32.392078, -86.677014 32.392052)), ((-86.679278 32.391969, -86.678959 32.391942, -86.678991 32.391674, -86.679310 32.391701, -86.679278 32.391969)), ((-86.682652 32.354935, -86.684246 32.355066, -86.684278 32.354799, -86.685234 32.354878, -86.685203 32.355145, -86.685522 32.355172, -86.685490 32.355439, -86.685171 32.355413, -86.685109 32.355949, -86.685428 32.355975, -86.685396 32.356243, -86.685715 32.356269, -86.685747 32.356001, -86.686065 32.356028, -86.686034 32.356296, -86.686672 32.356348, -86.686641 32.356616, -86.687278 32.356669, -86.687247 32.356936, -86.687566 32.356963, -86.687534 32.357231, -86.687853 32.357257, -86.687822 32.357525, -86.688460 32.357577, -86.688428 32.357845, -86.688747 32.357871, -86.688779 32.357604, -86.689098 32.357630, -86.689066 32.357898, -86.689385 32.357924, -86.689322 32.358460, -86.688366 32.358381, -86.688303 32.358916, -86.689260 32.358995, -86.689228 32.359263, -86.688910 32.359237, -86.688878 32.359505, -86.687603 32.359399, -86.687634 32.359131, -86.686039 32.359000, -86.686071 32.358732, -86.685752 32.358706, -86.685846 32.357902, -86.685527 32.357876, -86.685402 32.358947, -86.685083 32.358921, -86.685051 32.359189, -86.683457 32.359057, -86.683426 32.359325, -86.684063 32.359377, -86.684001 32.359913, -86.684638 32.359966, -86.684670 32.359698, -86.685308 32.359751, -86.685276 32.360018, -86.685595 32.360045, -86.685407 32.361651, -86.685088 32.361625, -86.685057 32.361893, -86.684419 32.361840, -86.684388 32.362108, -86.683750 32.362055, -86.683718 32.362323, -86.683399 32.362297, -86.683368 32.362565, -86.683049 32.362538, -86.683018 32.362806, -86.682380 32.362753, -86.682349 32.363021, -86.682667 32.363048, -86.682636 32.363315, -86.682317 32.363289, -86.682286 32.363557, -86.682605 32.363583, -86.682542 32.364119, -86.682861 32.364145, -86.682830 32.364413, -86.683148 32.364439, -86.683117 32.364707, -86.683755 32.364760, -86.683567 32.366366, -86.682610 32.366287, -86.682484 32.367359, -86.682803 32.367385, -86.682697 32.368289, -86.682381 32.366920, -86.682324 32.366653, -86.682299 32.366411, -86.682295 32.366370, -86.682299 32.366219, -86.682337 32.365924, -86.682340 32.365699, -86.682330 32.365550, -86.682264 32.365339, -86.682188 32.365206, -86.682147 32.365144, -86.681714 32.364593, -86.681565 32.364415, -86.681475 32.364289, -86.681397 32.364158, -86.681338 32.364021, -86.681298 32.363878, -86.681247 32.363608, -86.681191 32.363306, -86.681152 32.363163, -86.681128 32.363093, -86.681098 32.363027, -86.681018 32.362902, -86.680919 32.362786, -86.680860 32.362732, -86.680796 32.362685, -86.680656 32.362603, -86.680436 32.362493, -86.680257 32.362426, -86.680032 32.362370, -86.679868 32.362335, -86.679283 32.362200, -86.679120 32.362153, -86.678958 32.362102, -86.678641 32.361981, -86.678608 32.361966, -86.678616 32.361902, -86.678423 32.361886, -86.678334 32.361848, -86.678187 32.361770, -86.678043 32.361688, -86.677908 32.361597, -86.677795 32.361503, -86.677290 32.361085, -86.677202 32.361024, -86.677160 32.360993, -86.676537 32.360614, -86.676355 32.360498, -86.676215 32.360409, -86.676221 32.360352, -86.676113 32.360343, -86.676083 32.360323, -86.676022 32.360275, -86.675967 32.360221, -86.675882 32.360089, -86.675860 32.360016, -86.675852 32.359940, -86.675866 32.359848, -86.675894 32.359764, -86.675955 32.359671, -86.675794 32.359730, -86.675718 32.359769, -86.675650 32.359813, -86.675630 32.359828, -86.675345 32.359586, -86.675351 32.359541, -86.675435 32.359476, -86.675678 32.359496, -86.675703 32.359277, -86.675764 32.359233, -86.676028 32.359254, -86.676056 32.359017, -86.676093 32.358989, -86.676378 32.359013, -86.676410 32.358745, -86.676729 32.358771, -86.676760 32.358504, -86.677079 32.358530, -86.677110 32.358262, -86.677429 32.358289, -86.677483 32.357833, -86.677486 32.357830, -86.677535 32.357816, -86.677649 32.357804, -86.677704 32.357770, -86.677811 32.357779, -86.677824 32.357670, -86.677935 32.357576, -86.677982 32.357523, -86.678161 32.357538, -86.678192 32.357270, -86.678511 32.357296, -86.678543 32.357029, -86.678862 32.357055, -86.678924 32.356519, -86.679562 32.356572, -86.679594 32.356304, -86.679912 32.356331, -86.679944 32.356063, -86.679625 32.356037, -86.679635 32.355949, -86.679714 32.355896, -86.679808 32.355852, -86.679902 32.355801, -86.679925 32.355791, -86.680294 32.355821, -86.680263 32.356089, -86.680900 32.356142, -86.680932 32.355874, -86.681251 32.355900, -86.681313 32.355365, -86.681632 32.355391, -86.681651 32.355236, -86.681741 32.355246, -86.681790 32.355233, -86.681831 32.355206, -86.681903 32.355143, -86.682620 32.355202, -86.682652 32.354935), (-86.682286 32.363557, -86.681967 32.363530, -86.681936 32.363798, -86.682254 32.363825, -86.682286 32.363557)), ((-86.682683 32.354667, -86.682652 32.354935, -86.682333 32.354908, -86.682359 32.354683, -86.682477 32.354661, -86.682496 32.354651, -86.682683 32.354667)), ((-86.679703 32.385601, -86.679681 32.385783, -86.679519 32.385770, -86.679575 32.385728, -86.679703 32.385601)), ((-86.680095 32.385006, -86.680032 32.385542, -86.679783 32.385521, -86.679790 32.385514, -86.679879 32.385392, -86.679957 32.385258, -86.680098 32.384984, -86.680288 32.384638, -86.680503 32.384269, -86.680414 32.385033, -86.680095 32.385006)), ((-86.680909 32.383551, -86.680827 32.384256, -86.680526 32.384231, -86.680830 32.383708, -86.680909 32.383551)), ((-86.681428 32.381872, -86.681397 32.382140, -86.681158 32.382120, -86.681189 32.381852, -86.681428 32.381872)), ((-86.681491 32.381336, -86.681460 32.381604, -86.681221 32.381584, -86.681252 32.381317, -86.681491 32.381336)), ((-86.681423 32.379168, -86.681304 32.380183, -86.681281 32.380040, -86.681270 32.379894, -86.681272 32.379821, -86.681303 32.379531, -86.681333 32.379389, -86.681351 32.379327, -86.681395 32.379179, -86.681404 32.379153, -86.681451 32.379013, -86.681536 32.378757, -86.681577 32.378611, -86.681616 32.378473, -86.681649 32.378331, -86.682271 32.374547, -86.682318 32.374275, -86.682275 32.374642, -86.685784 32.374931, -86.685753 32.375199, -86.685434 32.375173, -86.685371 32.375708, -86.685052 32.375682, -86.685021 32.375950, -86.684702 32.375923, -86.684670 32.376191, -86.684351 32.376165, -86.684320 32.376433, -86.684001 32.376406, -86.683938 32.376942, -86.683619 32.376916, -86.683588 32.377183, -86.683269 32.377157, -86.683206 32.377693, -86.682887 32.377666, -86.682981 32.376863, -86.682662 32.376837, -86.682694 32.376569, -86.682375 32.376542, -86.682312 32.377078, -86.681993 32.377052, -86.681962 32.377320, -86.682281 32.377346, -86.682249 32.377614, -86.681930 32.377587, -86.681867 32.378123, -86.682505 32.378176, -86.682474 32.378443, -86.682793 32.378470, -86.682730 32.379005, -86.682411 32.378979, -86.682380 32.379247, -86.681423 32.379168)), ((-86.682997 32.368482, -86.682911 32.369215, -86.682737 32.368461, -86.682997 32.368482)), ((-86.683190 32.369580, -86.682939 32.371722, -86.682761 32.371708, -86.682793 32.371519, -86.682852 32.371085, -86.682965 32.370064, -86.682980 32.369771, -86.682976 32.369625, -86.682970 32.369562, -86.683190 32.369580)), ((-86.688479 32.398676, -86.687521 32.398597, -86.687553 32.398329, -86.687234 32.398303, -86.687265 32.398035, -86.686946 32.398009, -86.687009 32.397473, -86.686371 32.397421, -86.686402 32.397153, -86.686083 32.397127, -86.686055 32.397368, -86.685898 32.397243, -86.685732 32.397098, -86.685764 32.397100, -86.685827 32.396565, -86.685189 32.396512, -86.685220 32.396244, -86.684901 32.396218, -86.684840 32.396740, -86.684709 32.396743, -86.684519 32.396727, -86.684518 32.396737, -86.684431 32.396715, -86.684349 32.396668, -86.684596 32.396309, -86.684796 32.395980, -86.684868 32.395847, -86.684931 32.395712, -86.684983 32.395572, -86.685016 32.395426, -86.685034 32.395282, -86.685043 32.395148, -86.685345 32.395173, -86.685251 32.395977, -86.687166 32.396135, -86.687197 32.395867, -86.687835 32.395919, -86.687804 32.396187, -86.688123 32.396214, -86.687997 32.397285, -86.688954 32.397364, -86.688923 32.397631, -86.688604 32.397605, -86.688541 32.398141, -86.688860 32.398167, -86.688829 32.398435, -86.688510 32.398408, -86.688479 32.398676)), ((-86.686946 32.398009, -86.686918 32.398251, -86.686744 32.398055, -86.686690 32.397988, -86.686946 32.398009)), ((-86.686371 32.397421, -86.686348 32.397613, -86.686266 32.397539, -86.686091 32.397398, -86.686371 32.397421)), ((-86.685476 32.396806, -86.685468 32.396873, -86.685419 32.396833, -86.685343 32.396795, -86.685476 32.396806)), ((-86.687085 32.355571, -86.687053 32.355839, -86.685778 32.355734, -86.685841 32.355198, -86.687435 32.355330, -86.687445 32.355248, -86.687490 32.355260, -86.687611 32.355323, -86.687660 32.355337, -86.687716 32.355372, -86.687869 32.355421, -86.687917 32.355449, -86.688046 32.355485, -86.688060 32.355492, -86.688041 32.355650, -86.687085 32.355571)), ((-86.714223 32.385116, -86.713904 32.385090, -86.713935 32.384822, -86.713616 32.384796, -86.713679 32.384260, -86.713360 32.384234, -86.713391 32.383966, -86.713710 32.383992, -86.713741 32.383725, -86.712784 32.383646, -86.712847 32.383110, -86.713166 32.383136, -86.713228 32.382601, -86.713866 32.382653, -86.713929 32.382118, -86.713610 32.382091, -86.713641 32.381824, -86.713003 32.381771, -86.712941 32.382307, -86.712622 32.382281, -86.712653 32.382013, -86.712334 32.381986, -86.712271 32.382522, -86.711952 32.382496, -86.711984 32.382228, -86.711457 32.382185, -86.711454 32.382151, -86.711485 32.381641, -86.711471 32.381097, -86.711596 32.380030, -86.711604 32.380007, -86.711655 32.379762, -86.711687 32.379548, -86.711713 32.379064, -86.711713 32.379026, -86.711752 32.378694, -86.711684 32.378689, -86.711662 32.378577, -86.711633 32.378394, -86.711531 32.378123, -86.711449 32.377958, -86.711293 32.377615, -86.711219 32.377471, -86.711198 32.377398, -86.711171 32.377375, -86.711003 32.377108, -86.710975 32.377073, -86.710983 32.377009, -86.710917 32.377003, -86.710771 32.376880, -86.710706 32.376840, -86.710681 32.376833, -86.710758 32.376179, -86.710439 32.376153, -86.710470 32.375885, -86.709513 32.375806, -86.709544 32.375538, -86.709225 32.375512, -86.709194 32.375780, -86.708875 32.375754, -86.708813 32.376289, -86.708494 32.376263, -86.708525 32.375995, -86.708206 32.375969, -86.708112 32.376772, -86.708431 32.376799, -86.708462 32.376531, -86.709100 32.376583, -86.708975 32.377654, -86.708656 32.377628, -86.708687 32.377360, -86.707730 32.377282, -86.707762 32.377014, -86.707443 32.376987, -86.707380 32.377523, -86.707061 32.377497, -86.706936 32.378568, -86.707255 32.378594, -86.707224 32.378862, -86.707543 32.378888, -86.707511 32.379156, -86.707830 32.379182, -86.707768 32.379718, -86.708087 32.379744, -86.708024 32.380280, -86.708981 32.380359, -86.708950 32.380626, -86.709269 32.380653, -86.709206 32.381188, -86.709525 32.381215, -86.709432 32.382018, -86.710389 32.382097, -86.710357 32.382365, -86.710676 32.382391, -86.710614 32.382926, -86.710295 32.382900, -86.710232 32.383436, -86.710551 32.383462, -86.710426 32.384533, -86.711383 32.384612, -86.711352 32.384880, -86.711671 32.384906, -86.711639 32.385174, -86.712278 32.385226, -86.712215 32.385762, -86.712534 32.385788, -86.712440 32.386591, -86.712759 32.386618, -86.712634 32.387689, -86.712953 32.387715, -86.712828 32.388786, -86.713147 32.388813, -86.713116 32.389080, -86.712797 32.389054, -86.712734 32.389590, -86.712415 32.389563, -86.712478 32.389028, -86.712159 32.389002, -86.712190 32.388734, -86.711871 32.388708, -86.711902 32.388440, -86.711583 32.388414, -86.711614 32.388146, -86.711295 32.388119, -86.711327 32.387852, -86.711646 32.387878, -86.711708 32.387342, -86.711389 32.387316, -86.711452 32.386780, -86.711771 32.386807, -86.711833 32.386271, -86.711195 32.386219, -86.711227 32.385951, -86.710908 32.385925, -86.710939 32.385657, -86.711577 32.385709, -86.711608 32.385442, -86.711289 32.385415, -86.711320 32.385147, -86.711001 32.385121, -86.711033 32.384853, -86.710395 32.384801, -86.710363 32.385069, -86.710044 32.385042, -86.710013 32.385310, -86.710332 32.385336, -86.710238 32.386140, -86.710557 32.386166, -86.710495 32.386702, -86.710814 32.386728, -86.710782 32.386996, -86.711101 32.387022, -86.711008 32.387825, -86.710689 32.387799, -86.710595 32.388603, -86.710276 32.388576, -86.710338 32.388041, -86.710019 32.388014, -86.710082 32.387479, -86.709763 32.387453, -86.709825 32.386917, -86.709506 32.386891, -86.709538 32.386623, -86.709299 32.386603, -86.709281 32.386577, -86.709248 32.386536, -86.709232 32.386485, -86.709313 32.385793, -86.709072 32.385774, -86.709017 32.385643, -86.709010 32.385628, -86.709025 32.385499, -86.708913 32.385490, -86.708837 32.385399, -86.708618 32.385210, -86.708552 32.385168, -86.708531 32.385164, -86.708488 32.385142, -86.708309 32.385094, -86.708260 32.385067, -86.708171 32.385047, -86.708115 32.385015, -86.708130 32.384885, -86.707990 32.384873, -86.707967 32.384833, -86.707967 32.384807, -86.707950 32.384771, -86.707935 32.384718, -86.707902 32.384676, -86.707839 32.384622, -86.707874 32.384323, -86.707555 32.384297, -86.707586 32.384029, -86.707267 32.384003, -86.707298 32.383735, -86.706660 32.383682, -86.706692 32.383415, -86.707330 32.383467, -86.707424 32.382664, -86.707105 32.382637, -86.707136 32.382370, -86.706817 32.382343, -86.706880 32.381808, -86.706561 32.381782, -86.706435 32.382853, -86.706116 32.382826, -86.706085 32.383094, -86.705766 32.383068, -86.705703 32.383604, -86.704746 32.383525, -86.704715 32.383792, -86.704396 32.383766, -86.704365 32.384034, -86.704046 32.384008, -86.704077 32.383740, -86.702482 32.383609, -86.702513 32.383341, -86.702194 32.383314, -86.702226 32.383047, -86.700950 32.382942, -86.700918 32.383209, -86.700599 32.383183, -86.700631 32.382915, -86.700312 32.382889, -86.700280 32.383157, -86.699961 32.383130, -86.699899 32.383666, -86.699580 32.383640, -86.699486 32.384443, -86.698848 32.384391, -86.698942 32.383587, -86.698623 32.383561, -86.698811 32.381954, -86.698492 32.381928, -86.698523 32.381660, -86.698204 32.381634, -86.698267 32.381098, -86.697948 32.381072, -86.697979 32.380804, -86.697660 32.380778, -86.697691 32.380510, -86.697372 32.380484, -86.697404 32.380216, -86.697085 32.380190, -86.697116 32.379922, -86.696159 32.379843, -86.696222 32.379307, -86.695584 32.379255, -86.695615 32.378987, -86.695296 32.378961, -86.695359 32.378425, -86.695040 32.378399, -86.695134 32.377595, -86.694815 32.377569, -86.694846 32.377301, -86.694527 32.377275, -86.694558 32.377007, -86.693920 32.376955, -86.693952 32.376687, -86.693633 32.376660, -86.693664 32.376393, -86.693345 32.376366, -86.693376 32.376099, -86.692739 32.376046, -86.692770 32.375778, -86.692451 32.375752, -86.692482 32.375484, -86.692163 32.375458, -86.692226 32.374922, -86.691588 32.374870, -86.691619 32.374602, -86.691300 32.374575, -86.691332 32.374308, -86.691651 32.374334, -86.691776 32.373263, -86.691457 32.373236, -86.691614 32.371897, -86.691295 32.371871, -86.691201 32.372675, -86.690882 32.372648, -86.690851 32.372916, -86.689256 32.372784, -86.689287 32.372517, -86.688968 32.372490, -86.688937 32.372758, -86.688618 32.372732, -86.688555 32.373267, -86.687917 32.373215, -86.687949 32.372947, -86.687630 32.372921, -86.687661 32.372653, -86.687342 32.372627, -86.687373 32.372359, -86.688011 32.372411, -86.688043 32.372144, -86.688362 32.372170, -86.688393 32.371902, -86.688712 32.371928, -86.688743 32.371661, -86.689700 32.371740, -86.689731 32.371472, -86.690050 32.371498, -86.690082 32.371230, -86.690720 32.371283, -86.690782 32.370747, -86.690463 32.370721, -86.690432 32.370989, -86.690113 32.370963, -86.690176 32.370427, -86.689538 32.370374, -86.689507 32.370642, -86.689188 32.370616, -86.689156 32.370884, -86.688518 32.370831, -86.688487 32.371099, -86.688168 32.371072, -86.688199 32.370805, -86.686924 32.370699, -86.686892 32.370967, -86.686254 32.370914, -86.686348 32.370111, -86.686029 32.370085, -86.686061 32.369817, -86.686380 32.369843, -86.686505 32.368772, -86.685867 32.368719, -86.686150 32.366309, -86.686787 32.366362, -86.686725 32.366898, -86.687044 32.366924, -86.687106 32.366388, -86.687744 32.366441, -86.687776 32.366173, -86.688095 32.366199, -86.688126 32.365932, -86.688764 32.365984, -86.688795 32.365717, -86.689114 32.365743, -86.689083 32.366011, -86.690039 32.366090, -86.690071 32.365822, -86.690709 32.365874, -86.690677 32.366142, -86.690996 32.366169, -86.691028 32.365901, -86.691347 32.365927, -86.691315 32.366195, -86.692910 32.366326, -86.692878 32.366594, -86.692560 32.366568, -86.692528 32.366836, -86.692847 32.366862, -86.692816 32.367130, -86.692497 32.367104, -86.692340 32.368442, -86.692659 32.368469, -86.692628 32.368737, -86.693904 32.368842, -86.693872 32.369110, -86.694191 32.369136, -86.694223 32.368868, -86.694541 32.368894, -86.694510 32.369162, -86.695148 32.369215, -86.695054 32.370018, -86.695373 32.370045, -86.695342 32.370312, -86.694704 32.370260, -86.694672 32.370528, -86.693397 32.370422, -86.693365 32.370690, -86.692727 32.370637, -86.692696 32.370905, -86.694291 32.371037, -86.694259 32.371305, -86.694578 32.371331, -86.694610 32.371063, -86.695248 32.371116, -86.695279 32.370848, -86.695917 32.370901, -86.695823 32.371704, -86.696142 32.371730, -86.696173 32.371462, -86.696492 32.371489, -86.696523 32.371221, -86.696205 32.371195, -86.696267 32.370659, -86.696586 32.370685, -86.696555 32.370953, -86.697831 32.371058, -86.697862 32.370791, -86.696905 32.370712, -86.696936 32.370444, -86.696617 32.370418, -86.696649 32.370150, -86.696330 32.370123, -86.696392 32.369588, -86.696074 32.369562, -86.696261 32.367955, -86.697218 32.368034, -86.697156 32.368569, -86.697475 32.368596, -86.697443 32.368863, -86.699357 32.369021, -86.699326 32.369289, -86.699645 32.369315, -86.699613 32.369583, -86.699932 32.369609, -86.699901 32.369877, -86.700539 32.369930, -86.700508 32.370197, -86.700826 32.370224, -86.700764 32.370759, -86.701721 32.370838, -86.701689 32.371106, -86.702008 32.371132, -86.701977 32.371400, -86.702296 32.371426, -86.702265 32.371694, -86.702584 32.371720, -86.702552 32.371988, -86.702871 32.372015, -86.702840 32.372282, -86.703159 32.372309, -86.703096 32.372844, -86.703415 32.372871, -86.703384 32.373138, -86.703703 32.373165, -86.703672 32.373432, -86.704310 32.373485, -86.704278 32.373753, -86.704597 32.373779, -86.704566 32.374047, -86.704885 32.374073, -86.704822 32.374609, -86.705141 32.374635, -86.705110 32.374903, -86.705429 32.374929, -86.705366 32.375465, -86.706004 32.375517, -86.705973 32.375785, -86.707249 32.375890, -86.707280 32.375622, -86.706961 32.375596, -86.706993 32.375328, -86.707312 32.375354, -86.707343 32.375087, -86.707024 32.375060, -86.707055 32.374793, -86.707374 32.374819, -86.707499 32.373748, -86.707818 32.373774, -86.707850 32.373506, -86.707531 32.373480, -86.707562 32.373212, -86.707881 32.373238, -86.707912 32.372971, -86.707593 32.372944, -86.707624 32.372676, -86.707305 32.372650, -86.707368 32.372115, -86.706411 32.372036, -86.706505 32.371232, -86.706186 32.371206, -86.706217 32.370938, -86.705898 32.370912, -86.705836 32.371448, -86.705198 32.371395, -86.705229 32.371127, -86.704910 32.371101, -86.704942 32.370833, -86.704623 32.370807, -86.704654 32.370539, -86.704335 32.370513, -86.704398 32.369977, -86.704079 32.369951, -86.704047 32.370219, -86.703728 32.370193, -86.703822 32.369389, -86.702865 32.369310, -86.702897 32.369043, -86.702578 32.369016, -86.702546 32.369284, -86.701909 32.369231, -86.701940 32.368964, -86.701621 32.368937, -86.701652 32.368670, -86.701333 32.368643, -86.701365 32.368375, -86.701046 32.368349, -86.701077 32.368081, -86.700120 32.368003, -86.700183 32.367467, -86.699545 32.367414, -86.699514 32.367682, -86.698876 32.367630, -86.698907 32.367362, -86.697950 32.367283, -86.697981 32.367015, -86.697025 32.366936, -86.697087 32.366401, -86.696449 32.366348, -86.696418 32.366616, -86.695780 32.366563, -86.695812 32.366295, -86.695174 32.366243, -86.695205 32.365975, -86.694886 32.365949, -86.694917 32.365681, -86.694598 32.365655, -86.694630 32.365387, -86.694311 32.365360, -86.694342 32.365093, -86.694023 32.365066, -86.693992 32.365334, -86.693673 32.365308, -86.693704 32.365040, -86.693385 32.365014, -86.693417 32.364746, -86.692779 32.364693, -86.692842 32.364158, -86.692523 32.364131, -86.692554 32.363864, -86.692235 32.363837, -86.692266 32.363570, -86.691629 32.363517, -86.691691 32.362981, -86.691372 32.362955, -86.691435 32.362419, -86.691116 32.362393, -86.691179 32.361857, -86.690222 32.361779, -86.690191 32.362046, -86.689553 32.361994, -86.689490 32.362529, -86.689171 32.362503, -86.689140 32.362771, -86.690097 32.362850, -86.690034 32.363385, -86.690353 32.363412, -86.690322 32.363679, -86.690003 32.363653, -86.689971 32.363921, -86.689652 32.363895, -86.689715 32.363359, -86.689396 32.363333, -86.689365 32.363600, -86.689046 32.363574, -86.689109 32.363039, -86.688790 32.363012, -86.688821 32.362744, -86.687545 32.362639, -86.687577 32.362371, -86.686939 32.362319, -86.686970 32.362051, -86.686651 32.362025, -86.686683 32.361757, -86.686364 32.361730, -86.686395 32.361463, -86.686076 32.361436, -86.686170 32.360633, -86.686489 32.360659, -86.686552 32.360124, -86.686871 32.360150, -86.686902 32.359882, -86.688178 32.359988, -86.688209 32.359720, -86.688528 32.359746, -86.688497 32.360014, -86.689772 32.360119, -86.689804 32.359851, -86.690122 32.359878, -86.690217 32.359074, -86.691173 32.359153, -86.691205 32.358885, -86.691523 32.358912, -86.691461 32.359447, -86.691780 32.359474, -86.691811 32.359206, -86.692130 32.359232, -86.692099 32.359500, -86.692418 32.359526, -86.692449 32.359258, -86.692768 32.359285, -86.692924 32.357946, -86.693881 32.358025, -86.693850 32.358292, -86.694169 32.358319, -86.694137 32.358587, -86.695413 32.358692, -86.695444 32.358424, -86.695763 32.358450, -86.695638 32.359522, -86.696276 32.359574, -86.696244 32.359842, -86.698477 32.360026, -86.698445 32.360294, -86.699083 32.360346, -86.699052 32.360614, -86.699371 32.360641, -86.699340 32.360908, -86.698064 32.360803, -86.698095 32.360535, -86.695225 32.360299, -86.695162 32.360834, -86.695800 32.360887, -86.695738 32.361422, -86.694462 32.361317, -86.694399 32.361853, -86.695037 32.361905, -86.695006 32.362173, -86.695644 32.362226, -86.695612 32.362494, -86.696888 32.362599, -86.696919 32.362331, -86.697876 32.362410, -86.697782 32.363213, -86.698101 32.363240, -86.698070 32.363507, -86.698389 32.363534, -86.698326 32.364069, -86.698645 32.364096, -86.698582 32.364631, -86.698901 32.364657, -86.698870 32.364925, -86.699827 32.365004, -86.700140 32.362326, -86.702053 32.362484, -86.702022 32.362752, -86.702660 32.362804, -86.702629 32.363072, -86.703266 32.363125, -86.703235 32.363392, -86.704511 32.363498, -86.704479 32.363765, -86.704798 32.363792, -86.704736 32.364327, -86.705374 32.364380, -86.705342 32.364648, -86.705661 32.364674, -86.705630 32.364942, -86.706906 32.365047, -86.706874 32.365315, -86.708469 32.365446, -86.708438 32.365714, -86.708757 32.365740, -86.708726 32.366008, -86.709363 32.366060, -86.709332 32.366328, -86.709970 32.366381, -86.709939 32.366648, -86.710258 32.366675, -86.710226 32.366943, -86.711502 32.367048, -86.711471 32.367315, -86.712747 32.367420, -86.712715 32.367688, -86.713353 32.367741, -86.713322 32.368008, -86.713641 32.368035, -86.713578 32.368570, -86.713897 32.368597, -86.713866 32.368864, -86.714185 32.368891, -86.714154 32.369158, -86.714473 32.369185, -86.714441 32.369452, -86.714760 32.369479, -86.714729 32.369747, -86.715048 32.369773, -86.715017 32.370041, -86.715336 32.370067, -86.715305 32.370335, -86.715623 32.370361, -86.715592 32.370629, -86.715911 32.370655, -86.715880 32.370923, -86.716199 32.370949, -86.716168 32.371217, -86.716487 32.371243, -86.716424 32.371779, -86.716743 32.371805, -86.716712 32.372073, -86.717031 32.372099, -86.716999 32.372367, -86.717318 32.372393, -86.717287 32.372661, -86.717606 32.372687, -86.717575 32.372955, -86.717894 32.372981, -86.717831 32.373517, -86.718150 32.373543, -86.718119 32.373811, -86.718438 32.373837, -86.718188 32.375979, -86.717869 32.375953, -86.717900 32.375685, -86.717581 32.375659, -86.717550 32.375927, -86.717231 32.375901, -86.717419 32.374294, -86.716781 32.374241, -86.716343 32.377990, -86.716662 32.378017, -86.716631 32.378285, -86.716312 32.378258, -86.716281 32.378526, -86.714686 32.378395, -86.714654 32.378663, -86.714335 32.378636, -86.714304 32.378904, -86.713666 32.378852, -86.713604 32.379387, -86.713285 32.379361, -86.713316 32.379093, -86.712997 32.379067, -86.712934 32.379603, -86.712615 32.379576, -86.712459 32.380915, -86.712778 32.380942, -86.712715 32.381477, -86.713034 32.381503, -86.713066 32.381236, -86.713385 32.381262, -86.713416 32.380994, -86.713735 32.381020, -86.713704 32.381288, -86.714023 32.381314, -86.713960 32.381850, -86.714279 32.381876, -86.714217 32.382412, -86.714536 32.382438, -86.714504 32.382706, -86.714823 32.382732, -86.714761 32.383268, -86.714442 32.383241, -86.714379 32.383777, -86.714698 32.383803, -86.714667 32.384071, -86.714348 32.384045, -86.714223 32.385116), (-86.713166 32.383136, -86.713134 32.383404, -86.713453 32.383430, -86.713485 32.383163, -86.713166 32.383136), (-86.712334 32.381986, -86.712365 32.381719, -86.712046 32.381692, -86.712015 32.381960, -86.712334 32.381986), (-86.709544 32.375538, -86.709863 32.375565, -86.709895 32.375297, -86.709576 32.375271, -86.709544 32.375538), (-86.708875 32.375754, -86.708906 32.375486, -86.708587 32.375460, -86.708556 32.375727, -86.708875 32.375754), (-86.707443 32.376987, -86.707505 32.376452, -86.706548 32.376373, -86.706517 32.376641, -86.707155 32.376693, -86.707124 32.376961, -86.707443 32.376987), (-86.705766 32.383068, -86.705829 32.382532, -86.704872 32.382454, -86.704840 32.382721, -86.705478 32.382774, -86.705447 32.383042, -86.705766 32.383068), (-86.704872 32.382454, -86.704903 32.382186, -86.704584 32.382159, -86.704553 32.382427, -86.704872 32.382454), (-86.704903 32.382186, -86.705541 32.382238, -86.705572 32.381970, -86.704934 32.381918, -86.704903 32.382186), (-86.705447 32.383042, -86.705128 32.383015, -86.705097 32.383283, -86.705416 32.383309, -86.705447 32.383042), (-86.695823 32.371704, -86.695185 32.371651, -86.695154 32.371919, -86.695792 32.371972, -86.695823 32.371704), (-86.695792 32.371972, -86.695760 32.372240, -86.696079 32.372266, -86.696111 32.371998, -86.695792 32.371972), (-86.697862 32.370791, -86.698500 32.370843, -86.698531 32.370575, -86.697893 32.370523, -86.697862 32.370791), (-86.707818 32.373774, -86.707787 32.374042, -86.708106 32.374068, -86.708137 32.373800, -86.707818 32.373774), (-86.708106 32.374068, -86.708075 32.374336, -86.708394 32.374362, -86.708425 32.374094, -86.708106 32.374068), (-86.708075 32.374336, -86.707756 32.374310, -86.707724 32.374577, -86.708043 32.374604, -86.708075 32.374336), (-86.708043 32.374604, -86.708012 32.374871, -86.708331 32.374898, -86.708362 32.374630, -86.708043 32.374604), (-86.695812 32.366295, -86.696130 32.366322, -86.696162 32.366054, -86.695843 32.366028, -86.695812 32.366295), (-86.690222 32.361779, -86.690253 32.361511, -86.688978 32.361405, -86.688946 32.361673, -86.690222 32.361779), (-86.689171 32.362503, -86.689203 32.362235, -86.688884 32.362209, -86.688852 32.362477, -86.689171 32.362503), (-86.690122 32.359878, -86.690091 32.360145, -86.690729 32.360198, -86.690760 32.359930, -86.690122 32.359878), (-86.695225 32.360299, -86.695256 32.360031, -86.694937 32.360005, -86.694906 32.360272, -86.695225 32.360299), (-86.687368 32.369654, -86.687431 32.369119, -86.688388 32.369198, -86.688419 32.368930, -86.687143 32.368825, -86.687112 32.369093, -86.686793 32.369066, -86.686730 32.369602, -86.687368 32.369654), (-86.696917 32.376120, -86.696948 32.375852, -86.695353 32.375721, -86.695322 32.375989, -86.696917 32.376120), (-86.697042 32.375049, -86.697073 32.374781, -86.696754 32.374755, -86.696786 32.374487, -86.696467 32.374461, -86.696498 32.374193, -86.696179 32.374167, -86.696085 32.374970, -86.697042 32.375049), (-86.698893 32.375742, -86.698924 32.375475, -86.699243 32.375501, -86.699306 32.374965, -86.698668 32.374913, -86.698637 32.375180, -86.697999 32.375128, -86.697936 32.375663, -86.698893 32.375742), (-86.698668 32.374913, -86.698699 32.374645, -86.698380 32.374619, -86.698349 32.374886, -86.698668 32.374913), (-86.698380 32.374619, -86.698443 32.374083, -86.698124 32.374057, -86.698187 32.373521, -86.697549 32.373468, -86.697580 32.373201, -86.697261 32.373174, -86.697292 32.372907, -86.696973 32.372880, -86.697036 32.372345, -86.696398 32.372292, -86.696336 32.372828, -86.696654 32.372854, -86.696623 32.373122, -86.696942 32.373148, -86.696879 32.373684, -86.697198 32.373710, -86.697167 32.373978, -86.697486 32.374004, -86.697455 32.374272, -86.697774 32.374298, -86.697742 32.374566, -86.698380 32.374619), (-86.702727 32.378762, -86.702758 32.378494, -86.705948 32.378757, -86.706010 32.378221, -86.705691 32.378195, -86.705660 32.378463, -86.705341 32.378437, -86.705372 32.378169, -86.704734 32.378116, -86.704703 32.378384, -86.704384 32.378358, -86.704415 32.378090, -86.703777 32.378037, -86.703746 32.378305, -86.702470 32.378200, -86.702408 32.378736, -86.702727 32.378762)), ((-86.714830 32.385436, -86.714192 32.385384, -86.714223 32.385116, -86.714861 32.385168, -86.714830 32.385436)), ((-86.712847 32.383110, -86.712528 32.383084, -86.712559 32.382816, -86.712240 32.382790, -86.712271 32.382522, -86.712909 32.382575, -86.712847 32.383110)), ((-86.712240 32.382790, -86.712209 32.383058, -86.711427 32.382993, -86.711463 32.382889, -86.711519 32.382831, -86.711567 32.382772, -86.711584 32.382736, -86.711602 32.382737, -86.711608 32.382684, -86.711615 32.382669, -86.711612 32.382651, -86.711633 32.382470, -86.711952 32.382496, -86.711921 32.382764, -86.712240 32.382790)), ((-86.711452 32.386780, -86.710814 32.386728, -86.710845 32.386460, -86.711483 32.386513, -86.711452 32.386780)), ((-86.710332 32.385336, -86.710363 32.385069, -86.711001 32.385121, -86.710939 32.385657, -86.710620 32.385631, -86.710651 32.385363, -86.710332 32.385336)), ((-86.706692 32.383415, -86.706054 32.383362, -86.706085 32.383094, -86.706404 32.383120, -86.706435 32.382853, -86.706754 32.382879, -86.706692 32.383415)), ((-86.691457 32.373236, -86.691394 32.373772, -86.691075 32.373746, -86.691044 32.374014, -86.690725 32.373987, -86.690757 32.373719, -86.689481 32.373614, -86.689512 32.373346, -86.690150 32.373399, -86.690181 32.373131, -86.691457 32.373236)), ((-86.707562 32.373212, -86.706924 32.373160, -86.706987 32.372624, -86.707305 32.372650, -86.707274 32.372918, -86.707593 32.372944, -86.707562 32.373212)), ((-86.706987 32.372624, -86.706668 32.372598, -86.706699 32.372330, -86.707018 32.372356, -86.706987 32.372624)), ((-86.694886 32.365949, -86.694823 32.366484, -86.694504 32.366458, -86.694567 32.365922, -86.694886 32.365949)), ((-86.694567 32.365922, -86.693929 32.365870, -86.693961 32.365602, -86.694598 32.365655, -86.694567 32.365922)), ((-86.693673 32.365308, -86.693642 32.365576, -86.693323 32.365549, -86.693354 32.365282, -86.693673 32.365308)), ((-86.693354 32.365282, -86.693035 32.365255, -86.693067 32.364987, -86.693385 32.365014, -86.693354 32.365282)), ((-86.690191 32.362046, -86.690828 32.362099, -86.690766 32.362635, -86.689809 32.362556, -86.689840 32.362288, -86.690159 32.362314, -86.690191 32.362046)), ((-86.712941 32.382307, -86.713260 32.382333, -86.713228 32.382601, -86.712909 32.382575, -86.712941 32.382307)), ((-86.713610 32.382091, -86.713579 32.382359, -86.713260 32.382333, -86.713291 32.382065, -86.713610 32.382091)), ((-86.686658 32.397715, -86.686634 32.397920, -86.686549 32.397816, -86.686443 32.397700, -86.686441 32.397697, -86.686658 32.397715)), ((-86.688353 32.399747, -86.688672 32.399774, -86.688641 32.400036, -86.688580 32.399972, -86.688353 32.399749, -86.688353 32.399747)), ((-86.688353 32.399747, -86.688351 32.399747, -86.688131 32.399533, -86.688064 32.399464, -86.688065 32.399453, -86.688385 32.399480, -86.688353 32.399747)), ((-86.688065 32.399453, -86.688053 32.399452, -86.687915 32.399313, -86.687809 32.399199, -86.687778 32.399161, -86.687778 32.399159, -86.688097 32.399185, -86.688065 32.399453)), ((-86.689823 32.400950, -86.689768 32.401421, -86.689644 32.401274, -86.689443 32.401048, -86.689349 32.400933, -86.689166 32.400691, -86.688986 32.400442, -86.688892 32.400319, -86.688793 32.400199, -86.688647 32.400042, -86.688960 32.400068, -86.688929 32.400335, -86.689248 32.400362, -86.689216 32.400630, -86.689535 32.400656, -86.689504 32.400924, -86.689823 32.400950)), ((-86.691617 32.358108, -86.690661 32.358029, -86.690692 32.357762, -86.690054 32.357709, -86.690023 32.357977, -86.689704 32.357950, -86.689735 32.357683, -86.689417 32.357656, -86.689448 32.357388, -86.689129 32.357362, -86.689192 32.356827, -86.689829 32.356879, -86.689798 32.357147, -86.690436 32.357200, -86.690467 32.356932, -86.691105 32.356984, -86.691042 32.357520, -86.691361 32.357546, -86.691330 32.357814, -86.691649 32.357841, -86.691617 32.358108)), ((-86.690661 32.358029, -86.690629 32.358297, -86.690513 32.358288, -86.690372 32.358213, -86.690318 32.358205, -86.690342 32.358003, -86.690661 32.358029)), ((-86.689913 32.401498, -86.689865 32.401538, -86.689827 32.401491, -86.689913 32.401498)), ((-86.708976 32.363865, -86.709295 32.363892, -86.709263 32.364159, -86.708944 32.364133, -86.708976 32.363865)), ((-86.708976 32.363865, -86.708403 32.363818, -86.708349 32.363780, -86.708342 32.363775, -86.708369 32.363545, -86.707838 32.363501, -86.707798 32.363480, -86.707738 32.363437, -86.707763 32.363225, -86.707444 32.363198, -86.707475 32.362931, -86.708113 32.362983, -86.708082 32.363251, -86.708719 32.363304, -86.708688 32.363571, -86.709007 32.363598, -86.708976 32.363865)), ((-86.707475 32.362931, -86.706837 32.362878, -86.706868 32.362610, -86.706231 32.362558, -86.706262 32.362290, -86.705305 32.362211, -86.705274 32.362479, -86.704636 32.362426, -86.704667 32.362159, -86.704029 32.362106, -86.704061 32.361838, -86.703104 32.361759, -86.703135 32.361492, -86.702816 32.361465, -86.702785 32.361733, -86.702466 32.361707, -86.702497 32.361439, -86.701860 32.361386, -86.701828 32.361654, -86.700872 32.361575, -86.700903 32.361308, -86.700265 32.361255, -86.700422 32.359916, -86.701697 32.360021, -86.701666 32.360289, -86.702304 32.360342, -86.702272 32.360609, -86.702910 32.360662, -86.702942 32.360394, -86.703579 32.360447, -86.703548 32.360714, -86.704186 32.360767, -86.704155 32.361035, -86.704792 32.361087, -86.704761 32.361355, -86.705399 32.361408, -86.705368 32.361676, -86.706006 32.361728, -86.705974 32.361996, -86.706612 32.362048, -86.706581 32.362316, -86.707219 32.362369, -86.707187 32.362637, -86.707506 32.362663, -86.707475 32.362931)), ((-86.715624 32.384150, -86.715499 32.385221, -86.715180 32.385195, -86.715211 32.384927, -86.714892 32.384901, -86.714923 32.384633, -86.715242 32.384659, -86.715274 32.384391, -86.714955 32.384365, -86.715049 32.383562, -86.715368 32.383588, -86.715336 32.383856, -86.715655 32.383882, -86.715687 32.383614, -86.716325 32.383667, -86.716293 32.383934, -86.715974 32.383908, -86.715943 32.384176, -86.715624 32.384150)))');

    CREATE CUSTOM INDEX blocks_index on cities()
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                shape : {
                    type            : "geo_shape",
                    max_levels      : 15,
                    transformations : [{type:"bbox"}]
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
                number   : {type : "integer"},
                bool     : {type : "boolean"},
                date     : {type : "date", validated : true, pattern : "yyyy/MM/dd"},
                duration : {type : "date_range", from : "start_date", to : "stop_date", pattern : "yyyy/MM/dd"},
                place    : {type : "geo_point", latitude : "latitude", longitude : "longitude"},
                mapz     : {type : "string"},
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
                                        field : <field>,
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
            (, doc_values : <doc_values> )? }
    }');

where:

-  **doc\_values** (default = false): if the generated Lucene query should use doc values instead of inverted index.
   Doc values searches are typically slower, but they can be faster in the dense case where most rows match the search.

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
            (, doc_values : <doc_values> )? }
    }');

where:

-  **doc\_values** (default = false): if the generated Lucene query should use doc values instead of inverted index.
   Doc values searches are typically slower, but they can be faster in the dense case where most rows match the search.

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
            type       : "match",
            field      : "date",
            value      : "2014/01/01",
            doc_values : true}
    }');

Using `query builder <#query-builder>`__:

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
            (, lower : <lower>)?
            (, upper : <upper>)?
            (, include_lower : <include_lower> )?
            (, include_upper : <include_upper> )?
            (, doc_values : <doc_values> )? }
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

Lower and upper will default to :math:`-/+\\infty` for number. In the
case of byte and string like data (bytes, inet, string, text), all
values from lower up to upper will be returned if both are specified. If
only “lower” is specified, all rows with values from “lower” will be
returned. If only “upper” is specified then all rows with field values
up to “upper” will be returned. If both are omitted than all rows will
be returned.

**Example 1:** search for rows where *age* is in [65, ∞):

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type          : "range",
            field         : "age",
            lower         : 65,
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
            include_upper : true,
            doc_values    : true}
    }');

Using `query builder <#query-builder>`__:

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    ResultSet rs = session.execute(
        "SELECT * FROM users WHERE expr(users_index, ?)",
        search().filter(range("age").upper(0).includeUpper(true).docValues(true)).build());

**Example 3:** search for rows where *age* is in [-1, 1]:

.. code-block:: sql

    SELECT * FROM users WHERE expr(users_index, '{
        filter : {
            type          : "range",
            field         : "age",
            lower         : -1,
            upper         : 1,
            include_lower : true,
            include_upper : true,
            doc_values    : false }
    }');

Using `query builder <#query-builder>`__:

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

Bounding box
____________

Buffer transformation returns the `minimum bounding box <https://en.wikipedia.org/wiki/Minimum_bounding_box>`__ a shape,
that is, the minimum rectangle containing the shape.

**Syntax:**

.. code-block:: sql

    { type : "bbox" }

**Example:** The following `geo shape mapper <#geo-shape-mapper>`__ will index only the bounding box of the WKT shape
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
                    transformations : [{type:"bbox"}]
                }
            }
        }'
    };

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
    WHERE lucene = '{filter : {type : "match", field : "food", value : "chips"}}')
    AND token(name, gender) > token('Alicia', 'female');

Paging
======

Paging filtered results is fully supported. You can retrieve
the rows starting from a certain key. For example, if the primary key is
(userid, createdAt), you can search:

.. code-block:: sql

    SELECT * FROM tweets
    WHERE lucene = ‘{filter : {type:”match",  field:”text", value:”cassandra”}}'
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
