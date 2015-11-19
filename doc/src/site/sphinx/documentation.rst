++++++++++++++++++++++++++++++++
Stratio's Cassandra Lucene Index
++++++++++++++++++++++++++++++++

- `Overview <#overview>`__
    - `Features <#features>`__
    - `Architecture <#architecture>`__
    - `Requirements <#requirements>`__
    - `Installation <#installation>`__
    - `Example <#example>`__
- `Indexing <#indexing>`__
    - `Analyzers <#analysis>`__
        - `Classpath analyzer <#classpath-analyzer>`__
        - `Snowball analyzer <#snowball-analyzer>`__
    - `Mappers <#mapping>`__
        - `Big decimal mapper <#bigdecimal-mapper>`__
        - `Big integer mapper <#biginteger-mapper>`__
        - `Bitemporal mapper <#bitemporal-mapper>`__
        - `Blob mapper <#blob-mapper>`__
        - `Boolean mapper <#boolean-mapper>`__
        - `Date mapper <#date-mapper>`__
        - `Date range mapper <#daterange-mapper>`__
        - `Double mapper <#double-mapper>`__
        - `Float mapper <#float-mapper>`__
        - `GeoPoint mapper <#geopoint-mapper>`__
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
    - `Match search <#match-search>`__
    - `None search <#none-search>`__
    - `Phrase search <#phrase-search>`__
    - `Prefix search <#prefix-search>`__
    - `Range search <#range-search>`__
    - `Regexp search <#regexp-search>`__
    - `Wildcard search <#wildcard-search>`__
- `User Defined Types <#user-defined-types>`__
- `Collections <#collections>`__
- `Query builder <#query-builder>`__
- `Spark and Hadoop <#spark-and-hadoop>`__
    - `Token range searches <#token-range-searches>`__
    - `Paging <#paging>`__
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
    - `Use a large page size <#use-a-large-page-size>`__

Overview
********

`Cassandra <http://cassandra.apache.org/>`__ index functionality has
been extended to provide near real time search such as
`ElasticSearch <http://www.elasticsearch.org/>`__ or
`Solr <https://lucene.apache.org/solr/>`__, including full text search
capabilities and multivariable, geospatial and bitemporal search.

It is also fully compatible with `Apache
Spark <https://spark.apache.org/>`__ and `Apache
Hadoop <https://hadoop.apache.org/>`__, allowing you to filter data at
database level. This speeds up jobs reducing the amount of data to be
collected and processed.

This project is not intended to replace Apache Cassandra denormalized
tables, inverted indexes, and/or secondary indexes. It is just a tool
to perform some kind of queries which are really hard to be addressed
using Apache Cassandra out of the box features.

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
-  Bitemporal search
-  Boolean (and, or, not) search
-  Near real-time search
-  Relevance scoring and sorting
-  General top-k queries
-  Custom analyzers
-  CQL3 support
-  Third-party drivers compatibility
-  Spark compatibility
-  Hadoop compatibility

Not yet supported:

-  Thrift API
-  Legacy compact storage option
-  Indexing ``counter`` columns
-  Columns with TTL
-  CQL user defined types
-  Static columns

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
-  Java >= 1.7 (OpenJDK and Sun have been tested)
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
this task also delete previous plugin's JAR versions in CASSANDRA_HOME/lib/ directory:

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

We have created a column called *lucene* to link the index searches. This column will not store data. Now you can create
a custom Lucene index on it with the following statement:

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
                time  : {type : "date", pattern : "yyyy/MM/dd", sorted : true},
                place : {type : "geo_point", latitude:"latitude", longitude:"longitude"}
            }
        }'
    };

This will index all the columns in the table with the specified types, and it will be refreshed once per second.
Alternatively, you can explicitly refresh all the index shards with an empty search with consistency ``ALL``:

.. code-block:: sql

    CONSISTENCY ALL
    SELECT * FROM tweets WHERE lucene = '{refresh:true}';
    CONSISTENCY QUORUM

Now, to search for tweets within a certain date range:

.. code-block:: sql

    SELECT * FROM tweets WHERE lucene='{
        filter : {type:"range", field:"time", lower:"2014/04/25", upper:"2014/05/01"}
    }' limit 100;

The same search can be performed forcing an explicit refresh of the involved index shards:

.. code-block:: sql

    SELECT * FROM tweets WHERE lucene='{
        filter : {type:"range", field:"time", lower:"2014/04/25", upper:"2014/05/01"},
        refresh : true
    }' limit 100;

Now, to search the top 100 more relevant tweets where *body* field contains the phrase “big data gives organizations”
within the aforementioned date range:

.. code-block:: sql

    SELECT * FROM tweets WHERE lucene='{
        filter : {type:"range", field:"time", lower:"2014/04/25", upper:"2014/05/01"},
        query  : {type:"phrase", field:"body", value:"big data gives organizations", slop:1}
    }' limit 100;

To refine the search to get only the tweets written by users whose name starts with "a":

.. code-block:: sql

    SELECT * FROM tweets WHERE lucene='{
        filter : {type:"boolean", must:[
                       {type:"range", field:"time", lower:"2014/04/25", upper:"2014/05/01"},
                       {type:"prefix", field:"user", value:"a"} ] },
        query  : {type:"phrase", field:"body", value:"big data gives organizations", slop:1}
    }' limit 100;

To get the 100 more recent filtered results you can use the *sort* option:

.. code-block:: sql

    SELECT * FROM tweets WHERE lucene='{
        filter : {type:"boolean", must:[
                       {type:"range", field:"time", lower:"2014/04/25", upper:"2014/05/01"},
                       {type:"prefix", field:"user", value:"a"} ] },
        query  : {type:"phrase", field:"body", value:"big data gives organizations", slop:1},
        sort   : {fields: [ {field:"time", reverse:true} ] }
    }' limit 100;

The previous search can be restricted to a geographical bounding box:

.. code-block:: sql

    SELECT * FROM tweets WHERE lucene='{
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
    }' limit 100;

Alternatively, you can restrict the search to retrieve tweets that are within a specific distance from a geographical position:

.. code-block:: sql

    SELECT * FROM tweets WHERE lucene='{
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
    }' limit 100;

Indexing
********

Syntax:

.. code-block:: sql

    CREATE CUSTOM INDEX (IF NOT EXISTS)? <index_name>
                                      ON <table_name> ( <magic_column> )
                                   USING 'com.stratio.cassandra.lucene.Index'
                            WITH OPTIONS = <options>

where:

-  <magic\_column> is the name of a text column that does not contain
   any data and will be used to show the scoring for each resulting row
   of a search.
-  <options> is a JSON object:

.. code-block:: sql

    <options> := { ('refresh_seconds'       : '<int_value>',)?
                   ('ram_buffer_mb'         : '<int_value>',)?
                   ('max_merge_mb'          : '<int_value>',)?
                   ('max_cached_mb'         : '<int_value>',)?
                   ('indexing_threads'      : '<int_value>',)?
                   ('indexing_queues_size'  : '<int_value>',)?
                   ('directory_path'        : '<string_value>',)?
                   ('excluded_data_centers' : '<string_value>',)?
                   'schema'                 : '<schema_definition>'};

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

Example:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census(lucene)
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

Example:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census(lucene)
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
| `bigdec <#bigdecimal-mapper>`__     | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | integer_digits  | integer         | 32                             | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | decimal_digits  | integer         | 32                             | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `bigint <#biginteger-mapper>`__     | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
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
|                                     | pattern         | string          | yyyy/MM/dd HH:mm:ss.SSS Z      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | now_value       | object          | Long.MAX_VALUE                 | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `blob <#blob-mapper>`__             | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `boolean  <#boolean-mapper>`__      | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `date   <#date-mapper>`__           | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | pattern         | string          | yyyy/MM/dd HH:mm:ss.SSS Z      | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `date_range <#daterange-mapper>`__  | from            | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | to              | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | pattern         | string          | yyyy/MM/dd HH:mm:ss.SSS Z      | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `double  <#double-mapper>`__        | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | boost           | integer         | 0.1f                           | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `float  <#float-mapper>`__          | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | boost           | integer         | 0.1f                           | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `geo_point  <#geopoint-mapper>`__   | latitude        | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | longitude       | string          |                                | Yes       |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | max_levels      | integer         | 11                             | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `inet <#inet-mapper>`__             | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `integer <#integer-mapper>`__       | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | boost           | integer         | 0.1f                           | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `long <#long-mapper>`__             | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | boost           | integer         | 0.1f                           | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `string  <#string-mapper>`__        | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `text  <#text-mapper>`__            | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | analyzer        | string          | default_analyzer of the schema | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| `uuid <#uuid-mapper>`__             | column          | string          | mapper_name of the schema      | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | indexed         | boolean         | true                           | No        |
|                                     +-----------------+-----------------+--------------------------------+-----------+
|                                     | sorted          | boolean         | false                          | No        |
+-------------------------------------+-----------------+-----------------+--------------------------------+-----------+

Most mapping definitions have an ``indexed`` option indicating if
the field is searchable, it is true by default. There is also a ``sorted`` option
specifying if it is possible to sort rows by the corresponding field, false by default. List and set
columns can't be sorted because they produce multivalued fields.
These options should be set to false when no needed in order to have a smaller and faster index.

Note that Cassandra allows one custom index per table. On the other
hand, Cassandra does not allow a modify operation on indexes. To modify
an index it needs to be deleted first and created again.

Big decimal mapper
__________________

Maps arbitrary precision decimal values.

Example:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census(lucene)
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
                    column         : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, decimal, double, float, int, smallint, text, tinyint, varchar, varint

Big integer mapper
__________________

Maps arbitrary precision integer values.

Example:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census(lucene)
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                biginteger : {
                    type    : "bigint",
                    digits  : 10,
                    indexed : true,
                    sorted  : false,
                    column  : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, int, smallint, text, tinyint, varchar, varint

Bitemporal mapper
_________________

Maps four columns containing the four columns of a bitemporal fact.

Example:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census(lucene)
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
                    pattern   : "yyyy/MM/dd HH:mm:ss.SSS";,
                    now_value : "3000/01/01 00:00:00.000",
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, date, int, text, timestamp, varchar, varint

Blob mapper
___________

Maps a blob value.

Example:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census(lucene)
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

Example:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census(lucene)
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                bool : {
                    type    : "boolean",
                    indexed : true,
                    sorted  : false,
                    column  : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, boolean , text, varchar

Date mapper
___________

Maps dates using a either a pattern or a UNIX timestamp.

Example:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census(lucene)
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                date : {
                    type    : "date",
                    pattern : "yyyy/MM/dd HH:mm:ss.SSS",
                    indexed : true,
                    sorted  : false,
                    column  : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, date, int, text, timestamp, varchar, varint

Date range mapper
_________________

Maps a time duration/period defined by a start date and a stop date.

Example:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census(lucene)
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                date_range : {
                    type    : "date_range",
                    from    : "range_from",
                    to      : "range_to",
                    pattern : "yyyy/MM/dd HH:mm:ss.SSS"
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, date, int, text, timestamp, varchar, varint

Double mapper
_____________

Maps a 64-bit decimal number.

Example:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census(lucene)
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                double : {
                    type    : "double",
                    boost   : 2.0,
                    indexed : true,
                    sorted  : false,
                    column  : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, decimal, double, float, int, smallint, text, timestamp,  tinyint, varchar, varint

Float mapper
____________

Maps a 32-bit decimal number.

Example:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census(lucene)
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                float : {
                    type    : "float",
                    boost   : 2.0,
                    indexed : true,
                    sorted  : false,
                    column  : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, decimal, double, float, int, smallint, timestamp, tinyint, varchar, varint

GeoPoint mapper
_______________

Maps a geospatial location (point) defined by two columns containing a latitude and a longitude.

Example:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census(lucene)
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                geo_point : {
                    type       : "geo_point",
                    latitude   : "lat",
                    longitude  : "long",
                    max_levels : 15
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, decimal, double, float, int, smallint, text, timestamp, varchar, varint

Inet mapper
___________

Maps an IP address. Either IPv4 and IPv6 are supported.

Example:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census(lucene)
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                inet : {
                    type    : "inet",
                    indexed : true,
                    sorted  : false,
                    column  : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, inet, text, varchar

Integer mapper
______________

Maps a 32-bit integer number.

Example:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census(lucene)
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                integer : {
                    type    : "integer",
                    boost   : 2.0,
                    indexed : true,
                    sorted  : false,
                    column  : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, decimal, double, float, int, smallint, text, timestamp, tinyint, varchar, varint

Long mapper
___________

Maps a 64-bit integer number.

Example:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census(lucene)
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                long : {
                    type    : "long",
                    boost   : 2.0,
                    indexed : true,
                    sorted  : false,
                    column  : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, decimal, double, float, int, smallint, text, timestamp, tinyint, varchar, varint

String mapper
_____________

Maps a not-analyzed text value.

Example:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census(lucene)
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                string : {
                    type           : "string",
                    case_sensitive : false,
                    indexed        : true,
                    sorted         : false,
                    column         : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, blob, boolean, double, float, inet, int, smallint, text, timestamp, timeuuid, tinyint, uuid, varchar, varint

Text mapper
___________

Maps a language-aware text value analyzed according to the specified analyzer.

Example:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census(lucene)
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
                    type     : "text",
                    analyzer : "my_custom_analyzer",
                    indexed  : true,
                    sorted   : false,
                    column   : "column_name"
                }
            }
        }'
    };


Supported CQL types: ascii, bigint, blob, boolean, double, float, inet, int, smallint, text, timestamp, timeuuid, tinyint, uuid, varchar, varint

UUID mapper
___________

Maps an UUID value.

Example:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census(lucene)
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                bigdecimal : {
                    type    : "uuid",
                    indexed : true,
                    sorted  : false,
                    column  : "column_name"
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
    ON test.users (stratio_col)
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
                name   : {type : "string"},
                gender : {type : "string", sorted: true},
                animal : {type : "string"},
                age    : {type : "integer"},
                food   : {type : "string"},
                number : {type : "integer"},
                bool   : {type : "boolean"},
                date   : {type : "date", pattern  : "yyyy/MM/dd"},
                mapz   : {type : "string", sorted: true},
                setz   : {type : "string"},
                listz  : {type : "string"},
                phrase : {type : "text", analyzer : "my_custom_analyzer"}
            }
        }'
    };

Searching
*********

Syntax:

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table_name>
    WHERE <magic_column> = '{ (   filter  : <filter>  )?
                              ( , query   : <query>   )?
                              ( , sort    : <sort>    )?
                              ( , refresh : ( true | false ) )?
                            }';

where <filter> and <query> are a JSON object:

.. code-block:: sql

    <filter> := { type : <type> (, <option> : ( <value> | <value_list> ) )+ }
    <query>  := { type : <type> (, <option> : ( <value> | <value_list> ) )+ }

and <sort> is another JSON object:

.. code-block:: sql

        <sort> := { fields : <sort_field> (, <sort_field> )* }
        <sort_field> := { field : <field> (, reverse : <reverse> )? }

When searching by ``filter``, without any ``query`` or ``sort`` defined,
then the results are returned in the Cassandra’s natural order, which is
defined by the partitioner and the column name comparator. When searching
by ``query``, results are returned sorted by descending relevance. The
scores will be located in the column ``magic_column``. Sort option is used
to specify the order in which the indexed rows will be traversed. When
sorting is used, the query scoring is delayed.

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
    SELECT * FROM <table> WHERE <magic_column> = '{refresh:true}';
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

Syntax:

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE <magic_column> = '{ (filter | query) : { type  : "all"} }';

Example: will return all the indexed rows

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type  : "all" } }';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(all());
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));



Bitemporal search
=================

Syntax:

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE <magic_column> = '{ (filter | query) : {
                                type       : "bitemporal",
                                (vt_from   : <vt_from> ,)?
                                (vt_to     : <vt_to> ,)?
                                (tt_from   : <tt_from> ,)?
                                (tt_to     : <tt_to> ,)?
                                (operation : <operation> )?
                              }}';

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
        lucene text,
        PRIMARY KEY (name, vt_from, tt_from)
    );


Second, we create the index:

.. code-block:: sql

    CREATE CUSTOM INDEX census_index on census(lucene)
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

    INSERT INTO census(citizen_name, city, vt_from, vt_to, tt_from, tt_to)
    VALUES ('John', 'Madrid', '2015/01/01', '2200/12/31', '2015/01/01', '2200/12/31');

    INSERT INTO census(citizen_name, city, vt_from, vt_to, tt_from, tt_to)
    VALUES ('Margaret', 'Barcelona', '2015/01/01', '2200/12/31', '2015/01/01', '2200/12/31');

    INSERT INTO census(citizen_name, city, vt_from, vt_to, tt_from, tt_to)
    VALUES ('Cristian', 'Ceuta', '2015/01/01', '2200/12/31', '2015/01/01', '2200/12/31');

    INSERT INTO census(citizen_name, city, vt_from, vt_to, tt_from, tt_to)
    VALUES ('Edward', 'New York','2015/01/01', '2200/12/31', '2015/01/01', '2200/12/31');

    INSERT INTO census(citizen_name, city, vt_from, vt_to, tt_from, tt_to)
    VALUES ('Johnatan', 'San Francisco', '2015/01/01', '2200/12/31', '2015/01/01', '2200/12/31');


John moves to Amsterdam in '2015/03/05' but he does not comunicate this to census bureau until '2015/06/29' because he need it to apply for taxes reduction.

So, the system need to update last information from John,and insert the new. This is done with batch execution updating the transaction time end of previous data and inserting new.


.. code-block:: sql

    BEGIN BATCH
        UPDATE census SET tt_to = '2015/06/29'
        WHERE citizen_name = 'John' AND vt_from = '2015/01/01' AND tt_from = '2015/01/01'
        IF tt_to = '2200/12/31';

        INSERT INTO census(citizen_name, city, vt_from, vt_to, tt_from, tt_to)
        VALUES ('John', 'Amsterdam', '2015/03/05', '2200/12/31', '2015/06/30', '2200/12/31');
    APPLY BATCH;

Now , we can see the main difference between valid time and transaction time. The system knows from '2015/01/01' to '2015/06/29' that John resides in Madrid from '2015/01/01' until now, and resides in Amsterdam from '2015/03/05' until now.

There are several types of queries concerning this type of indexing

If its needed to get all the data in the table:

.. code-block:: sql

    SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM census ;


If you want to know what is the last info about where John resides, you perform a query with tt_from and tt_to setted to now_value:

.. code-block:: sql

    SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM census
    WHERE lucene = '{
        filter : {
            type    : "bitemporal",
            field   : "bitemporal",
            vt_from : 0,
            vt_to   : "2200/12/31",
            tt_from : "2200/12/31",
            tt_to   : "2200/12/31"
        }
    }'
    AND name='John';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "lucene";
    Search search = search().filter(bitemporal("bitemporal").ttFrom("2200/12/31").ttTo("2200/12/31")
                                    .vtFrom(0).vtTo("2200/12/31"));
    ResultSet rs = session.execute(QueryBuilder
                                        .select("name", "city", "vt_from", "vt_to", "tt_from", "tt_to")
                                        .from("test","census").where(eq(indexColumn, search.build()));



If the test case needs to know what the system was thinking at '2015/03/01' about where John resides.

.. code-block:: sql

    SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM census
    WHERE lucene = '{
        filter : {
            type    : "bitemporal",
            field   : "bitemporal",
            tt_from : "2015/03/01",
            tt_to   : "2015/03/01"
        }
    }'
    AND name = 'John';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "lucene";
    Search search = search().filter(bitemporal("bitemporal").ttFrom("2015/03/01").ttTo("2015/03/01"));
    ResultSet rs = session.execute(QueryBuilder
                                    .select("name", "city", "vt_from", "vt_to", "tt_from", "tt_to")
                                    .from("test","census").where(eq(indexColumn, search.build()));



This code is available in CQL script here: `example_bitemporal.cql </doc/resources/example_bitemporal.cql>`__.

Boolean search
==============

Syntax:

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE <magic_column> = '{ (filter | query) : {
                               type     : "boolean",
                               ( must   : [(search,)?] , )?
                               ( should : [(search,)?] , )?
                               ( not    : [(search,)?] , )? } }';

where:

-  **must**: represents the conjunction of searches: search_1 AND search_2
   AND … AND search_n
-  **should**: represents the disjunction of searches: search_1 OR search_2
   OR … OR search_n
-  **not**: represents the negation of the disjunction of searches:
   NOT(search_1 OR search_2 OR … OR search_n)

Since "not" will be applied to the results of a "must" or "should"
condition, it can not be used in isolation.

Example 1: will return rows where name ends with “a” AND food starts
with “tu”

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                            type : "boolean",
                            must : [{type : "wildcard", field : "name", value : "*a"},
                                    {type : "wildcard", field : "food", value : "tu*"}]}}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(bool().must(wildcard("name","*a"),wildcard("food","tu*")));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


Example 2: will return rows where food starts with “tu” but name does
not end with “a”

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                            type : "boolean",
                            not  : [{type : "wildcard", field : "name", value : "*a"}],
                            must : [{type : "wildcard", field : "food", value : "tu*"}]}}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(bool().not(wildcard("name","*a")).must(wildcard("food","tu*")));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));

Example 3: will return rows where name ends with “a” or food starts with
“tu”

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                            type   : "boolean",
                            should : [{type : "wildcard", field : "name", value : "*a"},
                                      {type : "wildcard", field : "food", value : "tu*"}]}}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(bool().should(wildcard("name","*a"),wildcard("food","tu*")));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


Example 4: will return zero rows independently of the index contents

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type   : "boolean"} }';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(bool());
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


Example 5: will return rows where name does not end with “a”, which is
a resource-intensive pure negation search

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                            not  : [{type : "wildcard", field : "name", value : "*a"}]}}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(bool().not(wildcard("name","*a")));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


Contains search
===============

Syntax:

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE <magic_column> = '{ (filter | query) : {
                                type   : "contains",
                                field  : <fieldname> ,
                                values : <value_list> }}';

Example 1: will return rows where name matches “Alicia” or “mancha”

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                            type   : "contains",
                            field  : "name",
                            values : ["Alicia","mancha"] }}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(contains("name","Alicia","mancha"));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


Example 2: will return rows where date matches “2014/01/01″,
“2014/01/02″ or “2014/01/03″

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                            type   : "contains",
                            field  : "date",
                            values : ["2014/01/01", "2014/01/02", "2014/01/03"] }}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(contains("date","2014/01/01", "2014/01/02", "2014/01/03"));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


Date range search
=================

Syntax:

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE <magic_column> = '{ (filter | query) : {
                                type  : "date_range",
                                (from : <from> ,)?
                                (to   : <to> ,)?
                                (operation: <operation> )?
                              }}';

where:

-  **from**: a string or a number being the beginning of the date
   range.
-  **to**: a string or a number being the end of the date range.
-  **operation**: the spatial operation to be performed, it can be
   **intersects**, **contains** and **is\_within**.

Example 1: will return rows where duration intersects "2014/01/01" and
"2014/12/31"

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{ filter : {
                        type      : "date_range",
                        field     : "duration",
                        from      : "2014/01/01",
                        to        : "2014/12/31",
                        operation : "intersects"}}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(dateRange("duration").from("2014/01/01").to("2014/12/31")
                                    .operation("intersects"));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


Example 2: will return rows where duration contains "2014/06/01" and
"2014/06/02"

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{ filter : {
                        type      : "date_range",
                        field     : "duration",
                        from      : "2014/06/01",
                        to        : "2014/06/02",
                        operation : "contains"}}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(dateRange("duration").from("2014/06/01").to("2014/06/02")
                                    .operation("contains"));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


Example 3: will return rows where duration is within "2014/01/01" and
"2014/12/31"

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{ filter : {
                        type      : "date_range",
                        field     : "duration",
                        from      : "2014/01/01",
                        to        : "2014/12/31",
                        operation : "is_within"}}';


Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(dateRange("duration").from("2014/01/01").to("2014/12/31")
                                    .operation("is_within"));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


Fuzzy search
============

Syntax:

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE <magic_column> = '{ (filter | query) : {
                                type  : "fuzzy",
                                field : <fieldname> ,
                                value : <value>
                                (, max_edits      : <max_edits> )?
                                (, prefix_length  : <prefix_length> )?
                                (, max_expansions : <max_expansion> )?
                                (, transpositions : <transposition> )?
                              }}';

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

Example 1: will return any rows where “phrase” contains a word that
differs in one edit operation from “puma”, such as “pumas”.

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type      : "fuzzy",
                                     field     : "phrase",
                                     value     : "puma",
                                     max_edits : 1 }}';


Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(fuzzy("phrase","puma").maxEdits(1));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


Example 2: same as example 1 but will limit the results to rows where
phrase contains a word that starts with “pu”.

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type          : "fuzzy",
                                     field         : "phrase",
                                     value         : "puma",
                                     max_edits     : 1,
                                     prefix_length : 2 }}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(fuzzy("phrase","puma").maxEdits(1).prefixLength(2));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


Geo bbox search
===============

Syntax:

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE <magic_column> = '{ (filter | query) : {
                                type          : "geo_bbox",
                                field         : <fieldname>,
                                min_latitude  : <min_latitude> ,
                                max_latitude  : <max_latitude> ,
                                min_longitude : <min_longitude> ,
                                max_longitude : <max_longitude>
                              }}';

where:

-  **min\_latitude** : a double value between -90 and 90 being the min
   allowed latitude.
-  **max\_latitude** : a double value between -90 and 90 being the max
   allowed latitude.
-  **min\_longitude** : a double value between -180 and 180 being the
   min allowed longitude.
-  **max\_longitude** : a double value between -180 and 180 being the
   max allowed longitude.

Example 1: will return any rows where “place” is formed by a latitude
between -90.0 and 90.0, and a longitude between -180.0 and
180.0.

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type          : "geo_bbox",
                                     field         : "place",
                                     min_latitude  : -90.0,
                                     max_latitude  : 90.0,
                                     min_longitude : -180.0,
                                     max_longitude : 180.0 }}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(geoBBox("place",-180.0,180.0,-90.0,90.0));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


Example 2: will return any rows where “place” is formed by a latitude
between -90.0 and 90.0, and a longitude between 0.0 and
10.0.

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type          : "geo_bbox",
                                     field         : "place",
                                     min_latitude  : -90.0,
                                     max_latitude  : 90.0,
                                     min_longitude : 0.0,
                                     max_longitude : 10.0 }}';


Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(geoBBox("place",0.0,10.0,-90.0,90.0));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


Example 3: will return any rows where “place” is formed by a latitude
between 0.0 and 10.0, and a longitude between -180.0 and
180.0.

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type          : "geo_bbox",
                                     field         : "place",
                                     min_latitude  : 0.0,
                                     max_latitude  : 10.0,
                                     min_longitude : -180.0,
                                     max_longitude : 180.0 }}';


Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(geoBBox("place",-180.0,180.0,0.0,10.0));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


Geo distance search
===================

Syntax:

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE <magic_column> = '{ (filter | query) : {
                                type            : "geo_distance",
                                field           : <fieldname> ,
                                latitude        : <latitude> ,
                                longitude       : <longitude> ,
                                max_distance    : <max_distance>
                                (, min_distance : <min_distance> )?
                              }}';

where:

-  **latitude** : a double value between -90 and 90 being the latitude
   of the reference point.
-  **longitude** : a double value between -180 and 180 being the
   longitude of the reference point.
-  **max\_distance** : a string value being the max allowed distance
   from the reference point.
-  **min\_distance** : a string value being the min allowed distance
   from the reference point.

Example 1: will return any rows where “place” is within one kilometer
from the geo point (40.225479, -3.999278).

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type         : "geo_distance",
                                     field        : "place",
                                     latitude     : 40.225479,
                                     longitude    : -3.999278,
                                     max_distance : "1km"}}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(geoDistance("place",-3.999278d,40.225479d,"1km"));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


Example 2: will return any rows where “place” is within one yard and ten
yards from the geo point (40.225479, -3.999278).

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type         : "geo_distance",
                                     field        : "place",
                                     latitude     : 40.225479,
                                     longitude    : -3.999278,
                                     max_distance : "10yd" ,
                                     min_distance : "1yd" }}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(geoDistance("place",-3.999278d,40.225479d,"10yd")
                                    .minDistance("1yd"));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


Match search
============

Syntax:

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE <magic_column> = '{ (filter | query) : {
                                  type  : "match",
                                  field : <fieldname> ,
                                  value : <value> }}';

Example 1: will return rows where name matches “Alicia”

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                           type  : "match",
                           field : "name",
                           value : "Alicia" }}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(match("name","Alicia"));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


Example 2: will return rows where phrase contains “mancha”

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                           type  : "match",
                           field : "phrase",
                           value : "mancha" }}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(match("phrase","mancha"));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


Example 3: will return rows where date matches “2014/01/01″

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                           type  : "match",
                           field : "date",
                           value : "2014/01/01" }}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(match("date","2014/01/01"));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


None search
===========

Syntax:

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE <magic_column> = '{ (filter | query) : { type  : "none"} }';

Example: will return no one of the indexed rows

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type  : "none" } }';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(none());
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));

Phrase search
=============

Syntax:

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE <magic_column> = '{ (filter | query) : {
                                type  :"phrase",
                                field : <fieldname> ,
                                value : <value>
                                (, slop : <slop> )?
                            }}';

where:

-  **values**: an ordered list of values.
-  **slop** (default = 0): number of words permitted between words.

Example 1: will return rows where “phrase” contains the word “camisa”
followed by the word “manchada”.

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                          type   : "phrase",
                          field  : "phrase",
                          values : "camisa manchada" }}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(phrase("phrase","camisa manchada"));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));

Example 2: will return rows where “phrase” contains the word “mancha”
followed by the word “camisa” having 0 to 2 words in between.

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                          type   : "phrase",
                          field  : "phrase",
                          values : "mancha camisa",
                          slop   : 2 }}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(phrase("phrase","camisa manchada").slop(2));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));

Prefix search
=============

Syntax:

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE <magic_column> = '{ (filter | query) : {
                                type  : "prefix",
                                field : <fieldname> ,
                                value : <value> }}';

Example: will return rows where “phrase” contains a word starting with
“lu”. If the column is indexed as “text” and uses an analyzer, words
ignored by the analyzer will not be retrieved.

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                           type  : "prefix",
                           field : "phrase",
                           value : "lu" }}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(prefix("phrase","lu"));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));

Range search
============

Syntax:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{(filter | query) : {
                            type     : "range",
                            field    : <fieldname>
                            (, lower : <min> , include_lower : <min_included> )?
                            (, upper : <max> , include_upper : <max_included> )?
                         }}';

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

Example 1: will return rows where *age* is in [1, ∞)

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                            type          : "range",
                            field         : "age",
                            lower         : 1,
                            include_lower : true }}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(range("age").lower(1).includeLower(true));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));

Example 2: will return rows where *age* is in (-∞, 0]

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                            type          : "range",
                            field         : "age",
                            upper         : 0,
                            include_upper : true }}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(range("age").upper(0).includeUpper(true));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));

Example 3: will return rows where *age* is in [-1, 1]

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                            type          : "range",
                            field         : "age",
                            lower         : -1,
                            upper         : 1,
                            include_lower : true,
                            include_upper : true }}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(range("age").lower(-1).upper(1)
                                    .includeLower(true).includeUpper(true));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));

Example 4: will return rows where *date* is in [2014/01/01, 2014/01/02]

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                            type          : "range",
                            field         : "date",
                            lower         : "2014/01/01",
                            upper         : "2014/01/02",
                            include_lower : true,
                            include_upper : true }}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(range("date").lower("2014/01/01").upper( "2014/01/02")
                                    .includeLower(true).includeUpper(true));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));

Regexp search
=============

Syntax:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{(filter | query) : {
                            type  : "regexp",
                            field : <fieldname>,
                            value : <regexp>
                         }}';

where:

-  **value**: a regular expression. See
   `org.apache.lucene.util.automaton.RegExp <http://lucene.apache.org/core/4_6_1/core/org/apache/lucene/util/automaton/RegExp.html>`__
   for syntax reference.

Example: will return rows where name contains a word that starts with
“p” and a vowel repeated twice (e.g. “pape”).

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                           type  : "regexp",
                           field : "name",
                           value : "[J][aeiou]{2}.*" }}';

Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(regexp("name","[J][aeiou]{2}.*"));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));

Wildcard search
===============

Syntax:

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{(filter | query) : {
                            type  : "wildcard" ,
                            field : <fieldname> ,
                            value : <wildcard_exp>
                         }}';

where:

-  **value**: a wildcard expression. Supported wildcards are \*, which
   matches any character sequence (including the empty one), and ?,
   which matches any single character. ” is the escape character.

Example: will return rows where food starts with or is “tu”.

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                           type  : "wildcard",
                           field : "food",
                           value : "tu*" }}';


Using Builder

.. code-block:: java

    import static com.stratio.cassandra.lucene.builder.Builder.*;
    (...)
    String indexColumn = "stratio_col";
    Search search = search().filter(wildcard("food","tu*"));
    ResultSet rs = session.execute(QueryBuilder.select().all().from("test","users")
                                    .where(eq(indexColumn, search.build()));


User Defined Types
******************

Since Cassandra 2.2.X users can declare User Defined Types as follows:

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
        address address_udt,
        lucene text
    );



and use it like a native CQL type.

Indexing part of this UDT is allowed just using the "." operator as follows:

.. code-block:: sql

    CREATE CUSTOM INDEX test_index ON test.user_profiles(lucene)
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


and searching:

.. code-block:: sql

    SELECT * FROM user_profiles
    WHERE lucene='{
        filter : {
            type  : "match",
            field : "address.city",
            value : "San Fransisco"
        }
    }';

or:

.. code-block:: sql

    SELECT * FROM user_profiles
    WHERE lucene='{
        filter : {
            type  : "range",
            field : "address.zip",
            lower : 0,
            upper : 10
        }
    }';

Collections
***********

It is allowed to index collections as well

List ans Sets are indexed so:

.. code-block:: sql

    CREATE TABLE user_profiles (
        login text PRIMARY KEY,
        first_name text,
        last_name text,
        cities list<text>,
        lucene text
    );

    CREATE CUSTOM INDEX test_index ON test.user_profiles(lucene)
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                cities : { type : "string"}
            }
        }'
    };


and searches:

.. code-block:: sql

    SELECT * FROM user_profiles
    WHERE lucene='{
        filter : {
            type  : "match",
            field : "cities",
            value : "San Francisco"
        }
    }';


Map values are indexed by Key value so:

.. code-block:: sql

    CREATE TABLE user_profiles (
        login text PRIMARY KEY,
        first_name text,
        last_name text,
        addresses map<text,text>,
        lucene text
    );

    CREATE CUSTOM INDEX test_index ON test.user_profiles(lucene)
    USING 'com.stratio.cassandra.lucene.Index'
    WITH OPTIONS = {
        'refresh_seconds' : '1',
        'schema' : '{
            fields : {
                addresses : { type : "string"}
            }
        }'
    };

and searches using $key:

.. code-block:: sql

    INSERT INTO user_profiles (login,first_name,last_name,addresses)
        VALUES('user','Peter','Handsome',
                {'San Francisco':'Market street 2', 'Madrid': 'Calle Velazquez' })

    SELECT * FROM user_profiles
    WHERE lucene='{
        filter : {
            type  : "match",
            field : "cities$Madrid",
            value : "San Francisco"
        }
    }';

Do NOT set map keys including characters like '.' or '$'


Indexing UDT inside collections are allowed too using the point operator

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
        address list<frozen<address_udt>>,
        lucene text
    );

    CREATE CUSTOM INDEX test_index ON test.user_profiles(lucene)
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
    session.execute(index("messages", "lucene").name("my_index")
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
    Search search = search().filter(match("user", "adelapena"))
                            .query(phrase("message", "cassandra rules"))
                            .sort(field("date").reverse(true))
                            .refresh(true);
    ResultSet rs = session.execute(select().from("table").where(eq(indexColumn, search.build()));

Spark and Hadoop
****************

Spark and Hadoop integrations are fully supported because Lucene searches
can be combined with token range restrictions and pagination, which are the
basis of MapReduce frameworks support.

Token Range Searches
====================

The token function allows computing the token for a given partition key.
The primary key of the example table “users” is ((name, gender), animal,
age) where (name, gender) is the partition key. When combining the token
function and a Lucene-based filter in a where clause, the filter on
tokens is applied first and then the condition of the filter clause.

Example: will retrieve rows which tokens are greater than (‘Alicia’,
‘female’) and then test them against the match condition.

.. code-block:: sql

    SELECT name,gender
      FROM test.users
     WHERE stratio_col='{filter : {type : "match", field : "food", value : "chips"}}'
       AND token(name, gender) > token('Alicia', 'female');

Paging
======

Paging filtered results is fully supported. You can retrieve
the rows starting from a certain key. For example, if the primary key is
(userid, createdAt), you can search:

.. code-block:: sql

    SELECT *
      FROM tweets
      WHERE stratio_col = ‘{ filter : {type:”match",  field:”text", value:”cassandra”} }’
        AND userid = 3543534
        AND createdAt > 2011-02-03 04:05+0000
      LIMIT 5000;

JMX Interface
*************

The existing Lucene indexes expose some attributes and operations
through JMX, using the same MBean server as Apache Cassandra. The MBeans
provided by Stratio are under the domain
**com.stratio.cassandra.lucene**.

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
    WHERE lucene = '{filter : {
                      type  : "match",
                      field : "name",
                      value : "Alice" }}';

However, this search could be a good use case for Lucene just because there is no easy counterpart:

.. code-block:: sql

    SELECT * FROM users
    WHERE lucene = '{filter : {
                       type : "boolean",
                       must : [{type : "regexp", field : "name", value : "[J][aeiou]{2}.*"},
                               {type:"range", field:"birthday",
                                lower:"2014/04/25",upper:"2014/05/01"}]}}';

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

    CREATE CUSTOM INDEX tweets_index ON tweets (lucene)
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

    CREATE CUSTOM INDEX tweets_index ON tweets (lucene)
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

Use a large page size
=====================

Cassandra native paging is fully supported even for top-k queries,
and we do not discourage its use in any way.
However getting - rows in a page is always faster than retrieving the same n rows in two or more pages.
For that reason, if you are interested in retrieving the best 200 rows matching a search,
then you should ideally use a page size of 200.
On the other hand, if you want to retrieve thousands or millions of rows,
then you should use a high page size, maybe 1000 rows per page.
Page size can be set in cqlsh in a per-session basis using the command `PAGING``
and in Java driver its set in a per-query basis using the attribute `pageSize``.
