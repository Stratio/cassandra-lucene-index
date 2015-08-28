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
                               {type:"range", field:"birthday", lower:"2014/04/25", upper:"2014/05/1"}]}}';

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
However getting n rows in a page is always faster than retrieving the same n rows in two or more pages.
For that reason, if you are interested in retrieving the best 200 rows matching a search,
then you should ideally use a page size of 200.
On the other hand, if you want to retrieve thousands or millions of rows,
then you should use a high page size, maybe 1000 rows per page.
Page size can be set in cqlsh in a per-session basis using the command `PAGING``
and in Java driver its set in a per-query basis using the attribute `pageSize``.
