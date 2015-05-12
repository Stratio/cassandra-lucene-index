Stratio Cassandra
=================

Stratio's Cassandra Lucene Index, derived from [Stratio Cassandra](https://github.com/Stratio/stratio-cassandra), is a plugin for [Apache Cassandra](http://cassandra.apache.org/) that extends its index functionality to provide near real time search such as ElasticSearch or Solr, including [full text search](http://en.wikipedia.org/wiki/Full_text_search) capabilities and free multivariable search. It is achieved through an [Apache Lucene](http://lucene.apache.org/) based implementation of Cassandra secondary indexes, where each node of the cluster indexes its own data. Stratio's Cassandra indexes are one of the core modules on which [Stratio's BigData platform (SDS)](http://www.stratio.com/) is based.

Index [relevance queries](http://en.wikipedia.org/wiki/Relevance_(information_retrieval)) allows you to retrieve the *n* more relevant results satisfying a query. The coordinator node sends the query to each node in the cluster, each node returns its *n* best results and then the coordinator combines these partial results and gives you the *n* best of them, avoiding full scan. You can also base the sorting in a combination of fields.

Index filtered queries are a powerful help when analyzing the data stored in Cassandra with [MapReduce](http://es.wikipedia.org/wiki/MapReduce) frameworks as [Apache Hadoop](http://hadoop.apache.org/) or, even better, [Apache Spark](http://spark.apache.org/) through [Stratio Deep](https://github.com/Stratio/stratio-deep). Adding Lucene filters in the jobs input can dramatically reduce the amount of data to be processed, avoiding full scan.

Any cell in the tables can be indexed, including those in the primary key as well as collections. Wide rows are also supported. You can scan token/key ranges, apply additional CQL3 clauses and page on the filtered results.

More detailed information is available at [Stratio's Cassandra Lucene Index documentation](doc/extended-search-in-cassandra.md) .

Features
--------

Stratio's Cassandra Lucene Index and its integration with Lucene search technology provides:

-   Big data full text search
-   Relevance scoring and sorting
-   General top-k queries
-   Complex boolean queries (and, or, not)
-   Near real-time search
-   Custom analyzers
-   CQL3 support
-   Wide rows support
-   Partition and cluster composite keys support
-   Support for indexing columns part of primary key
-   Third-party drivers compatibility
-   Spark compatibility
-   Hadoop compatibility

Not yet supported:

-   Thrift API
-   Legacy compact storage option
-   Indexing `counter` columns
-   Columns with TTL
-   CQL user defined types
-   Static columns

Requirements
------------

  * Cassandra 2.1.6
  * Java >= 1.7 (OpenJDK and Sun have been tested)
  * Maven >= 3.0

Build and install
-----------------

Stratio's Cassandra Lucene Index is distributed as a plugin for Apache Cassandra. Thus, you just need to build a JAR containing the plugin and add it to the Cassandra's classpath:

  * Build the plugin with Maven: ```mvn clean package```
  * Copy the generated JAR to the lib folder of your comaptible Cassandra installation: ```cp lib/cassandra-lucene-index-2.1.6.0.jar <CASSANDRA_HOME>/lib/```
  * Start/restart Cassandra as usual
  
Patching can also be done with this Maven profile, specifying the path of your Cassandra installation:

```
mvn clean package -Ppatch -Dcassandra_home=<CASSANDRA_HOME>
```
  
Alternatively, if you don't have an installed version of Cassandra, there is a profile to let Maven download and patch the proper version of Apache Cassandra:

```
mvn clean package -Pdownload_and_patch -Dcassandra_home=<CASSANDRA_HOME>
```

Now you can run Cassandra and do some tests using the Cassandra Query Language:

```
<CASSANDRA_HOME>/bin/cassandra -f
<CASSANDRA_HOME>/bin/cqlsh
```

The Lucene's index files will be stored in the same directories where the Cassandra's will be. The default data directory is `/var/lib/cassandra/data`, and each index is placed next to the SSTables of its indexed column family. 

For more details about Apache Cassandra please see its [documentation](http://cassandra.apache.org/).

Example
-------

We will create the following table to store tweets:

```
CREATE KEYSPACE demo
WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1};
USE demo;
CREATE TABLE tweets (
    id INT PRIMARY KEY,
    user TEXT,
    body TEXT,
    time TIMESTAMP,
    lucene TEXT
);
```

We have created a column called *lucene* to link the index queries. This column will not store data. Now you can create a custom Lucene index on it with the following statement:

```
CREATE CUSTOM INDEX tweets_index ON tweets (lucene) 
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = {
    'refresh_seconds' : '1',
    'schema' : '{
        fields : {
            id   : {type : "integer"},
            user : {type : "string"},
            body : {type : "text",  analyzer : "english"},
            time : {type : "date", pattern  : "yyyy/MM/dd"}
        }
    }'
};
```

This will index all the columns in the table with the specified types, and it will be refreshed once per second.

Now, to query the top 100 more relevant tweets where *body* field contains the phrase "big data gives organizations":

```
SELECT * FROM tweets WHERE lucene='{
	query : {type:"phrase", field:"body", values:["big","data","gives","organizations"]}
}' limit 100;
```
To restrict the search for tweets within a certain date range, then you must add to the search a filter as follows:

```
SELECT * FROM tweets WHERE lucene='{
    filter : {type:"range", field:"time", lower:"2014/04/25", upper:"2014/04/1"},
    query  : {type:"phrase", field:"body", values:["big","data","gives","organizations"]}
}' limit 100;
```
To refine the search to get only the tweets written by users whose name starts with "a":

```
SELECT * FROM tweets WHERE lucene='{
    filter : {type:"boolean", must:[
                   {type:"range", field:"time", lower:"2014/04/25", upper:"2014/04/1"},
                   {type:"prefix", field:"user", value:"a"} ] },
    query  : {type:"phrase", field:"body", values:["big","data","gives","organizations"]}
}' limit 100;
```

To get the 100 more recent filtered results you can use the *sort* option:

```
SELECT * FROM tweets WHERE lucene='{
    filter : {type:"boolean", must:[
                   {type:"range", field:"time", lower:"2014/04/25", upper:"2014/04/1"},
                   {type:"prefix", field:"user", value:"a"} ] },
    query  : {type:"phrase", field:"body", values:["big","data","gives","organizations"]},
    sort  : {fields: [ {field:"time", reverse:true} ] }
}' limit 100;
```

Finally, if you want to restrict the search to a certain token range:

```
SELECT * FROM tweets WHERE lucene='{
    filter : {type:"boolean", must:[
                   {type:"range", field:"time", lower:"2014/04/25", upper:"2014/04/1"},
                   {type:"prefix", field:"user", value:"a"} ] },
    query  : {type:"phrase", field:"body", values:["big","data","gives","organizations"]}
}' AND token(id) >= token(0) AND token(id) < token(10000000) limit 100;
```

This last is the basis for Hadoop, Spark and other MapReduce frameworks support.

Please, refer to the comprehensive [Stratio's Cassandra Lucene Index documentation](doc/extended-search-in-cassandra.md).
