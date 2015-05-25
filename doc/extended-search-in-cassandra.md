Extended Search in Cassandra
============================

[Cassandra](http://cassandra.apache.org/ "Apache Cassandra project") index functionality has been extended to provide near real time search such as [ElasticSearch](http://www.elasticsearch.org/ "ElasticSearch project") or [Solr](https://lucene.apache.org/solr/ "Apache Solr project"), including full text search capabilities and free multivariable search.

It is also fully compatible with [Apache Spark](https://spark.apache.org/) and [Apache Hadoop](https://hadoop.apache.org/), allowing you to filter data at database level. This speeds up jobs reducing the amount of data to be collected and processed.

Indexing is achieved through a Lucene based implementation of Cassandra secondary indexes, where each node of the cluster indexes its own data. Stratio Cassandra is one of the core modules on which Stratio's BigData platform (SDS) is based.

Table of Contents
-----------------

-   [Overview](#overview)
-   [Index creation](#index-creation)
-   [Queries](#queries)
    -   [Boolean](#boolean-query)
    -   [Contains](#contains-query)
    -   [Fuzzy](#fuzzy-query)
    -   [Match](#match-query)
    -   [Match all](#match-all-query)
    -   [Phrase](#phrase-query)
    -   [Prefix](#prefix-query)
    -   [Range](#range-query)
    -   [Regexp](#regexp-query)
    -   [Wildcard](#wildcard-query)
-   [Spark and Hadoop Integration](#spark-and-hadoop-integration)
    -   [Token Function](#token-function)
    -   [Server Side Filtering](#server-side-filtering)
-   [Datatypes Mapping](#datatypes-mapping)
    -   [CQL to Field type](#cql-to-field-type)
    -   [Field type to CQL](#field-type-to-cql)
-   [JMX Interface](#jmx-interface)

Overview
--------

Lucene search technology integration into Cassandra provides:

-   Full text search
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

Installation
------------

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


Index Creation
--------------

###Syntax

```sql
CREATE CUSTOM INDEX (IF NOT EXISTS)? <index_name>
                                  ON <table_name> ( <magic_column> )
                               USING 'com.stratio.cassandra.lucene.Index'
                        WITH OPTIONS = <options>
```

where:

-   &lt;magic_column> is the name of a text column that does not contain any data and will be used to show the scoring for each resulting row of a query,
-   &lt;options> is a JSON object:

```sql
<options> := { ('refresh_seconds'      : '<int_value>',)?
               ('ram_buffer_mb'        : '<int_value>',)?
               ('max_merge_mb'         : '<int_value>',)?
               ('max_cached_mb'        : '<int_value>',)?
               ('indexing_threads'     : '<int_value>',)?
               ('indexing_queues_size' : '<int_value>',)?
               'schema'                : '<schema_definition>'};
```

Options, except “schema”, take a positive integer value enclosed in single quotes:

-   **refresh_seconds**: number of seconds before refreshing the index (between writers and readers). Defaults to ’60’.
-   **ram_buffer_mb**: size of the write buffer. Its content will be committed to disk when full. Defaults to ’64’.
-   **max_merge_mb**: defaults to ’5’.
-   **max_cached_mb**: defaults to ’30’.
-   **indexing_threads**: number of asynchronous indexing threads. ’0’ means synchronous indexing. Defaults to ’0’.
-   **indexing_queues_size**: max number of queued documents per asynchronous indexing thread. Defaults to ’50’.
-   **schema**: see below

```sql
<schema_definition> := {
    (analyzers : { <analyzer_definition> (, <analyzer_definition>)* } ,)?
    (default_analyzer : "<analyzer_name>",)?
    fields : { <field_definition> (, <field_definition>)* }
}
```

Where default_analyzer defaults to ‘org.apache.lucene.analysis.standard.StandardAnalyzer’.

```sql
<analyzer_definition> := <analyzer_name> : {
    type : "<analyzer_type>" (, <option> : "<value>")*
}
```

Analyzer definition options depend on the analyzer type. Details and default values are listed in the table below.

<table>
    <thead>
    <tr>
        <th>Analyzer type</th>
        <th>Option</th>
        <th>Value type</th>
        <th>Default value</th>
    </tr>
    </thead>
    <tbody>
    <tr>
        <td>classpath</td>
        <td>class</td>
        <td>string</td>
        <td>null</td>
    </tr>
    <tr>
        <td>snowball</td>
        <td>language</td>
        <td>string</td>
        <td>null</td>
    </tr>
    <tr>
        <td></td>
        <td>stopwords</td>
        <td>string</td>
        <td>null</td>
    </tr>
    </tbody>
</table>

```sql
<field_definition> := <column_name> : {
    type : "<field_type>" (, <option> : "<value>")*
}
```

Field mapping definition options depend on the field type. Details and default values are listed in the table below.

<table>
    <thead>
    <tr>
        <th>Field type</th>
        <th>Option</th>
        <th>Value type</th>
        <th>Default value</th>
    </tr>
    </thead>
    <tbody>
    <tr>
        <td rowspan="4">bigdec</td>
        <td>indexed</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>sorted</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>integer_digits</td>
        <td>positive integer</td>
        <td>32</td>
    </tr>
    <tr>
        <td>decimal_digits</td>
        <td>positive integer</td>
        <td>32</td>
    </tr>
    <tr>
        <td rowspan="3">bigint</td>
        <td>indexed</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>sorted</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>digits</td>
        <td>positive integer</td>
        <td>32</td>
    </tr>
    <tr>
        <td rowspan="2">blob</td>
        <td>indexed</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>sorted</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td rowspan="2">boolean</td>
        <td>indexed</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>sorted</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td rowspan="3">date</td>
        <td>indexed</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>sorted</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>pattern</td>
        <td>date format (string)</td>
        <td>yyyy/MM/dd HH:mm:ss.SSS</td>
    </tr>
    <tr>
        <td rowspan="3">double</td>
        <td>indexed</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>sorted</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>boost</td>
        <td>float</td>
        <td>0.1f</td>
    </tr>
    <tr>
        <td rowspan="3">float</td>
        <td>indexed</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>sorted</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>boost</td>
        <td>float</td>
        <td>0.1f</td>
    </tr>
    <tr>
        <td rowspan="2">inet</td>
        <td>indexed</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>sorted</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td rowspan="3">integer</td>
        <td>indexed</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>sorted</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>boost</td>
        <td>float</td>
        <td>0.1f</td>
    </tr>
    <tr>
        <td rowspan="3">long</td>
        <td>indexed</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>sorted</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>boost</td>
        <td>float</td>
        <td>0.1f</td>
    </tr>
    <tr>
        <td rowspan="2">string</td>
        <td>indexed</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>sorted</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td rowspan="3">text</td>
        <td>indexed</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>sorted</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>analyzer</td>
        <td>class name (string)</td>
        <td>default_analyzer of the schema</td>
    </tr>
    <tr>
        <td rowspan="2">uuid</td>
        <td>indexed</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    <tr>
        <td>sorted</td>
        <td>boolean</td>
        <td>true</td>
    </tr>
    </tbody>
</table>

All mapping definitions has an “**indexed**” option indicating if the field is searchable. There is also a “**sorted**” option specifying if it is possible to sort rows by the corresponding field. Both fields are true by default, but they should be set to false when no needed in order to have a smaller and faster index. 

Note that Cassandra allows one custom index per table. On the other hand, Cassandra does not allow a modify 
operation on indexes. To modify an index it needs to be deleted first and created again.

###Example

This code below and the one for creating the corresponding keyspace and table is available in a CQL script that 
can be sourced from the Cassandra shell: 
[test-users-create.cql](resources/test-users-create.cql "Download CQL script for creating keyspace, table and index").

```sql
CREATE CUSTOM INDEX IF NOT EXISTS users_index
ON test.users (stratio_col)
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = {
    'refresh_seconds'      : '1',
    'ram_buffer_mb'        : '64',
    'max_merge_mb'         : '5',
    'max_cached_mb'        : '30',
    'indexing_threads'     : '4',
    'indexing_queues_size' : '50',
    'schema' : '{
        analyzers : {
              my_custom_analyzer : {
                  type:"snowball",
                  language:"Spanish",
                  stopwords : "el,la,lo,loas,las,a,ante,bajo,cabe,con,contra"}
        },
        default_analyzer : "english",
        fields : {
            name   : {type     : "string"},
            gender : {type     : "string", sorted: "false"},
            animal : {type     : "string"},
            age    : {type     : "integer"},
            food   : {type     : "string"},
            number : {type     : "integer"},
            bool   : {type     : "boolean"},
            date   : {type     : "date", pattern  : "yyyy/MM/dd"},
            mapz   : {type     : "string", sorted: "false"},
            setz   : {type     : "string", sorted: "false"},
            listz  : {type     : "string"},
            phrase : {type     : "text", analyzer : "my_custom_analyzer"}
        }
    }'
};
```

Queries
-------

###Syntax:

```sql
SELECT ( <fields> | * )
FROM <table_name>
WHERE <magic_column> = '{ (   query  : <query>  )?
                          ( , filter : <filter> )?
                          ( , sort   : <sort>   )?
                        }';
```

where &lt;query> and &lt;filter> are a JSON object:

```sql
<query> := { type : <type> (, <option> : ( <value> | <value_list> ) )+ }
```

and &lt;sort> is another JSON object:

```sql
    <sort> := { fields : <sort_field> (, <sort_field> )* }
    <sort_field> := { field : <field> (, reverse : <reverse> )? }
```

When searching by &lt;query>, results are returned ***sorted by descending relevance*** without pagination. The results will be located in the column ‘stratio_relevance’.

Filter types and options are the same as the query ones. The difference with queries is that filters have no effect on scoring.

Sort option is used to specify the order in which the indexed rows will be traversed. When sorting is used, the query scoring is delayed.

If no query or sorting options are specified then the results are returned in the Cassandra’s natural order, which is defined by the partitioner and the column name comparator.

Types of query and their options are summarized in the table below. Details for each of them are available in individual sections and the examples can be downloaded as a CQL script: [extended-search-examples.cql](resources/extended-search-examples.cql "Download CQL script of examples").

In addition to the options described in the table, all query types have a “**boost**” option that acts as a weight on the resulting score.

<table>
<col width="33%" />
<col width="33%" />
<col width="33%" />
<thead>
<tr class="header">
<th align="left">Query type</th>
<th align="left">Supported Field type</th>
<th align="left">Options</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td align="left"><a href="#boolean-query" title="Boolean query details">Boolean</a></td>
<td align="left">subqueries</td>
<td align="left"><ul>
<li><strong>must</strong>: a list of conditions.</li>
<li><strong>should</strong>: a list of conditions.</li>
<li><strong>not</strong>: a list of conditions.</li>
</ul></td>
</tr>
<tr class="even">
<td align="left"><a href="#contains-query" title="Contains query details">Contains</a></td>
<td align="left">All</td>
<td align="left"><ul>
<li><strong>field</strong>: the field name.</li>
<li><strong>values</strong>: the matched field values.</li>
</ul></td>
</tr>
<tr class="odd">
<td align="left"><a href="#fuzzy-query">Fuzzy</a></td>
<td align="left">bytes<br /> inet<br /> string<br /> text</td>
<td align="left"><ul>
<li><strong>field</strong>: the field name.</li>
<li><strong>value</strong>: the field value.</li>
<li><strong>max_edits</strong> (default = 2): a integer value between 0 and 2 (the <a href="http://en.wikipedia.org/wiki/Levenshtein_automaton" title="Wikipedia article on Levenshtein Automaton">Levenshtein automaton</a> maximum supported distance).</li>
<li><strong>prefix_length</strong> (default = 0): integer representing the length of common non-fuzzy prefix.</li>
<li><strong>max_expansions</strong> (default = 50): an integer for the maximum number of terms to match.</li>
<li><strong>transpositions</strong> (default = true): if transpositions should be treated as a primitive edit operation (<a href="http://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance" title="Wikipedia article on Damerau-Levenshtein Distance">Damerau-Levenshtein distance</a>). When false, comparisons will implement the classic <a href="http://en.wikipedia.org/wiki/Levenshtein_distance" title="Wikipedia article on Levenshtein Distance">Levenshtein distance</a>.</li>
</ul></td>
</tr>
<tr class="even">
<td align="left"><a href="#match-query">Match</a></td>
<td align="left">All</td>
<td align="left"><ul>
<li><strong>field</strong>: the field name.</li>
<li><strong>value</strong>: the field value.</li>
</ul></td>
</tr>
</tr>
<tr class="even">
<td align="left"><a href="#match-all-query">Match all</a></td>
<td align="left">All</td>
<td align="left"></td>
</tr>
<tr class="odd">
<td align="left"><a href="#phrase-query">Phrase</a></td>
<td align="left">bytes<br /> inet<br /> text</td>
<td align="left"><ul>
<li><strong>field</strong>: the field name.</li>
<li><strong>values</strong>: list of values.</li>
<li><strong>slop</strong> (default = 0): number of other words permitted between words.</li>
</ul></td>
</tr>
<tr class="even">
<td align="left"><a href="#prefix-query">Prefix</a></td>
<td align="left">bytes<br /> inet<br /> string<br /> text</td>
<td align="left"><ul>
<li><strong>field</strong>: fieldname.</li>
<li><strong>value</strong>: fieldvalue.</li>
</ul></td>
</tr>
<tr class="odd">
<td align="left"><a href="#range-query">Range</a></td>
<td align="left">All</td>
<td align="left"><ul>
<li><strong>field</strong>: field name.</li>
<li><strong>lower</strong> (default = $-\infty$ for number): lower bound of the range.</li>
<li><strong>include_lower</strong> (default = false): if the left value is included in the results (>=)</li>
<li><strong>upper</strong> (default = $+\infty$ for number): upper bound of the range.</li>
<li><strong>include_upper</strong> (default = false): if the right value is included in the results (&lt;=).</li>
</ul></td>
</tr>
<tr class="even">
<td align="left"><a href="#regexp-query">Regexp</a></td>
<td align="left">bytes<br /> inet<br /> string<br /> text</td>
<td align="left"><ul>
<li><strong>field</strong>: fieldname.</li>
<li><strong>value</strong>: regular expression.</li>
</ul></td>
</tr>
<tr class="odd">
<td align="left"><a href="#wildcard-query">Wildcard</a></td>
<td align="left">bytes<br /> inet<br /> string<br /> text</td>
<td align="left"><ul>
<li><strong>field</strong>: field name.</li>
<li><strong>value</strong>: wildcard expression.</li>
</ul></td>
</tr>
</tbody>
</table>

###Boolean query

Syntax:

```sql
SELECT ( <fields> | * )
FROM <table>
WHERE <magic_column> = '{ query : {
                           type     : "boolean",
                           ( must   : [(query,)?] , )?
                           ( should : [(query,)?] , )?
                           ( not    : [(query,)?] , )? } }';
```

where:

-   **must**: represents the conjunction of queries: query<sub>1</sub> AND query<sub>2</sub> AND … AND query<sub>n</sub>
-   **should**: represents the disjunction of queries: query<sub>1</sub> OR query<sub>12</sub> OR … OR query<sub>n</sub>
-   **not**: represents the negation of the disjunction of queries: NOT(query<sub>1</sub> OR query<sub>2</sub> OR … OR query<sub>n</sub>)

Since "not" will be applied to the results of a "must" or "should" condition, it can not be used in isolation.

Example 1: will return rows where name ends with “a” AND food starts with “tu”

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type : "boolean",
                        must : [{type : "wildcard", field : "name", value : "*a"},
                                {type : "wildcard", field : "food", value : "tu*"}]}}';
```

Example 2: will return rows where food starts with “tu” but name does not end with “a”

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type : "boolean",
                        not  : [{type : "wildcard", field : "name", value : "*a"}],
                        must : [{type : "wildcard", field : "food", value : "tu*"}]}}';
```

Example 3: will return rows where name ends with “a” or food starts with “tu”

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type   : "boolean",
                        should : [{type : "wildcard", field : "name", value : "*a"},
                                  {type : "wildcard", field : "food", value : "tu*"}]}}';
```

###Contains query

Syntax:

```sql
SELECT ( <fields> | * )
FROM <table>
WHERE <magic_column> = '{ query : {
                            type  : "contains",
                            field : <fieldname> ,
                            values : <value_list> }}';
```

Example 1: will return rows where name matches “Alicia” or “mancha”

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type   : "contains",
                        field  : "name",
                        values : ["Alicia","mancha"] }}';
```

Example 2: will return rows where date matches “2014/01/01″, “2014/01/02″ or “2014/01/03″

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type   : "contains",
                        field  : "date",
                        values : ["2014/01/01", "2014/01/02", "2014/01/03"] }}';
```

###Fuzzy query

Syntax:

```sql
SELECT ( <fields> | * )
FROM <table>
WHERE <magic_column> = '{ query : {
                            type  : "fuzzy",
                            field : <fieldname> ,
                            value : <value>
                            (, max_edits     : <max_edits> )?
                            (, prefix_length : <prefix_length> )?
                            (, max_expansions: <max_expansion> )?
                            (, transpositions: <transposition> )?
                          }}';
```

where:

-   **max_edits** (default = 2): a integer value between 0 and 2. Will return rows which distance from &lt;value> to &lt;field> content has a distance of at most &lt;max_edits>. Distance will be interpreted according to the value of “transpositions”.
-   **prefix_length** (default = 0): an integer value being the length of the common non-fuzzy prefix
-   **max_expansions** (default = 50): an integer for the maximum number of terms to match
-   **transpositions** (default = true): if transpositions should be treated as a primitive edit operation ([Damerau-Levenshtein distance](http://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance "Wikipedia article on Damerau-Levenshtein Distance")). When false, comparisons will implement the classic [Levenshtein distance](http://en.wikipedia.org/wiki/Levenshtein_distance "Wikipedia article on Levenshtein Distance").

Example 1: will return any rows where “phrase” contains a word that differs in one edit operation from “puma”, such as “pumas”.

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : { type      : "fuzzy",
                                field     : "phrase",
                                value     : "puma",
                                max_edits : 1 }}';
```

Example 2: same as example 1 but will limit the results to rows where phrase contains a word that starts with “pu”.

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : { type          : "fuzzy",
                                field         : "phrase",
                                value         : "puma",
                                max_edits     : 1,
                                prefix_length : 2 }}';
```

###Match query

Syntax:

```sql
SELECT ( <fields> | * )
FROM <table>
WHERE <magic_column> = '{ query : {
                            type  : "match",
                            field : <fieldname> ,
                            value : <value> }}';
```

Example 1: will return rows where name matches “Alicia”

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type  : "match",
                        field : "name",
                        value : "Alicia" }}';
```

Example 2: will return rows where phrase contains “mancha”

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type  : "match",
                        field : "phrase",
                        value : "mancha" }}';
```

Example 3: will return rows where date matches “2014/01/01″

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type  : "match",
                        field : "date",
                        value : "2014/01/01" }}';
```

###Match all query

Syntax:

```sql
SELECT ( <fields> | * )
FROM <table>
WHERE <magic_column> = '{ query : {
                            type  : "match_all",
                            field : <fieldname> ,
                            value : <value> }}';
```

Example: will return all the indexed rows

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type  : "match_all" }}';
```

###Phrase query

Syntax:

```sql
SELECT ( <fields> | * )
FROM <table>
WHERE <magic_column> = '{ query : {
                            type  :"phrase",
                            field : <fieldname> ,
                            values : <value_list>
                            (, slop : <slop> )?
                        }}';
```

where:

-   **values**: an ordered list of values.
-   **slop** (default = 0): number of words permitted between words.

Example 1: will return rows where “phrase” contains the word “camisa” followed by the word “manchada”.

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type   : "phrase",
                        field  : "phrase",
                        values : ["camisa", "manchada"] }}';
```

Example 2: will return rows where “phrase” contains the word “mancha” followed by the word “camisa” having 0 to 2 words in between.

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type   : "phrase",
                        field  : "phrase",
                        values : ["mancha", "camisa"],
                        slop   : 2 }}';
```

###Prefix query

Syntax:

```sql
SELECT ( <fields> | * )
FROM <table>
WHERE <magic_column> = '{ query : {
                            type  : "prefix",
                            field : <fieldname> ,
                            value : <value> }}';
```

Example: will return rows where “phrase” contains a word starting with “lu”. If the column is indexed as “text” and uses an analyzer, words ignored by the analyzer will not be retrieved.

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type          : "prefix",
                        field         : "phrase",
                        value         : "lu" }}';
```

###Range query

Syntax:

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type    : "range",
                        field   : <fieldname>
                        (, lower : <min> , include_lower : <min_included> )?
                        (, upper : <max> , include_upper : <max_included> )?
                     }}';
```

where:

-   **lower**: lower bound of the range.
-   **include_lower** (default = false): if the lower bound is included (left-closed range).
-   **upper**: upper bound of the range.
-   **include_upper** (default = false): if the upper bound is included (right-closed range).

Lower and upper will default to $-/+\\infty$ for number. In the case of byte and string like 
data (bytes, inet, string, text), all values from lower up to upper will be returned if both 
are specified. If only “lower” is specified, all rows with values from “lower” will be returned. 
If only “upper” is specified then all rows with field values up to “upper” will be returned. If 
both are omitted than all rows will be returned.

Example 1: will return rows where *age* is in [1, ∞)

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type          : "range",
                        field         : "age",
                        lower         : 1,
                        include_lower : true }}';
```

Example 2: will return rows where *age* is in (-∞, 0]

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type          : "range",
                        field         : "age",
                        upper         : 0,
                        include_upper : true }}';
```

Example 3: will return rows where *age* is in [-1, 1]

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type          : "range",
                        field         : "age",
                        lower         : -1,
                        upper         : 1,
                        include_lower : true,
                        include_upper : true }}';
```

Example 4: will return rows where *date* is in [2014/01/01, 2014/01/02]

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type          : "range",
                        field         : "date",
                        lower         : "2014/01/01",
                        upper         : "2014/01/02",
                        include_lower : true,
                        include_upper : true }}';
```

###Regexp query

Syntax:

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type  : "regexp",
                        field : <fieldname>,
                        value : <regexp>
                     }}';
```

where:

-   **value**: a regular expression. See [org.apache.lucene.util.automaton.RegExp](http://lucene.apache.org/core/4_6_1/core/org/apache/lucene/util/automaton/RegExp.html "Reference for Lucene regular expressions") for syntax reference.

Example: will return rows where name contains a word that starts with “p” and a vowel repeated twice (e.g. “pape”).

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type  : "regexp",
                        field : "name",
                        value : "[J][aeiou]{2}.*" }}';
```

###Wildcard query

Syntax:

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type    : "wildcard" ,
                        field   : <fieldname> ,
                        value   : <wildcard_exp>
                     }}';
```

where:

-   **value**: a wildcard expression. Supported wildcards are \*, which matches any character sequence (including the empty one), and ?, which matches any single character. ” is the escape character.

Example: will return rows where food starts with or is “tu”.

```sql
SELECT * FROM test.users
WHERE stratio_col = '{query : {
                        type  : "wildcard",
                        field : "food",
                        value : "tu*" }}';
```

Spark and Hadoop Integration
----------------------------

Spark and Hadoop integrations are fully supported because Lucene queries can be combined with token range queries and pagination, which are the basis of MapReduce frameworks support.

###Token Range Queries

The token function allows computing the token for a given partition key. The primary key of the example table “users” is ((name, gender), animal, age) where (name, gender) is the partition key. When combining the token function and a Lucene-based filter in a where clause, the filter on tokens is applied first and then the condition of the filter clause.

Example: will retrieve rows which tokens are greater than (‘Alicia’, ‘female’) and then test them against the match condition.

```sql
SELECT name,gender
  FROM test.users
 WHERE stratio_col='{filter : {type : "match", field : "food", value : "chips"}}'
   AND token(name, gender) > token('Alicia', 'female');
```

###Pagination

Pagination over filtered results is fully supported. You can retrieve the rows starting from a certain key. For example, if the primary key is (userid, createdAt), you can query:

```sql
SELECT *
  FROM tweets
  WHERE stratio_col = ‘{ filter : {type:”match",  field:”text", value:”cassandra”} }’
    AND userid = 3543534
    AND createdAt > 2011-02-03 04:05+0000
  LIMIT 5000;
```

Datatypes Mapping
-----------------

###CQL to Field type

<table>
    <thead>
    <tr>
        <th>CQL type</th>
        <th>Description</th>
        <th>Field type</th>
        <th>Query types</th>
    </tr>
    </thead>
    <tbody>
    <tr>
        <td>ascii</td>
        <td>US-ASCII character string</td>
        <td>string/text</td>
        <td>All</td>
    </tr>
    <tr>
        <td>bigint</td>
        <td>64-bit signed long</td>
        <td>long</td>
        <td>boolean<br />
            contains<br />
            match<br />
            range</td>
    </tr>
    <tr>
        <td>blob</td>
        <td>Arbitrary bytes (no validation), expressed as hexadecimal</td>
        <td>bytes</td>
        <td>All</td>
    </tr>
    <tr>
        <td>boolean</td>
        <td>true or false</td>
        <td>boolean</td>
        <td>All</td>
    </tr>
    <tr>
        <td>counter</td>
        <td>Distributed counter value (64-bit long)</td>
        <td><em>not supported</em></td>
        <td></td>
    </tr>
    <tr>
        <td>decimal</td>
        <td>Variable-precision decimal</td>
        <td>bigdec</td>
        <td>All</td>
    </tr>
    <tr>
        <td>double</td>
        <td>64-bit IEEE-754 floating point</td>
        <td>double</td>
        <td>boolean<br />
            contains<br />
            match<br />
            range</td>
    </tr>
    <tr>
        <td>float</td>
        <td>32-bit IEEE-754 floating point</td>
        <td>float</td>
        <td>boolean<br />
            contains<br />
            match<br />
            range</td>
    </tr>
    <tr>
        <td>inet</td>
        <td>IP address string in IPv4 or IPv6 format</td>
        <td>inet</td>
        <td>All</td>
    </tr>
    <tr>
        <td>int</td>
        <td>32-bit signed integer</td>
        <td>integer</td>
        <td>boolean<br />
            contains<br />
            match<br />
            range</td>
    </tr>
    <tr>
        <td>list&lt;T></td>
        <td>A collection of one or more ordered elements</td>
        <td>Type of list elements</td>
        <td><em>see element type</em></td>
    </tr>
    <tr>
        <td>map&lt;K,V></td>
        <td>A JSON-style array of literals: { literal : literal, literal : literal … }</td>
        <td>Type of values</td>
        <td><em>see element type</em></td>
    </tr>
    <tr>
        <td>set&lt;T></td>
        <td>A collection of one or more elements</td>
        <td>Type of set elements</td>
        <td><em>see element type</em></td>
    </tr>
    <tr>
        <td>text</td>
        <td>UTF-8 encoded string</td>
        <td>string/text</td>
        <td>All</td>
    </tr>
    <tr>
        <td>timestamp</td>
        <td>Date plus time, encoded as 8 bytes since epoch</td>
        <td>date</td>
        <td>boolean<br />
            contains<br />
            match<br />
            range</td>
    </tr>
    <tr>
        <td>uuid</td>
        <td>Type 1 or type 4 UUID</td>
        <td>uuid</td>
        <td>All</td>
    </tr>
    <tr>
        <td>timeuuid</td>
        <td>Type 1 UUID only (CQL3)</td>
        <td>uuid</td>
        <td>All</td>
    </tr>
    <tr>
        <td>varchar</td>
        <td>UTF-8 encoded string</td>
        <td>string/text</td>
        <td>All</td>
    </tr>
    <tr>
        <td>varint</td>
        <td>Arbitrary-precision integer</td>
        <td>bigint</td>
        <td>All</td>
    </tr>
    </tbody>
</table>

###Field type to CQL

<table>
    <thead>
    <tr>
        <th>field type</th>
        <th>CQL type</th>
        <th>Supported in Query types</th>
    </tr>
    </thead>
    <tbody>
    <tr>
        <td>bigdec</td>
        <td>decimal</td>
        <td>All</td>
    </tr>
    <tr>
        <td>bigint</td>
        <td>varint</td>
        <td>All</td>
    </tr>
    <tr>
        <td>boolean</td>
        <td>boolean</td>
        <td>All</td>
    </tr>
    <tr>
        <td>bytes</td>
        <td>blob</td>
        <td>All</td>
    </tr>
    <tr>
        <td>date</td>
        <td>timestamp</td>
        <td>boolean<br />
            match<br />
            range</td>
    </tr>
    <tr>
        <td>double</td>
        <td>double</td>
        <td>boolean<br />
            match<br />
            range</td>
    </tr>
    <tr>
        <td>float</td>
        <td>float</td>
        <td>boolean<br />
            match<br />
            range</td>
    </tr>
    <tr>
        <td>inet</td>
        <td>inet</td>
        <td>All</td>
    </tr>
    <tr>
        <td>integer</td>
        <td>int</td>
        <td>boolean<br />
            match<br />
            range</td>
    </tr>
    <tr>
        <td>long</td>
        <td>bigint</td>
        <td>boolean<br />
            match<br />
            range</td>
    </tr>
    <tr>
        <td>string/text</td>
        <td>ascii<br />
            text<br />
            varchar</td>
        <td>All</td>
    </tr>
    <tr>
        <td>uuid</td>
        <td>uuid<br />
            timeuuid</td>
        <td>All</td>
    </tr>
    </tbody>
</table>

JMX Interface
-------------

The existing Lucene indexes expose some attributes and operations through JMX, using the same MBean server as Apache Cassandra. The MBeans provided by Stratio are under the domain **com.stratio.cassandra.lucene**.

Please note that all the JMX attributes and operations refer to the index shard living inside the local JVM, and not to the globally distributed index.

<table>
    <tr>
        <th>Name</th>
        <th>Type</th>
        <th>Notes</th>
    </tr>
    <tr>
        <td>NumDeletedDocs</td>
        <td>Attribute</td>
        <td>Total number of deleted documents in the index.</td>
    </tr>
    <tr>
        <td>NumDocs</td>
        <td>Attribute</td>
        <td>Total number of documents in the index.</td>
    </tr>
    <tr>
        <td>commit</td>
        <td>Operation</td>
        <td>Commits all the pending index changes to disk.</td>
    </tr>
    <tr>
        <td>refresh</td>
        <td>Operation</td>
        <td>Reopens all the readers and searchers to provide a recent view of the index.</td>
    </tr>
    <tr>
        <td>forceMerge</td>
        <td>Operation</td>
        <td>Optimizes the index forcing merge segments leaving the specified number of segments. It also includes a boolean parameter to block until all merging completes.</td>
    </tr>
    <tr>
        <td>forceMergeDeletes</td>
        <td>Operation</td>
        <td>Optimizes the index forcing merge segments containing deletions, leaving the specified number of segments. It also includes a boolean parameter to block until all merging completes.</td>
    </tr>
</table>