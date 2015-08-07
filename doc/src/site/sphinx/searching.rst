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
a **boost** option that acts as a weight on the resulting score.

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

Bitemporal search
=================

Syntax:

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE <magic_column> = '{ (filter | query) : {
                                type  : "bitemporal",
                                (vt_from : <vt_from> ,)?
                                (vt_to   : <vt_to> ,)?
                                (tt_from : <tt_from> ,)?
                                (tt_to   : <tt_to> ,)?
                                (operation: <operation> )?
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
                    type : "bitemporal",
                    tt_from : "tt_from",
                    tt_to : "tt_to",
                    vt_from : "vt_from",
                    vt_to : "vt_to",
                    pattern : "yyyy/MM/dd",
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
            type : "bitemporal",
            field : "bitemporal",
            vt_from : 0,
            vt_to : "2200/12/31",
            tt_from : "2200/12/31",
            tt_to : "2200/12/31"
        }
    }'
    AND name='John';


If the test case needs to know what the system was thinking at '2015/03/01' about where John resides.

.. code-block:: sql

    SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM census
    WHERE lucene = '{
        filter : {
            type : "bitemporal",
            field : "bitemporal",
            tt_from : "2015/03/01",
            tt_to : "2015/03/01"
        }
    }'
    AND name = 'John';

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

Example 2: will return rows where food starts with “tu” but name does
not end with “a”

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                            type : "boolean",
                            not  : [{type : "wildcard", field : "name", value : "*a"}],
                            must : [{type : "wildcard", field : "food", value : "tu*"}]}}';

Example 3: will return rows where name ends with “a” or food starts with
“tu”

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                            type   : "boolean",
                            should : [{type : "wildcard", field : "name", value : "*a"},
                                      {type : "wildcard", field : "food", value : "tu*"}]}}';

Example 4: will return zero rows independently of the index contents

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type   : "boolean"} }';

Example 5: will return rows where name does not end with “a”, which is
a resource-intensive pure negation search

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                            not  : [{type : "wildcard", field : "name", value : "*a"}]}}';

Contains search
===============

Syntax:

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE <magic_column> = '{ (filter | query) : {
                                type  : "contains",
                                field : <fieldname> ,
                                values : <value_list> }}';

Example 1: will return rows where name matches “Alicia” or “mancha”

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                            type   : "contains",
                            field  : "name",
                            values : ["Alicia","mancha"] }}';

Example 2: will return rows where date matches “2014/01/01″,
“2014/01/02″ or “2014/01/03″

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                            type   : "contains",
                            field  : "date",
                            values : ["2014/01/01", "2014/01/02", "2014/01/03"] }}';

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


Example 3:

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
                                (, max_edits     : <max_edits> )?
                                (, prefix_length : <prefix_length> )?
                                (, max_expansions: <max_expansion> )?
                                (, transpositions: <transposition> )?
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

Example 2: same as example 1 but will limit the results to rows where
phrase contains a word that starts with “pu”.

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type          : "fuzzy",
                                     field         : "phrase",
                                     value         : "puma",
                                     max_edits     : 1,
                                     prefix_length : 2 }}';

Geo bbox search
===============

Syntax:

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE <magic_column> = '{ (filter | query) : {
                                type           : "geo_bbox",
                                field          : <fieldname>,
                                min_latitude   : <min_latitude> ,
                                max_latitude   : <max_latitude> ,
                                min_longitude  : <min_longitude> ,
                                max_longitude  : <max_longitude>
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
    WHERE stratio_col = '{filter : { type : "geo_bbox",
                                     field : "place",
                                     min_latitude : -90.0,
                                     max_latitude : 90.0,
                                     min_longitude : -180.0,
                                     max_longitude : 180.0 }}';

Example 2: will return any rows where “place” is formed by a latitude
between -90.0 and 90.0, and a longitude between 0.0 and
10.0.

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type : "geo_bbox",
                                     field : "place",
                                     min_latitude : -90.0,
                                     max_latitude : 90.0,
                                     min_longitude : 0.0,
                                     max_longitude : 10.0 }}';


Example 3: will return any rows where “place” is formed by a latitude
between 0.0 and 10.0, and a longitude between -180.0 and
180.0.

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type : "geo_bbox",
                                     field : "place",
                                     min_latitude : 0.0,
                                     max_latitude : 10.0,
                                     min_longitude : -180.0,
                                     max_longitude : 180.0 }}';


Geo distance search
===================

Syntax:

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE <magic_column> = '{ (filter | query) : {
                                type  : "geo_distance",
                                field : <fieldname> ,
                                latitude : <latitude> ,
                                longitude : <longitude> ,
                                max_distance : <max_distance>
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
    WHERE stratio_col = '{filter : { type : "geo_distance",
                                     field : "place",
                                     latitude : 40.225479,
                                     longitude : -3.999278,
                                     max_distance : "1km"}}';

Example 2: will return any rows where “place” is within one yard and ten
yards from the geo point (40.225479, -3.999278).

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type : "geo_distance",
                                     field : "place",
                                     latitude : 40.225479,
                                     longitude : -3.999278,
                                     max_distance : "10yd" ,
                                     min_distance : "1yd" }}';

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

Example 2: will return rows where phrase contains “mancha”

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                           type  : "match",
                           field : "phrase",
                           value : "mancha" }}';

Example 3: will return rows where date matches “2014/01/01″

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                           type  : "match",
                           field : "date",
                           value : "2014/01/01" }}';

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

Example 2: will return rows where “phrase” contains the word “mancha”
followed by the word “camisa” having 0 to 2 words in between.

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                          type   : "phrase",
                          field  : "phrase",
                          values : "mancha camisa",
                          slop   : 2 }}';

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

Example 2: will return rows where *age* is in (-∞, 0]

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                            type          : "range",
                            field         : "age",
                            upper         : 0,
                            include_upper : true }}';

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
