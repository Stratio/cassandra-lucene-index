Searching
*********

Syntax:

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table_name>
    WHERE <magic_column> = '{ (   filter : <filter>  )?
                              ( , query  : <query> )?
                              ( , sort   : <sort>   )?
                            }';

where <filter> and <query> are a JSON object:

.. code-block:: sql

    <filter> := { type : <type> (, <option> : ( <value> | <value_list> ) )+ }
    <query>  := { type : <type> (, <option> : ( <value> | <value_list> ) )+ }

and <sort> is another JSON object:

.. code-block:: sql

        <sort> := { fields : <sort_field> (, <sort_field> )* }
        <sort_field> := { field : <field> (, reverse : <reverse> )? }

When searching by <filter>, without <query> or <sort> defined, then the
results are returned in the Cassandra’s natural order, which is defined
by the partitioner and the column name comparator.

When searching by <query>, results are returned ***sorted by descending
relevance***. The scores will be located in the column <magic_column>.

Sort option is used to specify the order in which the indexed rows will
be traversed. When sorting is used, the query scoring is delayed.

Filters can be combined with Cassandra paging, whereas queries and sorts
can't be. So, you should disable paging when using relevance or sorting
queries.

Additionally, relevance queries must touch all the nodes in the
ring in order to find the globally best results, so definitely you should
prefer filters over queries when no relevance nor sorting are needed.

Types of search and their options are summarized in the table below.
Details for each of them are available in individual sections and the
examples can be downloaded as a CQL script:
`extended-search-examples.cql <resources/extended-search-examples.cql>`__.

In addition to the options described in the table, all search types have
a “\ **boost**\ ” option that acts as a weight on the resulting score.

+-----------------------------------------+-----------------+-----------------+--------------------------------+-----------+
| Search type                             | Option          | Value type      | Default value                  | Mandatory |
+=========================================+=================+=================+================================+===========+
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
|                                         | operation       | string          | is_within                      | No        |
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
|                                         | start           | string/long     | 0                              | No        |
|                                         +-----------------+-----------------+--------------------------------+-----------+
|                                         | stop            | string/long     | Integer.MAX_VALUE              | No        |
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
| `Match all <#match-all-search>`__       |                 |                 |                                |           |
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

-  **vt\_from**: a string or a number being the beginning of the valid date range.
-  **vt\_to**: a string or a number being the end of the valid date range.
-  **tt\_from**: a string or a number being the beginning of the transaction date range.
-  **tt\_to**: a string or a number being the end of the transaction date range.
-  **operation**: the spatial operation to be performed, it can be **intersects**,
   **contains** and **is\_within**.

Example 1: will return rows where valid time range is within "2014/02/01 00:00:00.000" and
"2014/02/28 23:59:59.999" and transaction time range is within "2014/02/01 00:00:00.000" and
"2014/03/31 23:59:59.999"

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{ filter : {
                            type  : "bitemporal",
                            vt_from : "2014/02/01 00:00:00.000",
                            vt_to : "2014/02/28 23:59:59.999",
                            tt_from  : "2014/02/01 00:00:00.000",
                            tt_to  : "2014/03/31 23:59:59.999",
                            operation : "is_within"}}';

Example 2: will return rows where valid time range intersects "2014/02/01 00:00:00.000" and
"2014/02/28 23:59:59.999" and transaction time range intersects "2014/02/01 00:00:00.000" and
"2014/03/31 23:59:59.999"

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{  filter : {
                            type  : "bitemporal",
                            vt_from : "2014/02/01 00:00:00.000",
                            vt_to : "2014/02/28 23:59:59.999",
                            tt_from  : "2014/02/01 00:00:00.000",
                            tt_to  : "2014/03/31 23:59:59.999",
                            operation : "intersects"}}';



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
                                type  : "contains",
                                (start : <start> ,)?
                                (stop  : <stop> ,)?
                                (operation: <operation> )?
                              }}';

where:

-  **start**: a string or a number being the beginning of the date
   range.
-  **stop**: a string or a number being the end of the date range.
-  **operation**: the spatial operation to be performed, it can be
   **intersects**, **contains** and **is\_within**.

Example 1: will return rows where duration is within "2013/05/02" and
:"2013/05/03"

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{ filter : {
                            type  : "date_range",
                            field : "duration",
                            start : "2013/05/02",
                            stop  : "2013/05/03",
                            operation : "is_within"}}';

Example 2: will return rows where duration intersects "2013/05/02" and
:"2013/05/03"

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{  filter : {
                            type   : "date_range",
                            field  : "duration",
                            start  : "2013/05/02",
                            stop   : "2013/05/03",
                            operation : "intersects"}}';

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
between 40.225479 and 40.560174, and a longitude between -3.999278 and
-3.378550.

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type : "geo_bbox",
                                     field : "place",
                                     min_latitude : 40.225479,
                                     max_latitude : 40.560174,
                                     min_longitude : -3.999278,
                                     max_longitude : -3.378550 }}';

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
                                     max_distance : "1km" }}';

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

Match all search
================

Syntax:

.. code-block:: sql

    SELECT ( <fields> | * )
    FROM <table>
    WHERE <magic_column> = '{ (filter | query) : {
                                type  : "match_all",
                                field : <fieldname> ,
                                value : <value> }}';

Example: will return all the indexed rows

.. code-block:: sql

    SELECT * FROM test.users
    WHERE stratio_col = '{filter : {
                           type  : "match_all" }}';

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
