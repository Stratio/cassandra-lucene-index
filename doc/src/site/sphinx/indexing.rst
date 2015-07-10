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
   of a search,
-  <options> is a JSON object:

.. code-block:: sql

    <options> := { ('refresh_seconds'      : '<int_value>',)?
                   ('ram_buffer_mb'        : '<int_value>',)?
                   ('max_merge_mb'         : '<int_value>',)?
                   ('max_cached_mb'        : '<int_value>',)?
                   ('indexing_threads'     : '<int_value>',)?
                   ('indexing_queues_size' : '<int_value>',)?
                   ('directory_path'       : '<string_value>',)?
                   'schema'                : '<schema_definition>'};

Options, except “schema” and “directory\_path”, take a positive integer
value enclosed in single quotes:

-  **refresh\_seconds**: number of seconds before refreshing the index
   (between writers and readers). Defaults to ’60’.
-  **ram\_buffer\_mb**: size of the write buffer. Its content will be
   committed to disk when full. Defaults to ’64’.
-  **max\_merge\_mb**: defaults to ’5’.
-  **max\_cached\_mb**: defaults to ’30’.
-  **indexing\_threads**: number of asynchronous indexing threads. ’0’
   means synchronous indexing. Defaults to ’0’.
-  **indexing\_queues\_size**: max number of queued documents per
   asynchronous indexing thread. Defaults to ’50’.
-  **paging_cache_size**: The max number of lucene cursors to be cached.
   Defaults to ’50’.
-  **directory\_path**: The path of the directory where the  Lucene index
   will be stored.
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

Analysis
========

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

Mapping
=======

Field mapping definition options depend on the field type. Details and
default values are listed in the table below.

+-----------------+-----------------+-----------------+--------------------------------+-----------+
| Mapper type     | Option          | Value type      | Default value                  | Mandatory |
+=================+=================+=================+================================+===========+
| bigdec          | indexed         | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | integer_digits  | integer         | 32                             | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | decimal_digits  | integer         | 32                             | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| bigint          | indexed         | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | digits          | integer         | 32                             | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| blob            | indexed         | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | true                           | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| boolean         | indexed         | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | true                           | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| date            | indexed         | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | pattern         | string          | yyyy/MM/dd HH:mm:ss.SSS        | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| date_range      | start           | string          |                                | Yes       |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | stop            | string          |                                | Yes       |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | pattern         | string          | yyyy/MM/dd HH:mm:ss.SSS        | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| double          | indexed         | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | boost           | integer         | 0.1f                           | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| float           | indexed         | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | boost           | integer         | 0.1f                           | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| geo_point       | latitude        | string          |                                | Yes       |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | longitude       | string          |                                | Yes       |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | max_levels      | integer         | 11                             | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| inet            | indexed         | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | true                           | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| integer         | indexed         | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | boost           | integer         | 0.1f                           | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| long            | indexed         | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | boost           | integer         | 0.1f                           | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| string          | indexed         | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | true                           | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| text            | indexed         | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | analyzer        | string          | default_analyzer of the schema | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| uuid            | indexed         | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | true                           | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+

All mapping definitions has an “\ **indexed**\ ” option indicating if
the field is searchable. There is also a “\ **sorted**\ ” option
specifying if it is possible to sort rows by the corresponding field.
Both fields are true by default, but they should be set to false when no
needed in order to have a smaller and faster index.

Note that Cassandra allows one custom index per table. On the other
hand, Cassandra does not allow a modify operation on indexes. To modify
an index it needs to be deleted first and created again.

Example
=======

This code below and the one for creating the corresponding keyspace and
table is available in a CQL script that can be sourced from the
Cassandra shell:
`test-users-create.cql <resources/test-users-create.cql>`__.

.. code-block:: sql

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
