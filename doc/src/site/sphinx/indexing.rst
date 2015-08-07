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

    <options> := { ('refresh_seconds'      : '<int_value>',)?
                   ('ram_buffer_mb'        : '<int_value>',)?
                   ('max_merge_mb'         : '<int_value>',)?
                   ('max_cached_mb'        : '<int_value>',)?
                   ('directory_path'       : '<string_value>',)?
                   'schema'                : '<schema_definition>'};

Options, except “schema” and “directory\_path”, take a positive integer
value enclosed in single quotes:

-  **refresh\_seconds**: number of seconds before auto-refreshing the
   index reader. It is the max time taken for writes to be searchable
   without forcing an index refresh. Defaults to '60'.
-  **ram\_buffer\_mb**: size of the write buffer. Its content will be
   committed to disk when full. Defaults to ’64’.
-  **max\_merge\_mb**: defaults to ’5’.
-  **max\_cached\_mb**: defaults to ’30’.
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
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | false                          | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | integer_digits  | integer         | 32                             | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | decimal_digits  | integer         | 32                             | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| bigint          | indexed         | boolean         | true                           | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | false                          | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | digits          | integer         | 32                             | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| bitemporal      | vt_from         | string          |                                | Yes       |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | vt_to           | string          |                                | Yes       |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | tt_from         | string          |                                | Yes       |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | tt_to           | string          |                                | Yes       |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | pattern         | string          | yyyy/MM/dd HH:mm:ss.SSS Z      | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | now_value       | object          | Long.MAX_VALUE                 | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| blob            | indexed         | boolean         | true                           | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | false                          | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| boolean         | indexed         | boolean         | true                           | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | false                          | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| date            | indexed         | boolean         | true                           | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | false                          | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | pattern         | string          | yyyy/MM/dd HH:mm:ss.SSS Z      | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| date_range      | from            | string          |                                | Yes       |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | to              | string          |                                | Yes       |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | pattern         | string          | yyyy/MM/dd HH:mm:ss.SSS Z      | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| double          | indexed         | boolean         | true                           | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | false                          | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | boost           | integer         | 0.1f                           | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| float           | indexed         | boolean         | true                           | No        |
+                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | false                          | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | boost           | integer         | 0.1f                           | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| geo_point       | latitude        | string          |                                | Yes       |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | longitude       | string          |                                | Yes       |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | max_levels      | integer         | 11                             | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| inet            | indexed         | boolean         | true                           | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | false                          | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| integer         | indexed         | boolean         | true                           | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | false                          | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | boost           | integer         | 0.1f                           | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| long            | indexed         | boolean         | true                           | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | false                          | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | boost           | integer         | 0.1f                           | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| string          | indexed         | boolean         | true                           | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | false                          | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| text            | indexed         | boolean         | true                           | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | false                          | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | analyzer        | string          | default_analyzer of the schema | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+
| uuid            | indexed         | boolean         | true                           | No        |
|                 +-----------------+-----------------+--------------------------------+-----------+
|                 | sorted          | boolean         | false                          | No        |
+-----------------+-----------------+-----------------+--------------------------------+-----------+

Most mapping definitions have an “\ **indexed**\ ” option indicating if
the field is searchable, it is true by default. There is also a “\ **sorted**\ ” option
specifying if it is possible to sort rows by the corresponding field, false by default. List and set
columns can't be sorted because they produce multivalued fields.
These options should be set to false when no needed in order to have a smaller and faster index.

Note that Cassandra allows one custom index per table. On the other
hand, Cassandra does not allow a modify operation on indexes. To modify
an index it needs to be deleted first and created again.

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
        'refresh_seconds'      : '60',
        'ram_buffer_mb'        : '64',
        'max_merge_mb'         : '5',
        'max_cached_mb'        : '30',
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
                gender : {type     : "string", sorted: true},
                animal : {type     : "string"},
                age    : {type     : "integer"},
                food   : {type     : "string"},
                number : {type     : "integer"},
                bool   : {type     : "boolean"},
                date   : {type     : "date", pattern  : "yyyy/MM/dd"},
                mapz   : {type     : "string", sorted: true},
                setz   : {type     : "string"},
                listz  : {type     : "string"},
                phrase : {type     : "text", analyzer : "my_custom_analyzer"}
            }
        }'
    };
