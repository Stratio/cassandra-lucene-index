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
