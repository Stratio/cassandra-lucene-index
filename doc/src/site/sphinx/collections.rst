Collections
***********

CQL collections (lists, sets and maps) can be indexed.

List ans sets are indexed in the same way as regular columns, using their base type:

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

Searches are also done in the same way as with regular columns:

.. code-block:: sql

    SELECT * FROM user_profiles
    WHERE lucene='{
        filter : {
            type  : "match",
            field : "cities",
            value : "San Francisco"
        }
    }';


Maps are indexed associating values to their keys:

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

For searching map values under a certain key you should use '$' as field-key separator:

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
        addresses list<frozen<address>>,
        lucene text
    );

    CREATE CUSTOM INDEX test_index ON test.user_profiles(lucene)
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
