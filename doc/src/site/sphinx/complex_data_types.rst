Complex data types
******************

Tuples
======

Cassandra 2.1.x introduces the `tuple type <http://docs.datastax.com/en/cql/3.1/cql/cql_reference/tupleType.html>`__.
You can index, search and sort tuples this way:

.. code-block:: sql

    CREATE TABLE collect_things (
      k int PRIMARY KEY,
      v pe<int, text, float>
    );

    INSERT INTO collect_things (k, v) VALUES(0, (1, 'bar', 2.1));
    INSERT INTO collect_things (k, v) VALUES(1, (2, 'bar', 2.1));
    INSERT INTO collect_things (k, v) VALUES(2, (3, 'foo', 2.1));

    ALTER TABLE collect_things ADD lucene text;
    CREATE CUSTOM INDEX idx ON  collect_things (lucene) USING 'com.stratio.cassandra.lucene.Index' WITH OPTIONS = {
    'refresh_seconds':'1',
    'schema':'{
        fields:{
            "v.0":{type:"integer"},
            "v.1":{type:"string"},
            "v.2":{type:"float"}
        }
     }'};

    SELECT * FROM collect_things WHERE lucene = '{
        filter : {
            type  : "match",
            field : "v.0",
            value : 1
        }
    }';

    SELECT * FROM collect_things WHERE lucene = '{
        filter : {
            type  : "match",
            field : "v.1",
            value : "bar"
        }
    }';

    SELECT * FROM collect_things WHERE lucene = '{
        sort : {
            fields : [ {field : "v.2"} ]
        }
    }';


User Defined Types
==================

Since Cassandra 2.1.X users can declare `User Defined Types <http://docs.datastax.com/en/developer/java-driver/2.1/java-driver/reference/userDefinedTypes.html>`__ as follows:

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

The components of UDTs can be indexed, searched and sorted this way :

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

    SELECT * FROM user_profiles
    WHERE lucene='{
        filter : {
            type  : "match",
            field : "address.city",
            value : "San Fransisco"
        }
    }';

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
===========

CQL `collections <http://docs.datastax.com/en/cql/3.0/cql/cql_using/use_collections_c.html>`__ (lists, sets and maps) can be indexed.

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
