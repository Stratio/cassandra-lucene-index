User Defined Types
******************

Since Cassandra 2.2.X users can declare User Defined Types as follows:

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



and use it like a native CQL type.

Indexing part of this UDT is allowed just using the "." operator as follows:

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


and searching:

.. code-block:: sql

    SELECT * FROM user_profiles
    WHERE lucene='{
        filter : {
            type  : "match",
            field : "address.city",
            value : "San Fransisco"
        }
    }';

or:

.. code-block:: sql

    SELECT * FROM user_profiles
    WHERE lucene='{
        filter : {
            type  : "range",
            field : "address.zip",
            lower : 0,
            upper : 10
        }
    }';
