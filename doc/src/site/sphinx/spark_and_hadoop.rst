Spark and Hadoop
****************

Spark and Hadoop integrations are fully supported because Lucene searches
can be combined with token range restrictions and pagination, which are the
basis of MapReduce frameworks support.

Token Range Searches
====================

The token function allows computing the token for a given partition key.
The primary key of the example table “users” is ((name, gender), animal,
age) where (name, gender) is the partition key. When combining the token
function and a Lucene-based filter in a where clause, the filter on
tokens is applied first and then the condition of the filter clause.

Example: will retrieve rows which tokens are greater than (‘Alicia’,
‘female’) and then test them against the match condition.

.. code-block:: sql

    SELECT name,gender
      FROM test.users
     WHERE stratio_col='{filter : {type : "match", field : "food", value : "chips"}}'
       AND token(name, gender) > token('Alicia', 'female');

Paging
======

Paging over filtered results is fully supported. You can retrieve
the rows starting from a certain key. For example, if the primary key is
(userid, createdAt), you can search:

.. code-block:: sql

    SELECT *
      FROM tweets
      WHERE stratio_col = ‘{ filter : {type:”match",  field:”text", value:”cassandra”} }’
        AND userid = 3543534
        AND createdAt > 2011-02-03 04:05+0000
      LIMIT 5000;
