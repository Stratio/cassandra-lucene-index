
-- For example, we want to implement a system for census bureau to track where resides a citizen and when the census bureau knows this. 

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
    }'
};

-- The population of 5 citizens lives in each city from 2015/01/01 until now 

INSERT INTO census(name, city, vt_from, vt_to, tt_from, tt_to) VALUES ('John', 'Madrid', '2015/01/01', '2200/12/31', '2015/01/01', '2200/12/31');
INSERT INTO census(name, city, vt_from, vt_to, tt_from, tt_to) VALUES ('Margaret', 'Barcelona', '2015/01/01', '2200/12/31', '2015/01/01', '2200/12/31');
INSERT INTO census(name, city, vt_from, vt_to, tt_from, tt_to) VALUES ('Cristian', 'Ceuta', '2015/01/01', '2200/12/31', '2015/01/01', '2200/12/31');
INSERT INTO census(name, city, vt_from, vt_to, tt_from, tt_to) VALUES ('Edward', 'New York','2015/01/01', '2200/12/31', '2015/01/01', '2200/12/31');
INSERT INTO census(name, city, vt_from, vt_to, tt_from, tt_to) VALUES ('Johnatan', 'San Francisco', '2015/01/01', '2200/12/31', '2015/01/01', '2200/12/31');


-- John moves to Amsterdam in '2015/03/05' but he does not comunicate this to census bureau until '2015/06/29' because he need it to apply for taxes reduction.

-- So, the system need to update last information from John,and insert the new. This is done with batch execution updating the transaction time end of previous data and inserting new. 

BEGIN BATCH
UPDATE census SET tt_to = '2015/06/29' 
WHERE name = 'John' AND vt_from = '2015/01/01' 
AND tt_from = '2015/01/01' IF tt_to = '2200/12/31';
INSERT INTO census(name, city, vt_from, vt_to, tt_from, tt_to) 
VALUES ('John', 'Amsterdam', '2015/03/05', '2200/12/31', '2015/06/30', '2200/12/31');
APPLY BATCH;

-- Now , we can see the main difference between valid time and transaction time. The system knows from '2015/01/01' to '2015/06/29' that John resides in Madrid from '2015/01/01' until now, and resides in Amsterdam from '2015/03/05' until now.

There are several types of queries concerning this type of indexing.

-- If its needed to get all the data in the table:

SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM census ;


-- If you want to know what is the last info about where John resides, you perform a query with tt_from and tt_to setted to now_value:

SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM census WHERE 
lucene='{
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


-- If the test case needs to know what the system was thinking at '2015/03/01' about where John resides:

SELECT name, city, vt_from, vt_to, tt_from, tt_to FROM census WHERE 
lucene='{
    filter : {
      type : "bitemporal", 
      field : "bitemporal", 
      tt_from : "2015/03/01", 
      tt_to : "2015/03/01"
    }
}' 
AND name='John';



