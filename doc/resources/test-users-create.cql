-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS test 
WITH REPLICATION = {
       'class' : 'SimpleStrategy', 
       'replication_factor': 1
};

-- Create table
USE test;

CREATE TABLE IF NOT EXISTS test.users (
  name varchar,
  gender varchar,
  animal varchar, 
  age int,
  food varchar, 
  number int,
  bool boolean,
  stratio_col text,
  phrase text,
  date text,
  setz set<text>,
  listz list<text>,
  mapz map<text, text>,
  PRIMARY KEY ((name, gender), animal, age)
);

-- Create index
CREATE CUSTOM INDEX IF NOT EXISTS users_index 
ON  test.users (stratio_col) 
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = {
      'refresh_seconds':'1', 
      'num_cached_filters':'1',
      'ram_buffer_mb':'64', 
      'max_merge_mb':'5', 
      'max_cached_mb':'30',
      'schema':'{
        default_analyzer:"standard",
        fields:{
                name:{type:"string"},
                gender:{type:"string"}, 
                animal:{type:"string"}, 
                age:{type:"integer"}, 
                food:{type:"string"}, 
                bool:{type:"boolean"}, 
                date:{type:"date", pattern:"yyyy/MM/dd"}, 
                mapz:{type:"string"}, 
                setz:{type:"string"}, 
                listz:{type:"string"}, 
                phrase:{type:"text", 
                analyzer:"spanish"}}
}'};

-- populate table
INSERT INTO test.users (name, gender, age, animal, food, number, bool, setz, listz, mapz) VALUES ( 'Joe', 'male', -2, 'monkey', 'chips', 1, true, {'s1', 's2'}, ['l1', 'l2'], {'k1':'v1','k2':'v2'});
INSERT INTO test.users (name, gender, age, animal, food, number, bool, setz, listz, mapz) VALUES ( 'Joe', 'male', 1, 'monkey', 'chips', 2, true, {'s1', 's2'}, ['l1', 'l2'], {'k1':'v1','k2':'v2'});
INSERT INTO test.users (name, gender, age, animal, food, number, bool, setz, listz, mapz) VALUES ( 'James', 'male', 1, 'monkey', 'chips', 3, true, {'s1', 's2'}, ['l1', 'l2'], {'k1':'v1','k2':'v2'});
INSERT INTO test.users (name, gender, age, animal, food, number, bool, setz, listz, mapz) VALUES ( 'James', 'male', 2, 'monkey', 'chips', 4, true, {'s1', 's3'}, ['l1', 'l2'], {'k1':'v1'});
INSERT INTO test.users (name, gender, age, animal, food, number, bool, setz, listz, mapz) VALUES ( 'Jane', 'female', -2, 'monkey', 'chips', 5, true, {'s3', 's4'}, ['l1', 'l3'], {'k3':'v3','k2':'v2'});
INSERT INTO test.users (name, gender, age, animal, food, number, bool, date) VALUES ( 'Jane', 'female', -1, 'monkey', 'chips', 6, true, '2014/01/01');
INSERT INTO test.users (name, gender, age, animal, food, number, bool, date) VALUES ( 'James', 'male', 0, 'monkey', 'chips', 7, true, '2014/01/02');
INSERT INTO test.users (name, gender, age, animal, food, number, bool, date) VALUES ( 'Jane', 'female', 1, 'monkey', 'chips', 8, true, '2010/01/01');
INSERT INTO test.users (name, gender, age, animal, food, number, bool) VALUES ( 'Alicia', 'female', 2, 'monkey', 'chips', 9, true);
INSERT INTO test.users (name, gender, age, animal, food, number, bool) VALUES ( 'Joe', 'male', 2, 'monkey', 'chips', 10, false);
INSERT INTO test.users (name, gender, age, animal, food, number, bool) VALUES ( 'James', 'male', -2, 'monkey', 'chips', 11, false);
INSERT INTO test.users (name, gender, age, animal, food, number, bool) VALUES ( 'James', 'male', -1, 'monkey', 'chips', 12, false);
INSERT INTO test.users (name, gender, age, animal, food, number, bool) VALUES ( 'Alicia', 'female', 0, 'monkey', 'chips', 13, false);
INSERT INTO test.users (name, gender, age, animal, food, number, bool) VALUES ( 'Joe', 'male', -1, 'monkey', 'chips', 14, false);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, phrase) VALUES ( 'Joe', 'male', 0, 'monkey', 'chips', 15, false, 'Ligero como una pluma');
INSERT INTO test.users (name, gender, age, animal, food, number, bool, phrase) VALUES ( 'Alicia', 'female', -2, 'monkey', 'chips', 16, false, 'Rápido como un puma');
INSERT INTO test.users (name, gender, age, animal, food, number, bool, phrase) VALUES ( 'Alicia', 'female', -1, 'monkey', 'chips', 17, false, 'Una mancha en la camisa morada');
INSERT INTO test.users (name, gender, age, animal, food, number, bool, phrase) VALUES ( 'Jane', 'female', 0, 'monkey', 'turkey', 18, false, 'Sopa de tortuga');
INSERT INTO test.users (name, gender, age, animal, food, number, bool, phrase) VALUES ( 'Alicia', 'female', 1, 'monkey', 'tuna', 19, false, 'En un lugar de la Mancha');
INSERT INTO test.users (name, gender, age, animal, food, number, bool, phrase) VALUES ( 'Alicia', 'female', 2, 'monkey', 'fish', 20, false, 'Una mancha en la camisa manchada');
