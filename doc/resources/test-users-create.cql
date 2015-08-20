-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS test 
WITH REPLICATION = {
       'class' : 'SimpleStrategy', 
       'replication_factor': 1
};


USE test;
-- Create table
CREATE TABLE IF NOT EXISTS test.users (
  name varchar,
  gender varchar,
  animal varchar, 
  age int,
  food varchar, 
  latitude float,
  longitude float,
  number int,
  bool boolean,
  stratio_col text,
  phrase text,
  date text,
  start_date text,
  stop_date text,
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
                duration:{type:"date_range", from:"start_date", to:"stop_date", pattern:"yyyy/MM/dd"},
                place:{type:"geo_point", latitude:"latitude", longitude:"longitude"},
                mapz:{type:"string"}, 
                setz:{type:"string"}, 
                listz:{type:"string"}, 
                phrase:{type:"text", analyzer:"spanish"}
               }
      }'
};

-- populate table
INSERT INTO test.users (name, gender, age, animal, food, number, bool, setz, listz, mapz, start_date, stop_date, latitude, longitude) VALUES ( 'Joe', 'male', -2, 'monkey', 'chips', 1, true, {'s1', 's2'}, ['l1', 'l2'], {'k1':'v1','k2':'v2'}, '2014/01/01','2014/01/31',-87.8, 0.5);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, setz, listz, mapz, start_date, stop_date, latitude, longitude) VALUES   ( 'Joe', 'male', 1, 'monkey', 'chips', 2, true, {'s1', 's2'}, ['l1', 'l2'], {'k1':'v1','k2':'v2'}, '2014/02/01','2014/02/28',45.8, 0.5);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, setz, listz, mapz, start_date, stop_date, latitude, longitude) VALUES   ( 'James', 'male', 1, 'monkey', 'chips', 3, true, {'s1', 's2'}, ['l1', 'l2'], {'k1':'v1','k2':'v2'}, '2014/03/01','2014/03/31',2.38, -170.6);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, setz, listz, mapz, start_date, stop_date, latitude, longitude) VALUES   ( 'James', 'male', 2, 'monkey', 'chips', 4, true, {'s1', 's3'}, ['l1', 'l2'], {'k1':'v1'}, '2014/04/01','2014/04/30',88.9, -156.8);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, setz, listz, mapz, start_date, stop_date, latitude, longitude) VALUES   ( 'Jane', 'female', -2, 'monkey', 'chips', 5, true, {'s3', 's4'}, ['l1', 'l3'], {'k3':'v3','k2':'v2'}, '2014/05/01','2014/05/31',1.45, 94.23);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, date, start_date, stop_date, latitude, longitude) VALUES   ( 'Jane', 'female', -1, 'monkey', 'chips', 6, true, '2014/01/01', '2014/06/01','2014/06/30',-42.8, 12.4);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, date, start_date, stop_date, latitude, longitude) VALUES   ( 'James', 'male', 0, 'monkey', 'chips', 7, true, '2014/01/02', '2014/07/01','2014/07/31',-12, 13.6);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, date, start_date, stop_date, latitude, longitude) VALUES   ( 'Jane', 'female', 1, 'monkey', 'chips', 8, true, '2010/01/01', '2014/08/01','2014/08/31',-5.6, 34.6);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, start_date, stop_date, latitude, longitude) VALUES   ( 'Alicia', 'female', 2, 'monkey', 'chips', 9, true, '2014/09/01','2014/09/30',-32.3, -170.0);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, start_date, stop_date, latitude, longitude) VALUES   ( 'Joe', 'male', 2, 'monkey', 'chips', 10, false, '2014/10/01','2014/10/31',-32.8, 115.6);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, start_date, stop_date, latitude, longitude) VALUES   ( 'James', 'male', -2, 'monkey', 'chips', 11, false, '2014/11/01','2014/11/30',-21.4, 32.7);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, start_date, stop_date, latitude, longitude) VALUES   ( 'James', 'male', -1, 'monkey', 'chips', 12, false, '2014/12/01','2014/12/31',-33.2, 167.0);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, start_date, stop_date, latitude, longitude) VALUES   ( 'Alicia', 'female', 0, 'monkey', 'chips', 13, false, '2014/01/01','2014/06/30',65.5, 2.3);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, start_date, stop_date, latitude, longitude) VALUES   ( 'Joe', 'male', -1, 'monkey', 'chips', 14, false, '2014/02/01','2014/07/31',-22.1, 180.0);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, phrase, start_date, stop_date, latitude, longitude) VALUES   ( 'Joe', 'male', 0, 'monkey', 'chips', 15, false, 'Ligero como una pluma', '2014/03/01','2014/08/31',-35.4, 97.4);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, phrase, start_date, stop_date, latitude, longitude) VALUES   ( 'Alicia', 'female', -2, 'monkey', 'chips', 16, false, 'Rápido como un puma', '2014/04/01','2014/09/30',0.7, -167);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, phrase, start_date, stop_date, latitude, longitude) VALUES   ( 'Alicia', 'female', -1, 'monkey', 'chips', 17, false, 'Una mancha en la camisa morada', '2014/05/01','2014/10/31',-42.3, 25.5);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, phrase, start_date, stop_date, latitude, longitude) VALUES   ( 'Jane', 'female', 0, 'monkey', 'turkey', 18, false, 'Sopa de tortuga', '2014/06/01','2014/11/30',-12.8, 176);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, phrase, start_date, stop_date, latitude, longitude) VALUES   ( 'Alicia', 'female', 1, 'monkey', 'tuna', 19, false, 'En un lugar de la Mancha', '2014/07/01','2014/12/31',-0.5, 1.2);
INSERT INTO test.users (name, gender, age, animal, food, number, bool, phrase, start_date, stop_date, latitude, longitude) VALUES   ( 'Alicia', 'female', 2, 'monkey', 'fish', 20, false, 'Una mancha en la camisa manchada', '2014/01/01','2014/12/31',-76.6,5.4);
