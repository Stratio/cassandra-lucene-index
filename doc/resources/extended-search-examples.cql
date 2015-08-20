USE test;

-- Refresh index
SELECT * FROM test.users
WHERE stratio_col = '{refresh : true}';

-- Boolean queries
SELECT * FROM test.users 
WHERE stratio_col = '{query : {
                        type : "boolean", 
                        must : [{type : "wildcard", field : "name", value : "*_2"}, 
                                {type : "wildcard", field : "food", value : "tu*"}]}}';
SELECT * FROM test.users 
WHERE stratio_col = '{query : {
                        type : "boolean", 
                        not  : [{type : "wildcard", field : "name", value : "*_2"}], 
                        must : [{type : "wildcard", field : "food", value : "tu*"}]}}';
SELECT * FROM test.users 
WHERE stratio_col = '{query : { 
                        type   : "boolean", 
                        should : [{type : "wildcard", field : "name", value : "*_2"}, 
                                  {type : "wildcard", field : "food", value : "tu*"}]}}';


-- Contains queries
SELECT * FROM test.users
WHERE stratio_col = '{filter : {
                        type   : "contains",
                        field  : "name",
                        values : ["Alicia","mancha"] }}';

SELECT * FROM test.users
WHERE stratio_col = '{filter : {
                        type   : "contains",
                        field  : "date",
                        values : ["2014/01/01", "2014/01/02", "2014/01/03"] }}';

-- Date Range queries
SELECT * FROM test.users
WHERE stratio_col = '{  filter : {
                        type   : "date_range",
                        field  : "duration",
                        from   : "2014/01/01",
                        to     : "2014/12/31",
                        operation : "intersects"}}';


SELECT * FROM test.users
WHERE stratio_col = '{  filter : {
                        type   : "date_range",
                        field  : "duration",
                        from   : "2014/06/01",
                        to     : "2014/06/02",
                        operation : "contains"}}';


SELECT * FROM test.users
WHERE stratio_col = '{  filter : {
                        type   : "date_range",
                        field  : "duration",
                        from   : "2014/01/01",
                        to     : "2014/12/31",
                        operation : "is_within"}}';


-- Fuzzy queries
SELECT * FROM test.users 
WHERE stratio_col = '{query : { type      : "fuzzy", 
                                field     : "phrase", 
                                value     : "puma", 
                                max_edits : 1 }}';
SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type          : "fuzzy",
                                     field         : "phrase",
                                     value         : "puma",
                                     max_edits     : 1,
                                     prefix_length : 2 }}';


-- Geo bbox queries
SELECT * FROM test.users
WHERE stratio_col = '{filter : { type : "geo_bbox",
                                 field : "place",
                                 min_latitude : -90.0,
                                 max_latitude : 90.0,
                                 min_longitude : -180.0,
                                 max_longitude : 180.0 }}';

SELECT * FROM test.users
WHERE stratio_col = '{filter : { type : "geo_bbox",
                                 field : "place",
                                 min_latitude : -90.0,
                                 max_latitude : 90.0,
                                 min_longitude : 0.0,
                                 max_longitude : 10.0 }}';

SELECT * FROM test.users
WHERE stratio_col = '{filter : { type : "geo_bbox",
                                 field : "place",
                                 min_latitude : 0.0,
                                 max_latitude : 10.0,
                                 min_longitude : -180.0,
                                 max_longitude : 180.0 }}';
-- Geo distance queries
 SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type : "geo_distance",
                                     field : "place",
                                     latitude : 40.225479,
                                     longitude : -3.999278,
                                     max_distance : "1km"}}';

SELECT * FROM test.users
    WHERE stratio_col = '{filter : { type : "geo_distance",
                                     field : "place",
                                     latitude : 40.225479,
                                     longitude : -3.999278,
                                     max_distance : "10yd" ,
                                     min_distance : "1yd" }}';

-- Match queries
SELECT * FROM test.users 
WHERE stratio_col = '{query : {
                        type  : "match", 
                        field : "name", 
                        value : "Alicia" }}';
SELECT * FROM test.users 
WHERE stratio_col = '{query : {
                        type  : "match", 
                        field : "phrase", 
                        value : "mancha" }}';
SELECT * FROM test.users 
WHERE stratio_col = '{query : {
                        type  : "match", 
                        field : "date", 
                        value : "2014/01/01" }}';


-- Match all queries
SELECT * FROM test.users
WHERE stratio_col = '{filter : { type  : "all" } }';

-- Phrase queries
SELECT * FROM test.users 
WHERE stratio_col = '{query : {
                        type   : "phrase", 
                        field  : "phrase", 
                        value : "camisa manchada" }}';
SELECT * FROM test.users 
WHERE stratio_col = '{query : {
                        type   : "phrase", 
                        field  : "phrase", 
                        value : "mancha camisa", 
                        slop   : 2 }}';

-- Prefix queries
SELECT * FROM test.users 
WHERE stratio_col = '{query : {
                        type          : "prefix", 
                        field         : "phrase", 
                        value         : "lu" }}';

-- Range queries
SELECT * FROM test.users 
WHERE stratio_col = '{query : {
                        type          : "range", 
                        field         : "age", 
                        lower         : 1, 
                        include_lower : true }}';
SELECT * FROM test.users 
WHERE stratio_col = '{query : {
                        type          : "range", 
                        field         : "age", 
                        upper         : 0, 
                        include_upper : true }}';
SELECT * FROM test.users 
WHERE stratio_col = '{query : {
                        type          : "range", 
                        field         : "age", 
                        lower         : -1, 
                        upper         : 1, 
                        include_lower : true, 
                        include_upper : true }}';
SELECT * FROM test.users 
WHERE stratio_col = '{query : {
                        type          : "range", 
                        field         : "date", 
                        lower         : "2014/01/01", 
                        upper         : "2014/01/02", 
                        include_lower : true, 
                        include_upper : true }}';

-- Regexp queries
SELECT * FROM test.users 
WHERE stratio_col = '{query : {
                        type  : "regexp", 
                        field : "name",
                        value : "([J][aeiou]){2}.*" }}';

-- Wildcard queries
SELECT * FROM test.users 
WHERE stratio_col = '{query : {
                        type  : "wildcard", 
                        field : "food", 
                        value : "tu*" }}';