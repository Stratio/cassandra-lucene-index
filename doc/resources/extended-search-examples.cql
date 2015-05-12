USE test;

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

-- Fuzzy queries
SELECT * FROM test.users 
WHERE stratio_col = '{query : { type      : "fuzzy", 
                                field     : "phrase", 
                                value     : "puma", 
                                max_edits : 1 }}';
SELECT * FROM test.users 
WHERE stratio_col = '{query : { type          : "fuzzy", 
                                field         : "phrase", 
                                value         : "puma", 
                                prefix_length : 2 }}';

-- Match queries
SELECT * FROM test.users 
WHERE stratio_col = '{query : {
                        type  : "match", 
                        field : "name", 
                        value : "pepito_3" }}';
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

-- Phrase queries
SELECT * FROM test.users 
WHERE stratio_col = '{query : {
                        type   : "phrase", 
                        field  : "phrase", 
                        values : ["camisa", "manchada"] }}';
SELECT * FROM test.users 
WHERE stratio_col = '{query : {
                        type   : "phrase", 
                        field  : "phrase", 
                        values : ["mancha", "camisa"], 
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
                        value : "([p][aeiou]){2}.*" }}';

-- Wildcard queries
SELECT * FROM test.users 
WHERE stratio_col = '{query : {
                        type  : "wildcard", 
                        field : "food", 
                        value : "tu*" }}';

