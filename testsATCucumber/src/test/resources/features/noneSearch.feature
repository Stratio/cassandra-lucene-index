@test @search 
Feature: Test none searching

Scenario: I connect to Cassandra cluster
	Given I connect to Cassandra cluster at '${CASSANDRA_HOST}'
	When I create a Cassandra keyspace named 'opera' 
	And I create a Cassandra table named 'noneTable' using keyspace 'opera' with:
		| name   | surname | age		 |lucene|
		| TEXT   | TEXT    |  DECIMAL 	 |TEXT  |
		|  PK    |         |           	 |      | 
	And I insert in keyspace 'opera' and table 'noneTable' with:
	 	| name    | surname   | age	 |
		|'John'   | 'Connor'  |22    |
		|'Michael'| 'Packard' |35    |
		|'Kurt'   | 'James'   |47    |


Scenario: Do a basic String mapper
    Given I create a Cassandra index named 'place' with schema 'schemas/mapping/stringMap.conf' of type 'string' in table 'noneTable' using magic_column 'lucene' using keyspace 'opera' with:
		    | __column_name  | UPDATE  | name |
		    
Scenario: I execute a none query checking  results
		Given I execute a query over fields '*' with schema 'schemas/queries/noneSearch.conf' of type 'string' with magic_column 'lucene' from table: 'noneTable' using keyspace: 'opera' with:
				|__type|UPDATE|none|
		Then There are results found with:
			|name      |occurrences |
			|Kurt	   |0           |	
			|Michael   |0           |
			|John      |0           |		


Scenario: I remove all data
	Given I drop a Cassandra keyspace 'opera' 