@test @search 
Feature: Test match searching

Scenario: I connect to Cassandra cluster
	Given I connect to Cassandra cluster at '${CASSANDRA_HOST}'
	When I create a Cassandra keyspace named 'opera' 
	And I create a Cassandra table named 'matchTable' using keyspace 'opera' with:
		| name   | surname | age		 |lucene|
		| TEXT   | TEXT    |  DECIMAL 	 |TEXT  |
		|  PK    |         |           	 |      | 
	And I insert in keyspace 'opera' and table 'matchTable' with:
	 	| name    | surname   | age	 |
		|'John'   | 'Connor'  |22    |
		|'Michael'| 'Packard' |35    |
		|'Kurt'   | 'James'   |47    |


Scenario: Do a basic String mapper
    Given I create a Cassandra index named 'place' with schema 'schemas/mapping/stringMap.conf' of type 'string' in table 'matchTable' using magic_column 'lucene' using keyspace 'opera' with:
		    | __column_name  | UPDATE  | name |
		    
Scenario: I execute a match query without results
		Given I execute a query over fields '*' with schema 'schemas/queries/matchSearch.conf' of type 'string' with magic_column 'lucene' from table: 'matchTable' using keyspace: 'opera' with:
		    | __field  | UPDATE  |place   |
		  	| __value  | UPDATE  |Lucke|
		Then There are results found with:
			|name      |occurrences |
			|Lucke	   |0           |
			|Kurt	   |0           |	
			|Michael   |0           |
			|John      |0           |		

Scenario: I execute a match query with a result
		Given I execute a query over fields '*' with schema 'schemas/queries/matchSearch.conf' of type 'string' with magic_column 'lucene' from table: 'matchTable' using keyspace: 'opera' with:
		    | __field  | UPDATE  |place   |
		  	| __value  | UPDATE  |Kurt|
		Then There are results found with:
			|name      |occurrences |
			|Lucke	   |0           |
			|Kurt	   |1           |	
			|Michael   |0           |
			|John      |0           |		
			
Scenario: I execute a match query with a result
		Given I execute a query over fields '*' with schema 'schemas/queries/matchSearch.conf' of type 'string' with magic_column 'lucene' from table: 'matchTable' using keyspace: 'opera' with:
		    | __field  | UPDATE  |place   |
		  	| __value  | UPDATE  |Michael|
		Then There are results found with:
			|name      |occurrences |
			|Lucke	   |0           |
			|Kurt	   |0           |	
			|Michael   |1           |
			|John      |0           |		

Scenario: I execute a match query with a result
		Given I execute a query over fields '*' with schema 'schemas/queries/matchSearch.conf' of type 'string' with magic_column 'lucene' from table: 'matchTable' using keyspace: 'opera' with:
		    | __field  | UPDATE  |place   |
		  	| __value  | UPDATE  |John|
		Then There are results found with:
			|name      |occurrences |
			|Lucke	   |0           |
			|Kurt	   |0           |	
			|Michael   |0           |
			|John      |1           |		

	
			
Scenario: I remove all data
	Given I drop a Cassandra keyspace 'opera' 