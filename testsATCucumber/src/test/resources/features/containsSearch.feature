@test @search 
Feature: Test contains searching

Scenario: I connect to Cassandra cluster
	Given I connect to Cassandra cluster at '${CASSANDRA_HOST}'
	When I create a Cassandra keyspace named 'opera' 
	And I create a Cassandra table named 'containsTable' using keyspace 'opera' with:
		| place  | latitude | longitude |lucene |
		| TEXT   | DECIMAL  |  DECIMAL  |TEXT   |
		|  PK    |          |           |       |
	And I insert in keyspace 'opera' and table 'containsTable' with:
	 	|latitude|longitude|place      |
		|2.5     |2.6      |'Madrid'   |
		|12.5    |12.6     |'Barcelona'|
		|12.5    |12.7     |'Valencia' |
		|12.5    |13.7     |'Sevilla'  |
		|12.9    |11.7     |'Santa Cruz'  |



Scenario: Do a basic boolean search 
    Given I create a Cassandra index named 'index_place' with schema 'schemas/mapping/stringMap.conf' of type 'string' in table 'containsTable' using magic_column 'lucene' using keyspace 'opera' with:
		    | __column_name  | UPDATE  | place |

#contains

Scenario: Do a basic contains search with results
	Given I execute a query over fields '*' with schema 'schemas/queries/containsSearch.conf' of type 'string' with magic_column 'lucene' from table: 'containsTable' using keyspace: 'opera' with:
		    | __field  | UPDATE  |place   |
		  	| __value1  | UPDATE  | Madrid   |
		  	| __value2  | UPDATE  | Barcelona   |
		  	
	Then There are results found with:
	 	| place     | occurrences|
		| Madrid   	|1           |
		| Barcelona |1           |
		| Valencia |0           |
		

Scenario: Do a basic contains search without results
	Given I execute a query over fields '*' with schema 'schemas/queries/containsSearch.conf' of type 'string' with magic_column 'lucene' from table: 'containsTable' using keyspace: 'opera' with:
		    | __field  | UPDATE  |place   |
		  	| __value1  | UPDATE  | Roma   |
		  	| __value2  | UPDATE  | San Petersburgo   |
		  	
	Then There are results found with:
	 	| place     | occurrences|
		| Madrid   	|0           |
		| Barcelona |0           |
		| Valencia |0           |
		| Roma |0           |

Scenario: Do a basic contains search with partial results
	Given I execute a query over fields '*' with schema 'schemas/queries/containsSearch.conf' of type 'string' with magic_column 'lucene' from table: 'containsTable' using keyspace: 'opera' with:
		    | __field  | UPDATE  |place   |
		  	| __value1  | UPDATE  | Santa Cruz  |
		  	| __value2  | UPDATE  | Barcelona  |
		  	
	Then There are results found with:
	 	| place     | occurrences|
		| Madrid   	|0           |
		| Barcelona |1           |
		| Valencia |0           |
		| Sevilla |0           |
		| Santa Cruz |1           |
		


Scenario: I remove all data
	Given I drop a Cassandra keyspace 'opera' 