@test @search 
Feature: Test boolean searching 

Scenario: I connect to Cassandra cluster 
	Given I connect to Cassandra cluster at '${CASSANDRA_HOST}' 
	When I create a Cassandra keyspace named 'opera' 
	And I create a Cassandra table named 'booleanTable' using keyspace 'opera' with: 
		| place  | latitude | longitude |lucene |
		| TEXT   | DECIMAL  |  DECIMAL  |TEXT   |
		|  PK    |          |           |       |
	And I insert in keyspace 'opera' and table 'booleanTable' with: 
		|latitude|longitude|place      |
		|2.5     |2.6      |'Madrid'   |
		|12.5    |12.6     |'Barcelona'|
		|12.5    |12.7     |'Valencia' |
		|12.5    |13.7     |'Sevilla'  |
		
		
		
Scenario: Do a basic boolean search 
	Given I create a Cassandra index named 'index_place' with schema 'schemas/mapping/stringMap.conf' of type 'string' in table 'booleanTable' using magic_column 'lucene' using keyspace 'opera' with: 
		| __column_name  | UPDATE  | place |
		
		#wildcard
		
Scenario: Do a basic boolean search without results 
	Given I execute a query over fields '*' with schema 'schemas/queries/booleanSearchWildCard.conf' of type 'string' with magic_column 'lucene' from table: 'booleanTable' using keyspace: 'opera' with: 
		| __firstfield  | UPDATE  |place   |
		| __firstvalue  | UPDATE  | Roma   |
	Then There are results found with: 
		| place     | occurrences|
		| Roma   	|0           |
		
Scenario: Do a basic boolean search with a result 
	Given I execute a query over fields '*' with schema 'schemas/queries/booleanSearchWildCard.conf' of type 'string' with magic_column 'lucene' from table: 'booleanTable' using keyspace: 'opera' with: 
		| __firstfield  | UPDATE  |place   |
		| __firstvalue  | UPDATE  | Madrid |
	Then There are results found with: 
		| place     | occurrences|
		| Madrid   	|1           |
		
Scenario: Do a basic boolean search with *a pattern 
	Given I execute a query over fields '*' with schema 'schemas/queries/booleanSearchWildCard.conf' of type 'string' with magic_column 'lucene' from table: 'booleanTable' using keyspace: 'opera' with: 
		| __firstfield  | UPDATE  |place |
		| __firstvalue  | UPDATE  | *a   |
	Then There are results found with: 
		| place     		| occurrences|
		| Barcelona  		|1           |
		| Valencia  		|1           |
		| Sevilla  			|1           |
		
Scenario: Do a basic boolean search with *i* pattern 
	Given I execute a query over fields '*' with schema 'schemas/queries/booleanSearchWildCard.conf' of type 'string' with magic_column 'lucene' from table: 'booleanTable' using keyspace: 'opera' with: 
		| __firstfield  | UPDATE  |place |
		| __firstvalue  | UPDATE  | *i*   |
	Then There are results found with: 
		| place     		| occurrences|
		| Madrid     		|1           |
		| Valencia  		|1           |
		| Sevilla  			|1           |
		| Barcelona 	    |0           |
		
		#match
		
Scenario: Do a basic boolean search with match 
	Given I execute a query over fields '*' with schema 'schemas/queries/booleanSearchMatch.conf' of type 'string' with magic_column 'lucene' from table: 'booleanTable' using keyspace: 'opera' with: 
		| __firstfield  | UPDATE  |place |
		| __firstvalue  | UPDATE  | Madrid   |
	Then There are results found with: 
		| place     		| occurrences|
		| Madrid     		|1           |
		| Valencia  		|0           |
		| Sevilla  			|0           |
		| Barcelona 	    |0           |
		
Scenario: Do a basic boolean search with match 
	Given I execute a query over fields '*' with schema 'schemas/queries/booleanSearchMatch.conf' of type 'string' with magic_column 'lucene' from table: 'booleanTable' using keyspace: 'opera' with: 
		| __firstfield  | UPDATE  |place |
		| __firstvalue  | UPDATE  |    Valencia|
	Then There are results found with: 
		| place     		| occurrences|
		| Madrid     		|0           |
		| Valencia  		|1           |
		| Sevilla  			|0           |
		| Barcelona 	    |0           |
		
Scenario: Do a basic boolean search with match 
	Given I execute a query over fields '*' with schema 'schemas/queries/booleanSearchMatch.conf' of type 'string' with magic_column 'lucene' from table: 'booleanTable' using keyspace: 'opera' with: 
		| __firstfield  | UPDATE  |place    |
		| __firstvalue  | UPDATE  |Barcelona|
	Then There are results found with: 
		| place     		| occurrences|
		| Madrid     		|0           |
		| Valencia  		|0           |
		| Sevilla  			|0           |
		| Barcelona 	    |1           |
		
Scenario: Do a basic boolean search with match 
	Given I execute a query over fields '*' with schema 'schemas/queries/booleanSearchMatch.conf' of type 'string' with magic_column 'lucene' from table: 'booleanTable' using keyspace: 'opera' with: 
		| __firstfield  | UPDATE  |place |
		| __firstvalue  | UPDATE  |    Valencia|
	Then There are results found with: 
		| place     		| occurrences|
		| Madrid     		|0           |
		| Valencia  		|1           |
		| Sevilla  			|0           |
		| Barcelona 	    |0           |
		
		# Not
		
Scenario: Do a basic boolean search with match 
	Given I execute a query over fields '*' with schema 'schemas/queries/booleanSearchNotMatch.conf' of type 'string' with magic_column 'lucene' from table: 'booleanTable' using keyspace: 'opera' with: 
		| __firstfield  | UPDATE  |place |
		| __firstvalue  | UPDATE  | Madrid   |
	Then There are results found with: 
		| place     		| occurrences|
		| Madrid     		|0           |
		| Valencia  		|1           |
		| Sevilla  			|1           |
		| Barcelona 	    |1           |
		
Scenario: Do a basic boolean search with match 
	Given I execute a query over fields '*' with schema 'schemas/queries/booleanSearchNotMatch.conf' of type 'string' with magic_column 'lucene' from table: 'booleanTable' using keyspace: 'opera' with: 
		| __firstfield  | UPDATE  |place |
		| __firstvalue  | UPDATE  |    Valencia|
	Then There are results found with: 
		| place     		| occurrences|
		| Madrid     		|1           |
		| Valencia  		|0           |
		| Sevilla  			|1           |
		| Barcelona 	    |1           |
		
Scenario: Do a basic boolean search with match 
	Given I execute a query over fields '*' with schema 'schemas/queries/booleanSearchNotMatch.conf' of type 'string' with magic_column 'lucene' from table: 'booleanTable' using keyspace: 'opera' with: 
		| __firstfield  | UPDATE  |place    |
		| __firstvalue  | UPDATE  |Barcelona|
	Then There are results found with: 
		| place     		| occurrences|
		| Madrid     		|1           |
		| Valencia  		|1           |
		| Sevilla  			|1           |
		| Barcelona 	    |0           |
		
Scenario: Do a basic boolean search with match 
	Given I execute a query over fields '*' with schema 'schemas/queries/booleanSearchNotMatch.conf' of type 'string' with magic_column 'lucene' from table: 'booleanTable' using keyspace: 'opera' with: 
		| __firstfield  | UPDATE  |place |
		| __firstvalue  | UPDATE  |    Valencia|
	Then There are results found with: 
		| place     		| occurrences|
		| Madrid     		|1           |
		| Valencia  		|0           |
		| Sevilla  			|1           |
		| Barcelona 	    |1           |
		
Scenario: I remove all data 
	Given I drop a Cassandra keyspace 'opera' 