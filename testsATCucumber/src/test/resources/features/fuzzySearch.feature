@test @search 
Feature: Test fuzzy searching

Scenario: I connect to Cassandra cluster
	Given I connect to Cassandra cluster at '${CASSANDRA_HOST}'
	When I create a Cassandra keyspace named 'opera' 
	And I create a Cassandra table named 'containsTable' using keyspace 'opera' with:
		| serie   | lucene|
		| TEXT   | TEXT|
		|  PK    | |
	And I insert in keyspace 'opera' and table 'containsTable' with:
	 	|serie      |
		|'00001'    |
		|'00002'    |
		|'00003'    |
		|'000034'    |
		|'000035'    |
		|'000036'    |
		|'000063'    |
		


Scenario: Do a basic String mapper
    Given I create a Cassandra index named 'place' with schema 'schemas/mapping/stringMap.conf' of type 'string' in table 'containsTable' using magic_column 'lucene' using keyspace 'opera' with:
		    | __column_name  | UPDATE  | serie |

#fuzzy maxEdits

Scenario: Do a fuzzy search with partial results
	Given I execute a query over fields '*' with schema 'schemas/queries/fuzzySearch.conf' of type 'string' with magic_column 'lucene' from table: 'containsTable' using keyspace: 'opera' with:
		    | __field  | UPDATE  |place   |
		  	| __value  | UPDATE  |0000|
		  	| __maxEdits  | UPDATE  | 1  |
		  	
	Then There are results found with:
		|serie      |occurrences |
		|00002|1           |	
		|00001|1           |	
		|00003|1           |	
		|000034|0           |	
		|000035|0           |	
		|000036|0           |	

Scenario: Do a fuzzy search with all results
	Given I execute a query over fields '*' with schema 'schemas/queries/fuzzySearch.conf' of type 'string' with magic_column 'lucene' from table: 'containsTable' using keyspace: 'opera' with:
		    | __field  | UPDATE  |place   |
		  	| __value  | UPDATE  |0000|
		  	| __maxEdits  | UPDATE  | 2  |
		  	
	Then There are results found with:
		|serie      |occurrences |
		|00002|1           |	
		|00001|1           |	
		|00003|1           |	
		|000034|1           |	
		|000035|1           |	
		|000036|1           |	

Scenario: Do a fuzzy search without results
	Given I execute a query over fields '*' with schema 'schemas/queries/fuzzySearch.conf' of type 'string' with magic_column 'lucene' from table: 'containsTable' using keyspace: 'opera' with:
		    | __field  | UPDATE  |place   |
		  	| __value  | UPDATE  |0000|
		  	| __maxEdits  | UPDATE  | 0  |
		  	
	Then There are results found with:
		|serie      |occurrences |
		|00002|0           |	
		|00001|0           |	
		|00003|0           |	
		|000034|0           |	
		|000035|0           |	
		|000036|0           |	

Scenario: Do a fuzzy search with invalid maxEdits up
	Given I execute a query over fields '*' with schema 'schemas/queries/fuzzySearch.conf' of type 'string' with magic_column 'lucene' from table: 'containsTable' using keyspace: 'opera' with:
		    | __field  | UPDATE  |place   |
		  	| __value  | UPDATE  |0000|
		  	| __maxEdits  | UPDATE  | 3  |
	Then an exception 'IS' thrown

Scenario: Do a fuzzy search with invalid maxEdits down
	Given I execute a query over fields '*' with schema 'schemas/queries/fuzzySearch.conf' of type 'string' with magic_column 'lucene' from table: 'containsTable' using keyspace: 'opera' with:
		    | __field  | UPDATE  |place   |
		  	| __value  | UPDATE  |0000|
		  	| __maxEdits  | UPDATE  | -1  |
	Then an exception 'IS' thrown

#fuzzy prefixLength
	
Scenario: Do a fuzzy search using prefixLength with results
	Given I execute a query over fields '*' with schema 'schemas/queries/fuzzySearchPrefix.conf' of type 'string' with magic_column 'lucene' from table: 'containsTable' using keyspace: 'opera' with:
		    | __field    | UPDATE  |place   |
		  	| __value    | UPDATE  |0000    |
		  	| __prefixLength | UPDATE  | 1      |
		  	
	Then There are results found with:
		|serie |occurrences |
		|00002 |1           |	
		|00001 |1           |	
		|00003 |1           |	
		|000034|1           |	
		|000035|1           |	
		|000036|1           |	
		
Scenario: Do a fuzzy search using prefixLength with partial results
	Given I execute a query over fields '*' with schema 'schemas/queries/fuzzySearchPrefix.conf' of type 'string' with magic_column 'lucene' from table: 'containsTable' using keyspace: 'opera' with:
		    | __field    | UPDATE 	   |place   |
		  	| __value    | UPDATE  	   |000034    |
		  	| __prefixLength | UPDATE  | 5      |
		  	
	Then There are results found with:
		|serie |occurrences |
		|00002 |0           |	
		|00001 |0           |	
		|00003 |1           |
		|000034|1           |		
		|000035|1           |	
		|000036|1           |	

Scenario: Do a fuzzy search with invalid prefixLength
	Given I execute a query over fields '*' with schema 'schemas/queries/fuzzySearchPrefix.conf' of type 'string' with magic_column 'lucene' from table: 'containsTable' using keyspace: 'opera' with:
		    | __field  | UPDATE  |place   |
		  	| __value  | UPDATE  |0000|
		  	| __maxEdits  | UPDATE  | -1  |
	Then an exception 'IS' thrown


Scenario: Do a fuzzy search using maxExpansions with results
	Given I execute a query over fields '*' with schema 'schemas/queries/fuzzySearchMaxExpansions.conf' of type 'string' with magic_column 'lucene' from table: 'containsTable' using keyspace: 'opera' with:
		    | __field    | UPDATE 	   |place   |
		  	| __value    | UPDATE  	   |0000    |
		  	| __prefixLength | UPDATE  | 3      |
		  	| __maxEpansions | UPDATE  | 20      |
		  	
		  	
	Then There are results found with:
		|serie |occurrences |
		|00002 |1           |	
		|00001 |1           |	
		|00003 |1           |
		|000034|1           |		
		|000035|1           |	
		|000036|1           |	

Scenario: Do a fuzzy search using maxExpansions without results
	Given I execute a query over fields '*' with schema 'schemas/queries/fuzzySearchMaxExpansions.conf' of type 'string' with magic_column 'lucene' from table: 'containsTable' using keyspace: 'opera' with:
		    | __field    | UPDATE 	   |place   |
		  	| __value    | UPDATE  	   |0000    |
		  	| __prefixLength | UPDATE  | 3      |
		  	| __maxEpansions | UPDATE  | 0      |
	Then an exception 'IS' thrown


Scenario: Do a fuzzy search using maxExpansions with invalid value
	Given I execute a query over fields '*' with schema 'schemas/queries/fuzzySearchMaxExpansions.conf' of type 'string' with magic_column 'lucene' from table: 'containsTable' using keyspace: 'opera' with:
		    | __field    | UPDATE 	   |place   |
		  	| __value    | UPDATE  	   |0000    |
		  	| __prefixLength | UPDATE  | 3      |
		  	| __maxEpansions | UPDATE  | -1      |
	Then an exception 'IS' thrown

Scenario: Do a fuzzy search using transpositions = false


	Given I execute a query over fields '*' with schema 'schemas/queries/fuzzySearchTransposition.conf' of type 'string' with magic_column 'lucene' from table: 'containsTable' using keyspace: 'opera' with:
		    | __field    | UPDATE 	   |place   |
		  	| __value    | UPDATE  	   |000036    |
		  	| __maxEdits  | UPDATE  | 1  |
		  	| __transpositions | UPDATE  | false      |		  	
	Then There are results found with:
		|serie |occurrences |
		|000036|1          |
		|000063|0          |	
			

Scenario: Do a fuzzy search using transpositions = true
	Given I execute a query over fields '*' with schema 'schemas/queries/fuzzySearchTransposition.conf' of type 'string' with magic_column 'lucene' from table: 'containsTable' using keyspace: 'opera' with:
		    | __field    | UPDATE 	   |place   |
		  	| __value    | UPDATE  	   |000036    |
		  	| __maxEdits  | UPDATE  | 1  |
		  	| __transpositions | UPDATE  | true      |		  	
	Then There are results found with:
		|serie |occurrences |
		|000036| 1          |	
		|000063|1         |	


Scenario: I remove all data
	Given I drop a Cassandra keyspace 'opera' 