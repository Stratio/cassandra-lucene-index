@test @search 
Feature: Test phrase searching

Scenario: I connect to Cassandra cluster
	Given I connect to Cassandra cluster at '${CASSANDRA_HOST}'
	When I create a Cassandra keyspace named 'opera' 
	And I create a Cassandra table named 'phraseTable' using keyspace 'opera' with:
		| comment |lucene|
		| TEXT    |TEXT  |
		|    PK     |      | 
	And I insert in keyspace 'opera' and table 'phraseTable' with:
	 	| comment   |
		| 'This is a comment'  |
		| 'This is other comment' |
		| 'This is the last comment'   |


Scenario: Do a basic String mapper
    Given I create a Cassandra index named 'place' with schema 'schemas/mapping/stringMap.conf' of type 'string' in table 'phraseTable' using magic_column 'lucene' using keyspace 'opera' with:
		    | __column_name  | UPDATE  | comment |
		    
Scenario: I execute a phrase query with results
		Given I execute a query over fields '*' with schema 'schemas/queries/phraseSearch.conf' of type 'string' with magic_column 'lucene' from table: 'phraseTable' using keyspace: 'opera' with:
		    | __field  | UPDATE  |place   |
		  	| __values  | UPDATE |This is a comment |
		  	|__slop		 |UPDATE | 0 |
		Then There are results found with:
			|comment      			  |occurrences |
			|This is a comment	      |1           |	
	
			
Scenario: I remove all data
	Given I drop a Cassandra keyspace 'opera' 