@test @search 
Feature: Test analyzers

Scenario: I connect to Cassandra cluster
	Given I connect to Cassandra cluster at '${CASSANDRA_HOST}'
	When I create a Cassandra keyspace named 'opera' 
	And I create a Cassandra table named 'analyzertable' using keyspace 'opera' with:
		|name  | comment |lucene |
		| TEXT |TEXT     |TEXT	 |
		|  PK  |         |       |
	And I insert in keyspace 'opera' and table 'analyzertable' with: 
		|name 			|comment				|
		|'Kurt'      	|'Hello to a man'   			|
		|'Michael'    	|'Hello to a woman'			|
		|'Louis'     	|'Bye to a man' 				|
		|'John'     	|'Bye to a woman'  			|
		|'James'     	|'Hello to a man and a woman'  	|



Scenario: Create a snowball analyzer
    Given I create a Cassandra index named 'snowballMap' with schema 'schemas/analyzers/snowball.conf' of type 'string' in table 'analyzertable' using magic_column 'lucene' using keyspace 'opera' with:
		    | __analyzerName | UPDATE  | snowball |
		    | __language  | UPDATE  | English |
		    | __stopwords  | UPDATE  | a,to,and |
		    |__refresh_seconds|	UPDATE | 1			|

Scenario: Searching using the snowball test 1
		Given I execute a query over fields '*' with schema 'schemas/queries/phraseSearch.conf' of type 'string' with magic_column 'lucene' from table: 'analyzertable' using keyspace: 'opera' with:
		    | __field  | UPDATE  |comment |
		  	| __values  | UPDATE |woman |
		  	|__slop		 |UPDATE | 2 |
		Then There are results found with:
			|name|occurrences|
			|Michael|1|
			|James|1|
			|John|1|
			|Kurt|0|
			|Louis|0|
Scenario: Searching using the snowball test 2
		Given I execute a query over fields '*' with schema 'schemas/queries/phraseSearch.conf' of type 'string' with magic_column 'lucene' from table: 'analyzertable' using keyspace: 'opera' with:
		    | __field  | UPDATE  |comment |
		  	| __values  | UPDATE |man |
		  	|__slop		 |UPDATE | 2 |
		Then There are results found with:
			|name|occurrences|
			|Michael|0|
			|James|1|
			|John|0|
			|Kurt|1|
			|Louis|1|

Scenario: Searching using the snowball test 3
		Given I execute a query over fields '*' with schema 'schemas/queries/phraseSearch.conf' of type 'string' with magic_column 'lucene' from table: 'analyzertable' using keyspace: 'opera' with:
		    | __field  | UPDATE  |comment |
		  	| __values  | UPDATE |Hello |
		  	|__slop		 |UPDATE | 2 |
		Then There are results found with:
			|name|occurrences|
			|Michael|1|
			|James|1|
			|John|0|
			|Kurt|1|
			|Louis|0|

Scenario: Searching using the snowball test 4
		Given I execute a query over fields '*' with schema 'schemas/queries/phraseSearch.conf' of type 'string' with magic_column 'lucene' from table: 'analyzertable' using keyspace: 'opera' with:
		    | __field  | UPDATE  |comment |
		  	| __values  | UPDATE |Bye |
		  	|__slop		 |UPDATE | 2 |
		Then There are results found with:
			|name|occurrences|
			|Michael|0|
			|James|0|
			|John|1|
			|Kurt|0|
			|Louis|1|			
			
Scenario: I remove all data
	Given I drop a Cassandra keyspace 'opera' 