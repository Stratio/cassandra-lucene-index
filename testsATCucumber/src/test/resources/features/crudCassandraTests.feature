@test @search 
Feature: CRUD tests for cassandra 

Scenario: I connect to Cassandra cluster 
	Given I connect to Cassandra cluster at '${CASSANDRA_HOST}' 
	
Scenario: Create a table without keyspace 
	Given I create a Cassandra table named 'incorrectTable' using keyspace 'opera' with: 
		|firstRow|lucene|
		|TEXT	 |TEXT	|
		|PK		 |		|
	Then an exception 'InvalidQueryException' thrown 
	
Scenario: Create a correct keyspace 
	Given I create a Cassandra keyspace named 'opera' 
	Then a Cassandra keyspace 'opera' exists 
	
	
Scenario: Create a table with keyspace and insert data 
	Given I create a Cassandra table named 'basic' using keyspace 'opera' with: 
		|firstRow|lucene|
		|TEXT	 |TEXT	|
		|PK		 |		|
	Then a Casandra keyspace 'opera' contains a table 'basic' 
	And I create a Cassandra index named 'first' in table 'basic' using magic_column 'lucene' using keyspace 'opera' 
	And I insert in keyspace 'opera' and table 'basic' with: 
		|firstRow|
		|'firstRowTest1'|
		|'firstRowTest2'|
		|'firstRowTest3'|
		|'firstRowTest4'|
		
Scenario: Check data from table 
	Given I execute a query over fields '*' with schema 'empty' of type 'string' with magic_column 'lucene' from table: 'basic' using keyspace: 'opera' with: 
		||
	Then There are results found with: 
		|firstrow			 |occurrences|
		|	firstRowTest1	 |		1	 |
		|	firstRowTest2	 |		1	 |
		|	firstRowTest3	 |		1	 |
		|	firstRowTest4	 |		1	 |
		
Scenario: Truncate data from table 
	Given I truncate a Cassandra table named 'basic' using keyspace 'opera' 
	When I execute a query over fields '*' with schema 'empty' of type 'string' with magic_column 'lucene' from table: 'basic' using keyspace: 'opera' with: 
		||
	Then There are results found with: 
		|firstrow			 |occurrences|
		|	firstRowTest1	 |		0	 |
		|	firstRowTest2	 |		0	 |
		|	firstRowTest3	 |		0	 |
		|	firstRowTest4	 |		0	 |
		
Scenario: Create a table already created 
	Given I create a Cassandra table named 'basic' using keyspace 'opera' with: 
		|firstRow|lucene|
		|TEXT	 |TEXT	|
		|PK		 |		|
	Then an exception 'AlreadyExistsException' thrown 
	
Scenario: Drop a existing table 
	Given I drop a Cassandra table named 'basic' using keyspace 'opera' 
	
Scenario: I remove all data 
	Given I drop a Cassandra keyspace 'opera'