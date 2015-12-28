@test @search 
Feature: Test date range searching

Scenario: I connect to Cassandra cluster
	Given I connect to Cassandra cluster at '${CASSANDRA_HOST}'
	When I create a Cassandra keyspace named 'opera' 
	And I create a Cassandra table named 'bitemporal' using keyspace 'opera' with:
		| name   |city| date_from | date_to |lucene|
		| TEXT   |TEXT| TEXT    |  TEXT |TEXT    |
		|  PK    |    | PK      |       |      |
	And I insert in keyspace 'opera' and table 'bitemporal' with:
	 	| name        |city        | date_from    			 	 | date_to      			   	   |
		|'Mathew'     |'Madrid'    |'2015/01/02 00:00:00.000'|'2015/02/01 12:00:00.000'|
		|'Michael'    |'Amsterdam' |'2015/04/01 00:00:00.000'|'2015/05/01 12:00:00.000'|
		|'Meghan'     |'Paris'     |'2015/07/01 00:00:00.000'|'2015/08/01 12:00:00.000'|
		|'Kurt'       |'New York'  |'2015/09/01 00:00:00.000'|'2015/10/01 12:00:00.000'|
		|'Louis'      |'Roma'      |'2015/12/01 00:00:00.000'|'2015/12/31 12:00:00.000'|
		

Scenario: Do a date_range map for tests

    Given I create a Cassandra index named 'date_range' with schema 'schemas/mapping/dateRangeMap.conf' of type 'string' in table 'bitemporal' using magic_column 'lucene' using keyspace 'opera' with:
		    | __range_from  | UPDATE  | date_from 			  	  |
		    | __range_to    | UPDATE  | date_to  			  	  |
		  		
Scenario: Do a dateRange search with a result

	Given I execute a query over fields '*' with schema 'schemas/queries/dateRange.conf' of type 'string' with magic_column 'lucene' from table: 'bitemporal' using keyspace: 'opera' with:
		    | __field  | UPDATE  | date_range |
		    | __dateTo    | UPDATE  | 2015/02/02 00:00:00.000 |
		  	| __dateFrom  | UPDATE  | 2015/01/01 00:00:00.000 |
		  	| __operation   | UPDATE  | intersects  |
	Then There are results found with:
	 	| name        |city        | occurrences|
		|Mathew       |Madrid      |1           |
		|Michael      |Amsterdam   |0           |
		|Meghan       |Paris       |0           |
		|Kurt         |New York    |0           |
		|Louis        |Roma        |0           |
		
Scenario: Do a dateRange search with all results

	Given I execute a query over fields '*' with schema 'schemas/queries/dateRange.conf' of type 'string' with magic_column 'lucene' from table: 'bitemporal' using keyspace: 'opera' with:
		    | __field  | UPDATE  | date_range |
		    | __dateFrom    | UPDATE  | 2015/01/01 00:00:00.000 |
		  	| __dateTo  | UPDATE  | 2016/01/01 00:00:00.000 |
		  	| __operation   | UPDATE  | intersects  |
	Then There are results found with:
	 	| name        |city        | occurrences|
		|Mathew       |Madrid      |1           |
		|Michael      |Amsterdam   |1           |
		|Meghan       |Paris       |1           |
		|Kurt         |New York    |1           |
		|Louis        |Roma        |1           |

Scenario: Do a dateRange search out of a valid range without results

	Given I execute a query over fields '*' with schema 'schemas/queries/dateRange.conf' of type 'string' with magic_column 'lucene' from table: 'bitemporal' using keyspace: 'opera' with:
		    | __field  | UPDATE  | date_range |
		    | __dateFrom    | UPDATE  | 2013/01/01 00:00:00.000 |
		  	| __dateTo  | UPDATE  | 2014/01/01 00:00:00.000 |
		  	| __operation   | UPDATE  | intersects  |
	Then There are results found with:
	 	| name        |city        | occurrences|
		|Mathew       |Madrid      |0           |
		|Michael      |Amsterdam   |0           |
		|Meghan       |Paris       |0           |
		|Kurt         |New York    |0           |
		|Louis        |Roma        |0           |

Scenario: Do a dateRange search with invalid date range

	Given I execute a query over fields '*' with schema 'schemas/queries/dateRange.conf' of type 'string' with magic_column 'lucene' from table: 'bitemporal' using keyspace: 'opera' with:
		    | __field  | UPDATE  | date_range |
		    | __dateFrom    | UPDATE  | 2016/01/01 00:00:00.000 |
		  	| __dateTo  | UPDATE  | 2015/01/01 00:00:00.000 |
		  	| __operation   | UPDATE  | intersects  |
	Then an exception 'IS' thrown

Scenario: I remove all data
	Given I drop a Cassandra keyspace 'opera' 