@test @search 
Feature: Test geoSpatial searching geoBbox

Scenario: I connect to Cassandra cluster
	Given I connect to Cassandra cluster at '${CASSANDRA_HOST}'
	When I create a Cassandra keyspace named 'opera' 
	And I create a Cassandra table named 'bitemporal' using keyspace 'opera' with:
		| name   |city| vt_from | vt_to |tt_from |tt_to |lucene|
		| TEXT   |TEXT| TEXT    |  TEXT |TEXT    |TEXT  |TEXT  |
		|  PK    |    | PK      |       | PK     |      |      |
	And I insert in keyspace 'opera' and table 'bitemporal' with:
	 	| name        |city        | vt_from    			 | vt_to      			   |tt_from     			 |tt_to                     |
		|'Mathew'     |'Madrid'    |'2015/01/01 00:00:00.000'|'2015/02/01 12:00:00.000'|'2015/01/15 12:00:00.001'|'2015/02/15 12:00:00.000' |
		|'Michael'    |'Amsterdam' |'2015/01/01 00:00:00.000'|'2015/02/01 12:00:00.000'|'2015/01/15 12:00:00.001'|'2015/02/15 12:00:00.000' | 
		|'Meghan'     |'Paris'     |'2015/01/01 00:00:00.000'|'2015/02/01 12:00:00.000'|'2015/01/15 12:00:00.001'|'2015/02/15 12:00:00.000' |
		|'Kurt'       |'New York'  |'2015/01/01 00:00:00.000'|'2015/02/01 12:00:00.000'|'2015/01/15 12:00:00.001'|'2015/02/15 12:00:00.000' |
		|'Louis'      |'Roma'      |'2015/01/01 00:00:00.000'|'2015/02/01 12:00:00.000'|'2015/01/15 12:00:00.001'|'2015/02/15 12:00:00.000' |
		

Scenario: Do a bitemporal search with all values

    Given I create a Cassandra index named 'bitemporal_index' with schema 'schemas/mapping/bitemporalMap.conf' of type 'string' in table 'bitemporal' using magic_column 'lucene' using keyspace 'opera' with:
		    | __vt_from  | UPDATE  | vt_from 			  	  |
		    | __vt_to    | UPDATE  | vt_to  			  	  |
		  	| __tt_from  | UPDATE  | tt_from  			  	  |
		  	| __tt_to    | UPDATE  | tt_to  			  	  |
		  	| __nowvalue | UPDATE  | 2015/12/01 00:00:00.000  |
		  		
Scenario: Do a geoSpatial search with some data in keyspace with all results

	Given I execute a query over fields '*' with schema 'schemas/queries/bitemporalSearch.conf' of type 'string' with magic_column 'lucene' from table: 'bitemporal' using keyspace: 'opera' with:
		    | __vt_from  | UPDATE  | 2015/01/01 00:00:00.000  |
		    | __vt_to    | UPDATE  | 2015/02/27 00:00:00.000  |
		  	| __tt_from  | UPDATE  | 2015/01/01 00:00:00.000  |
		  	| __tt_to    | UPDATE  | 2015/02/27 00:00:00.000  |
		  	| __field    | UPDATE  | bitemporal  			  |	 
	Then There are results found with:
	 	| name        |city        | occurrences|
		|Mathew       |Madrid      |1           |
		|Michael      |Amsterdam   |1           |
		|Meghan       |Paris       |1           |
		|Kurt         |New York    |1           |
		|Louis        |Roma        |1           |
		
Scenario: Do a geoSpatial search with some data in keyspace without results

	Given I execute a query over fields '*' with schema 'schemas/queries/bitemporalSearch.conf' of type 'string' with magic_column 'lucene' from table: 'bitemporal' using keyspace: 'opera' with:
		    | __vt_from  | UPDATE  | 2015/03/12 00:00:00.000  |
		    | __vt_to    | UPDATE  | 2015/04/02 00:00:00.000  |
		  	| __tt_from  | UPDATE  | 2015/01/04 00:00:00.000  |
		  	| __tt_to    | UPDATE  | 2015/02/27 00:00:00.000  |
		  	| __field    | UPDATE  | bitemporal  			  |	 
	Then There are results found with:
	 	| name        |city    | occurrences|
		|Mathew     |Madrid    |0		 	|
		|Michael    |Amsterdam |0			|
		|Meghan     |Paris     |0			|
		|Kurt       |New York  |0			|
		|Louis      |Roma      |0			|
		

		
Scenario: I remove all data
	Given I drop a Cassandra keyspace 'opera' 