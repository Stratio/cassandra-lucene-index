@test @search 
Feature: Test geoSpatial searching geoDistance

Scenario: I connect to Cassandra cluster
	
	Given I connect to Cassandra cluster at '${CASSANDRA_HOST}'
	When I create a Cassandra keyspace named 'opera' 
	And I create a Cassandra table named 'location' using keyspace 'opera' with:
		| place  | latitude | longitude |lucene |
		| TEXT   | DECIMAL  |  DECIMAL  |TEXT   |
		|  PK        | PK       |         |       |
	And I insert in keyspace 'opera' and table 'location' with:
	 	|latitude|longitude|place|
		|2.5     |2.6      |'Madrid'|
		|12.5    |12.6     |'Barcelona'|

Scenario: Do a geoSpatial search with all results

    Given I create a Cassandra index named 'location_index' with schema 'schemas/mapping/geoPointMap.conf' of type 'string' in table 'location' using magic_column 'lucene' using keyspace 'opera' with:
		    | _Lat       | UPDATE  | latitude |
		    | _Lon       | UPDATE  | longitude |
		  	| maxLevels | UPDATE  | 15  |
	And I execute a query over fields '*' with schema 'schemas/queries/geoDistance.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with:
		    | col       | UPDATE  | geo_point |
		    | __lat    | UPDATE  | 2 |
		    | __lon    | UPDATE  | 2 |
		  	| __maxDist    | UPDATE  | 61000km |
		  	| __minDist    | UPDATE  | 0 |	
	Then There are results found with:
			| latitude       | longitude  | place |occurrences|
		    | 2.5    | 2.6  | Madrid |1|
		    |12.5    |12.6     |Barcelona|1|
Scenario: Do a geoSpatial search out of the range with low distance

	Given I execute a query over fields '*' with schema 'schemas/queries/geoDistance.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with:
		    | col       | UPDATE  | geo_point |
		    | __lat    | UPDATE  | 2.5 |
		    | __lon    | UPDATE  | 2.6 |
		  	| __maxDist    | UPDATE  | 620m |
		  	| __minDist    | UPDATE  | 600m |	
	Then There are results found with:
			| latitude       | longitude  | place |occurrences|
		    | 2.5    | 2.6  | Madrid |	0|
		    |12.5    |12.6     |Barcelona|0|
Scenario: Do a geoSpatial search out of the range with high distance

	Given I execute a query over fields '*' with schema 'schemas/queries/geoDistance.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with:
		    | col       | UPDATE  | geo_point |
		    | __lat    | UPDATE  | 2.5 |
		    | __lon    | UPDATE  | 2.6 |
		  	| __maxDist    | UPDATE  | 620000km |
		  	| __minDist    | UPDATE  | 600000km |	
	Then There are results found with:
			| latitude       | longitude  | place |occurrences|
		    | 2.5    | 2.6  | Madrid |	0|
		    |12.5    |12.6     |Barcelona|0|
	
Scenario: Do a geoSpatial search with a result expected

	Given I execute a query over fields '*' with schema 'schemas/queries/geoDistance.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with:
		    | col       | UPDATE  | geo_point |
		    | __lat    | UPDATE  | 0 |
		    | __lon    | UPDATE  | 0 |
		  	| __maxDist    | UPDATE  | 720km |
		  	| __minDist    | UPDATE  | 0km |	
	Then There are results found with:
			| latitude       | longitude  | place |occurrences|
		    | 2.5    | 2.6  | Madrid |	1|
		    |12.5    |12.6     |Barcelona|0|
		   
Scenario: Do a geoSpatial search with negative maxDistance distance

	Given I execute a query over fields '*' with schema 'schemas/queries/geoDistance.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with:
		    | col       | UPDATE  | geo_point |
		    | __lat    | UPDATE  | 0 |
		    | __lon    | UPDATE  | 0 |
		  	| __maxDist    | UPDATE  | -720km |
		  	| __minDist    | UPDATE  | 0km |	
Then an exception 'IS' thrown  with class 'Exception'

Scenario: Do a geoSpatial search with negative minDistance distance

	Given I execute a query over fields '*' with schema 'schemas/queries/geoDistance.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with:
		    | col       | UPDATE  | geo_point |
		    | __lat    | UPDATE  | 0 |
		    | __lon    | UPDATE  | 0 |
		  	| __maxDist    | UPDATE  | 720km |
		  	| __minDist    | UPDATE  | -100km |	
Then an exception 'IS' thrown  with class 'Exception' 

Scenario: I remove all data
	Given I drop a Cassandra keyspace 'opera' 