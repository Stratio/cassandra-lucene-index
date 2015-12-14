@test @search 
Feature: Test geoSpatial searching geoDistance

Scenario: I connect to Cassandra cluster
	Given I connect to Cassandra cluster with '2' nodes and this url '172.17.0.4'
	When I create a Cassandra keyspace named 'opera' 
	And I create table named: 'location' using keyspace: 'opera' with:
		| place  | latitude | longitude |lucene |
		| TEXT   | DECIMAL  |  DECIMAL  |TEXT   |
		|  PK        | PK       |         |       |
	And I insert in keyspace 'opera' and table 'location' with:
	 	|latitude|longitude|place|
		|2.5     |2.6      |'Madrid'|
		|12.5    |12.6     |'Barcelona'|

Scenario: Do a geoSpatial search with all results

    Given I create a map with index name 'location_index' with scheme 'schemes/mapping/geoPointMap.conf' of type 'string' in table 'location' using magic_column 'lucene' using keyspace 'opera' with:
		    | _Lat       | UPDATE  | latitude |
		    | _Lon       | UPDATE  | longitude |
		  	| maxLevels | UPDATE  | 15  |
	And I send a query with scheme 'schemes/queries/geoDistance.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with: 
		    | col       | UPDATE  | geo_point |
		    | __lat    | UPDATE  | 2 |
		    | __lon    | UPDATE  | 2 |
		  	| __maxDist    | UPDATE  | 61000km |
		  	| __minDist    | UPDATE  | 0 |	
	And There are '2' results after executing the last query
	
Scenario: Do a geoSpatial search out of the range with low distance

	Given I send a query with scheme 'schemes/queries/geoDistance.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with:
		    | col       | UPDATE  | geo_point |
		    | __lat    | UPDATE  | 2.5 |
		    | __lon    | UPDATE  | 2.6 |
		  	| __maxDist    | UPDATE  | 620m |
		  	| __minDist    | UPDATE  | 600m |	
	Then There are '0' results after executing the last query
	
Scenario: Do a geoSpatial search out of the range high distance

	Given I send a query with scheme 'schemes/queries/geoDistance.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with:
		    | col       | UPDATE  | geo_point |
		    | __lat    | UPDATE  | 2.5 |
		    | __lon    | UPDATE  | 2.6 |
		  	| __maxDist    | UPDATE  | 62000km |
		  	| __minDist    | UPDATE  | 60000km |	
	Then There are '0' results after executing the last query

Scenario: I remove all data
	Given I drop a Cassandra keyspace 'opera' 