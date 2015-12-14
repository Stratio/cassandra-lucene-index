@test @search 
Feature: Test geoSpatial searching geoBbox

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

Scenario: Do a geoSpatial search out of the range of data

    Given I create a map with index name 'location_index' with scheme 'schemes/mapping/geoPointMap.conf' of type 'string' in table 'location' using magic_column 'lucene' using keyspace 'opera' with:
		    | _Lat       | UPDATE  | latitude |
		    | _Lon       | UPDATE  | longitude |
		  	| maxLevels | UPDATE  | 15  |

	And I send a query with scheme 'schemes/queries/geoBboxSearch.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with: 
		    | col       | UPDATE  | geo_point |
		    | minlat    | UPDATE  | -90 |
		    | minlon    | UPDATE  | -90 |
		  	| maxlat    | UPDATE  | -60 |
		  	| maxlon    | UPDATE  | -60 |	
	And There are '0' results after executing the last query
	
Scenario: Do a geoSpatial search with some data in keyspace with 2 results

	Given I send a query with scheme 'schemes/queries/geoBboxSearch.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with: 
		    | col       | UPDATE  | geo_point |
		    | minlat    | UPDATE  | 1 |
		    | minlon    | UPDATE  | 1 |
		  	| maxlat    | UPDATE  | 90 |
		  	| maxlon    | UPDATE  | 90 |		 

	Then There are '2' results after executing the last query
	
Scenario: Do a geoSpatial search with some data in keyspace with 1 result

	Given I send a query with scheme 'schemes/queries/geoBboxSearch.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with: 
		    | col       | UPDATE  | geo_point |
		    | minlat    | UPDATE  | 1 |
		    | minlon    | UPDATE  | 1 |
		  	| maxlat    | UPDATE  | 11 |
		  	| maxlon    | UPDATE  | 11 |		 

	Then There are '1' results after executing the last query
	
Scenario: Do a geoSpatial search with some data in keyspace with 1 result

	Given I send a query with scheme 'schemes/queries/geoBboxSearch.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with: 
		    | col       | UPDATE  | geo_point |
		    | minlat    | UPDATE  | 2.5 |
		    | minlon    | UPDATE  | 2.5 |
		  	| maxlat    | UPDATE  | 2.7 |
		  	| maxlon    | UPDATE  | 2.8 |		 

	Then There are '1' results after executing the last query
	
Scenario: Do a geoSpatial search with some data in keyspace with 1 result

	Given I send a query with scheme 'schemes/queries/geoBboxSearch.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with: 
		    | col       | UPDATE  | geo_point |
		    | minlat    | UPDATE  | -90 |
		    | minlon    | UPDATE  | -90 |
		  	| maxlat    | UPDATE  | 90 |
		  	| maxlon    | UPDATE  | 90 |		 

	Then There are '2' results after executing the last query
	
Scenario: I remove all data
	Given I drop a Cassandra keyspace 'opera' 