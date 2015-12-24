@test @search 
Feature: Test geoSpatial searching geoBbox

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
		|12.5    |12.7     |'Valencia'|
		|12.5    |13.7     |'Sevilla'|

Scenario: Do a geoSpatial search out of the range of data

    Given I create a Cassandra index named 'location_index' with schema 'schemas/mapping/geoPointMap.conf' of type 'string' in table 'location' using magic_column 'lucene' using keyspace 'opera' with:
		    | _Lat       | UPDATE  | latitude |
		    | _Lon       | UPDATE  | longitude |
		  	| maxLevels | UPDATE  | 15  |


	
Scenario: Do a geoSpatial search with some data in keyspace with results

	Given I execute a query over fields '*' with schema 'schemas/queries/geoBboxSearch.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with:
		    | col       | UPDATE  | geo_point |
		    | minlat    | UPDATE  | -90 |
		    | minlon    | UPDATE  | -90 |
		  	| maxlat    | UPDATE  | 90 |
		  	| maxlon    | UPDATE  | 90 |		 

	Then There are results found with:
			|latitude| longitude|place     |occurrences|
			|12.5|12.7|Valencia    |1|
		    |2.5| 2.6 |Stratio   |0|
		    |12.5|13.7|Sevilla    |1|
		    |2.5|2.6|Madrid    |1|
		    
Scenario: Do a geoSpatial search with some data in keyspace with results

	Given I execute a query over fields '*' with schema 'schemas/queries/geoBboxSearch.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with:
		    | col       | UPDATE  | geo_point |
		    | minlat    | UPDATE  | 0 |
		    | minlon    | UPDATE  | -90 |
		  	| maxlat    | UPDATE  | 12.4 |
		  	| maxlon    | UPDATE  | 90 |		 

	Then There are results found with:
			|latitude| longitude|place     |occurrences|
			|2.5     |2.6      |Madrid|1|
			|12.5    |12.6     |Barcelona|0|
			|12.5    |12.7     |Valencia|0|
			|12.5    |13.7     |Sevilla|0|
		    
Scenario: Do a geoSpatial search with some data in keyspace with results

	Given I execute a query over fields '*' with schema 'schemas/queries/geoBboxSearch.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with:
		    | col       | UPDATE  | geo_point |
		    | minlat    | UPDATE  | 0 |
		    | minlon    | UPDATE  | -90 |
		  	| maxlat    | UPDATE  | 12.4 |
		  	| maxlon    | UPDATE  | 90 |		 

	Then There are results found with:
			|latitude| longitude|place     |occurrences|
			|2.5     |2.6      |Madrid|1|
			|12.5    |12.6     |Barcelona|0|
			|12.5    |12.7     |Valencia|0|
			|12.5    |13.7     |Sevilla|0|

Scenario: Do a geoSpatial search with some data in keyspace with results

	Given I execute a query over fields '*' with schema 'schemas/queries/geoBboxSearch.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with:
		    | col       | UPDATE  | geo_point |
		    | minlat    | UPDATE  | -90 |
		    | minlon    | UPDATE  | -180 |
		  	| maxlat    | UPDATE  | 90 |
		  	| maxlon    | UPDATE  | 180 |		 

	Then There are results found with:
			|latitude| longitude|place     |occurrences|
			|2.5     |2.6      |Madrid|1|
			|12.5    |12.6     |Barcelona|1|
			|12.5    |12.7     |Valencia|1|
			|12.5    |13.7     |Sevilla|1|

Scenario: Do a geoSpatial search with some data in keyspace with results

	Given I execute a query over fields '*' with schema 'schemas/queries/geoBboxSearch.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with:
		    | col       | UPDATE  | geo_point |
		    | minlat    | UPDATE  | -91 |
		    | minlon    | UPDATE  | -180 |
		  	| maxlat    | UPDATE  | 90 |
		  	| maxlon    | UPDATE  | 180 |		 
	Then an exception 'IS' thrown  with class 'Exception'

Scenario: Do a geoSpatial search with some data in keyspace with results

	Given I execute a query over fields '*' with schema 'schemas/queries/geoBboxSearch.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with:
		    | col       | UPDATE  | geo_point |
		    | minlat    | UPDATE  | -90 |
		    | minlon    | UPDATE  | -181 |
		  	| maxlat    | UPDATE  | 90 |
		  	| maxlon    | UPDATE  | 180 |		 
	Then an exception 'IS' thrown  with class 'Exception'
	
Scenario: Do a geoSpatial search with some data in keyspace with results

	Given I execute a query over fields '*' with schema 'schemas/queries/geoBboxSearch.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with:
		    | col       | UPDATE  | geo_point |
		    | minlat    | UPDATE  | -90 |
		    | minlon    | UPDATE  | -180 |
		  	| maxlat    | UPDATE  | 91 |
		  	| maxlon    | UPDATE  | 180 |		 
	Then an exception 'IS' thrown  with class 'Exception'

Scenario: Do a geoSpatial search with some data in keyspace with results

	Given I execute a query over fields '*' with schema 'schemas/queries/geoBboxSearch.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' with:
		    | col       | UPDATE  | geo_point |
		    | minlat    | UPDATE  | -90 |
		    | minlon    | UPDATE  | -180 |
		  	| maxlat    | UPDATE  | 91 |
		  	| maxlon    | UPDATE  | 181 |		 
	Then an exception 'IS' thrown  with class 'Exception'


Scenario: I remove all data
	Given I drop a Cassandra keyspace 'opera' 