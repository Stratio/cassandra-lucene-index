@test @search 
Feature: Test geoSpatial searching 

Scenario: I connect to Cassandra cluster
	Given I connect to Cassandra cluster with '2' nodes and this url '172.17.0.4'
	When I create a Cassandra keyspace named 'opera' 
	And I create a table named: 'location' using the keyspace: 'opera' and this datatable:
		| place  | latitude | longitude |lucene |
		| TEXT   | DECIMAL  |  DECIMAL  |TEXT   |
		|  PK        | PK       |         |       |

Scenario: Do a geoSpatial search with a clean keyspace 

    And I create a mapping with index name 'location_index' with scheme 'schemes/mapping/geoPointMap.conf' of type 'string' in table 'location' using magic_column 'lucene' using keyspace 'opera' and this options:
		    | _Lat       | UPDATE  | latitude |
		    | _Lon       | UPDATE  | longitude |
		  	| maxLevels | UPDATE  | 15  |
	And I create a mapping with index name 'location_place' in table 'location' using magic_column 'lucene' using keyspace 'opera'		  	
	And I send a query with scheme 'schemes/queries/geoBboxSearch.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' and this modifications: 
		    | col       | UPDATE  | location_place |
		    | minlat    | UPDATE  | -10 |
		    | minlon    | UPDATE  | -10 |
		  	| maxlat    | UPDATE  | 90 |
		  	| maxlon    | UPDATE  | 90 |	
	And There are '0' results after execute the last query
	
Scenario: Do a geoSpatial search with some data in keyspace
	Given I insert in keyspace 'opera' and table 'location' this data:
	 	|latitude|longitude|place|
		|2.5     |2.6      |'Madrid'|
		|12.5    |12.6     |'Barcelona'|

	And I send a query with scheme 'schemes/queries/geoBboxSearch.conf' of type 'string' with magic_column 'lucene' from table: 'location' using keyspace: 'opera' and this modifications: 
		    | col       | UPDATE  | location_place |
		    | minlat    | UPDATE  | -10 |
		    | minlon    | UPDATE  | -10 |
		  	| maxlat    | UPDATE  | 90 |
		  	| maxlon    | UPDATE  | 90 |		 

	And There are '1' results after execute the last query
	Then I drop a C keyspace 'opera'
	
Scenario: I remove all data
	Given I drop a C keyspace 'opera' 