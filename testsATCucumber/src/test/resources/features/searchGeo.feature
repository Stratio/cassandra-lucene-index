@test @search 
Feature: Test geoSpatial searching 

Scenario: Do a geoSpatial search with a clean keyspace 
	Given I connect to Cassandra cluster with '2' nodes and this url '172.17.0.4'
		And I drop a C keyspace 'opera' 
	When I create a C keyspace named 'opera' 
	And I create a table named: 'location' using the keyspace: 'opera' and this datatable:
		| place | latitude | longitude | lucene      |
		 | TEXT  | DECIMAL  |  DECIMAL  |TEXT       |
		|   PK       | PK   |        |           |
	And I create a mapping of type 'opera' in table 'location' using magic_column 'lucene', max levels '1', latitude '2.5' and longitude '2.6'
	
	And I send a query with geoBbox search from table: 'location' with keyspace: 'opera' and this datatable: 
		|magic_colum|min_latitude|min_longitude|max_latitude|max_longitude|filter_query|field|
		|lucene     |0.0         |0.0          |10.0        |10.0         |filter      |place|
	And There are '0' results after execute the last query
	And I drop a C keyspace 'opera'
	
Scenario: Do a geoSpatial search with some data in keyspace
	Given I connect to Cassandra cluster with '2' nodes and this url '172.17.0.4' 
	When I create a C keyspace named 'opera' 
	And I create a table named: 'location' using the keyspace: 'opera' and this datatable:
		| place | latitude | longitude | lucene      |
		 | TEXT  | DECIMAL  |  DECIMAL  |TEXT       |
		|    PK      | PK    |          |           |
	And I insert in keyspace 'opera' and table 'location' this data:
	 	|latitude|longitude|place|
		|2.5     |2.6      |'Madrid'|
		|12.5    |12.6     |'Barcelona'|
	And I create a mapping of type 'opera' in table 'location' using magic_column 'lucene', max levels '1', latitude '2.5' and longitude '2.6'
	And I send a query with geoBbox search from table: 'location' with keyspace: 'opera' and this datatable: 
		|magic_colum|min_latitude|min_longitude|max_latitude|max_longitude|filter_query|field|
		|lucene     |0.0         |0.0          |10.0        |10.0         |filter      |place|

	And There are '1' results after execute the last query
	And I drop a C keyspace 'opera'