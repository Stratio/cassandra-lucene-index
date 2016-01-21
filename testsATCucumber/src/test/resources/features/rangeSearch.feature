@test @search 
Feature: Test rangeSearch searching

Scenario: I connect to Cassandra cluster
	Given I connect to Cassandra cluster at '${CASSANDRA_HOST}'
	When I create a Cassandra keyspace named 'opera' 
	And I create a Cassandra table named 'rangeTable' using keyspace 'opera' with:
		| name   | surname | age		 |lucene|
		| TEXT   | TEXT    |  BIGINT	 |TEXT  |
		|  PK   |         |          |      | 
	And I insert in keyspace 'opera' and table 'rangeTable' with:
	 	| name    | surname   | age	 |
		|'John'   | 'Connor'  |22    |
		|'Michael'| 'Packard' |35    |
		|'Kurt'   | 'James'   |47    |

    Given I create a Cassandra index named 'age' with schema 'schemas/mapping/bigintMap.conf' of type 'string' in table 'rangeTable' using magic_column 'lucene' using keyspace 'opera' with:
		    | __digits      | UPDATE  | 10 |
		  	| __indexed 	| UPDATE  | true  	 |
		  	|__sorted 		| UPDATE  | true  	 |
		  	| __column_name | UPDATE  |age  	 |
		  	|__field		| UPDATE  |	age|
		    
Scenario: I execute a rangeSearch query checking  results [0,25]
		Given I execute a query over fields '*' with schema 'schemas/queries/rangeSearch.conf' of type 'string' with magic_column 'lucene' from table: 'rangeTable' using keyspace: 'opera' with:
			|__field|UPDATE|age|
			|__lowervalue|UPDATE|0|
			|__uppervalue|UPDATE|25|
			|__includelower|UPDATE|true|	
			|__includeupper|UPDATE|true|	
			
				
		Then There are results found with:
			|name      |occurrences |
			|Kurt	   |0           |	
			|Michael   |0           |
			|John      |1           |		

Scenario: I execute a rangeSearch query checking  results [25,31]
		Given I execute a query over fields '*' with schema 'schemas/queries/rangeSearch.conf' of type 'string' with magic_column 'lucene' from table: 'rangeTable' using keyspace: 'opera' with:
			|__field|UPDATE|age|
			|__lowervalue|UPDATE|25|
			|__uppervalue|UPDATE|31|
			|__includelower|UPDATE|true|	
			|__includeupper|UPDATE|true|	
			
				
		Then There are results found with:
			|name      |occurrences |
			|Kurt	   |0           |	
			|Michael   |0           |
			|John      |0           |		
			
			
Scenario: I execute a rangeSearch query checking  results [35,47)
		Given I execute a query over fields '*' with schema 'schemas/queries/rangeSearch.conf' of type 'string' with magic_column 'lucene' from table: 'rangeTable' using keyspace: 'opera' with:
			|__field|UPDATE|age|
			|__lowervalue|UPDATE|35|
			|__uppervalue|UPDATE|47|
			|__includelower|UPDATE|true|	
			|__includeupper|UPDATE|false|	
			
				
		Then There are results found with:
			|name      |occurrences |
			|Kurt	   |0           |	
			|Michael   |1           |
			|John      |0           |		

Scenario: I execute a rangeSearch query checking  results [35,47]
		Given I execute a query over fields '*' with schema 'schemas/queries/rangeSearch.conf' of type 'string' with magic_column 'lucene' from table: 'rangeTable' using keyspace: 'opera' with:
			|__field|UPDATE|age|
			|__lowervalue|UPDATE|35|
			|__uppervalue|UPDATE|47|
			|__includelower|UPDATE|true|	
			|__includeupper|UPDATE|true|	
			
				
		Then There are results found with:
			|name      |occurrences |
			|Kurt	   |1           |	
			|Michael   |1           |
			|John      |0           |		

Scenario: I execute a rangeSearch query checking  results (22,47]
		Given I execute a query over fields '*' with schema 'schemas/queries/rangeSearch.conf' of type 'string' with magic_column 'lucene' from table: 'rangeTable' using keyspace: 'opera' with:
			|__field|UPDATE|age|
			|__lowervalue|UPDATE|22|
			|__uppervalue|UPDATE|47|
			|__includelower|UPDATE|false|	
			|__includeupper|UPDATE|true|	
			
				
		Then There are results found with:
			|name      |occurrences |
			|Kurt	   |1           |	
			|Michael   |1           |
			|John      |0           |		

Scenario: I execute a rangeSearch query checking  results [22,47]
		Given I execute a query over fields '*' with schema 'schemas/queries/rangeSearch.conf' of type 'string' with magic_column 'lucene' from table: 'rangeTable' using keyspace: 'opera' with:
			|__field|UPDATE|age|
			|__lowervalue|UPDATE|22|
			|__uppervalue|UPDATE|47|
			|__includelower|UPDATE|true|	
			|__includeupper|UPDATE|true|	
			
				
		Then There are results found with:
			|name      |occurrences |
			|Kurt	   |1           |	
			|Michael   |1           |
			|John      |1           |		

Scenario: I remove all data
	Given I drop a Cassandra keyspace 'opera' 