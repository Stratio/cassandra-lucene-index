# Changelog

## 3.8.5 (April 10, 2017)

* Fix collection columns indexing being part of clustering key (#286)
* Avoid unsupported usage of multicolumn mappers with collections
* Add support for indexing map values
* Add support for indexing map keys (#253)

## 3.8.4 (February 06, 2017)

* Fix mutual exclusion on partition key during read before write (#261)
* Ignore tombstones during read before write (#255)
* Fix deletion of ranges of rows in a partition
* Add virtual node based partitioner

## 3.8.3 (December 15, 2016)

* Upgrade to Apache Lucene 5.5.3
* Add column-based index partitioner
* Add support for local searches over index partitions with more than 2147483519 total documents
* Fix CQL query handler activation at startup
* Fix best effort mapping of collections (#246)

## 3.8.2 (December 09, 2016)

* Optimize columns mapping (improves indexing performance)
* Add generic support for index partitioning
* Add token-based index partitioner
* Upgrade to Scala 2.12.0
* Avoid not required string interpolations in logging
* Avoid not required string interpolations in tracing
* Add support for geospatial shapes in bounding box search
* Add support for geospatial shapes in distance search
* Improve performance of needs before write calculation
* Show error message when unsupported PER PARTITION LIMIT option is used
* Upgrade all JSON serializers to FasterXML Jackson 2.8.6

## 3.8.1 (October 17, 2016)

* Fix conflict between Lucene and SASI Snowball stemmer version (#216)

## 3.8.0 (October 13, 2016)

* Upgrade to Apache Cassandra 3.8

## 3.7.3 (October 07, 2016)

* Add support for indexing non-frozen UDTs
* Optimize clustering key index format (breaks backward compatibility)

## 3.7.2 (September 14, 2016)

* Allow associativity in search-time geospatial transformations
* Simplify naming of builder static methods for creating geospatial transformations
* Fix paged index-sorted queries matching more than 65535 rows
* Fix partition directed queries using dummy column syntax in skinny tables
* Upgrade query builder JSON serializer to Jackson 2.8.0
* Add geospatial post filtering (ensures accuracy with any tree levels)
* Set default number of indexing threads to number of processors available to the JVM
* Fix mapping of timestamps and dates by their underlying numeric value (#177)
* Fix mapper referenced by alias in sortFields

## 3.7.1 (July 05, 2016)

* Modernize search syntax keeping backward compatibility
* Add Lucene-specific CQL tracing
* Fix parsing of data with CQL "date" type (#158)
* Fix handling of immense term insertion mapping error
* Fix indexing of tables with descending clustering key order (#165)

## 3.7.0 (June 20, 2016)

* Upgrade to Apache Cassandra 3.7

## 3.6.0 (June 18, 2016)

* Upgrade to Apache Cassandra 3.6
* Fix clustering query collisions in Lucene's query cache
* Fix deletion of columns with frozen type (UDT, tuples and frozen collections)
* Add support for CQL paging over sorted searches
* Add support for sorted searches using IN operator
* Add support for indexing columns with TTL 

## 3.5.2 (May 27, 2016)

* Add transformation for getting the bounding box of a geographical shape
* Fix complex clustering keys building (#143)

## 3.5.1 (May 19, 2016)

* Upgrade to Apache Lucene 5.5.1
* Add support for old dummy column syntax (Spark compatibility and score retrieval)
* Add option to use doc values in match, contains and range searches
* Add heuristic to build token range queries according to their selectivity
* Add transformation for getting the convex hull of a geographical shape
* Add early termination in inner pagination (improves allow filtering performance)
* Remove indexed/sorted mapping options, all mappers store doc values when possible
* Fix invalid WKT shapes by zero-buffering them
* Fix memory consumption issues with high fetch sizes
* Fix clustering key filtering with better token prefix collation
* Fix deletion of unique component of a collection (#132)

## 3.5.0 (May 03, 2016)

* Upgrade to Apache Cassandra 3.5

## 3.0.5.0 (April 22, 2016)

* Upgrade to Apache Cassandra 3.0.5

## 3.0.4.1 (April 22, 2016)

* Add sorting merge policy (dramatically improves filter performance)
* Add support for altering unmapped columns
* Remove score docs cache, overtaken by sorting merge policy
* Remove token range cache in favour of Lucene query cache
* Fix NPE while mapping geo points with explicitly null latitude/longitude value
* Fix missed validation of sort fields

## 3.0.4.0 (March 10, 2016)

* Upgrade to Apache Cassandra 3.0.4
* Add indexing of time UUID columns with date, date_range and bitemporal mappers

## 3.0.3.1 (March 04, 2016)

* Fix performance issues with ClusteringIndexNamesFilter
* Add indexing of WKT geographical shapes (point, linestring, polygon and their multipart)
* Add search by WKT geographical shapes (point, linestring, polygon and their multipart)
* Add API for search-time transformation of WKT geographical shapes
* Add API for index-time transformation of WKT geographical shapes
* Add transformation for getting the buffer around a geographical shape
* Add transformation for getting the centroid of a geographical shape
* Add transformation for getting the difference between two geographical shapes
* Add transformation for getting the intersection between two geographical shapes
* Add transformation for getting the union between two geographical shapes
* Fix geo distance parsing of nautical miles

## 3.0.3.0 (February 19, 2016)

* Upgrade to Apache Cassandra 3.0.3
* Force disable paging for top-k searches
* Add search cache (improves paging performance)
* Add token range cache (improves search performance)
* Remove support for other partitioners than Murmur3
* Remove Sphinx documentation module

## 2.2.5.1 (February 19, 2016)

* Fixed explicit null values insertion (#94)

## 2.2.5.0 (February 09, 2016)

* Upgrade to Apache Cassandra 2.2.5

## 2.2.4.2 (February 09, 2016)

* Fixed missed bound statements paging handling (fixes top-k issues and improves MapReduce performance)

## 2.2.4.1 (January 12, 2016)

* Returns static columns (#70)
* Fixed UDT bug (#85)
* Sort by geographical distance
    
## 2.2.4.0 (December 11, 2015)

* Upgrade to Apache Cassandra 2.2.4
* Add optional CQL-level write validation (CASSANDRA-10092)

## 2.2.3.2 (December 09, 2015)

* Add support for CQL DISTINCT operator (#69)

## 2.2.3.1 (November 27, 2015)

* Add support for CQL tuples
* Add quoted field names to query builder
* Fix mapping on columns with multiple mappers
* Fix coordinator sorting to be based on mapper's base type

## 2.2.3.0 (November 20, 2015)

* Upgrade to Apache Cassandra 2.2.3
* Add support for CQL UDFs (#43)
* Add support for CQL UDTs
* Improve collections support
* Add support for new CQL types smallint, tinyint and date

## 2.1.13.0 (February 10, 2016)

* Upgrade to Apache Cassandra 2.1.13
 
## 2.1.12.0 (January 11, 2016)

* Upgrade to Apache Cassandra 2.1.12
 
## 2.1.11.1 (November 18, 2015)

* Fixed bitemporal bug (#46)
* Fixed default directory path
* Added query builder module (#50)
* Add acceptance tests

## 2.1.11.0 (October 27, 2015)

* Upgrade to Apache Cassandra 2.1.11

## 2.1.10.0 (October 27, 2015)

* Upgrade to Apache Cassandra 2.1.10
* Add ability to exclude data centers from indexing (#44)
* Add support for predictions in bitemporal index (#46)
* Add asynchronous indexing queue

## 2.1.9.0 (September 09, 2015)

* Upgrade to Apache Cassandra 2.1.9

## 2.1.8.5 (September 09, 2015)

* Remove problematic logback.xml

## 2.1.8.4 (August 27, 2015)

* Fix searches with both sorting and relevance
* Improve wide rows data range filtering to increase performance
* Use doc values in token range filters to increase performance
* Replace base 256 by BytesRef (breaks backward compatibility)
* Upgrade to Lucene 5.3.0
* Don't propagate internal index exceptions
* Allow the deletion of old unsupported indexes
* Detect wrong sorting in date ranges (#36) 

## 2.1.8.3 (August 20, 2015)

* Fix analyzer selection in maps (#18)
* Change logger fixed name from `stratio` to class-based `com.stratio`
* Add performance tips section to documentation

## 2.1.8.2 (August 13, 2015)

* Add force index refresh option to searches
* Add condition type `none` to return no rows
* Rename `match_all` condition to `all`
* Allow resource-intensive pure negation searches
* Remove unneeded asynchronous indexing queue
* Change default date pattern to `yyyy/MM/dd HH:mm:ss.SSS Z`
* Fix multi-mappers when all columns are null (#28)
* Rename `date_range` limits to `from` and `to`
* Add bitemporal search features
* Silently discard tokens over 32766 bytes in length (just log)
* Best effort mapping, per mapper errors are just logged
* Allow several mappers on the same column

## 2.1.8.1 (July 31, 2015)

* Add complete support for CQL paging, even for top-k queries.
* Fix numeric collections (#12)
* Fix match condition with not tokenized fields (#16)
* Fix map columns sorting (#17)
* Fix bounding box queries
* Avoid sorting in lists and sets
* Set default sorted value to false
* Upgrade to Lucene 5.2.1

## 2.1.8.0 (July 10, 2015)

* Upgrade to Apache Cassandra 2.1.8

## 2.1.7.1 (July 10, 2015)

* Add paging cache to remember Lucene cursors
* Fix JavaDoc generation with Java 8
* Homogenize JSON API

## 2.1.7.0 (June 26, 2015)

* Upgrade to Apache Cassandra 2.1.7

## 2.1.6.2 (June 25, 2015)

* Add date range features
* Add basic geospatial features

## 2.1.6.1 (June 17, 2015)

* Fix row updated skipping first column (#6)
* Avoid analysis at prefix, regexp, range and wildcard queries

## 2.1.6.0 (June 08, 2015)

* Become a plugin instead of a fork of Apache Cassandra
* Upgrade to Apache Cassandra 2.1.6
* Upgrade to Lucene 5.1.0
* Sorting through doc values
* Add "indexed" and "sorted" options to mappers

## 2.1.5.0 (April 30, 2015)

* Upgrade to Apache Cassandra 2.1.5 (#28)
* Removed clustering key mapper columns

## 2.1.4.1 (April 21, 2015)

* Improve top-k (CASSANDRA-8717)
* Fix build.xml

## 2.1.4.0 (April 07, 2015)

* Upgrade to Apache Cassandra 2.1.4 (#16)
* Fix reverse clustering order (#14)
* Support for snowball and possibly other analyzers (#11)
* Fix mapping in column-based clustering key mapper (#7)

## 2.1.3.1 (March 12, 2015)

* Fix mapping bug in column-based clustering key mapper
* Upgrade to Lucene 4.10.4
* Added case sensitive option to StringMapper

## 2.1.3.0 (February 18, 2015)

* Upgrade to Apache Cassandra 2.1.3

## 2.1.2.2 (February 02, 2015)

* Fix #7 (data inserted during update is not indexed)
* Remove boolean query max clauses limit
* Add contains condition
* Add basic support for geospatial search
* Add basic support for multiple fields per mapper
* Add collation for UUID mapper

## 2.1.2.1 (December 15, 2014)

* Improve logging time counting
* Set synchronous indexing as default

## 2.1.2.0 (December 05, 2014)

* Upgrade to Apache Cassandra 2.1.2