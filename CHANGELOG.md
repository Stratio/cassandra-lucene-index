# Changelog

## 3.0.4.1 (upcoming)

* Allow altering unmapped columns
* Fix NPE while mapping geo points with explicitly null latitude/longitude value

## 3.0.4.0 (10 March 2016)

* Upgrade to Apache Cassandra 3.0.4
* Add indexing of time UUID columns with date, date_range and bitemporal mappers

## 3.0.3.1 (4 March 2016)

* Fix performance issues with ClusteringIndexNamesFilter
Merged from 2.2.5.2:
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

## 3.0.3.0 (19 February 2016)

* Upgrade to Apache Cassandra 3.0.3
* Force disable paging for top-k searches
* Add search cache (improves paging performance)
* Add token range cache (improves search performance)
* Remove support for other partitioners than Murmur3
* Remove Sphinx documentation module

## 2.2.5.1 (19 February 2016)

Merged from 2.2.4.3:
* Fixed explicit null values insertion (#94)

## 2.2.5.0 (9 February 2016)

* Upgrade to Apache Cassandra 2.2.5

## 2.2.4.2 (9 February 2016)

* Fixed missed bound statements paging handling (fixes top-k issues and improves MapReduce performance)

## 2.2.4.1 (12 January 2016)

* Returns static columns (#70)
* Fixed UDT bug (#85)
* Sort by geographical distance
    
## 2.2.4.0 (11 December 2015)

* Upgrade to Apache Cassandra 2.2.4
* Add optional CQL-level write validation (CASSANDRA-10092)

## 2.2.3.2 (9 December 2015)

* Add support for CQL DISTINCT operator (#69)

## 2.2.3.1 (27 November 2015)

* Add support for CQL tuples
* Add quoted field names to query builder
* Fix mapping on columns with multiple mappers
* Fix coordinator sorting to be based on mapper's base type

## 2.2.3.0 (20 November 2015)

* Upgrade to Apache Cassandra 2.2.3
* Add support for CQL UDFs (#43)
* Add support for CQL UDTs
* Improve collections support
* Add support for new CQL types smallint, tinyint and date

## 2.1.13.0 (10 February 2016)

* Upgrade to Apache Cassandra 2.1.13
 
## 2.1.12.0 (11 January 2016)

* Upgrade to Apache Cassandra 2.1.12
 
## 2.1.11.1 (18 November 2015)

* Fixed bitemporal bug (#46)
* Fixed default directory path
* Added query builder module (#50)
* Add acceptance tests

## 2.1.11.0 (27 October 2015)

* Upgrade to Apache Cassandra 2.1.11

## 2.1.10.0 (27 October 2015)

* Upgrade to Apache Cassandra 2.1.10
* Add ability to exclude data centers from indexing (#44)
* Add support for predictions in bitemporal index (#46)
* Add asynchronous indexing queue

## 2.1.9.0 (9 September 2015)

* Upgrade to Apache Cassandra 2.1.9

## 2.1.8.5 (9 September 2015)

* Remove problematic logback.xml

## 2.1.8.4 (27 August 2015)

* Fix searches with both sorting and relevance
* Improve wide rows data range filtering to increase performance
* Use doc values in token range filters to increase performance
* Replace base 256 by BytesRef (breaks backward compatibility)
* Upgrade to Lucene 5.3.0
* Don't propagate internal index exceptions
* Allow the deletion of old unsupported indexes
* Detect wrong sorting in date ranges (#36) 

## 2.1.8.3 (20 August 2015)

* Fix analyzer selection in maps (#18)
* Change logger fixed name from `stratio` to class-based `com.stratio`
* Add performance tips section to documentation

## 2.1.8.2 (13 August 2015)

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

## 2.1.8.1 (31 July 2015)

* Add complete support for CQL paging, even for top-k queries.
* Fix numeric collections (#12)
* Fix match condition with not tokenized fields (#16)
* Fix map columns sorting (#17)
* Fix bounding box queries
* Avoid sorting in lists and sets
* Set default sorted value to false
* Upgrade to Lucene 5.2.1

## 2.1.8.0 (10 July 2015)

* Upgrade to Apache Cassandra 2.1.8

## 2.1.7.1 (10 July 2015)

* Add paging cache to remember Lucene cursors
* Fix JavaDoc generation with Java 8
* Homogenize JSON API

## 2.1.7.0 (26 June 2015)

* Upgrade to Apache Cassandra 2.1.7

## 2.1.6.2 (25 June 2015)

* Add date range features
* Add basic geospatial features

## 2.1.6.1 (17 June 2015)

* Fix row updated skipping first column (#6)
* Avoid analysis at prefix, regexp, range and wildcard queries

## 2.1.6.0 (8 June 2015)

* Become a plugin instead of a fork of Apache Cassandra
* Upgrade to Apache Cassandra 2.1.6
* Upgrade to Lucene 5.1.0
* Sorting through doc values
* Add "indexed" and "sorted" options to mappers

## 2.1.5.0 (30 April 2015)

* Upgrade to Apache Cassandra 2.1.5 (#28)
* Removed clustering key mapper columns

## 2.1.4.1 (21 April 2015)

* Improve top-k (CASSANDRA-8717)
* Fix build.xml

## 2.1.4.0 (7 April 2015)

* Upgrade to Apache Cassandra 2.1.4 (#16)
* Fix reverse clustering order (#14)
* Support for snowball and possibly other analyzers (#11)
* Fix mapping in column-based clustering key mapper (#7)

## 2.1.3.1 (12 March 2015)

* Fix mapping bug in column-based clustering key mapper
* Upgrade to Lucene 4.10.4
* Added case sensitive option to StringMapper

## 2.1.3.0 (18 February 2015)

* Upgrade to Apache Cassandra 2.1.3

## 2.1.2.2 (2 February 2015)

* Fix #7 (data inserted during update is not indexed)
* Remove boolean query max clauses limit
* Add contains condition
* Add basic support for geospatial search
* Add basic support for multiple fields per mapper
* Add collation for UUID mapper

## 2.1.2.1 (15 December 2014)

* Improve logging time counting
* Set synchronous indexing as default

## 2.1.2.0 (5 December 2014)

* Upgrade to Apache Cassandra 2.1.2