# Changelog

## 2.1.8.2-SNAPSHOT ()

 * Add force index refresh option to searches
 * Remove unneeded asynchronous indexing queue
 * Change default date pattern to yyyy/MM/dd HH:mm:ss.SSS z

## 2.1.8.1 (31 July 2015)

 * Add complete support for CQL paging, even for top-k queries.
 * Fix numeric collections (issue#12)
 * Fix match condition with not tokenized fields (issue#16)
 * Fix map columns sorting (issue#17)
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

 * Fix row updated skipping first column (issue#6)
 * Avoid analysis at prefix, regexp, range and wildcard queries

## 2.1.6.0 (8 June 2015)

 * Become a plugin instead of a fork of Apache Cassandra
 * Upgrade to Apache Cassandra 2.1.6
 * Upgrade to Lucene 5.1.0
 * Sorting through doc values
 * Add "indexed" and "sorted" options to mappers

## 2.1.5.0 (30 April 2015)

 * Upgrade to Apache Cassandra 2.1.5 (issue#28)
 * Removed clustering key mapper columns

## 2.1.4.1 (21 April 2015)

 * Improve top-k (CASSANDRA-8717)
 * Fix build.xml

## 2.1.4.0 (7 April 2015)

 * Upgrade to Apache Cassandra 2.1.4 (issue#16)
 * Fix reverse clustering order (issue#14)
 * Support for snowball and possibly other analyzers (issue#11)
 * Fix mapping in column-based clustering key mapper (issue#7)

## 2.1.3.1 (12 March 2015)

 * Fix mapping bug in column-based clustering key mapper
 * Upgrade to Lucene 4.10.4
 * Added case sensitive option to StringMapper

## 2.1.3.0 (18 February 2015)

 * Upgrade to Apache Cassandra 2.1.3

## 2.1.2.2 (2 February 2015)

 * Fix issue#7 (data inserted during update is not indexed)
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