/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene

import java.nio.file.{Path, Paths}

import com.stratio.cassandra.lucene.IndexOptions._
import com.stratio.cassandra.lucene.partitioning.{Partitioner, PartitionerOnNone}
import com.stratio.cassandra.lucene.schema.{Schema, SchemaBuilder}
import com.stratio.cassandra.lucene.util.SchemaValidator
import org.apache.cassandra.config.{CFMetaData, DatabaseDescriptor}
import org.apache.cassandra.db.Directories
import org.apache.cassandra.schema.IndexMetadata

import scala.collection.JavaConverters._

/** Index user-specified configuration options parser.
  *
  * @param tableMetadata the indexed table metadata
  * @param indexMetadata the index metadata
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class IndexOptions(tableMetadata: CFMetaData, indexMetadata: IndexMetadata) {

  val options = indexMetadata.options.asScala.toMap

  /** The Lucene index searcher refresh frequency, in seconds */
  val refreshSeconds = parseRefresh(options)

  /** The Lucene's max RAM buffer size, in MB */
  val ramBufferMB = parseRamBufferMB(options)

  /** The Lucene's max segments merge size size, in MB */
  val maxMergeMB = parseMaxMergeMB(options)

  /** The Lucene's max cache size, in MB */
  val maxCachedMB = parseMaxCachedMB(options)

  /** The number of asynchronous indexing threads */
  val indexingThreads = parseIndexingThreads(options)

  /** The size of the asynchronous indexing queues */
  val indexingQueuesSize = parseIndexingQueuesSize(options)

  /** The names of the data centers excluded from indexing */
  val excludedDataCenters = parseExcludedDataCenters(options)

  /** The mapping schema */
  val schema = parseSchema(options, tableMetadata)

  val pathAndPartitioner = parsePathAndPartitioner(options,
    tableMetadata,
    DatabaseDescriptor.getAllDataFileLocations.map(Paths.get(_)),
    getBaseTablePath(tableMetadata))

  /** The path of the directory where the index files will be stored */
  val path = pathAndPartitioner._1

  /** The index partitioner */
  val partitioner = pathAndPartitioner._2

  /** If the index is sparse or not */
  val sparse = parseSparse(options, tableMetadata)
}

/** Companion object for [[IndexOptions]]. */
object IndexOptions {

  val REFRESH_SECONDS_OPTION = "refresh_seconds"
  val DEFAULT_REFRESH_SECONDS = 60D

  val RAM_BUFFER_MB_OPTION = "ram_buffer_mb"
  val DEFAULT_RAM_BUFFER_MB = 64

  val MAX_MERGE_MB_OPTION = "max_merge_mb"
  val DEFAULT_MAX_MERGE_MB = 5

  val MAX_CACHED_MB_OPTION = "max_cached_mb"
  val DEFAULT_MAX_CACHED_MB = 30

  val INDEXING_THREADS_OPTION = "indexing_threads"
  val DEFAULT_INDEXING_THREADS = Runtime.getRuntime.availableProcessors

  val INDEXING_QUEUES_SIZE_OPTION = "indexing_queues_size"
  val DEFAULT_INDEXING_QUEUES_SIZE = 50

  val EXCLUDED_DATA_CENTERS_OPTION = "excluded_data_centers"
  val DEFAULT_EXCLUDED_DATA_CENTERS = List[String]()

  val DIRECTORY_PATH_OPTION = "directory_path"
  val INDEXES_DIR_NAME = "lucene"

  val SCHEMA_OPTION = "schema"

  val PARTITIONER_OPTION = "partitioner"
  val DEFAULT_PARTITIONER = PartitionerOnNone()

  val SPARSE_OPTION = "sparse"
  val DEFAULT_SPARSE = false

  /** Validates the specified index options.
    *
    * @param options  the options to be validated
    * @param metadata the indexed table metadata
    */
  def validate(options: java.util.Map[String, String], metadata: CFMetaData) {
    val o = options.asScala.toMap
    parseRefresh(o)
    parseRamBufferMB(o)
    parseMaxMergeMB(o)
    parseMaxCachedMB(o)
    parseIndexingThreads(o)
    parseIndexingQueuesSize(o)
    parseExcludedDataCenters(o)
    parseSchema(o, metadata)
    parsePathAndPartitioner(o,
      metadata,
      DatabaseDescriptor.getAllDataFileLocations.map(Paths.get(_)),
      getBaseTablePath(metadata))
  }

  def parseRefresh(options: Map[String, String]): Double = {
    parseStrictlyPositiveDouble(options, REFRESH_SECONDS_OPTION, DEFAULT_REFRESH_SECONDS)
  }

  private def parseStrictlyPositiveDouble(
      options: Map[String, String],
      name: String,
      default: Double): Double = {
    options.get(name).map(
      string => try string.toDouble catch {
        case e: NumberFormatException =>
          throw new IndexException(s"'$name' must be a strictly positive decimal, found: $string")
      }).map(
      double => if (double > 0) double
      else {
        throw new IndexException(s"'$name' must be strictly positive, found: $double")
      }).getOrElse(default)
  }

  def parseRamBufferMB(options: Map[String, String]): Int = {
    parseStrictlyPositiveInt(options, RAM_BUFFER_MB_OPTION, DEFAULT_RAM_BUFFER_MB)
  }

  def parseMaxMergeMB(options: Map[String, String]): Int = {
    parseStrictlyPositiveInt(options, MAX_MERGE_MB_OPTION, DEFAULT_MAX_MERGE_MB)
  }

  def parseMaxCachedMB(options: Map[String, String]): Int = {
    parseStrictlyPositiveInt(options, MAX_CACHED_MB_OPTION, DEFAULT_MAX_CACHED_MB)
  }

  private def parseStrictlyPositiveInt(
      options: Map[String, String],
      name: String,
      default: Int): Int = {
    options.get(name).map(
      string => try string.toInt catch {
        case e: NumberFormatException =>
          throw new IndexException(s"'$name' must be a strictly positive integer, found: $string")
      }).map(
      integer => if (integer > 0) integer
      else {
        throw new IndexException(s"'$name' must be strictly positive, found: $integer")
      }).getOrElse(default)
  }

  def parseIndexingThreads(options: Map[String, String]): Int = {
    parseInt(options, INDEXING_THREADS_OPTION, DEFAULT_INDEXING_THREADS)
  }

  private def parseInt(options: Map[String, String], name: String, default: Int): Int = {
    options.get(name).map(
      string => try string.toInt catch {
        case e: NumberFormatException =>
          throw new IndexException(s"'$name' must be an integer, found: $string")
      }).getOrElse(default)
  }

  def parseIndexingQueuesSize(options: Map[String, String]): Int = {
    parseStrictlyPositiveInt(options, INDEXING_QUEUES_SIZE_OPTION, DEFAULT_INDEXING_QUEUES_SIZE)
  }

  def parseExcludedDataCenters(options: Map[String, String]): List[String] = {
    options
      .get(EXCLUDED_DATA_CENTERS_OPTION)
      .map(_.split(",").map(_.trim).filterNot(_.isEmpty).toList)
      .getOrElse(DEFAULT_EXCLUDED_DATA_CENTERS)
  }

  def parsePathAndPartitioner(
      options: Map[String, String],
      table: CFMetaData,
      cassandraPathDirs: Array[Path],
      baseTablePath: Path): (Option[Path], Partitioner) = {
    var path = parsePath(options)
    val partitioner = parsePartitioner(options, table)
    val customPartitionerPaths = partitioner.pathsForEachPartitions
    if (cassandraPathDirs.length > 1) {
      if (customPartitionerPaths.isDefined) {
        for (cassandraFile <- cassandraPathDirs) {
          for (partitionerPath <- customPartitionerPaths.get) {
            if (partitionerPath.startsWith(cassandraFile)) {
              throw new IndexException(s"When cassandra is configured with more than one 'data_file_directory', custom partitioner paths must not be inside any of those 'data_file_directory','$partitionerPath' is inside: '$cassandraFile'")
            }
          }
        }
      } else {
        if (path.isDefined) {
          for (cassandraFile <- cassandraPathDirs) {
            if (path.get.startsWith(cassandraFile)) {
              throw new IndexException(s"When cassandra is configured with more than one 'data_file_directory', 'directory_path' must not be inside any of those 'data_file_directory','${path.get}' is inside: '$cassandraFile'")
            }
          }
        } else {
          throw new IndexException(s"When cassandra is configured with more than one 'data_file_directory', 'directory_path' required")
        }
      }
    } else if ((cassandraPathDirs.length == 1) && customPartitionerPaths.isEmpty && path.isEmpty) {
      path = Some(baseTablePath)
    }
    (path, partitioner)
  }

  def parsePartitioner(options: Map[String, String], table: CFMetaData): Partitioner = {
    options.get(PARTITIONER_OPTION).map(
      value => try {
        Partitioner.fromJson(table, value)
      } catch {
        case e: Exception => throw new IndexException(e,
          s"'$PARTITIONER_OPTION' is invalid : ${e.getMessage}")
      }).getOrElse(DEFAULT_PARTITIONER)
  }

  def parsePath(options: Map[String, String]): Option[Path] =
    options.get(DIRECTORY_PATH_OPTION).map(Paths.get(_)).orElse(None)

  def parseSchema(options: Map[String, String], table: CFMetaData): Schema = {
    options.get(SCHEMA_OPTION).map(
      value => try {
        val schema = SchemaBuilder.fromJson(value).build
        SchemaValidator.validate(schema, table)
        schema
      } catch {
        case e: Exception => throw new IndexException(e,
          s"'$SCHEMA_OPTION' is invalid : ${e.getMessage}")
      }).getOrElse(throw new IndexException(s"'$SCHEMA_OPTION' is required"))
  }

  def getBaseTablePath(table: CFMetaData): Path =
    Paths.get(new Directories(table).getDirectoryForNewSSTables.getAbsolutePath)

  def parseSparse(options: Map[String, String], table: CFMetaData): Boolean = {
    options.get(SPARSE_OPTION).map(
      value => try value.toBoolean catch {
        case e: Exception => throw new IndexException(e,
          s"'$SPARSE_OPTION' is invalid : ${e.getMessage}")
      }).getOrElse(DEFAULT_SPARSE)
  }
}
