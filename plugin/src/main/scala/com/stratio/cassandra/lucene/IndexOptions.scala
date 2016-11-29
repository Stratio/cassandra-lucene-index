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

import java.io.File
import java.nio.file.{Path, Paths}

import com.stratio.cassandra.lucene.IndexOptions._
import com.stratio.cassandra.lucene.schema.{Schema, SchemaBuilder}
import com.stratio.cassandra.lucene.util.SchemaValidator
import org.apache.cassandra.config.CFMetaData
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

  /** The path of the directory where the index files will be stored */
  val path = parsePath(options, tableMetadata, Some(indexMetadata))
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
    parsePath(o, metadata, None)
  }

  def parseRefresh(options: Map[String, String]): Double = {
    parseStrictlyPositiveDouble(options, REFRESH_SECONDS_OPTION, DEFAULT_REFRESH_SECONDS)
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

  def parseIndexingThreads(options: Map[String, String]): Int = {
    parseInt(options, INDEXING_THREADS_OPTION, DEFAULT_INDEXING_THREADS)
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

  def parsePath(
      options: Map[String, String],
      table: CFMetaData,
      index: Option[IndexMetadata]): Path = {
    options.get(DIRECTORY_PATH_OPTION).map(Paths.get(_)).getOrElse(
      index.map(
        index => {
          val directories = new Directories(table)
          val basePath = directories.getDirectoryForNewSSTables.getAbsolutePath
          Paths.get(basePath + File.separator + INDEXES_DIR_NAME + File.separator + index.name)
        }).orNull)
  }

  def parseSchema(options: Map[String, String], table: CFMetaData): Schema = {
    options.get(SCHEMA_OPTION).map(
      value => try {
        val schema = SchemaBuilder.fromJson(value).build
        SchemaValidator.validate(schema, table)
        schema
      } catch {
        case e: Exception => throw new IndexException(
          e,
          s"'$SCHEMA_OPTION' is invalid : ${e.getMessage}")
      }).getOrElse(throw new IndexException(s"'$SCHEMA_OPTION' is required"))
  }

  private def parseInt(options: Map[String, String], name: String, default: Int): Int = {
    options.get(name).map(
      string => try string.toInt catch {
        case e: NumberFormatException =>
          throw new IndexException(s"'$name' must be an integer, found: $string")
      }).getOrElse(default)
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


}
