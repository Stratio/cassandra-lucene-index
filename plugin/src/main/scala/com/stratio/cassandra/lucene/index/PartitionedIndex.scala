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
package com.stratio.cassandra.lucene.index

import java.io.File
import java.nio.file.{Path, Paths}

import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.util.Logging
import org.apache.cassandra.io.util.FileUtils.deleteRecursive
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.document.Document
import org.apache.lucene.index.Term
import org.apache.lucene.search.{Query, Sort}

/** An [[FSIndex]] partitioned by some not specified criterion.
  *
  * @param partitions     the number of index partitions
  * @param name           the index name
  * @param globalPath     the directory path
  * @param analyzer       the index writer analyzer
  * @param refreshSeconds the index reader refresh frequency in seconds
  * @param ramBufferMB    the index writer RAM buffer size in MB
  * @param maxMergeMB     the directory max merge size in MB
  * @param maxCachedMB    the directory max cache size in MB
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class PartitionedIndex(
    partitions: Int,
    name: String,
    localPaths: Array[Path],
    globalPath: Path,
    analyzer: Analyzer,
    refreshSeconds: Double,
    ramBufferMB: Int,
    maxMergeMB: Int,
    maxCachedMB: Int) extends Logging {

  private[this] val indexes: List[FSIndex] = {
    var outputList: List[FSIndex] = List()

    partitions match {
      case 1 =>
        if (useLocalPath) {
          List(new FSIndex(name,
            localPaths(0),
            analyzer,
            refreshSeconds,
            ramBufferMB,
            maxMergeMB,
            maxCachedMB))
        } else {
          List(new FSIndex(name,
            Paths.get(globalPath.toFile.getAbsolutePath + File.separator + "0"),
            analyzer,
            refreshSeconds,
            ramBufferMB,
            maxMergeMB,
            maxCachedMB))
        }
      case n if n > 1 =>
        for (index <- 0 until n) {
          val path = if (useLocalPath) {
            localPaths(index) + File.separator + index
          } else {
            globalPath.toFile.getAbsolutePath + File.separator + index
          }
          outputList = outputList ++ List(new FSIndex(name,
            Paths.get(path),
            analyzer,
            refreshSeconds,
            ramBufferMB,
            maxMergeMB,
            maxCachedMB))
        }
        outputList
      case _ => throw new IndexException(
        s"The number of partitions should be strictly positive but found $partitions")
    }
  }
  private[this] var mergeSort: Sort = _
  private[this] var fields: java.util.Set[String] = _

  /** Initializes this index with the specified merge sort and fields to be loaded.
    *
    * @param mergeSort the sort to be applied to the index during merges
    * @param fields    the names of the document fields to be loaded
    */
  def init(mergeSort: Sort, fields: java.util.Set[String]) {
    this.mergeSort = mergeSort
    this.fields = fields
    indexes.foreach(_.init(mergeSort, fields))
  }

  /** Deletes all the documents. */
  def truncate() {
    indexes.foreach(_.truncate())
    logger.info(s"Truncated $name")
  }

  /** Commits the pending changes. */
  def commit() {
    indexes.foreach(_.commit())
    logger.debug(s"Committed $name")
  }

  /** Commits all changes to the index, waits for pending merges to complete, and closes all
    * associated resources.
    */
  def close() {
    indexes.foreach(_.close())
    logger.info(s"Closed $name")
  }

  /** Closes the index and removes all its files. */
  def delete() {
    try {
      indexes.foreach(_.delete())
      if (useLocalPath) localPaths.foreach((localPath: Path) => deleteRecursive(localPath.toFile))
    } finally if (partitions > 1) if (!useLocalPath) deleteRecursive(globalPath.toFile)
    logger.info(s"Deleted $name")
  }

  private[this] def useLocalPath = localPaths != null && localPaths.length > 0

  /** Optimizes the index forcing merge segments leaving the specified number of segments.
    * This operation may block until all merging completes.
    *
    * @param maxNumSegments the maximum number of segments left in the index after merging finishes
    * @param doWait         `true` if the call should block until the operation completes
    */
  def forceMerge(maxNumSegments: Int, doWait: Boolean) {
    logger.info(s"Merging $name segments to $maxNumSegments")
    indexes.foreach(_.forceMerge(maxNumSegments, doWait))
    logger.info(s"Merged $name segments to $maxNumSegments")
  }

  /** Optimizes the index forcing merge of all segments that have deleted documents.
    * This operation may block until all merging completes.
    *
    * @param doWait `true` if the call should block until the operation completes
    */
  def forceMergeDeletes(doWait: Boolean) {
    logger.info(s"Merging $name segments with deletions")
    indexes.foreach(_.forceMergeDeletes(doWait))
    logger.info(s"Merged $name segments with deletions")
  }

  /** Refreshes the index readers. */
  def refresh(): Unit = {
    indexes.foreach(_.refresh())
    logger.debug(s"Refreshed $name readers")
  }

  /** Returns the total number of documents in this index.
    *
    * @return the number of documents
    */
  def getNumDocs: Long = {
    logger.debug(s"Getting $name num docs")
    (0L /: indexes) (_ + _.getNumDocs)
  }

  /** Returns the total number of deleted documents in this index.
    *
    * @return the number of deleted documents
    */
  def getNumDeletedDocs: Long = {
    logger.debug(s"Getting $name num deleted docs")
    (0L /: indexes) (_ + _.getNumDeletedDocs)
  }

  /** Upserts the specified document by first deleting the documents containing the specified term
    * and then adding the new document. The delete and then add are atomic as seen by a reader on
    * the same index (flush may happen only after the addition).
    *
    * @param partition the index partition where the operation will be done
    * @param term      the term to identify the document(s) to be deleted
    * @param document  the document to be added
    */
  def upsert(partition: Int, term: Term, document: Document) {
    logger.debug(s"Indexing $document with term $term in $name in partition $partition")
    indexes(partition).upsert(term, document)
  }

  /** Deletes all the documents containing the specified term.
    *
    * @param partition the index partition where the operation will be done
    * @param term      the term identifying the documents to be deleted
    */
  def delete(partition: Int, term: Term) {
    logger.debug(s"Deleting $term from $name in partition $partition")
    indexes(partition).delete(term)
  }

  /** Deletes all the documents satisfying the specified query.
    *
    * @param partition the index partition where the operation will be done
    * @param query     the query identifying the documents to be deleted
    */
  def delete(partition: Int, query: Query) {
    logger.debug(s"Deleting $query from $name in partition $partition")
    indexes(partition).delete(query)
  }

  /** Finds the top hits for a query and sort, starting from an optional position.
    *
    * @param partitions the index partitions where the operation will be done
    * @param query      the query to search for
    * @param sort       the sort to be applied
    * @param count      the max number of results to be collected
    * @return the found documents, sorted first by `sort`, then by `query` relevance
    */
  def search(partitions: List[(Int, Option[Term])], query: Query, sort: Sort, count: Int)
  : DocumentIterator = {
    logger.debug(
      s"""Searching in $name
          | partitions : ${partitions.map(_._1).mkString(", ")}
          |      after : ${partitions.map(_._2).mkString(", ")}
          |      query : $query
          |      count : $count
          |       sort : $sort
       """.stripMargin)
    val cursors = partitions.map { case (p, a) => (indexes(p).searcherManager, a) }
    new DocumentIterator(cursors, mergeSort, sort, query, count, fields)
  }
}