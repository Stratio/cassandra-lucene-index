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

import java.nio.file.Path

import org.apache.cassandra.io.util.FileUtils
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.document.Document
import org.apache.lucene.index._
import org.apache.lucene.search._
import org.apache.lucene.store.{Directory, FSDirectory, NRTCachingDirectory}

/** Class wrapping a Lucene file system-based directory and its readers, writers and searchers.
  *
  * @param name           the index name
  * @param path           the directory path
  * @param analyzer       the index writer analyzer
  * @param refreshSeconds the index reader refresh frequency in seconds
  * @param ramBufferMB    the index writer RAM buffer size in MB
  * @param maxMergeMB     the directory max merge size in MB
  * @param maxCachedMB    the directory max cache size in MB
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class FSIndex(
    name: String,
    path: Path,
    analyzer: Analyzer,
    refreshSeconds: Double,
    ramBufferMB: Int,
    maxMergeMB: Int,
    maxCachedMB: Int) {

  private[this] var mergeSort: Sort = _
  private[this] var fields: java.util.Set[String] = _
  private[this] var directory: Directory = _
  private[this] var writer: IndexWriter = _
  private[this] var manager: SearcherManager = _
  private[this] var reopener: ControlledRealTimeReopenThread[IndexSearcher] = _

  /** Initializes this index with the specified merge sort and fields to be loaded.
    *
    * @param mergeSort the sort to be applied to the index during merges
    * @param fields    the names of the document fields to be loaded
    */
  def init(mergeSort: Sort, fields: java.util.Set[String]) {
    this.mergeSort = mergeSort
    this.fields = fields

    // Open or create directory
    directory = new NRTCachingDirectory(FSDirectory.open(path), maxMergeMB, maxCachedMB)

    // Setup index writer
    val indexWriterConfig = new IndexWriterConfig(analyzer)
    indexWriterConfig.setRAMBufferSizeMB(ramBufferMB)
    indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
    indexWriterConfig.setUseCompoundFile(true)
    indexWriterConfig.setMergePolicy(new SortingMergePolicy(new TieredMergePolicy, mergeSort))
    writer = new IndexWriter(directory, indexWriterConfig)

    // Setup NRT search
    val searcherFactory: SearcherFactory = new SearcherFactory {
      override def newSearcher(reader: IndexReader, previousReader: IndexReader): IndexSearcher = {
        val searcher = new IndexSearcher(reader)
        searcher.setSimilarity(new NoIDFSimilarity)
        searcher
      }
    }
    val tracker = new TrackingIndexWriter(writer)
    manager = new SearcherManager(writer, true, searcherFactory)
    reopener = new ControlledRealTimeReopenThread(tracker, manager, refreshSeconds, refreshSeconds)
    reopener.start()
  }

  private[this] def doWithSearcher[A](f: IndexSearcher => A): A = {
    val searcher = manager.acquire
    try f.apply(searcher) finally manager.release(searcher)
  }

  /** Upserts the specified document by first deleting the documents containing the specified term
    * and then adding the new document. The delete and then add are atomic as seen by a reader on
    * the same index (flush may happen only after the addition).
    *
    * @param term     the term to identify the document(s) to be deleted
    * @param document the document to be added
    */
  def upsert(term: Term, document: Document) {
    writer.updateDocument(term, document)
  }

  /** Deletes all the documents containing the specified term.
    *
    * @param term the term identifying the documents to be deleted
    */
  def delete(term: Term) {
    writer.deleteDocuments(term)
  }

  /** Deletes all the documents satisfying the specified query.
    *
    * @param query the query identifying the documents to be deleted
    */
  def delete(query: Query) {
    writer.deleteDocuments(query)
  }

  /** Deletes all the documents. */
  def truncate() {
    writer.deleteAll()
    writer.commit()
  }

  /** Commits the pending changes. */
  def commit() {
    writer.commit()
  }

  /** Commits all changes to the index, waits for pending merges to complete, and closes all
    * associated resources.
    */
  def close() {
    reopener.close()
    manager.close()
    writer.close()
    directory.close()
  }

  /** Closes the index and removes all its files. */
  def delete() {
    try close() finally FileUtils.deleteRecursive(path.toFile)
  }

  /** Finds the top hits for a query and sort, starting from an optional position.
    *
    * @param after the starting term
    * @param query the query to search for
    * @param sort  the sort to be applied
    * @param count the max number of results to be collected
    * @return the found documents, sorted according to the supplied [[Sort]] instance
    */
  def search(after: Option[Term], query: Query, sort: Sort, count: Int): DocumentIterator = {
    val searcher = manager.acquire()
    val releaseSearcher = () => manager.release(searcher)
    new DocumentIterator(searcher, releaseSearcher, after, mergeSort, sort, query, count, fields)
  }

  def searcher: (IndexSearcher, () => Unit) = {
    val searcher = manager.acquire()
    val releaseSearcher = () => manager.release(searcher)
    (searcher, releaseSearcher)
  }

  /** Returns the total number of documents in this index.
    *
    * @return the number of documents
    */
  def getNumDocs: Int = {
    doWithSearcher(searcher => searcher.getIndexReader.numDocs)
  }

  /** Returns the total number of deleted documents in this index.
    *
    * @return the number of deleted documents
    */
  def getNumDeletedDocs: Int = {
    doWithSearcher(searcher => searcher.getIndexReader.numDeletedDocs)
  }

  /** Optimizes the index forcing merge segments leaving the specified number of segments.
    * This operation may block until all merging completes.
    *
    * @param maxNumSegments the maximum number of segments left in the index after merging finishes
    * @param doWait         `true` if the call should block until the operation completes
    */
  def forceMerge(maxNumSegments: Int, doWait: Boolean) {
    writer.forceMerge(maxNumSegments, doWait)
    writer.commit()
  }

  /** Optimizes the index forcing merge of all segments that have deleted documents.
    * This operation may block until all merging completes.
    *
    * @param doWait `true` if the call should block until the operation completes
    */
  def forceMergeDeletes(doWait: Boolean) {
    writer.forceMergeDeletes(doWait)
    writer.commit()
  }

  /** Refreshes the index readers. */
  def refresh() {
    manager.maybeRefreshBlocking()
  }
}

/** Companion object for [[FSIndex]]. */
object FSIndex {

  // Disable max boolean query clauses limit
  BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE)
}
