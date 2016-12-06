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

import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.index.DocumentIterator._
import com.stratio.cassandra.lucene.util.{Logging, TimeCounter, Tracing}
import org.apache.cassandra.utils.CloseableIterator
import org.apache.lucene.document.Document
import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanClause.Occur._
import org.apache.lucene.search.EarlyTerminatingSortingCollector._
import org.apache.lucene.search._

/** [[CloseableIterator]] for retrieving Lucene documents satisfying a query.
  *
  * @param searcher        the Lucene index searcher
  * @param releaseSearcher a function for releasing the searcher
  * @param indexSort       the sort of the index
  * @param querySort       the sort in which the documents are going to be retrieved
  * @param query           the query to be satisfied by the documents
  * @param limit           the iteration page size
  * @param fields          the names of the document fields to be loaded
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class DocumentIterator(
    searcher: IndexSearcher,
    releaseSearcher: () => Unit,
    afterTerm: Option[Term],
    indexSort: Sort,
    querySort: Sort,
    query: Query,
    limit: Int,
    fields: java.util.Set[String])
  extends Iterator[(Document, ScoreDoc)] with AutoCloseable with Logging with Tracing {

  private[this] val pageSize = Math.min(limit, MAX_PAGE_SIZE) + 1
  private[this] val documents = new java.util.LinkedList[(Document, ScoreDoc)]

  private[this] var afterOffset = 0
  private[this] var finished = false
  private[this] var closed = false

  /** The sort of the query rewritten by the searcher. */
  private[this] val sort = try {
    querySort.rewrite(searcher)
  } catch {
    case e: Exception =>
      releaseSearcher()
      throw new IndexException(e, s"Error rewriting sort $indexSort")
  }

  /** The start after position. */
  private[this] var after = try {
    afterTerm.map(
      term => {
        val time = TimeCounter.start
        val builder = new BooleanQuery.Builder
        builder.add(new TermQuery(term), FILTER)
        builder.add(query, MUST)
        val scores = searcher.search(builder.build, 1, sort).scoreDocs
        if (scores.nonEmpty) {
          tracer.trace("Lucene index seeks last index position")
          logger.debug(s"Start position found in $time")
          scores.head
        } else throw new IndexException("Last page position not found")
      })
  } catch {
    case e: Exception =>
      releaseSearcher()
      throw new IndexException(e, "Error while searching for the last page position")
  }

  private[this] def fetch() = {
    try {
      val time = TimeCounter.start

      val topDocs = if (afterTerm.isEmpty && canEarlyTerminate(sort, indexSort)) {
        val fieldDoc = after.map(_.asInstanceOf[FieldDoc]).orNull
        val collector = TopFieldCollector.create(sort, pageSize, fieldDoc, true, false, false)
        val hits = afterOffset + pageSize
        val earlyCollector = new EarlyTerminatingSortingCollector(collector, sort, hits, indexSort)
        searcher.search(query, earlyCollector)
        collector.topDocs
      } else searcher.searchAfter(after.orNull, query, pageSize, sort)

      val scoreDocs = topDocs.scoreDocs
      val numFetched = scoreDocs.length
      afterOffset += numFetched
      finished = numFetched < pageSize
      for (scoreDoc <- scoreDocs) {
        after = Some(scoreDoc)
        val document = searcher.doc(scoreDoc.doc, fields)
        documents.add((document, scoreDoc))
      }

      tracer.trace(s"Lucene index fetches $numFetched documents")
      logger.debug(s"Page fetched with $numFetched documents in $time")

    } catch {
      case e: Exception =>
        close()
        throw new IndexException(e, s"Error searching with $query and $sort")
    }
    if (finished) close()
  }

  /** Returns if more documents should be fetched from the Lucene index.
    *
    * @return `true` if more documents should be fetched, `false` otherwise
    */
  def needsFetch: Boolean = !finished && documents.isEmpty

  /** Returns if the iteration has more documents.
    *
    * @return `true` if the iteration has more documents, `false` otherwise
    */
  override def hasNext: Boolean = {
    if (needsFetch) fetch()
    !documents.isEmpty
  }

  /** Returns the next document-score tuple in the iteration.
    *
    * @return the next document-score tuple
    * @throws NoSuchElementException if the iteration has no more documents
    */
  override def next: (Document, ScoreDoc) = {
    if (hasNext) documents.poll else throw new NoSuchElementException
  }

  /** Closes the [[IndexSearcher]] and any other resources */
  override def close() = {
    if (!closed) try releaseSearcher() finally closed = true
  }

}

/** Companion object for [[DocumentIterator]]. */
object DocumentIterator {

  /** The max number of rows to be read per iteration. */
  val MAX_PAGE_SIZE = 10000
}
