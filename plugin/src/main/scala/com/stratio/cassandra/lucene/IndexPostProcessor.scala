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

import java.util.Collections
import java.util.function.BiFunction

import com.stratio.cassandra.lucene.IndexPostProcessor._
import com.stratio.cassandra.lucene.index.RAMIndex
import com.stratio.cassandra.lucene.search.Search
import com.stratio.cassandra.lucene.util._
import org.apache.cassandra.db.SinglePartitionReadCommand.Group
import org.apache.cassandra.db.partitions.PartitionIterator
import org.apache.cassandra.db.rows.Row
import org.apache.cassandra.db.{DecoratedKey, ReadCommand, ReadQuery, SinglePartitionReadCommand}
import org.apache.lucene.document.{Document, StoredField}

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Post processes in the coordinator node the results of a distributed search. In other words,
  * gets the k globally best results from all the k best node-local results.
  *
  * @param service the index service
  * @author Andres de la Pena `adelapena@stratio.com`
  */
sealed abstract class IndexPostProcessor[A <: ReadQuery](service: IndexService)
  extends BiFunction[PartitionIterator, A, PartitionIterator] with Logging with Tracing {

  /** Returns a partition iterator containing the top-k rows of the specified partition iterator
    * according to the specified search.
    *
    * @param partitions a partition iterator
    * @param search     a search defining the ordering
    * @param limit      the number of results to be returned
    * @param now        the operation time in seconds
    * @return
    */
  protected def process(partitions: PartitionIterator, search: Search, limit: Int, now: Int)
  : PartitionIterator = {
    if (search.requiresFullScan) {
      val rows = collect(partitions)
      if (search.requiresPostProcessing && rows.nonEmpty) {
        return top(rows, search, limit, now)
      }
    }
    partitions
  }

  /** Collects the rows of the specified partition iterator. The iterator gets traversed after this
    * operation so it can't be reused.
    *
    * @param partitions a partition iterator
    * @return the rows contained in the partition iterator
    */
  private def collect(partitions: PartitionIterator): List[(DecoratedKey, SingleRowIterator)] = {
    val time = TimeCounter.start
    val rows = mutable.ListBuffer[(DecoratedKey, SingleRowIterator)]()
    for (partition <- partitions.asScala) {
      try {
        val key = partition.partitionKey
        while (partition.hasNext) {
          rows += ((key, new SingleRowIterator(partition)))
        }
      } finally partition.close()
    }
    logger.debug(s"Collected ${rows.size} rows in $time")
    rows.toList
  }

  /** Takes the k best rows of the specified rows according to the specified search.
    *
    * @param rows   the rows to be sorted
    * @param search a search defining the ordering
    * @param limit  the number of results to be returned
    * @param now    the operation time in seconds
    * @return
    */
  private def top(
      rows: List[(DecoratedKey, SingleRowIterator)],
      search: Search,
      limit: Int,
      now: Int): PartitionIterator = {

    val time = TimeCounter.start
    val index = new RAMIndex(service.schema.analyzer)
    try {

      // Index collected rows in memory
      for (id <- rows.indices) {
        val (key, rowIterator) = rows(id)
        val row = rowIterator.row
        val doc = document(key, row, search, now)
        doc.add(new StoredField(ID_FIELD, id)) // Mark document
        index.add(doc)
      }

      // Repeat search to sort partial results
      val query = search.postProcessingQuery(service.schema)
      val sort = service.sort(search)
      val docs = index.search(query, sort, limit, FIELDS_TO_LOAD)

      // Collect and decorate
      val merged = for ((doc, score) <- docs) yield {
        val id = doc.get(ID_FIELD).toInt
        val rowIterator = rows(id)._2
        rowIterator.decorated(row => service.expressionMapper.decorate(row, score, now))
      }

      tracer.trace(s"Lucene post-process ${rows.size} collected rows to ${merged.size} rows")
      logger.debug(s"Post-processed ${rows.size} rows to ${merged.size} rows in $time")
      new SimplePartitionIterator(merged)

    } finally index.close()
  }

  /** Returns a [[Document]] representing the specified row with only the fields required to satisfy
    * the specified [[Search]].
    *
    * @param key    a partition key
    * @param row    a row
    * @param search a search
    * @return a document with just the fields required to satisfy the search
    */
  private def document(key: DecoratedKey, row: Row, search: Search, now: Int): Document = {
    val document = new Document
    val clustering = row.clustering()
    val columns = service.columnsMapper.columns(key, row, now)
    service.keyIndexableFields(key, clustering).foreach(document.add)
    service.schema.postProcessingIndexableFields(columns, search).forEach(document add _)
    document
  }
}

/** Companion object for [[IndexPostProcessor]]. */
object IndexPostProcessor {

  val ID_FIELD = "_id"
  val FIELDS_TO_LOAD: java.util.Set[String] = Collections.singleton(ID_FIELD)

}

/** An [[IndexPostProcessor]] for [[ReadCommand]]s.
  *
  * @param service the index service
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class ReadCommandPostProcessor(service: IndexService)
  extends IndexPostProcessor[ReadCommand](service) {

  /** @inheritdoc */
  override def apply(partitions: PartitionIterator, command: ReadCommand): PartitionIterator = {
    if (!partitions.hasNext || command.isInstanceOf[SinglePartitionReadCommand]) return partitions
    val search = service.expressionMapper.search(command)
    process(partitions, search, command.limits.count, command.nowInSec)
  }

}

/** An [[IndexPostProcessor]] for [[Group]] commands.
  *
  * @param service the index service
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class GroupPostProcessor(service: IndexService) extends IndexPostProcessor[Group](service) {

  /** @inheritdoc */
  override def apply(partitions: PartitionIterator, group: Group): PartitionIterator = {
    if (!partitions.hasNext || group.commands.size <= 1) return partitions
    val search = service.expressionMapper.search(group.commands.get(0))
    process(partitions, search, group.limits.count, group.nowInSec)
  }

}