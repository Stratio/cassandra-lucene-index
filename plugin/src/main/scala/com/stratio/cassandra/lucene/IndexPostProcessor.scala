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

import java.util.function.BiFunction

import com.stratio.cassandra.lucene.IndexPostProcessor.logger
import com.stratio.cassandra.lucene.index.RAMIndex
import com.stratio.cassandra.lucene.search.Search
import com.stratio.cassandra.lucene.util._
import org.apache.cassandra.db.SinglePartitionReadCommand.Group
import org.apache.cassandra.db.partitions.PartitionIterator
import org.apache.cassandra.db.rows.Row
import org.apache.cassandra.db.{DecoratedKey, ReadCommand, ReadQuery, SinglePartitionReadCommand}
import org.apache.lucene.document.{Document, StoredField}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

/** Post processes in the coordinator node the results of a distributed search. In other words,
  * gets the k globally best results from all the k best node-local results.
  *
  * @param service the index service
  * @author Andres de la Pena `adelapena@stratio.com`
  */
sealed abstract class IndexPostProcessor[A <: ReadQuery](service: IndexService)
  extends BiFunction[PartitionIterator, A, PartitionIterator] {

  protected def postProcess(
      partitions: PartitionIterator,
      command: ReadCommand,
      limit: Int,
      nowInSec: Int)
  : PartitionIterator = {
    val search = service.expressionMapper.search(command)
    if (search.requiresFullScan) {
      val rows = collect(partitions)
      if (search.requiresPostProcessing && rows.nonEmpty) {
        return merge(search, limit, nowInSec, rows)
      }
    }
    partitions
  }

  private def collect(partitions: PartitionIterator): List[(DecoratedKey, SimpleRowIterator)] = {
    val time = TimeCounter.create.start
    val rows = mutable.ListBuffer[(DecoratedKey, SimpleRowIterator)]()
    for (partition <- partitions) {
      try {
        val key = partition.partitionKey
        while (partition.hasNext) {
          rows += ((key, new SimpleRowIterator(partition)))
        }
      } finally partition.close()
    }
    logger.debug(s"Collected ${rows.size} rows in ${time.stop}")
    rows.toList
  }

  private def merge(
      search: Search,
      limit: Int,
      nowInSec: Int,
      rows: List[(DecoratedKey, SimpleRowIterator)])
  : PartitionIterator = {

    val time = TimeCounter.create.start
    val field = "_id"
    val index = new RAMIndex(service.schema.analyzer())
    try {

      // Index collected rows in memory
      for (id <- rows.indices) {
        val (key, rowIterator) = rows(id)
        val row = rowIterator.row
        val doc = document(key, row, search)
        doc.add(new StoredField(field, id)) // Mark document
        index.add(doc)
      }

      // Repeat search to sort partial results
      val query = search.postProcessingQuery(service.schema)
      val sort = service.sort(search)
      val docs = index.search(query, sort, limit, Set(field))

      // Collect and decorate
      val merged = for ((doc, score) <- docs) yield {
        val id = doc.get(field).toInt
        val rowIterator = rows(id)._2
        rowIterator.decorated(row => service.expressionMapper.decorate(row, score, nowInSec))
      }

      Tracer.trace(s"Lucene post-process ${rows.size} collected rows to ${merged.size} rows")
      logger.debug(s"Post-processed ${rows.size} rows to ${merged.size} rows in ${time.stop}")
      new SimplePartitionIterator(merged)

    } finally index.close()
  }

  private def document(key: DecoratedKey, row: Row, search: Search): Document = {
    val doc = new Document
    val cols = service.columns(key, row)
    service.keyIndexableFields(key, row).foreach(doc.add)
    service.schema.postProcessingIndexableFields(cols, search).foreach(doc.add)
    doc
  }
}

object IndexPostProcessor {

  val logger = LoggerFactory.getLogger(classOf[IndexPostProcessor[_]])

}

/** An [[IndexPostProcessor]] for [[ReadCommand]]s.
  *
  * @param service the index service
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class ReadCommandPostProcessor(service: IndexService)
  extends IndexPostProcessor[ReadCommand](service) {

  /** @inheritdoc */
  override def apply(
      partitions: PartitionIterator,
      command: ReadCommand)
  : PartitionIterator = command match {
    case c: SinglePartitionReadCommand => partitions
    case _ => postProcess(
      partitions,
      command,
      command.limits.count,
      command.nowInSec)
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
    if (group.commands.size <= 1) return partitions
    postProcess(partitions, group.commands.get(0), group.limits.count, group.nowInSec)
  }

}