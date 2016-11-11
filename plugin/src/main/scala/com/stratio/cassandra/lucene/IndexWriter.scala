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

import com.stratio.cassandra.lucene.IndexWriter.logger
import org.apache.cassandra.db._
import org.apache.cassandra.db.filter.{ClusteringIndexNamesFilter, ColumnFilter}
import org.apache.cassandra.db.rows.{Row, UnfilteredRowIterator}
import org.apache.cassandra.index.Index.Indexer
import org.apache.cassandra.index.transactions.IndexTransaction
import org.apache.cassandra.utils.concurrent.OpOrder
import org.slf4j.LoggerFactory

/** [[Indexer]] for Lucene-based index.
  *
  * @param service         the service to perform the indexing operation
  * @param key             key of the partition being modified
  * @param nowInSec        current time of the update operation
  * @param opGroup         operation group spanning the update operation
  * @param transactionType what kind of update is being performed on the base data
  * @author Andres de la Pena `adelapena@stratio.com`
  */
abstract class IndexWriter(
    service: IndexService,
    key: DecoratedKey,
    nowInSec: Int,
    opGroup: OpOrder.Group,
    transactionType: IndexTransaction.Type) extends Indexer {

  val metadata = service.metadata
  val table = service.table

  /** @inheritdoc */
  override def begin() {
  }

  /** @inheritdoc */
  override def partitionDelete(deletionTime: DeletionTime) {
    logger.trace(s"Delete partition during $transactionType: $deletionTime")
    delete()
  }

  /** @inheritdoc */
  override def rangeTombstone(tombstone: RangeTombstone) {
    logger.trace(s"Range tombstone during $transactionType: $tombstone")
  }

  /** @inheritdoc */
  override def insertRow(row: Row): Unit = {
    logger.trace(s"Insert rows during $transactionType: $row")
    index(row)
  }

  /** @inheritdoc */
  override def updateRow(oldRowData: Row, newRowData: Row): Unit = {
    logger.trace(s"Update row during $transactionType: $oldRowData TO $newRowData")
    index(newRowData)
  }

  /** @inheritdoc */
  override def removeRow(row: Row): Unit = {
    logger.trace(s"Remove row during $transactionType: $row")
    index(row)
  }

  /** Deletes all the partition. */
  protected def delete()

  /** Indexes the specified row. It behaviours as an upsert and may involve read-before-write.
    *
    * @param row the row to be indexed.
    */
  protected def index(row: Row)

  /** Retrieves from the local storage all the rows in the specified partition.
    *
    * @param key      the partition key
    * @param nowInSec max allowed time in seconds
    * @return a row iterator
    */
  protected def read(key: DecoratedKey, nowInSec: Int): UnfilteredRowIterator = {
    val clusterings = new java.util.TreeSet[Clustering](metadata.comparator)
    clusterings.add(Clustering.EMPTY)
    read(key, clusterings, nowInSec)
  }

  /** Retrieves from the local storage the rows in the specified partition slice.
    *
    * @param key         the partition key
    * @param clusterings the clustering keys
    * @param nowInSec    max allowed time in seconds
    * @return a row iterator
    */
  protected def read(
      key: DecoratedKey,
      clusterings: java.util.NavigableSet[Clustering],
      nowInSec: Int): UnfilteredRowIterator = {
    val filter = new ClusteringIndexNamesFilter(clusterings, false)
    val columnFilter = ColumnFilter.all(metadata)
    val command = SinglePartitionReadCommand.create(metadata, nowInSec, key, columnFilter, filter)
    val controller = command.executionController()
    try command.queryMemtableAndDisk(table, controller) finally controller.close()
  }

}

object IndexWriter {

  protected val logger = LoggerFactory.getLogger(classOf[IndexWriter])

}