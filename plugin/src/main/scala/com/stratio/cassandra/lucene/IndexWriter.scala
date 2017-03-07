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

import com.stratio.cassandra.lucene.util.{Logging, Tracing}
import org.apache.cassandra.db._
import org.apache.cassandra.db.rows.{Row, RowIterator, UnfilteredRowIterators}
import org.apache.cassandra.index.Index.Indexer
import org.apache.cassandra.index.transactions.IndexTransaction
import org.apache.cassandra.index.transactions.IndexTransaction.Type.CLEANUP
import org.apache.cassandra.utils.concurrent.OpOrder

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
    transactionType: IndexTransaction.Type) extends Indexer with Logging with Tracing {

  val metadata = service.metadata
  val table = service.table

  /** @inheritdoc */
  override def begin() {
    logger.trace(s"Begin transaction $transactionType")
  }

  /** @inheritdoc */
  override def partitionDelete(deletionTime: DeletionTime) {
    logger.trace(s"Delete partition during $transactionType: $deletionTime")
    delete()
  }

  /** @inheritdoc */
  override def rangeTombstone(tombstone: RangeTombstone) {
    logger.trace(s"Range tombstone during $transactionType: $tombstone")
    delete(tombstone)
  }

  /** @inheritdoc */
  override def insertRow(row: Row): Unit = {
    logger.trace(s"Insert rows during $transactionType: $row")
    tryIndex(row)
  }

  /** @inheritdoc */
  override def updateRow(oldRowData: Row, newRowData: Row): Unit = {
    logger.trace(s"Update row during $transactionType: $oldRowData TO $newRowData")
    tryIndex(newRowData)
  }

  /** @inheritdoc */
  override def removeRow(row: Row): Unit = {
    logger.trace(s"Remove row during $transactionType: $row")
    tryIndex(row)
  }

  /** Deletes all the partition. */
  protected def delete()

  /** Deletes all the rows in the specified tombstone. */
  protected def delete(tombstone: RangeTombstone)

  /** Try indexing the row. If the row does not affect index it is not indexed */
  private[this] def tryIndex(row: Row): Unit = {
    if (service.doesAffectIndex(row)) {
      index(row)
    } else {
      tracer.trace("Lucene index skipping row")
    }
  }

  /** Indexes the specified row. It behaviours as an upsert and may involve read-before-write.
    *
    * @param row the row to be indexed.
    */
  protected def index(row: Row)

  /** Retrieves from the local storage the rows satisfying the specified read command.
    *
    * @param command a single partition read command
    * @return a row iterator
    */
  protected def read(command: SinglePartitionReadCommand): RowIterator = {
    val controller = command.executionController
    try {
      val unfilteredRows = command.queryMemtableAndDisk(table, controller)
      UnfilteredRowIterators.filter(unfilteredRows, nowInSec)
    } finally controller.close()
  }

  /** @inheritdoc */
  override final def finish() {

    // Skip on cleanups
    if (transactionType == CLEANUP) return

    // Finish with mutual exclusion on partition
    service.readBeforeWriteLocker.run(key, () => commit())
  }

  /** Commits all pending writes */
  protected def commit()

}