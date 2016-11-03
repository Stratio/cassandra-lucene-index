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

import com.stratio.cassandra.lucene.index.DocumentIterator
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db._
import org.apache.cassandra.db.filter.ClusteringIndexFilter
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator
import org.apache.cassandra.db.rows.UnfilteredRowIterator

/** [[UnfilteredPartitionIterator]] for retrieving rows from a [[DocumentIterator]].
  *
  * @param command    the read command
  * @param table      the base table
  * @param controller the read execution controller
  * @param documents  the documents iterator
  * @author Andres de la Pena `adelapena@stratio.com`
  */
abstract class IndexReader(
    command: ReadCommand,
    table: ColumnFamilyStore,
    controller: ReadExecutionController,
    documents: DocumentIterator)
  extends UnfilteredPartitionIterator {

  protected var nextData: Option[UnfilteredRowIterator] = None

  /** @inheritdoc */
  override def isForThrift: Boolean = {
    command.isForThrift
  }

  /** @inheritdoc */
  override def metadata: CFMetaData = {
    table.metadata
  }

  /** @inheritdoc */
  override def hasNext: Boolean = {
    prepareNext()
  }

  /** @inheritdoc */
  override def next(): UnfilteredRowIterator = {
    if (nextData.isEmpty) prepareNext()
    val result = nextData.orNull
    nextData = None
    result
  }

  /** @inheritdoc */
  override def remove() = {
    throw new UnsupportedOperationException
  }

  /** @inheritdoc */
  override def close() = {
    try nextData.foreach(_.close()) finally documents.close()
  }

  protected def prepareNext(): Boolean

  protected def read(key: DecoratedKey, filter: ClusteringIndexFilter): UnfilteredRowIterator = {
    SinglePartitionReadCommand.create(
      isForThrift,
      table.metadata,
      command.nowInSec,
      command.columnFilter,
      command.rowFilter,
      command.limits,
      key,
      filter).queryMemtableAndDisk(table, controller)
  }


}
