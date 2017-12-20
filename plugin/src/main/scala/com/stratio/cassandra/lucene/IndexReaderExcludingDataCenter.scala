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
import com.stratio.cassandra.lucene.util.Logging
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db.{ColumnFamilyStore, ReadCommand}
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator
import org.apache.cassandra.db.rows.UnfilteredRowIterator

/** [[UnfilteredPartitionIterator]] for retrieving rows from a [[DocumentIterator]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class IndexReaderExcludingDataCenter(command: ReadCommand,
                                     table: ColumnFamilyStore) extends UnfilteredPartitionIterator with Logging {
  override def metadata(): CFMetaData = table.metadata

  override def isForThrift: Boolean = command.isForThrift

  override def close(): Unit = {}

  override def next(): UnfilteredRowIterator = {
    logger.warn("You are executing a query against a excluded datacenter node")
    null
  }

  override def hasNext: Boolean = {
    logger.warn("You are executing a query against a excluded datacenter node")
    false
  }
}
