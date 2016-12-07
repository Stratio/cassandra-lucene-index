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
package com.stratio.cassandra.lucene.util

import java.util.Collections

import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db.rows.{Row, RowIterator}
import org.apache.cassandra.db.{DecoratedKey, PartitionColumns}

/** [[RowIterator]] representing a single CQL [[Row]], gotten from the head position of the
  * specified [[RowIterator]]. Any other rows in the specified iterator won't be read.
  *
  * @param iterator  the [[Row]] iterator
  * @param headRow   a row to override the first row in the iterator
  * @param decorator a function to decorate the row
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class SingleRowIterator(
    iterator: RowIterator,
    headRow: Option[Row] = None,
    decorator: Option[Row => Row] = None)
  extends RowIterator {

  val row = headRow.getOrElse(iterator.next)

  private[this] val _metadata = iterator.metadata
  private[this] val _partitionKey = iterator.partitionKey
  private[this] val _columns = iterator.columns
  private[this] val _staticRow = iterator.staticRow
  private[this] val singleIterator = Collections.singletonList(row).iterator

  /** Return a copy of this iterator with the specified row decorator.
    *
    * @param decorator a function to decorate the returned row
    * @return a new iterator with the decorator
    */
  def decorated(decorator: Row => Row): SingleRowIterator = {
    new SingleRowIterator(iterator, Some(row), Option(decorator))
  }

  /** @inheritdoc */
  override def metadata: CFMetaData = _metadata

  /** @inheritdoc */
  override def isReverseOrder: Boolean = false

  /** @inheritdoc */
  override def columns: PartitionColumns = _columns

  /** @inheritdoc */
  override def partitionKey: DecoratedKey = _partitionKey

  /** @inheritdoc */
  override def staticRow: Row = _staticRow

  /** @inheritdoc */
  override def close() {}

  /** @inheritdoc */
  override def hasNext: Boolean = singleIterator.hasNext

  /** @inheritdoc */
  override def next: Row = {
    val row = singleIterator.next
    decorator.map(_ apply row).getOrElse(row)
  }
}
