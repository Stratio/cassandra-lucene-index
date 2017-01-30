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

import org.apache.cassandra.db.partitions.PartitionIterator
import org.apache.cassandra.db.rows.RowIterator

/** [[PartitionIterator]] composed by a list of [[SingleRowIterator]]s.
  *
  * @param rows the rows to be iterated
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class SimplePartitionIterator(rows: Seq[SingleRowIterator]) extends PartitionIterator {

  private[this] val iterator = rows.iterator

  /** @inheritdoc */
  def hasNext: Boolean = iterator.hasNext

  /** @inheritdoc */
  def next(): RowIterator = iterator.next

  /** @inheritdoc */
  def close() = iterator.foreach(_.close())
}
