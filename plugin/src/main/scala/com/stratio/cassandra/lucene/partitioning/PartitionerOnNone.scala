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
package com.stratio.cassandra.lucene.partitioning

import java.nio.file.Path

import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db.{DecoratedKey, ReadCommand}

/** [[Partitioner]] with no action, equivalent to just don't partitioning the index.
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
case class PartitionerOnNone() extends Partitioner {

  /** @inheritdoc */
  override def numPartitions: Int = 1

  /** @inheritdoc */
  override def partition(key: DecoratedKey): Int = 0

  /** @inheritdoc */
  override def partitions(command: ReadCommand): List[Int] = allPartitions

  /** @inheritdoc */
  override def toString: String = "PartitionerOnNone()"

  /** @inheritdoc */
  override def pathsForEachPartitions: Option[Array[Path]] = None

}

/** Companion object for [[PartitionerOnNone]]. */
object PartitionerOnNone {

  /** [[PartitionerOnNone]] builder. */
  case class Builder() extends Partitioner.Builder {

    /** @inheritdoc */
    override def build(metadata: CFMetaData): PartitionerOnNone = PartitionerOnNone()
  }

}
