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

import com.fasterxml.jackson.annotation.JsonProperty
import com.stratio.cassandra.lucene.IndexException
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db._
import org.apache.cassandra.dht.Token

/** [[Partitioner]] based on the partition key token. Rows will be stored in an index partition
  * determined by the hash of the partition key token. Partition-directed searches will be routed to
  * a single partition, increasing performance. However, token range searches will be routed to all
  * the partitions, with a slightly lower performance.
  *
  * This partitioner guarantees an excellent load balancing between index partitions.
  *
  * @param partitions the number of index partitions per node
  * @author Andres de la Pena `adelapena@stratio.com`
  */
case class PartitionerOnToken(partitions: Int) extends Partitioner {

  if (partitions <= 0) throw new IndexException(
    s"The number of partitions should be strictly positive but found $partitions")

  /** @inheritdoc */
  private[this] def partition(token: Token): Int =
    (Math.abs(token.getTokenValue.asInstanceOf[Long]) % partitions).toInt

  /** @inheritdoc */
  override def numPartitions: Int = partitions

  /** @inheritdoc */
  override def partition(key: DecoratedKey): Int = partition(key.getToken)

  /** @inheritdoc */
  override def partitions(command: ReadCommand): List[Int] = command match {
    case c: SinglePartitionReadCommand => List(partition(c.partitionKey))
    case c: PartitionRangeReadCommand =>
      val range = c.dataRange()
      val start = range.startKey().getToken
      val stop = range.stopKey().getToken
      if (start.equals(stop) && !start.isMinimum) List(partition(start)) else allPartitions
    case _ => throw new IndexException(s"Unsupported read command type: ${command.getClass}")
  }

}

/** Companion object for [[PartitionerOnToken]]. */
object PartitionerOnToken {

  /** [[PartitionerOnToken]] builder.
    *
    * @param partitions the number of index partitions per node
    */
  case class Builder(@JsonProperty("partitions") partitions: Int) extends Partitioner.Builder {
    override def build(metadata: CFMetaData): PartitionerOnToken = PartitionerOnToken(partitions)
  }

}
