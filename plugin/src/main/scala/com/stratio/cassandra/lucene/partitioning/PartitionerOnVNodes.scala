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

import com.stratio.cassandra.lucene.IndexException
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db._
import org.apache.cassandra.dht.Token
import org.apache.cassandra.service.StorageService

/** [[Partitioner]] based on the partition key token. Rows will be stored in an index partition
  * determined by the virtual nodes token range. Partition-directed searches will be routed to
  * a single partition, increasing performance. However, token range searches will be routed to all
  * the partitions, with a slightly lower performance.
  *
  * This partitioner guarantees an excellent load balancing between index partitions.
  *
  * @author Eduardo Alonso `eduardoalonso@stratio.com`
  */
case class PartitionerOnVNodes() extends Partitioner {

  val tokens = StorageService.instance.getLocalTokens.stream().map[Long](_.getTokenValue.asInstanceOf[Long]).sorted()

  /** Returns the number of partitions. */

  override def numPartitions: Int = tokens.toArray.length

  /** Returns the involved partitions for the specified read command.
    *
    * @param command a read command to be routed to some partitions
    * @return the partitions containing the all data required to satisfy `command`
    */
  override def partitions(command: ReadCommand): List[Int] = command match {
    case c: SinglePartitionReadCommand => List(partition(c.partitionKey))
    case c: PartitionRangeReadCommand =>
      val range = c.dataRange()
      val start = range.startKey().getToken
      val stop = range.stopKey().getToken
      partitions(start, stop)
    case _ => throw new IndexException(s"Unsupported read command type: ${command.getClass}")
  }

  /** Returns a List of the partitions used in the range. */
  private[this] def partitions(left: Token, right: Token): List[Int] =
    List.range(partition(left), partition(right) + 1)

  /** @inheritdoc*/
  private[this] def partition(token: Token): Int = {
    val token_long: Long = token.getTokenValue.asInstanceOf[Long]
    tokens.filter(x => token_long >= x).count().toInt - 1
  }

  /** Returns the partition for the specified partition key.
    *
    * @param key a partition key to be routed to a partition
    * @return the partition owning `key`
    */
  override def partition(key: DecoratedKey): Int = partition(key.getToken)

}


/** Companion object for [[PartitionerOnToken]]. */
object PartitionerOnVNodes {

  /** [[PartitionerOnToken]] builder. */
  case class Builder() extends Partitioner.Builder {
    override def build(metadata: CFMetaData): PartitionerOnVNodes = PartitionerOnVNodes()
  }

}
