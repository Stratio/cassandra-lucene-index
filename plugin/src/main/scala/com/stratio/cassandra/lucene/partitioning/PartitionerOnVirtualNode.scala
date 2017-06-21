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
import com.stratio.cassandra.lucene.util.Logging
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db._
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken
import org.apache.cassandra.dht.{Bounds, Token}
import org.apache.cassandra.service.StorageService

import scala.collection.JavaConverters._
import scala.collection.mutable

/** [[Partitioner]] based on the partition key token. Rows will be stored in an index partition
  * determined by the virtual nodes token range. Partition-directed searches will be routed to a
  * single partition, increasing performance. However, unbounded token range searches will be routed
  * to all the partitions, with a slightly lower performance. Virtual node token range queries will
  * be routed to only one partition which increase performance in spark queries with virtual nodes rather
  * than partitioning on token.
  *
  * This partitioner load balance depends on virtual node token ranges assignation. The more virtual
  * nodes, the better distribution (more similarity in number of tokens that falls inside any virtual
  * node) between virtual nodes, the better load balance with this partitioner.
  *
  * @param vnodes_per_partition the number of virtual nodes that falls inside an index partition
  * @author Eduardo Alonso `eduardoalonso@stratio.com`
  */
case class PartitionerOnVirtualNode(
    vnodes_per_partition: Int,
    tokens: List[Token]) extends Partitioner with Logging {

  if (vnodes_per_partition <= 0) throw new IndexException(
    s"The number of virtual nodes per partition should be strictly positive but found $vnodes_per_partition")

  val numTokens = tokens.size
  val partitionPerBound = new mutable.HashMap[Bounds[Token], Int]()

  if (numTokens == 1) logger.warn(
    "You are using a PartitionerOnVirtualNode but cassandra is only configured with one token (not using virtual nodes.)")
  val partition = ((numPartitions - 1).toDouble / vnodes_per_partition.toDouble).floor.toInt


  for (i <- 0 until (numPartitions - 1)) {
    val bound = new Bounds(tokens(i),
      new LongToken(tokens(i + 1).getTokenValue.asInstanceOf[Long] - 1))
    val partition = (i.toDouble / vnodes_per_partition.toDouble).floor.toInt
    partitionPerBound(bound) = partition
  }

  /** @inheritdoc*/
  override def numPartitions: Int = (numTokens.toDouble / vnodes_per_partition.toDouble).ceil.toInt

  partitionPerBound(new Bounds(tokens(numPartitions - 1), new LongToken(Long.MaxValue))) = partition

  if (tokens.head.getTokenValue.asInstanceOf[Long] != Long.MinValue) {
    partitionPerBound(new Bounds(new LongToken(Long.MinValue), tokens.head)) = partition
  }

  /** @inheritdoc*/
  override def partitions(command: ReadCommand): List[Int] = command match {
    case c: SinglePartitionReadCommand => List(partition(c.partitionKey))
    case c: PartitionRangeReadCommand =>
      val range = c.dataRange
      partitions(range.startKey.getToken, range.stopKey.getToken)
    case _ => throw new IndexException(s"Unsupported read command type: ${command.getClass}")
  }

  /** Returns a list of the partitions involved in the range.
    *
    * @param lower the lower bound partition
    * @param upper the upper bound partition
    * @return a list of partitions involved in the range
    */
  def partitions(lower: Token, upper: Token): List[Int] = {
    if (lower.equals(upper)) {
      if (lower.isMinimum) {
        allPartitions
      } else {
        List(partition(lower))
      }
    } else {
      val lowerPartition = partition(lower)
      val upperPartition = partition(upper)

      if (lowerPartition <= upperPartition)
        (lowerPartition to upperPartition).toList
      else
        (lowerPartition to numTokens).toList ::: (0 to upperPartition).toList
    }
  }

  /** @inheritdoc*/
  override def partition(key: DecoratedKey): Int = partition(key.getToken)

  /** @inheritdoc*/
  private[this] def partition(token: Token): Int =
    partitionPerBound.filter(_._1.contains(token)).toList.head._2

}

/** Companion object for [[PartitionerOnVirtualNode]]. */
object PartitionerOnVirtualNode {

  /** [[PartitionerOnVirtualNode]] builder. */
  case class Builder(@JsonProperty("vnodes_per_partition") vnodes_per_partition: Int) extends Partitioner.Builder {

    /** @inheritdoc*/
    override def build(metadata: CFMetaData): PartitionerOnVirtualNode = PartitionerOnVirtualNode(
      vnodes_per_partition,
      StorageService.instance.getLocalTokens.asScala.toList.sorted)
  }

}
