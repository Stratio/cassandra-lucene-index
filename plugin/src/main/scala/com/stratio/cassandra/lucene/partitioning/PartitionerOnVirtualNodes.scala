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
import org.apache.cassandra.dht.Token
import org.apache.cassandra.service.StorageService

import scala.collection.JavaConverters._

/** [[Partitioner]]  based on the partition key token. Rows will be stored in an index partition
  * determined by the virtual nodes token range. Partition-directed searches will be routed to a
  * single partition, increasing performance. However,unfiltered token range searches will be routed
  * to all the partitions, with a slightly lower performance. Virtual node token range queries will
  * be routed to only one partition which increase performance in spark queries with vnodes rather
  * than partitioning on token.
  *
  * This partitioner load balance depends on virtual node token ranges asignation. The more virtual
  * nodes, the better distribution (more similarity in number of tokens that falls inside any virtual
  * node) between virtual nodes, the better load balance with this partitioner.
  *
  * @param partitions_number the number of index partitions per node
  * @author Eduardo Alonso `eduardoalonso@stratio.com`
  */
case class PartitionerOnVirtualNodes(partitions_number: Int) extends Partitioner with Logging {

  if (partitions_number <= 0) throw new IndexException(
    s"The number of partitions should be strictly positive but found $partitions_number")

  val tokens: List[Long] = StorageService.instance.getLocalTokens.asScala.toList.map(_.getTokenValue.asInstanceOf[Long])

  val numTokens = tokens.size
  if (numTokens == 1) logger.warn("You are using a PartitionerOnVNodes but cassandra is only configured with one token (non using virtual nodes.)")

  /** Returns a list of the partitions involved in the range.
    *
    * @param lower the lower bound partition
    * @param upper the upper bound partition
    * @return a list of partitions involved in the range
    **/
  def partitions(lower: Token, upper: Token): List[Int] = {
    val lowerPartition = partition(lower)
    val upperPartition = partition(upper)

    if (lowerPartition <= upperPartition)
      (lowerPartition to upperPartition).toList
    else
      (lowerPartition to numTokens).toList ::: (0 to upperPartition).toList
  }


  /** @inheritdoc*/
  override def partition(key: DecoratedKey): Int = partition(key.getToken)

  /** @inheritdoc */
  override def partitions(command: ReadCommand): List[Int] = command match {
    case c: SinglePartitionReadCommand => List(partition(c.partitionKey))
    case c: PartitionRangeReadCommand =>
      val range = c.dataRange()
      val start = range.startKey().getToken
      val stop = range.stopKey().getToken

      if (start.equals(stop)) {
        if (start.isMinimum) {
          //this is a unbounded select
          allPartitions
        } else {
          List(partition(start))
        }
      } else {
        partitions(start, stop)
      }
    case _ => throw new IndexException(s"Unsupported read command type: ${command.getClass}")
  }

  /** @inheritdoc */
  override def numPartitions: Int = partitions_number

  /** Returns the virtual node index for the token
    *
    * @param token the token Long value
    * @return the virtual node index where this token falls into
    */
  private[this] def virtualNode(token: Long): Int = {
    val vnode = tokens.count(x => token >= x) - 1
    if (vnode < 0) numTokens + vnode else vnode
  }

  /** @inheritdoc*/
  private[this] def partition(token: Token): Int =
    virtualNode(token.getTokenValue.asInstanceOf[Long]) % partitions_number

}

/** Companion object for [[PartitionerOnVirtualNodes]]. */
object PartitionerOnVirtualNodes {

  /** [[PartitionerOnVirtualNodes]] builder. */
  case class Builder(@JsonProperty("partitions") partitions: Int) extends Partitioner.Builder {
    override def build(metadata: CFMetaData): PartitionerOnVirtualNodes = PartitionerOnVirtualNodes(partitions)
  }

}
