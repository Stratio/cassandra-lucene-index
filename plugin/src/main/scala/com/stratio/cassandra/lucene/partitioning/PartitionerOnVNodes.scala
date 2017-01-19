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

/** [[Partitioner]] based on the partition key token. Rows will be stored in an index partition
  * determined by the virtual nodes. Partition-directed searches will be routed to
  * a single partition, increasing performance. However, token range searches will be routed to all
  * the partitions, with a slightly lower performance.
  *
  * This partitioner guarantees an excellent load balancing between index partitions.
  *
  * @param partitions_number the number of index partitions per node
  * @author Eduardo Alonso `eduardoalonso@stratio.com`
  */
case class PartitionerOnVNodes(partitions_number: Int) extends Partitioner with Logging {

  if (partitions_number <= 0) throw new IndexException(
    s"The number of partitions should be strictly positive but found $partitions_number")

  val tokens: List[Long] = StorageService.instance.getLocalTokens.asScala.toList.map(_.getTokenValue.asInstanceOf[Long])

  val numTokens = tokens.size
  if (numTokens == 1) logger.warn("You are using a PartitionerOnVNodes but cassandra is only configured with one token (non using virtual nodes.)")

  /** Returns a List of the partitions used in the range. */
  def partitions(left: Token, right: Token): List[Int] = {

    val ret = buildRange(partition(left), partition(right))
    val ret_String= ret.map(_.toString).mkString("")
    logger.debug(s"calculating partitions:  left:  $left right: $right returning $ret_String")
    ret
  }

  /** @inheritdoc */
  override def partition(key: DecoratedKey): Int = partition(key.getToken)

  /** @inheritdoc*/
  override def partitions(command: ReadCommand): List[Int] = command match {



    case c: SinglePartitionReadCommand => {

      val ret= List(partition(c.partitionKey))
      val ret_String= ret.map(_.toString).mkString("")
      logger.debug(s"calculating partitions for SinglePartitionReadCommand returning $ret_String")
      ret
    }
    case c: PartitionRangeReadCommand =>

      c.dataRange()
      val range = c.dataRange()
      val start = range.startKey().getToken
      val stop = range.stopKey().getToken
      if (start.equals(stop) && !start.isMinimum) partitions(start, stop) else allPartitions
    case _ => throw new IndexException(s"Unsupported read command type: ${command.getClass}")
  }

  /** @inheritdoc*/
  override def numPartitions: Int = partitions_number

  private[this] def buildRange(leftPartition: Int, rightPartition: Int): List[Int] = {
    val ret = if (leftPartition <= rightPartition)
      (leftPartition to rightPartition).toList
    else
      (leftPartition to numTokens).toList ::: (0 to rightPartition).toList
    val ret_String= ret.map(_.toString).mkString("")
    logger.debug(s"buildRange for left: ${leftPartition.toString}, right: ${rightPartition.toString} returning ${ret.toString}")
    ret
  }

  private[this] def vnode_number(token: Long): Int = {
    val vnode = tokens.count(x => token >= x) - 1
    val ret=  if (vnode < 0) numTokens + vnode else vnode

    tokens.map(_.toString)
    logger.debug(s"calculating vnode for token: $token returning ${ret.toString}")
    ret
  }

  /** @inheritdoc */
  private[this] def partition(token: Token): Int = {
    val ret = vnode_number(token.getTokenValue.asInstanceOf[Long]) % partitions_number
    logger.debug(s"calculating partitions for token: $token returning ${ret.toString}")
    ret
  }
}

/** Companion object for [[PartitionerOnToken]]. */
object PartitionerOnVNodes {

  /** [[PartitionerOnToken]] builder. */
  case class Builder(@JsonProperty("partitions") partitions: Int) extends Partitioner.Builder {
    override def build(metadata: CFMetaData): PartitionerOnVNodes = PartitionerOnVNodes(partitions)
  }

}
