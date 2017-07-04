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

import java.nio.file.{Path, Paths}

import com.fasterxml.jackson.annotation.JsonProperty
import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.partitioning.Partitioner.StaticPartitioner
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
case class PartitionerOnToken(partitions: Int, paths: Array[Path]) extends StaticPartitioner {

  if (partitions <= 0) throw new IndexException(
    s"The number of partitions should be strictly positive but found $partitions")

  if (paths != null) {
    if (paths.length != partitions) throw new IndexException(
      s"The paths size must be equal to number of partitions")
  }

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

  /** @inheritdoc */
  override def partition(key: DecoratedKey): Int = partition(key.getToken)

  /** @inheritdoc */
  private[this] def partition(token: Token): Int =
    (Math.abs(token.getTokenValue.asInstanceOf[Long]) % partitions).toInt

  /** @inheritdoc */
  override def pathForPartition(partition: Int): Path = {
    if ((partition < 0) || (partition >= numPartitions)) {
      throw new IndexOutOfBoundsException(s"partition must be [0,$numPartitions)")
    } else {
      paths(partition)
    }
  }

  /** @inheritdoc */
  override def numPartitions: Int = partitions

  /** @inheritdoc */
  override def pathsForEveryPartition: Array[Path] = paths

  /** @inheritdoc */
  override def equals(that: Any): Boolean =
    that match {
      case that: PartitionerOnToken => this.partitions.equals(that.partitions) && this.paths.sameElements(
        that.paths)
      case _ => false
    }
}

/** Companion object for [[PartitionerOnToken]]. */
object PartitionerOnToken {

  /** [[PartitionerOnToken]] builder.
    *
    * @param partitions the number of index partitions per node
    */
  case class Builder(
      @JsonProperty("partitions") partitions: Int,
      @JsonProperty("paths") paths: Array[String]) extends Partitioner.Builder {

    /** @inheritdoc */
    override def build(metadata: CFMetaData): PartitionerOnToken = PartitionerOnToken(partitions,
      paths.map(Paths.get(_)))
    /** @inheritdoc */
    override def equals(that: Any): Boolean =
      that match {
        case that: Builder => this.partitions.equals(that.partitions) && this.paths.sameElements(
          that.paths)
        case _ => false
      }

  }

}
