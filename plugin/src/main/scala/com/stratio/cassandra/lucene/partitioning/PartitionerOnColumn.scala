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

import java.nio.ByteBuffer
import java.nio.file.{Path, Paths}

import com.fasterxml.jackson.annotation.JsonProperty
import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.partitioning.Partitioner.StaticPartitioner
import com.stratio.cassandra.lucene.util.ByteBufferUtils
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db.PartitionPosition.Kind.ROW_KEY
import org.apache.cassandra.db._
import org.apache.cassandra.db.marshal.{AbstractType, UTF8Type}
import org.apache.cassandra.utils.MurmurHash
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConverters._

/** [[Partitioner]] based on a partition key column. Rows will be stored in an index partition
  * determined by the hash of the specified partition key column. Both partition-directed and token
  * range searches containing an CQL equality filter over the selected partition key column will be
  * routed to a single partition, increasing performance. However, token range searches without
  * filters over the partitioning column will be routed to all the partitions, with a slightly lower
  * performance.
  *
  * Load balancing depends on the cardinality and distribution of the values of the partitioning
  * column. Both high cardinalities and uniform distributions will provide better load balancing
  * between partitions.
  *
  * @param partitions   the number of index partitions per node
  * @param column       the name of the partition key column
  * @param paths        the paths where parttions should write to.
  * @param position     the position of the partition column in the partition key
  * @param keyValidator the type of the partition key
  * @author Andres de la Pena `adelapena@stratio.com`
  */
case class PartitionerOnColumn(
    partitions: Int,
    column: String,
    paths: Array[Path],
    position: Int,
    keyValidator: AbstractType[_]) extends StaticPartitioner {

  if (partitions <= 0) throw new IndexException(
    s"The number of partitions should be strictly positive but found $partitions")

  if (StringUtils.isBlank(column)) throw new IndexException(
    s"A partition column should be specified")

  if (position < 0) throw new IndexException(
    s"The column position in the partition key should be positive")

  if (keyValidator == null) throw new IndexException(
    s"The partition key type should be specified")

  if (paths != null) {
    if (paths.length != partitions) throw new IndexException(
      s"The paths size must be equal to number of partitions")
  }


  /** @inheritdoc*/
  override def partitions(command: ReadCommand): List[Int] = command match {
    case c: SinglePartitionReadCommand => List(partition(c.partitionKey))
    case c: PartitionRangeReadCommand =>
      val range = c.dataRange
      val start = range.startKey
      val stop = range.stopKey
      if (start.kind == ROW_KEY && stop.kind == ROW_KEY && !start.isMinimum && start.equals(stop)) {
        List(partition(start.asInstanceOf[DecoratedKey]))
      } else {
        val expressions = command.rowFilter.getExpressions.asScala
        val expression = expressions.filter(!_.isCustom).find(_.column.name.toString == column)
        expression.map(_.getIndexValue).map(partition).map(List(_)).getOrElse(allPartitions)
      }
    case _ => throw new IndexException(s"Unsupported read command type: ${command.getClass}")
  }

  /** @inheritdoc*/
  override def partition(key: DecoratedKey): Int =
    partition(ByteBufferUtils.split(key.getKey, keyValidator)(position))

  private def partition(bb: ByteBuffer): Int = {
    val hash = new Array[Long](2)
    MurmurHash.hash3_x64_128(bb, bb.position, bb.remaining, 0, hash)
    (Math.abs(hash(0)) % partitions).toInt
  }

  /** @inheritdoc*/
  override def pathForPartition(partition: Int): Path = {
    if ((partition < 0) || (partition >= numPartitions)) {
      throw new IndexOutOfBoundsException(s"partition must be [0,$numPartitions)")
    } else {
      paths(partition)
    }
  }

  /** @inheritdoc*/
  override def numPartitions: Int = partitions

  /** @inheritdoc*/
  override def pathsForEveryPartition: Array[Path] = paths

  /** @inheritdoc*/
  override def equals(that: Any): Boolean =
    that match {
      case that: PartitionerOnColumn => this.partitions.equals(that.partitions) && this.column.equals(
        that.column) && this.paths.sameElements(
        that.paths) && this.position.equals(that.position) && this.keyValidator.equals(that.keyValidator)
      case _ => false
    }
}

/** Companion object for [[PartitionerOnColumn]]. */
object PartitionerOnColumn {

  /** [[PartitionerOnColumn]] builder.
    *
    * @param partitions the number of index partitions per node
    * @param column     the name of the partition key column
    */
  case class Builder(
      @JsonProperty("partitions") partitions: Int,
      @JsonProperty("column") column: String,
      @JsonProperty("paths") paths: Array[String])

    extends Partitioner.Builder {

    /** @inheritdoc*/
    override def build(metadata: CFMetaData): PartitionerOnColumn = {
      val name = UTF8Type.instance.decompose(column)
      metadata.getColumnDefinition(name) match {
        case null =>
          throw new IndexException(s"Partitioner's column '$column' not found in table schema")
        case d if d.isPartitionKey =>
          PartitionerOnColumn(partitions, column, paths.map(Paths.get(_)), d.position, metadata.getKeyValidator)
        case _ =>
          throw new IndexException(s"Partitioner's column '$column' is not part of partition key")
      }
    }

    /** @inheritdoc*/
    override def equals(that: Any): Boolean =
      that match {
        case that: Builder => this.partitions.equals(that.partitions) && this.column.equals(that.column) && this.paths.sameElements(
          that.paths)
        case _ => false
      }
  }

}