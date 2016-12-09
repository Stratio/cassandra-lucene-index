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

import com.fasterxml.jackson.annotation.JsonProperty
import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.util.ByteBufferUtils
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db.PartitionPosition.Kind.ROW_KEY
import org.apache.cassandra.db._
import org.apache.cassandra.db.marshal.{AbstractType, UTF8Type}
import org.apache.cassandra.utils.MurmurHash

import scala.collection.JavaConverters._

/** [[Partitioner]] partitioner based on the partition key token.
  *
  * Partitioning on token guarantees a good load balancing between partitions while speeding up
  * partition-directed searches to the detriment of token range searches performance. It allows to
  * efficiently run partition directed queries in nodes indexing more than 2147483519 rows. However,
  * token range searches in nodes with more than 2147483519 rows will fail.
  *
  * @param partitions the number of partitions
  * @param column     the name of the column
  * @author Andres de la Pena `adelapena@stratio.com`
  */
case class PartitionerOnColumn(
    partitions: Int,
    column: String,
    position: Int,
    keyValidator: AbstractType[_]) extends Partitioner {

  if (partitions <= 0) throw new IndexException(
    s"The number of partitions should be strictly positive but found $partitions")

  protected def partition(bb: ByteBuffer): Int = {
    val hash = new Array[Long](2)
    MurmurHash.hash3_x64_128(bb, bb.position, bb.remaining, 0, hash)
    (Math.abs(hash(0)) % partitions).toInt
  }

  /** @inheritdoc*/
  override def numPartitions: Int = partitions

  /** @inheritdoc */
  override def partition(key: DecoratedKey): Int =
    partition(ByteBufferUtils.split(key.getKey, keyValidator)(position))

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

}

/** Companion object for [[PartitionerOnColumn]]. */
object PartitionerOnColumn {

  /** [[PartitionerOnColumn]] builder.
    *
    * @param partitions the number of partitions
    * @param column     the name of the column
    */
  class Builder(
      @JsonProperty("partitions") partitions: Int,
      @JsonProperty("column") column: String)
    extends Partitioner.Builder {
    override def build(metadata: CFMetaData): PartitionerOnColumn = {
      val name = UTF8Type.instance.decompose(column)
      metadata.getColumnDefinition(name) match {
        case null =>
          throw new IndexException(s"Partitioner's column '$column' not found in table schema")
        case d if d.isPartitionKey =>
          PartitionerOnColumn(partitions, column, d.position, metadata.getKeyValidator)
        case _ =>
          throw new IndexException(s"Partitioner's column '$column' is not part of partition key")
      }
    }
  }

}