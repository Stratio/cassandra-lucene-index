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

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.stratio.cassandra.lucene.common.JsonSerializer
import org.apache.cassandra.db.{DecoratedKey, ReadCommand}

/** Class defining an index partitioning strategy.
  *
  * Index partitioning is useful to speed up some searches to the detriment of others, depending on
  * the implementation.
  *
  * It is also useful to overcome the Lucene's hard limit of 2147483519 documents per index.
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type",
  defaultImpl = classOf[PartitionerOnNone])
@JsonSubTypes(Array(
  new JsonSubTypes.Type(value = classOf[PartitionerOnNone], name = "none"),
  new JsonSubTypes.Type(value = classOf[PartitionerOnToken], name = "token")))
trait Partitioner {

  /** Returns the number of partitions. */
  def numPartitions: Int

  /** Returns the partition for the specified key.
    *
    * @param key a partition key to be routed to a partition
    * @return the partition owning `key`
    */
  def partition(key: DecoratedKey): Int

  /** Returns the optional partition for the specified read command, or [[None]] if all partitions
    * should be fetched.
    *
    * @param command a read command to be routed to either one or all partitions
    * @return the partition containing the all data required to satisfy `command`,
    *         or [[None]] if all partitions should be fetched
    */
  def partition(command: ReadCommand): Option[Int]

}

/** Companion object for [[Partitioner]]. */
object Partitioner {

  /** The [[Partitioner]] represented by the specified JSON string.
    *
    * @param json a JSON string representing a [[Partitioner]]
    * @return the partitioner represented by `json`
    */
  def fromJson(json: String): Partitioner =
    JsonSerializer.fromString(json, classOf[Partitioner])

}
