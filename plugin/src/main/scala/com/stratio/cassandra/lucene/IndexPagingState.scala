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
package com.stratio.cassandra.lucene

import java.nio.ByteBuffer
import java.{util => java}

import com.google.common.base.MoreObjects
import com.stratio.cassandra.lucene.IndexPagingState._
import com.stratio.cassandra.lucene.search.SearchBuilder
import com.stratio.cassandra.lucene.util.{ByteBufferUtils, SimplePartitionIterator, SimpleRowIterator}
import org.apache.cassandra.config.DatabaseDescriptor
import org.apache.cassandra.db._
import org.apache.cassandra.db.filter.RowFilter
import org.apache.cassandra.db.marshal.UTF8Type
import org.apache.cassandra.db.partitions.PartitionIterator
import org.apache.cassandra.service.LuceneStorageProxy
import org.apache.cassandra.service.pager.PagingState

import scala.collection.JavaConversions._

/** The paging state of a CQL query using Lucene. It tracks the primary keys of the last seen rows
  * for each internal read command of a CQL query. It also keeps the count of the remaining rows.
  * This state can be serialized to be attached to a [[PagingState]] and/or to a search predicate.
  *
  * @param remaining the number of remaining rows to be retrieved
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class IndexPagingState(var remaining: Int) {

  /** If there could be more results. */
  private var hasMorePages: Boolean = true

  /** The last row positions */
  private val entries = new java.HashMap[DecoratedKey, Clustering]

  /** Returns the primary key of the last seen row for the specified read command.
    *
    * @param command a read command
    * @return the primary key of the last seen row for `command`
    */
  def forCommand(command: ReadCommand): Option[(DecoratedKey, Clustering)] = command match {
    case c: SinglePartitionReadCommand =>
      entries.find { case (key, clustering) => c.partitionKey == key }
    case c: PartitionRangeReadCommand =>
      entries.find { case (key, clustering) => c.dataRange contains key }
    case _ => throw new IndexException(s"Unsupported read command type: ${command.getClass}")
  }

  @throws[ReflectiveOperationException]
  private def indexExpression(command: ReadCommand): RowFilter.Expression = {

    // Try with custom expressions
    command.rowFilter.getExpressions.find(_.isCustom).foreach(return _)

    // Try with dummy column
    val cfs = Keyspace.open(command.metadata.ksName).getColumnFamilyStore(command.metadata.cfName)
    for (expr <- command.rowFilter.getExpressions) {
      for (index <- cfs.indexManager.listIndexes) {
        if (index.isInstanceOf[Index] && index.supportsExpression(
          expr.column,
          expr.operator)) return expr
      }
    }
    throw new IndexException("Not found expression")
  }

  /** Adds this paging state to the specified read query.
    *
    * @param query a CQL query using the Lucene index
    * @throws ReflectiveOperationException if there is any problem with reflection
    */
  @throws[ReflectiveOperationException]
  def rewrite(query: ReadQuery): Unit = query match {
    case group: SinglePartitionReadCommand.Group =>
      group.commands.foreach(rewrite)
    case read: ReadCommand =>
      val expression = indexExpression(read)
      val oldValue = expressionValueField.get(expression).asInstanceOf[ByteBuffer]
      val search = SearchBuilder.fromJson(UTF8Type.instance.compose(oldValue)).paging(this)
      val newValue = UTF8Type.instance.decompose(search.toJson)
      expressionValueField.set(expression, newValue)
    case _ =>
      throw new IndexException(s"Unsupported query type ${query.getClass}")
  }

  /** Updates this paging state with the results of the specified query.
    *
    * @param query       the query
    * @param partitions  the results
    * @param consistency the query consistency level
    * @return a copy of the query results
    */
  def update(
      query: ReadQuery,
      partitions: PartitionIterator,
      consistency: ConsistencyLevel): PartitionIterator = query match {
    case c: SinglePartitionReadCommand.Group => update(c, partitions)
    case c: PartitionRangeReadCommand => update(c, partitions, consistency)
    case _ => throw new IndexException(s"Unsupported query type ${query.getClass}")
  }

  private def update(
      group: SinglePartitionReadCommand.Group,
      partitions: PartitionIterator): PartitionIterator = {
    val rowIterators = new java.LinkedList[SimpleRowIterator]
    var count = 0
    for (partition <- partitions) {
      val key = partition.partitionKey
      while (partition.hasNext) {
        val newRowIterator = new SimpleRowIterator(partition)
        rowIterators.add(newRowIterator)
        entries.put(key, newRowIterator.row.clustering)
        if (remaining > 0) remaining -= 1
        count += 1
      }
      partition.close()
    }
    partitions.close()
    hasMorePages = remaining > 0 && count >= group.limits.count
    new SimplePartitionIterator(rowIterators)
  }

  private def update(
      command: PartitionRangeReadCommand,
      partitions: PartitionIterator,
      consistency: ConsistencyLevel): PartitionIterator = {

    // Collect query bounds
    val rangeMerger = LuceneStorageProxy.rangeMerger(command, consistency)
    val bounds = rangeMerger.map(_.range).toList

    val rowIterators = new java.LinkedList[SimpleRowIterator]

    var count = 0
    for (partition <- partitions) {

      val key = partition.partitionKey
      val bound = bounds.find(_ contains key)
      while (partition.hasNext) {
        bound.foreach(bound => entries.keys.filter(bound.contains).foreach(entries.remove))
        val newRowIterator = new SimpleRowIterator(partition)
        rowIterators.add(newRowIterator)
        val clustering = newRowIterator.row.clustering
        entries.put(key, clustering)
        if (remaining > 0) remaining -= 1
        count += 1
      }
      partition.close()
    }
    partitions.close()

    hasMorePages = remaining > 0 && count >= command.limits.count
    new SimplePartitionIterator(rowIterators)
  }

  /** Returns a CQL [[PagingState]] containing this Lucene paging state.
    *
    * @return a CQL paging state
    */
  def toPagingState: PagingState = {
    if (hasMorePages) new PagingState(toByteBuffer, null, remaining, remaining) else null
  }

  /** @inheritdoc */
  override def toString: String = {
    MoreObjects.toStringHelper(this).add("remaining", remaining).add("entries", entries).toString
  }

  /** Returns a byte buffer representation of this.
    * The returned result can be read with [[fromByteBuffer(ByteBuffer)]].
    *
    * @return a byte buffer representing this
    */
  def toByteBuffer: ByteBuffer = {
    val entryValues = entries.map { case (key, clustering) =>
      val clusteringValues = clustering.getRawValues
      val values = new Array[ByteBuffer](1 + clusteringValues.length)
      values(0) = key.getKey
      System.arraycopy(clusteringValues, 0, values, 1, clusteringValues.length)
      ByteBufferUtils.compose(values: _*)
    }
    val values = ByteBufferUtils.compose(entryValues.toArray: _*)
    val out = ByteBuffer.allocate(4 + values.remaining)
    out.putInt(remaining).put(values).flip
    out
  }

}

object IndexPagingState {

  private lazy val expressionValueField = classOf[RowFilter.Expression].getDeclaredField("value")
  expressionValueField.setAccessible(true)

  /** Returns the paging state represented by the specified byte buffer, which should have been
    * generated with [[IndexPagingState.toByteBuffer()]].
    *
    * @param bb a byte buffer generated by [[IndexPagingState.toByteBuffer()]]
    * @return the paging state represented by `bb`
    */
  def fromByteBuffer(bb: ByteBuffer): IndexPagingState = {
    val remaining = bb.getInt
    val state = new IndexPagingState(remaining)
    ByteBufferUtils.decompose(bb).map(
      bbe => {
        val values = ByteBufferUtils.decompose(bbe)
        val key = DatabaseDescriptor.getPartitioner.decorateKey(values(0))
        val clustering = new Clustering(values.slice(1, values.length): _*)
        state.entries.put(key, clustering)
      })
    state
  }

  /** Returns the Lucene paging state contained in the specified CQL [[PagingState]].
    * If the specified paging state is null, then an empty Lucene paging state will be returned.
    *
    * @param state a CQL paging state
    * @param limit the query user limit
    * @return a Lucene paging state
    */
  def build(state: PagingState, limit: Int): IndexPagingState = {
    if (state == null) new IndexPagingState(limit) else fromByteBuffer(state.partitionKey)
  }
}
