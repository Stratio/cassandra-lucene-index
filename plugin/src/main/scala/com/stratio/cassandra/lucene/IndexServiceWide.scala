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

import com.google.common.collect.Sets
import com.stratio.cassandra.lucene.index.DocumentIterator
import com.stratio.cassandra.lucene.mapping.ClusteringMapper._
import com.stratio.cassandra.lucene.mapping.{ClusteringMapper, KeyMapper, PartitionMapper}
import org.apache.cassandra.db.PartitionPosition.Kind._
import org.apache.cassandra.db._
import org.apache.cassandra.db.filter._
import org.apache.cassandra.index.transactions.IndexTransaction
import org.apache.cassandra.schema.IndexMetadata
import org.apache.cassandra.utils.concurrent.OpOrder
import org.apache.lucene.document.Document
import org.apache.lucene.index.{IndexableField, Term}
import org.apache.lucene.search.BooleanClause.Occur._
import org.apache.lucene.search.{BooleanQuery, Query, SortField}

import scala.collection.mutable

/** [[IndexService]] for wide rows.
  *
  * @param table the indexed table
  * @param index the index metadata
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class IndexServiceWide(table: ColumnFamilyStore, index: IndexMetadata)
  extends IndexService(table, index) {

  val clusteringMapper = new ClusteringMapper(metadata)
  val keyMapper = new KeyMapper(metadata)

  init()

  /** @inheritdoc */
  override def fieldsToLoad: java.util.Set[String] = {
    Sets.newHashSet(PartitionMapper.FIELD_NAME, ClusteringMapper.FIELD_NAME)
  }

  /** @inheritdoc */
  override def keySortFields: List[SortField] = {
    List(tokenMapper.sortField, partitionMapper.sortField, clusteringMapper.sortField)
  }

  /** Returns the clustering key contained in the specified document.
    *
    * @param document a document containing the clustering key to be get
    * @return the clustering key contained in `document`
    */
  def clustering(document: Document): Clustering = {
    clusteringMapper.clustering(document)
  }

  /** @inheritdoc */
  override def writer(
      key: DecoratedKey,
      nowInSec: Int,
      opGroup: OpOrder.Group,
      transactionType: IndexTransaction.Type): IndexWriter = {
    new IndexWriterWide(this, key, nowInSec, opGroup, transactionType)
  }

  /** @inheritdoc */
  override def keyIndexableFields(key: DecoratedKey, clustering: Clustering)
  : List[IndexableField] = {
    val fields = mutable.ListBuffer.empty[IndexableField]
    fields += tokenMapper.indexableField(key)
    fields += partitionMapper.indexableField(key)
    fields += keyMapper.indexableField(key, clustering)
    fields ++= clusteringMapper.indexableFields(key, clustering)
    fields.toList
  }

  /** @inheritdoc */
  def term(key: DecoratedKey, clustering: Clustering): Term = {
    keyMapper.term(key, clustering)
  }

  /** @inheritdoc */
  override def query(key: DecoratedKey, filter: ClusteringIndexFilter): Query = filter match {
    case f if f.selectsAllPartition => partitionMapper.query(key)
    case f: ClusteringIndexNamesFilter => keyMapper.query(key, f)
    case f: ClusteringIndexSliceFilter => clusteringMapper.query(key, f)
    case _ => throw new IndexException(s"Unknown filter type $filter")
  }

  def query(position: PartitionPosition): Query = position match {
    case key: DecoratedKey => partitionMapper.query(key)
    case _ => tokenMapper.query(position.getToken)
  }

  def query(
      position: PartitionPosition,
      start: Option[ClusteringPrefix],
      stop: Option[ClusteringPrefix]): Query = {
    if (start.isEmpty && stop.isEmpty) return query(position)
    new BooleanQuery.Builder()
      .add(query(position), FILTER)
      .add(clusteringMapper.query(position, start, stop), FILTER)
      .build
  }

  /** @inheritdoc */
  override def query(dataRange: DataRange): Option[Query] = {

    // Check trivia
    if (dataRange.isUnrestricted) return None

    // Extract data range data
    val startPosition = dataRange.startKey
    val stopPosition = dataRange.stopKey
    val startToken = startPosition.getToken
    val stopToken = stopPosition.getToken
    val startClustering = startClusteringPrefix(dataRange).filter(_.size > 0)
    val stopClustering = stopClusteringPrefix(dataRange).filter(_.size > 0)

    // Try single partition
    if (startToken.compareTo(stopToken) == 0) {
      if (startClustering.isEmpty && stopClustering.isEmpty) return Some(query(startPosition))
      return Some(query(startPosition, startClustering, stopClustering))
    }

    // Prepare query builder
    val builder = new BooleanQuery.Builder

    // Add token range filter
    val includeStartToken = startPosition.kind == MIN_BOUND && startClustering.isEmpty
    val includeStopToken = stopPosition.kind == MAX_BOUND && stopClustering.isEmpty
    tokenMapper
      .query(startToken, stopToken, includeStartToken, includeStopToken)
      .foreach(builder.add(_, SHOULD))

    // Add first and last partition filters
    if (startClustering.isDefined) builder.add(query(startPosition, startClustering, None), SHOULD)
    if (stopClustering.isDefined) builder.add(query(stopPosition, None, stopClustering), SHOULD)

    // Return query, or empty if there are no restrictions
    val booleanQuery = builder.build
    if (booleanQuery.clauses.isEmpty) None else Some(booleanQuery)
  }

  /** @inheritdoc */
  override def after(key: DecoratedKey, clustering: Clustering): Term = {
    keyMapper.term(key, clustering)
  }

  /** @inheritdoc */
  override def reader(
      documents: DocumentIterator,
      command: ReadCommand,
      orderGroup: ReadOrderGroup): IndexReader = {
    new IndexReaderWide(this, command, table, orderGroup, documents)
  }

}
