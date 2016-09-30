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

import com.stratio.cassandra.lucene.column.{Columns, ColumnsMapper}
import com.stratio.cassandra.lucene.index.DocumentIterator
import com.stratio.cassandra.lucene.key.{ClusteringMapper, KeyMapper, PartitionMapper}
import org.apache.cassandra.db.PartitionPosition.Kind._
import org.apache.cassandra.db._
import org.apache.cassandra.db.filter.{ClusteringIndexFilter, ClusteringIndexNamesFilter, ClusteringIndexSliceFilter}
import org.apache.cassandra.db.rows.Row
import org.apache.cassandra.index.transactions.IndexTransaction
import org.apache.cassandra.schema.IndexMetadata
import org.apache.cassandra.utils.concurrent.OpOrder
import org.apache.lucene.document.Document
import org.apache.lucene.index.{IndexableField, Term}
import org.apache.lucene.search.BooleanClause.Occur._
import org.apache.lucene.search.{BooleanQuery, Query, SortField}

import scala.collection.JavaConversions._

/** [[IndexService]] for wide rows.
  *
  * @param cfs the indexed table
  * @param im  the index metadata
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class IndexServiceWide(val cfs: ColumnFamilyStore, val im: IndexMetadata)
  extends IndexService(cfs, im) {

  val clusteringMapper = new ClusteringMapper(metadata)
  val keyMapper = new KeyMapper(metadata)

  init()

  /** @inheritdoc */
  override def fieldsToLoad: Set[String] = {
    Set[String](PartitionMapper.FIELD_NAME, ClusteringMapper.FIELD_NAME)
  }

  /** @inheritdoc */
  override def keySortFields: List[SortField] = {
    List(tokenMapper.sortField, partitionMapper.sortField, clusteringMapper.sortField)
  }

  /**
    * Returns the clustering key contained in the specified document.
    *
    * @param document a { @link Document} containing the clustering key to be get
    * @return the clustering key contained in { @code document}
    */
  def clustering(document: Document): Clustering = {
    clusteringMapper.clustering(document)
  }

  /** @inheritdoc */
  override def indexWriter(key: DecoratedKey,
                  nowInSec: Int,
                  opGroup: OpOrder.Group,
                  transactionType: IndexTransaction.Type): IndexWriterWide = {
    new IndexWriterWide(this, key, nowInSec, opGroup, transactionType)
  }

  /** @inheritdoc */
  override def columns(key: DecoratedKey, row: Row): Columns = {
    Columns() + partitionMapper.columns(key) + clusteringMapper.columns(row.clustering) + ColumnsMapper.columns(row)
  }

  /** @inheritdoc */
  override def keyIndexableFields(key: DecoratedKey, row: Row): List[IndexableField] = {
    val clustering = row.clustering
    val fields = scala.collection.mutable.ListBuffer[IndexableField]()
    fields.add(tokenMapper.indexableField(key))
    fields.add(partitionMapper.indexableField(key))
    fields.add(keyMapper.indexableField(key, clustering))
    fields.addAll(clusteringMapper.indexableFields(key, clustering))
    fields.toList
  }

  /** @inheritdoc */
  override def term(key: DecoratedKey, row: Row): Term = term(key, row.clustering)

  /**
    * Returns a Lucene term identifying the document representing the row identified by the
    * specified partition and clustering keys.
    *
    * @param key        the partition key
    * @param clustering the clustering key
    * @return the term identifying the document
    */
  def term(key: DecoratedKey, clustering: Clustering): Term = {
    keyMapper.term(key, clustering)
  }

  /** @inheritdoc */
  override def query(key: DecoratedKey, filter: ClusteringIndexFilter): Query = filter match {
    case f if f.selectsAllPartition() => partitionMapper.query(key)
    case f: ClusteringIndexNamesFilter => keyMapper.query(key, f)
    case f: ClusteringIndexSliceFilter => clusteringMapper.query(key, f)
    case _ => throw new IndexException("Unknown filter type {}", filter)
  }

  def query(position: PartitionPosition): Query = position match {
    case key: DecoratedKey => partitionMapper.query(key)
    case _ => tokenMapper.query(position.getToken)
  }

  def query(position: PartitionPosition, start: ClusteringPrefix, stop: ClusteringPrefix): Query = {
    val builder = new BooleanQuery.Builder
    builder.add(query(position), FILTER)
    builder.add(clusteringMapper.query(position, start, stop), FILTER)
    builder.build
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
    val startClustering = ClusteringMapper.startClusteringPrefix(dataRange).orNull(null)
    val stopClustering = ClusteringMapper.stopClusteringPrefix(dataRange).orNull(null)
    val includeStartClustering = startClustering != null && startClustering.size > 0
    val includeStopClustering = stopClustering != null && stopClustering.size > 0

    // Try single partition
    if (startToken.compareTo(stopToken) == 0) {
      if (!includeStartClustering && !includeStopClustering) return Some(query(startPosition))
      return Some(query(startPosition, startClustering, stopClustering))
    }
    // Prepare query builder
    val builder = new BooleanQuery.Builder

    // Add token range filter
    val includeStartToken = (startPosition.kind eq MIN_BOUND) && !includeStartClustering
    val includeStopToken = (stopPosition.kind eq MAX_BOUND) && !includeStopClustering
    tokenMapper.query(startToken, stopToken, includeStartToken, includeStopToken).foreach(builder.add(_, SHOULD))

    // Add first and last partition filters
    if (includeStartClustering) builder.add(query(startPosition, startClustering, null), SHOULD)
    if (includeStopClustering) builder.add(query(stopPosition, null, stopClustering), SHOULD)

    // Return query, or empty if there are no restrictions
    val booleanQuery = builder.build
    if (booleanQuery.clauses.nonEmpty) Some(booleanQuery) else None
  }

  /** @inheritdoc */
  override def after(key: DecoratedKey, clustering: Clustering): Option[Query] = {
    if (key == null) None
    else if (clustering == null) Some(partitionMapper.query(key))
    else Some(keyMapper.query(key, clustering))
  }

  /** @inheritdoc */
  override def indexReader(docs: DocumentIterator, command: ReadCommand, group: ReadOrderGroup): IndexReaderWide = {
    new IndexReaderWide(this, command, table, group, docs)
  }

}
