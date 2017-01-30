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
import com.stratio.cassandra.lucene.mapping.PartitionMapper
import org.apache.cassandra.db.PartitionPosition.Kind._
import org.apache.cassandra.db._
import org.apache.cassandra.db.filter.ClusteringIndexFilter
import org.apache.cassandra.index.transactions.IndexTransaction
import org.apache.cassandra.schema.IndexMetadata
import org.apache.cassandra.utils.concurrent.OpOrder
import org.apache.lucene.index.{IndexableField, Term}
import org.apache.lucene.search.{Query, SortField, TermQuery}

/** [[IndexService]] for skinny rows.
  *
  * @param table the indexed table
  * @param index the index metadata
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class IndexServiceSkinny(table: ColumnFamilyStore, index: IndexMetadata)
  extends IndexService(table, index) {

  init()

  /** @inheritdoc */
  override def fieldsToLoad: java.util.Set[String] = {
    Sets.newHashSet(PartitionMapper.FIELD_NAME)
  }

  /** @inheritdoc */
  override def keySortFields: List[SortField] = {
    List(tokenMapper.sortField, partitionMapper.sortField)
  }

  /** @inheritdoc */
  override def writer(
      key: DecoratedKey,
      nowInSec: Int,
      opGroup: OpOrder.Group,
      transactionType: IndexTransaction.Type): IndexWriter = {
    new IndexWriterSkinny(this, key, nowInSec, opGroup, transactionType)
  }

  /** @inheritdoc */
  override def keyIndexableFields(key: DecoratedKey, clustering: Clustering)
  : List[IndexableField] = {
    List(tokenMapper.indexableField(key), partitionMapper.indexableField(key))
  }

  /** @inheritdoc */
  override def term(key: DecoratedKey, clustering: Clustering): Term = {
    partitionMapper.term(key)
  }

  /** @inheritdoc */
  override def query(key: DecoratedKey, filter: ClusteringIndexFilter): Query = {
    new TermQuery(term(key))
  }

  /** @inheritdoc */
  override def query(dataRange: DataRange): Option[Query] = {
    val start = dataRange.startKey
    val stop = dataRange.stopKey
    if ((start.kind eq ROW_KEY) && (stop.kind eq ROW_KEY) && start == stop) {
      Some(partitionMapper.query(start.asInstanceOf[DecoratedKey]))
    } else {
      tokenMapper.query(
        start.getToken,
        stop.getToken,
        start.kind == MIN_BOUND,
        stop.kind == MAX_BOUND)
    }
  }

  /** @inheritdoc */
  override def after(key: DecoratedKey, clustering: Clustering): Term = {
    partitionMapper.term(key)
  }

  /** @inheritdoc */
  override def reader(
      documents: DocumentIterator,
      command: ReadCommand,
      orderGroup: ReadOrderGroup): IndexReader = {
    new IndexReaderSkinny(this, command, table, orderGroup, documents)
  }
}
