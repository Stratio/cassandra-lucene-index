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
package com.stratio.cassandra.lucene.key

import java.nio.ByteBuffer

import com.google.common.primitives.Longs
import com.stratio.cassandra.lucene.column.{Column, Columns, ColumnsMapper}
import com.stratio.cassandra.lucene.key.TokenMapper
import com.stratio.cassandra.lucene.key.ClusteringMapper._
import com.stratio.cassandra.lucene.util.ByteBufferUtils
import com.stratio.cassandra.lucene.util.ByteBufferUtils._
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db._
import org.apache.cassandra.db.filter.{ClusteringIndexNamesFilter, ClusteringIndexSliceFilter}
import org.apache.cassandra.db.marshal.CompositeType
import org.apache.cassandra.dht.Token
import org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER
import org.apache.lucene.document.{Document, Field, FieldType, StoredField}
import org.apache.lucene.index.{DocValuesType, IndexOptions, IndexableField}
import org.apache.lucene.search.BooleanClause.Occur.SHOULD
import org.apache.lucene.search.{BooleanQuery, Query, SortField}
import org.apache.lucene.util.BytesRef

import scala.collection.JavaConversions._

/** Class for several clustering key mappings between Cassandra and Lucene.
  *
  * @param metadata the indexed table metadata
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class ClusteringMapper(metadata: CFMetaData) {

  /** The clustering key comparator */
  var comparator: ClusteringComparator = metadata.comparator

  /** A composite type composed by the types of the clustering key */
  var clusteringType: CompositeType = CompositeType.getInstance(comparator.subtypes)

  /** Returns the columns contained in the specified [[Clustering]].
    *
    * @param clustering the clustering key
    * @return the columns
    */
  def columns(clustering: Clustering): Columns =
  metadata.clusteringColumns.foldLeft(new Columns)((columns, columnDefinition) => {
    val name = columnDefinition.name.toString
    val position = columnDefinition.position
    val value = clustering.get(position)
    val valueType = columnDefinition.cellValueType
    columns.add(Column.apply(name).withValue(ColumnsMapper.compose(value, valueType)))
  })

  /** Returns a list of Lucene [[IndexableField]]s representing the specified primary key.
    *
    * @param key        the partition key
    * @param clustering the clustering key
    * @return a indexable field
    */
  def indexableFields(key: DecoratedKey, clustering: Clustering): java.util.List[IndexableField] = {
    // TODO: return Seq

    // Build stored field for clustering key retrieval
    val plainClustering: BytesRef = bytesRef(byteBuffer(clustering))
    val storedField: Field = new StoredField(fieldName, plainClustering)

    // Build indexed field prefixed by token value collation
    val bb2 = ByteBuffer.allocate(prefixBytes + plainClustering.length)
    bb2.put(prefix(key.getToken)).put(plainClustering.bytes).flip
    val indexedField = new Field(fieldName, bytesRef(bb2), fieldType)

    List(indexedField, storedField)
  }

  def byteBuffer(clustering: Clustering): ByteBuffer =
    clustering.getRawValues.foldLeft(clusteringType.builder)((b, bb) => b.add(bb)).build()

  /** Returns the @code String human-readable representation of the specified [[ClusteringPrefix]].
    *
    * @param prefix the clustering prefix
    * @return a @code String representing @code prefix
    */
  def toString(prefix: ClusteringPrefix): String =
  if (prefix == null) null else prefix.toString(metadata)

  /** Returns the clustering key represented by the specified [[ByteBuffer]].
    *
    * @param clustering a byte buffer containing a [[Clustering]]
    * @return a Lucene field binary value
    */
  def clustering(clustering: ByteBuffer): Clustering =
  new Clustering(clusteringType.split(clustering): _*)

  /** Returns the clustering key contained in the specified [[Document]].
    *
    * @param document a { @link Document} containing the clustering key to be get
    * @return the clustering key contained in { @code document}
    */
  def clustering(document: Document): Clustering = {
    val bytesRef = document.getBinaryValue(fieldName)
    clustering(ByteBufferUtils.byteBuffer(bytesRef))
  }

  /** Returns a Lucene [[SortField]] to sort documents by primary key according to Cassandra's natural order.
    *
    * @return the sort field
    */
  def sortField: SortField = new ClusteringSort(this)

  /** Returns a Lucene [[Query]] to retrieve all the rows in the specified partition slice.
    *
    * @param position the partition position
    * @param start    the start clustering prefix
    * @param stop     the stop clustering prefix
    * @return the Lucene query
    */
  def query(position: PartitionPosition, start: ClusteringPrefix, stop: ClusteringPrefix): Query =
  new ClusteringQuery(this, position, start, stop)

  /** Returns a Lucene [[Query]] to retrieve all the rows in the specified clustering slice.
    *
    * @param key   the partition key
    * @param slice the slice
    * @return the Lucene query
    */
  def query(key: DecoratedKey, slice: Slice): Query = query(key, slice.start, slice.end)

  /** Returns a Lucene [[Query]] to retrieve all the rows in the specified clustering slice filter.
    *
    * @param key    the partition key
    * @param filter the slice filter
    * @return the Lucene query
    */
  def query(key: DecoratedKey, filter: ClusteringIndexSliceFilter): Query = {
    filter.requestedSlices.foldLeft(new BooleanQuery.Builder)((b, s) => b.add(query(key, s), SHOULD)).build
  }

}

object ClusteringMapper {

  /** The Lucene field name. */
  val fieldName: String = "_clustering"

  /** The Lucene field type. */
  val fieldType: FieldType = new FieldType
  fieldType.setOmitNorms(true)
  fieldType.setIndexOptions(IndexOptions.DOCS)
  fieldType.setTokenized(false)
  fieldType.setStored(false)
  fieldType.setDocValuesType(DocValuesType.SORTED)
  fieldType.freeze()

  /** The number of bytes produced by token collation. */
  var prefixBytes: Int = 8

  /** Returns a lexicographically sortable representation of the specified token.
    *
    * @param token a token
    * @return a lexicographically sortable 8 bytes array
    */
  @SuppressWarnings(Array("NumericOverflow"))
  def prefix(token: Token): Array[Byte] = {
    val value: Long = TokenMapper.value(token)
    val collated: Long = Long.MinValue * -1 + value
    Longs.toByteArray(collated)
  }

  /** Returns the start [[ClusteringPrefix]] of the first partition of the specified [[DataRange]].
    *
    * @param range the data range
    * @return the optional start clustering prefix of @code dataRange, empty if there is no such start
    */
  def startClusteringPrefix(range: DataRange): Option[ClusteringPrefix] = {
    val filter = range.startKey match {
      case key: DecoratedKey => range.clusteringIndexFilter(key)
      case p => range.clusteringIndexFilter(new BufferDecoratedKey(p.getToken, EMPTY_BYTE_BUFFER))
    }
    filter match {
      case f: ClusteringIndexSliceFilter => Some(f.requestedSlices.get(0).start)
      case f: ClusteringIndexNamesFilter => Some(f.requestedRows.first)
      case _ => None
    }
  }

  /** Returns the stop [[ClusteringPrefix]] of the last partition of the specified [[DataRange]].
    *
    * @param range the data range
    * @return the optional stop clustering prefix of @code dataRange, empty if there is no such start
    */
  def stopClusteringPrefix(range: DataRange): Option[ClusteringPrefix] = {
    val filter = range.stopKey match {
      case key: DecoratedKey => range.clusteringIndexFilter(key)
      case p => range.clusteringIndexFilter(new BufferDecoratedKey(p.getToken, EMPTY_BYTE_BUFFER))
    }
    filter match {
      case f: ClusteringIndexSliceFilter => Some(f.requestedSlices.get(f.requestedSlices.size - 1).end)
      case f: ClusteringIndexNamesFilter => Some(f.requestedRows.last)
      case _ => None
    }
  }
}
