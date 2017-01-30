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
package com.stratio.cassandra.lucene.mapping

import java.nio.ByteBuffer

import com.google.common.base.MoreObjects
import com.google.common.primitives.Longs
import com.stratio.cassandra.lucene.mapping.ClusteringMapper._
import com.stratio.cassandra.lucene.util.ByteBufferUtils
import com.stratio.cassandra.lucene.util.ByteBufferUtils._
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db._
import org.apache.cassandra.db.filter.{ClusteringIndexNamesFilter, ClusteringIndexSliceFilter}
import org.apache.cassandra.db.marshal.CompositeType
import org.apache.cassandra.dht.Token
import org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER
import org.apache.cassandra.utils.FastByteOperations._
import org.apache.lucene.document.{Document, Field, FieldType, StoredField}
import org.apache.lucene.index.FilteredTermsEnum.AcceptStatus
import org.apache.lucene.index._
import org.apache.lucene.search.BooleanClause.Occur.SHOULD
import org.apache.lucene.search.FieldComparator.TermValComparator
import org.apache.lucene.search._
import org.apache.lucene.util.{AttributeSource, BytesRef}

import scala.collection.JavaConverters._

/** Class for several clustering key mappings between Cassandra and Lucene.
  *
  * @param metadata the indexed table metadata
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class ClusteringMapper(metadata: CFMetaData) {

  /** The clustering key comparator */
  val comparator = metadata.comparator

  /** A composite type composed by the types of the clustering key */
  val clusteringType = CompositeType.getInstance(comparator.subtypes)

  val clusteringColumns = metadata.clusteringColumns.asScala

  /** Returns a list of Lucene [[IndexableField]]s representing the specified primary key.
    *
    * @param key        the partition key
    * @param clustering the clustering key
    * @return a indexable field
    */
  def indexableFields(key: DecoratedKey, clustering: Clustering): List[IndexableField] = {

    // Build stored field for clustering key retrieval
    val plainClustering = bytesRef(byteBuffer(clustering))
    val storedField = new StoredField(FIELD_NAME, plainClustering)

    // Build indexed field prefixed by token value collation
    val bb = ByteBuffer.allocate(PREFIX_SIZE + plainClustering.length)
    bb.put(prefix(key.getToken)).put(plainClustering.bytes).flip
    val indexedField = new Field(FIELD_NAME, bytesRef(bb), FIELD_TYPE)

    List(indexedField, storedField)
  }

  /** Returns the [[ByteBuffer]] representation of the specified [[Clustering]].
    *
    * @param clustering a clustering key
    * @return a byte buffer representing `clustering`
    */
  def byteBuffer(clustering: Clustering): ByteBuffer = {
    (clusteringType.builder /: clustering.getRawValues) (_ add _) build()
  }

  /** Returns the [[String]] human-readable representation of the specified [[ClusteringPrefix]].
    *
    * @param prefix the clustering prefix
    * @return a [[String]] representing the prefix
    */
  def toString(prefix: Option[ClusteringPrefix]): String = {
    prefix.map(_.toString(metadata)).orNull
  }

  /** Returns the clustering key represented by the specified [[ByteBuffer]].
    *
    * @param clustering a byte buffer containing a [[Clustering]]
    * @return a Lucene field binary value
    */
  def clustering(clustering: ByteBuffer): Clustering = {
    new Clustering(clusteringType.split(clustering): _*)
  }

  /** Returns the clustering key contained in the specified [[Document]].
    *
    * @param document a document containing the clustering key to be get
    * @return the clustering key contained in the document
    */
  def clustering(document: Document): Clustering = {
    val bytesRef = document.getBinaryValue(FIELD_NAME)
    clustering(ByteBufferUtils.byteBuffer(bytesRef))
  }

  /** Returns a Lucene [[SortField]] to sort documents by primary key.
    *
    * @return the sort field
    */
  def sortField: SortField = {
    new ClusteringSort(this)
  }

  /** Returns a Lucene [[Query]] to retrieve all the rows in the specified partition slice.
    *
    * @param position the partition position
    * @param start    the start clustering prefix
    * @param stop     the stop clustering prefix
    * @return the Lucene query
    */
  def query(
      position: PartitionPosition,
      start: Option[ClusteringPrefix],
      stop: Option[ClusteringPrefix]): Query = {
    new ClusteringQuery(this, position, start, stop)
  }

  /** Returns a Lucene [[Query]] to retrieve all the rows in the specified clustering slice.
    *
    * @param key   the partition key
    * @param slice the slice
    * @return the Lucene query
    */
  def query(key: DecoratedKey, slice: Slice): Query = {
    query(key, Option(slice.start), Option(slice.end))
  }

  /** Returns a Lucene [[Query]] to retrieve all the rows in the specified clustering slice filter.
    *
    * @param key    the partition key
    * @param filter the slice filter
    * @return the Lucene query
    */
  def query(key: DecoratedKey, filter: ClusteringIndexSliceFilter): Query = {
    (new BooleanQuery.Builder /: filter.requestedSlices.asScala) (
      (builder, slice) => builder.add(query(key, slice), SHOULD)).build()
  }

}

/** Companion object for [[ClusteringMapper]]. */
object ClusteringMapper {

  /** The Lucene field name. */
  val FIELD_NAME = "_clustering"

  /** The Lucene field type. */
  val FIELD_TYPE = new FieldType
  FIELD_TYPE.setOmitNorms(true)
  FIELD_TYPE.setIndexOptions(IndexOptions.DOCS)
  FIELD_TYPE.setTokenized(false)
  FIELD_TYPE.setStored(false)
  FIELD_TYPE.setDocValuesType(DocValuesType.SORTED)
  FIELD_TYPE.freeze()

  /** The number of bytes produced by token collation. */
  val PREFIX_SIZE = 8

  /** Returns a lexicographically sortable representation of the specified token.
    *
    * @param token a token
    * @return a lexicographically sortable 8 bytes array
    */
  @SuppressWarnings(Array("NumericOverflow"))
  def prefix(token: Token): Array[Byte] = {
    val value = TokenMapper.longValue(token)
    val collated = Long.MinValue * -1 + value
    Longs.toByteArray(collated)
  }

  /** Returns the start [[ClusteringPrefix]] of the first partition of the specified [[DataRange]].
    *
    * @param range the data range
    * @return the optional start clustering prefix of the data range
    */
  def startClusteringPrefix(range: DataRange): Option[ClusteringPrefix] = {
    val filter = range.startKey match {
      case key: DecoratedKey => range.clusteringIndexFilter(key)
      case position =>
        range.clusteringIndexFilter(new BufferDecoratedKey(position.getToken, EMPTY_BYTE_BUFFER))
    }
    filter match {
      case slices: ClusteringIndexSliceFilter => Some(slices.requestedSlices.get(0).start)
      case names: ClusteringIndexNamesFilter => Some(names.requestedRows.first)
      case _ => None
    }
  }

  /** Returns the stop [[ClusteringPrefix]] of the last partition of the specified [[DataRange]].
    *
    * @param range the data range
    * @return the optional stop clustering prefix of the data range
    */
  def stopClusteringPrefix(range: DataRange): Option[ClusteringPrefix] = {
    val filter = range.stopKey match {
      case key: DecoratedKey => range.clusteringIndexFilter(key)
      case position =>
        range.clusteringIndexFilter(new BufferDecoratedKey(position.getToken, EMPTY_BYTE_BUFFER))
    }
    filter match {
      case slices: ClusteringIndexSliceFilter =>
        Some(slices.requestedSlices.get(slices.requestedSlices.size - 1).end)
      case names: ClusteringIndexNamesFilter =>
        Some(names.requestedRows.last)
      case _ => None
    }
  }
}

/** [[SortField]] to sort by token and clustering key.
  *
  * @param mapper the primary key mapper to be used
  */
class ClusteringSort(mapper: ClusteringMapper) extends SortField(
  FIELD_NAME, (field, hits, sortPos, reversed) => new TermValComparator(hits, field, false) {
    override def compareValues(t1: BytesRef, t2: BytesRef): Int = {
      val comp = compareUnsigned(t1.bytes, 0, PREFIX_SIZE, t2.bytes, 0, PREFIX_SIZE)
      if (comp != 0) return comp
      val bb1 = ByteBuffer.wrap(t1.bytes, PREFIX_SIZE, t1.length - PREFIX_SIZE)
      val bb2 = ByteBuffer.wrap(t2.bytes, PREFIX_SIZE, t2.length - PREFIX_SIZE)
      val clustering1 = mapper.clustering(bb1)
      val clustering2 = mapper.clustering(bb2)
      mapper.comparator.compare(clustering1, clustering2)
    }
  }) {

  /** @inheritdoc */
  override def toString: String = "<clustering>"

  /** @inheritdoc */
  override def equals(o: Any): Boolean = o match {
    case cs: ClusteringSort => true
    case _ => false
  }

}

/** [[MultiTermQuery]] to get a range of clustering keys.
  *
  * @param mapper   the clustering key mapper to be used
  * @param position the partition position
  * @param start    the start clustering
  * @param stop     the stop clustering
  */
class ClusteringQuery(
    val mapper: ClusteringMapper,
    val position: PartitionPosition,
    val start: Option[ClusteringPrefix],
    val stop: Option[ClusteringPrefix]) extends MultiTermQuery(FIELD_NAME) {

  val token = position.getToken
  val seek = ClusteringMapper.prefix(token)
  val comparator = mapper.comparator

  /** @inheritdoc */
  override def getTermsEnum(terms: Terms, attributes: AttributeSource): TermsEnum = {
    new FullKeyDataRangeFilteredTermsEnum(terms.iterator)
  }

  /** Important to avoid collisions in Lucene's query cache. */
  override def equals(o: Any): Boolean = o match {
    case q: ClusteringQuery => token == q.token && start == q.start && stop == q.stop
    case _ => false
  }

  /** Important to avoid collisions in Lucene's query cache. */
  override def hashCode: Int = {
    var result = super.hashCode
    result = 31 * result + token.hashCode
    result = 31 * result + start.map(_.hashCode).getOrElse(0)
    result = 31 * result + stop.map(_.hashCode).getOrElse(0)
    result
  }

  /** @inheritdoc */
  override def toString(field: String): String = {
    val fieldName = if (field.isEmpty) ClusteringMapper.FIELD_NAME else field
    MoreObjects.toStringHelper(this)
      .add("field", fieldName)
      .add("token", token)
      .add("start", mapper.toString(start))
      .add("stop", mapper.toString(stop))
      .toString
  }

  class FullKeyDataRangeFilteredTermsEnum(tenum: TermsEnum) extends FilteredTermsEnum(tenum) {

    // Jump to the start of the partition
    setInitialSeekTerm(new BytesRef(seek))

    /** @inheritdoc */
    override def accept(term: BytesRef): AcceptStatus = {

      // Check token range
      val comp = compareUnsigned(term.bytes, 0, PREFIX_SIZE, seek, 0, PREFIX_SIZE)
      if (comp < 0) return AcceptStatus.NO
      if (comp > 0) return AcceptStatus.END

      // Check clustering range
      val bb = ByteBuffer.wrap(term.bytes, PREFIX_SIZE, term.length - PREFIX_SIZE)
      val clustering = mapper.clustering(bb)
      if (start.exists(comparator.compare(_, clustering) > 0)) return AcceptStatus.NO
      if (stop.exists(comparator.compare(_, clustering) < 0)) return AcceptStatus.NO

      AcceptStatus.YES
    }
  }

}