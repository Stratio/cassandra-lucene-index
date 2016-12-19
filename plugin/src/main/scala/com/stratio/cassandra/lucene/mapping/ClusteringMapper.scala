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
import java.util
import java.util.Comparator

import com.google.common.base.MoreObjects
import com.stratio.cassandra.lucene.column.{Column, Columns, ColumnsMapper}
import com.stratio.cassandra.lucene.util.{ByteBufferUtils, Logging}
import org.apache.cassandra.config.{CFMetaData, ColumnDefinition, Schema}
import org.apache.cassandra.db.ClusteringPrefix.Kind._
import org.apache.cassandra.db._
import org.apache.cassandra.db.filter.{ClusteringIndexNamesFilter, ClusteringIndexSliceFilter}
import org.apache.cassandra.db.marshal.{AbstractType, LongType}
import org.apache.cassandra.dht.Token
import org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER
import org.apache.lucene.document.{Document, Field, FieldType, SerializableSortedDocValuesComparator}
import org.apache.lucene.index.FilteredTermsEnum.AcceptStatus
import org.apache.lucene.index._
import org.apache.lucene.search.BooleanClause.Occur.SHOULD
import org.apache.lucene.search.FieldComparator.TermValComparator
import org.apache.lucene.search._
import org.apache.lucene.store.{DataInput, DataOutput}
import org.apache.lucene.util.{AttributeSource, BytesRef}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** Class for several clustering key mappings between Cassandra and Lucene.
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class ClusteringMapper(metadata: CFMetaData) extends SerializableSortedDocValuesComparator with Logging {

  /** The Lucene field type. */
  val FIELD_TYPE = new FieldType
  FIELD_TYPE.setOmitNorms(true)
  FIELD_TYPE.setIndexOptions(IndexOptions.DOCS)
  FIELD_TYPE.setTokenized(false)
  FIELD_TYPE.setStored(true)
  FIELD_TYPE.setDocValuesType(DocValuesType.SORTED)
  FIELD_TYPE.setDocValuesComparator(ClusteringMapper.this)
  FIELD_TYPE.freeze()

  /** The clustering keys [[ColumnDefinition]] */
  val clusteringColumns = metadata.clusteringColumns.asScala

  /** */
  val types : java.util.List[AbstractType[_]] = new util.ArrayList()
  types.add(LongType.instance)
  types.addAll(metadata.comparator.subtypes())

  /** Comparator for the globalCtype composed by chained Long comparator and clustering columns comparator */
  val clusteringComparator: ClusteringComparator = new ClusteringComparator(types)

  def this() = this(CFMetaData.createFake("fake_ks", "fake_table"))

  /** Returns the columns contained in the specified [[Clustering]].
    *
    * @param clustering the clustering key
    * @return the columns
    */
  def columns(clustering: Clustering): Columns = {
    (Columns() /: clusteringColumns) (
      (columns, columnDefinition) => {
        val name = columnDefinition.name.toString
        val position = columnDefinition.position
        val value = clustering.get(position)
        val valueType = columnDefinition.cellValueType
        columns.add(Column(name).withValue(ColumnsMapper.compose(value, valueType)))
      })
  }

  /** Returns a Lucene [[IndexableField]]s representing the specified primary key.
    *
    * @param key        the partition key
    * @param clustering the clustering key
    * @return a indexable field
    */
  def indexableField(key: DecoratedKey, clustering: Clustering): IndexableField = {
    val bb: ByteBuffer = byteBuffer(key.getToken, clustering.clustering)
    new Field(ClusteringMapper.FIELD_NAME, ByteBufferUtils.bytesRef(bb), FIELD_TYPE)
  }

  /** Returns the [[ByteBuffer]] representation of the specifieds [[Token]] and [[Clustering]].
    *
    * @param token      the partition key token
    * @param clustering a clustering key
    * @return a byte buffer representing `clustering`
    */
  def byteBuffer(token: Token, clustering: ClusteringPrefix): ByteBuffer = {
    val longTokenBB = LongType.instance.decompose(token.getTokenValue.asInstanceOf[Long])
    val bbs: Array[ByteBuffer] =Array(longTokenBB).++(clustering.getRawValues)
    ByteBufferUtils.compose(bbs: _*)
  }

  /** Returns the [[String]] human-readable representation of the specified [[ClusteringPrefix]].
    *
    * @param prefix the clustering prefix
    * @return a [[String]] representing the prefix
    */
  def toString(prefix: Option[ClusteringPrefix]): String = {
    if (prefix.isDefined) {
      prefix.get.toString(metadata)
    } else {
      "None"
    }
  }

  /** Returns the clustering key contained in the specified [[Document]].
    *
    * @param document a document containing the clustering key to be get
    * @return the clustering key contained in the document
    */
  def clustering(document: Document): Clustering = {
    val bytesRef = document.getBinaryValue(ClusteringMapper.FIELD_NAME)
    clustering(ByteBufferUtils.byteBuffer(bytesRef))
  }

  /** Returns the clustering key contained by the specified [[ByteBuffer]].
    *
    * @param clustering a byte buffer containing a [[Clustering]]
    * @return a Lucene field binary value
    */
  def clustering(clustering: ByteBuffer): Clustering = {
    val components: Array[ByteBuffer] = ByteBufferUtils.decompose(clustering)
    new Clustering(components.slice(1, components.length): _*)
  }

  /** Returns a Lucene [[SortField]] to sort documents by primary key.
    *
    * @return the sort field
    */
  def sortField: SortField = {
    new ClusteringSort(this)
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

  /** Returns a Lucene [[Query]] to retrieve all the rows in the specified clustering slice.
    *
    * @param key   the partition key
    * @param slice the slice
    * @return the Lucene query
    */
  def query(key: DecoratedKey, slice: Slice): Query = {
    query(key, Option(slice.start), Option(slice.end))
  }

  /** Returns a Lucene [[Query]] to retrieve all the rows in the specified partition slice.
    *
    * @param position the partition position
    * @param start    the start clustering prefix
    * @param stop     the stop clustering prefix
    * @return the Lucene query
    */
  def query(position: PartitionPosition,
            start: Option[ClusteringPrefix],
            stop: Option[ClusteringPrefix]): Query = {
    new ClusteringQuery(this, position, start, stop)
  }

  /** Compare two BytesRef arguments for sort based in totalComparator
    *
    * @param val1 left value [[BytesRef]]
    * @param val2 rigth value [[BytesRef]]
    * @return -1 if val1 < val, 0 if val1 = val2, +1 if val1 > val2
    */
  def compare(val1: BytesRef, val2: BytesRef): Int = {
    compare(ByteBufferUtils.byteBuffer(val1), ByteBufferUtils.byteBuffer(val2))
  }

  /** Compare two ByteBuffer arguments for sort based in totalComparator
    *
    * @param val1 left value [[ByteBuffer]]
    * @param val2 rigth value [[ByteBuffer]]
    * @return -1 if val1 < val, 0 if val1 = val2, +1 if val1 > val2
    */
  def compare(val1: ByteBuffer, val2: ByteBuffer): Int = {
    clusteringComparator.compare(val1, val2)
  }

  /** @inheritdoc*/
  override def write(input: DataOutput): Unit = {
    input.writeString(this.metadata.ksName)
    input.writeString(this.metadata.cfName)
  }

  /** @inheritdoc*/
  override def read(input: DataInput): SerializableSortedDocValuesComparator = {
    val keyspace = input.readString()
    val table = input.readString()
    val metadata = Schema.instance.getCFMetaData(keyspace, table)
    ClusteringMapper.getClusteringMapper(metadata)
  }
}

/** Companion object for [[ClusteringMapper]]. */
object ClusteringMapper {

  /** The Lucene field name. */
  val FIELD_NAME = "_clustering"

  var clusteringMappers: scala.collection.mutable.Map[CFMetaData, ClusteringMapper] = scala.collection.mutable.Map[CFMetaData, ClusteringMapper]()

  def getClusteringMapper(cfMetaData: CFMetaData): ClusteringMapper = {
    if (clusteringMappers.contains(cfMetaData)) {
      clusteringMappers.get(cfMetaData).get
    } else {
      val clust = new ClusteringMapper(cfMetaData)
      clusteringMappers += Tuple2(cfMetaData, clust)
      clust
    }
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
  ClusteringMapper.FIELD_NAME, (field, hits, sortPos, reversed) => new TermValComparator(hits, field, false) {
    override def compareValues(t1: BytesRef, t2: BytesRef): Int = {
      mapper.compare(t1, t2)
    }
  }) {

  /** @inheritdoc*/
  override def toString: String = "<clustering>"

  /** @inheritdoc*/
  override def equals(o: Any): Boolean = o match {
    case cs: ClusteringSort => true
    case _ => false
  }
}

/** [[Comparator[ByteBuffer]] to sort docValues
  *
  * @param types the clustering key mapper to be used
  */
class ClusteringComparator(val types: java.util.List[AbstractType[_]]) extends Comparator[ByteBuffer] with Logging {

  /** @inheritdoc*/
  override def compare(o1: ByteBuffer, o2: ByteBuffer): Int = {
    val val1Components = ByteBufferUtils.decompose(o1)
    val val2Components = ByteBufferUtils.decompose(o2)

    val min = Math.min(val1Components.length, val2Components.length)
    var comparison = 0
    for (i <- 0 until min) {
      comparison = types.get(i).compare(val1Components(i), val2Components(i))
      if (comparison != 0) return comparison
    }
    comparison
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
                       val stop: Option[ClusteringPrefix]) extends MultiTermQuery(ClusteringMapper.FIELD_NAME) with Logging {

  val token = position.getToken
  val startBB = Option(if (isStartBounded) mapper.byteBuffer(token, start.get) else null)
  val stopBB = Option(if (isEndBounded) mapper.byteBuffer(token, stop.get) else null)
  val seek = Option(if (isStartBounded) ByteBufferUtils.bytesRef(mapper.byteBuffer(token, start.get)) else null)
  setRewriteMethod(new DocValuesRewriteMethod)

  /** @inheritdoc*/
  override def getTermsEnum(terms: Terms, attributes: AttributeSource): TermsEnum = {
    new FullKeyDataRangeFilteredTermsEnum(terms.iterator, seek)
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
    result = 31 * result + seek.map(_.hashCode).getOrElse(0)
    result
  }

  /** @inheritdoc*/
  override def toString(field: String): String = {
    val fieldName = if (field.isEmpty) ClusteringMapper.FIELD_NAME else field
    MoreObjects.toStringHelper(this)
      .add("field", fieldName)
      .add("token", token)
      .add("start", mapper.toString(start))
      .add("stop", mapper.toString(stop))
      .toString
  }

  /** Returns If start bound is included in results.
    *
    * @return true if clustering key logical expression includes equality, false i.o.c.
    */
  private def includeStartBound: Boolean = {
    isStartBounded && ((start.get.kind eq INCL_START_BOUND) || (start.get.kind eq EXCL_END_INCL_START_BOUNDARY))
  }

  /** Returns if start logical expression exists. */
  private def isStartBounded: Boolean = {
    start != null && start.isDefined
  }

  /** Returns If end bound is included in results.
    *
    * @return true if clustering key logical expression includes equality, false i.o.c.
    */
  private def includeEndBound: Boolean = {
    isEndBounded && ((stop.get.kind eq INCL_END_BOUND) || (stop.get.kind eq INCL_END_EXCL_START_BOUNDARY))
  }

  /** Returns if end logical expression exists. */
  private def isEndBounded: Boolean = {
    stop != null && stop.isDefined
  }

  class FullKeyDataRangeFilteredTermsEnum(tenum: TermsEnum, seek: Option[BytesRef]) extends FilteredTermsEnum(tenum, seek.isDefined) with Logging {
    private var first: Boolean = true
    // Jump to the start of the partition
    if (seek.isDefined)
      setInitialSeekTerm(seek.get)

    /** @inheritdoc */
    override def accept(term: BytesRef): AcceptStatus = {

      val bb1: ByteBuffer = ByteBufferUtils.byteBuffer(term)
      if (first && startBB.isDefined) {
        first = false
        val comp: Int = mapper.compare(bb1, startBB.get)
        if ((comp == 0) && includeStartBound) {
          return AcceptStatus.YES
        }
      }
      if (stopBB.isDefined) {
        val comp: Int = mapper.compare(bb1, stopBB.get)
        if (comp < 0) {
          AcceptStatus.YES
        } else if ((comp == 0) && includeEndBound) {
          AcceptStatus.YES
        } else AcceptStatus.END
      }
      AcceptStatus.YES
    }
  }

}