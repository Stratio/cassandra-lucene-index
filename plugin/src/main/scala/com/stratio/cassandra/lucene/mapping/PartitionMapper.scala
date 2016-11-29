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

import com.stratio.cassandra.lucene.mapping.PartitionMapper._
import com.stratio.cassandra.lucene.util.ByteBufferUtils
import org.apache.cassandra.config.{CFMetaData, DatabaseDescriptor}
import org.apache.cassandra.db.DecoratedKey
import org.apache.lucene.document.{Document, Field, FieldType}
import org.apache.lucene.index.{DocValuesType, IndexOptions, IndexableField, Term}
import org.apache.lucene.search.FieldComparator.TermValComparator
import org.apache.lucene.search._
import org.apache.lucene.util.BytesRef

import scala.collection.JavaConverters._

/** Class for several partition key mappings between Cassandra and Lucene.
  *
  * @param metadata the indexed table metadata
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class PartitionMapper(metadata: CFMetaData) {

  val partitioner = DatabaseDescriptor.getPartitioner
  val validator = metadata.getKeyValidator
  val partitionKeyColumns = metadata.partitionKeyColumns.asScala

  /** Returns the Lucene indexable field representing to the specified partition key.
    *
    * @param partitionKey the partition key to be converted
    * @return a indexable field
    */
  def indexableField(partitionKey: DecoratedKey): IndexableField = {
    val bb = partitionKey.getKey
    val bytesRef = ByteBufferUtils.bytesRef(bb)
    new Field(FIELD_NAME, bytesRef, FIELD_TYPE)
  }

  /** Returns the specified raw partition key as a Lucene term.
    *
    * @param partitionKey the raw partition key to be converted
    * @return a Lucene term
    */
  def term(partitionKey: ByteBuffer): Term = {
    val bytesRef = ByteBufferUtils.bytesRef(partitionKey)
    new Term(FIELD_NAME, bytesRef)
  }

  /** Returns the specified raw partition key as a Lucene term.
    *
    * @param partitionKey the raw partition key to be converted
    * @return a Lucene term
    */
  def term(partitionKey: DecoratedKey): Term = {
    term(partitionKey.getKey)
  }

  /** Returns the specified raw partition key as a Lucene query.
    *
    * @param partitionKey the raw partition key to be converted
    * @return the specified raw partition key as a Lucene query
    */
  def query(partitionKey: DecoratedKey): Query = {
    new TermQuery(term(partitionKey))
  }

  /** Returns the specified raw partition key as a Lucene query.
    *
    * @param partitionKey the raw partition key to be converted
    * @return the specified raw partition key as a Lucene query
    */
  def query(partitionKey: ByteBuffer): Query = {
    new TermQuery(term(partitionKey))
  }

  /** Returns the partition key contained in the specified Lucene document.
    *
    * @param document the document containing the partition key to be get
    * @return the key contained in the specified Lucene document
    */
  def decoratedKey(document: Document): DecoratedKey = {
    val bytesRef = document.getBinaryValue(FIELD_NAME)
    val bb = ByteBufferUtils.byteBuffer(bytesRef)
    partitioner.decorateKey(bb)
  }

  /** Returns a Lucene sort field for sorting documents/rows according to the partition key.
    *
    * @return a sort field for sorting by partition key
    */
  def sortField: SortField = {
    new PartitionSort(this)
  }

}

/** Companion object for [[PartitionMapper]]. */
object PartitionMapper {

  /** The Lucene field name. */
  val FIELD_NAME = "_partition"

  /** The Lucene field type. */
  val FIELD_TYPE = new FieldType
  FIELD_TYPE.setOmitNorms(true)
  FIELD_TYPE.setIndexOptions(IndexOptions.DOCS)
  FIELD_TYPE.setTokenized(false)
  FIELD_TYPE.setStored(true)
  FIELD_TYPE.setDocValuesType(DocValuesType.SORTED)
  FIELD_TYPE.freeze()
}

/** [[SortField]] to sort by partition key.
  *
  * @param mapper the partition key mapper to be used
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class PartitionSort(mapper: PartitionMapper) extends SortField(
  FIELD_NAME, (field, hits, sortPos, reversed) => new TermValComparator(hits, field, false) {
    override def compareValues(t1: BytesRef, t2: BytesRef): Int = {
      val bb1 = ByteBufferUtils.byteBuffer(t1)
      val bb2 = ByteBufferUtils.byteBuffer(t2)
      mapper.validator.compare(bb1, bb2)
    }
  }) {

  /** @inheritdoc **/
  override def toString: String = "<partition>"

  /** @inheritdoc **/
  override def equals(o: Any): Boolean = o match {
    case ps: PartitionSort => true
    case _ => false
  }

}
