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

import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.mapping.TokenMapper._
import org.apache.cassandra.config.DatabaseDescriptor
import org.apache.cassandra.db.DecoratedKey
import org.apache.cassandra.dht.{Murmur3Partitioner, Token}
import org.apache.lucene.document.{FieldType, LongField}
import org.apache.lucene.index.{DocValuesType, IndexOptions, IndexableField, Term}
import org.apache.lucene.search._
import org.apache.lucene.util.{BytesRef, BytesRefBuilder, NumericUtils}

/** Class for several token mappings between Cassandra and Lucene.
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class TokenMapper {

  if (!DatabaseDescriptor.getPartitioner.isInstanceOf[Murmur3Partitioner]) {
    throw new IndexException("Only Murmur3 partitioner is supported")
  }

  /** Returns the Lucene [[IndexableField]] associated to the token of the specified row key.
    *
    * @param key the raw partition key to be added
    * @return a indexable field
    */
  def indexableField(key: DecoratedKey): IndexableField = {
    val token = key.getToken
    val value = longValue(token)
    new LongField(FIELD_NAME, value, FIELD_TYPE)
  }

  /** Returns a Lucene [[SortField]] for sorting documents according to the partitioner's order.
    *
    * @return a sort field for sorting by token
    */
  def sortField: SortField = {
    new SortField(FIELD_NAME, SortField.Type.LONG)
  }

  /** Returns a query to find the documents containing a token inside the specified token range.
    *
    * @param lower        the lower token
    * @param upper        the upper token
    * @param includeLower if the lower token should be included
    * @param includeUpper if the upper token should be included
    * @return the query to find the documents containing a token inside the range
    */
  def query(
      lower: Token,
      upper: Token,
      includeLower: Boolean,
      includeUpper: Boolean): Option[Query] = {

    // Skip if it's full data range
    if (lower.isMinimum && upper.isMinimum) return None

    // Get token values
    val min: java.lang.Long = if (lower.isMinimum) Long.MinValue else longValue(lower)
    val max: java.lang.Long = if (upper.isMinimum) Long.MaxValue else longValue(upper)

    // Do query using doc values or inverted index depending on empirical heuristic
    if (max / 10 - min / 10 > 1222337203685480000L) {
      Some(DocValuesRangeQuery.newLongRange(FIELD_NAME, min, max, includeLower, includeUpper))
    } else {
      Some(NumericRangeQuery.newLongRange(FIELD_NAME, min, max, includeLower, includeUpper))
    }
  }

  /** Returns a Lucene query to find the documents containing the specified token.
    *
    * @param token the token
    * @return the query to find the documents containing `token`
    */
  def query(token: Token): Query = {
    new TermQuery(new Term(FIELD_NAME, bytesRef(token)))
  }

}

/** Companion object for [[TokenMapper]]. */
object TokenMapper {

  /** The Lucene field name */
  val FIELD_NAME = "_token"

  /** The Lucene field type */
  val FIELD_TYPE = new FieldType
  FIELD_TYPE.setTokenized(true)
  FIELD_TYPE.setOmitNorms(true)
  FIELD_TYPE.setIndexOptions(IndexOptions.DOCS)
  FIELD_TYPE.setNumericType(FieldType.NumericType.LONG)
  FIELD_TYPE.setDocValuesType(DocValuesType.NUMERIC)
  FIELD_TYPE.freeze()

  /** Returns the `Long` value of the specified Murmur3 partitioning [[Token]].
    *
    * @param token a Murmur3 token
    * @return the `token`'s `Long` value
    */
  def longValue(token: Token): Long = {
    token.getTokenValue.asInstanceOf[Long]
  }

  /** Returns the [[BytesRef]] indexing value of the specified Murmur3 partitioning [[Token]].
    *
    * @param token a Murmur3 token
    * @return the `token`'s indexing value
    */
  def bytesRef(token: Token): BytesRef = {
    val value = longValue(token)
    val bytesRefBuilder = new BytesRefBuilder
    NumericUtils.longToPrefixCoded(value, 0, bytesRefBuilder)
    bytesRefBuilder.get
  }
}
