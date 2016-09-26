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

import com.google.common.base.MoreObjects
import com.stratio.cassandra.lucene.key.ClusteringMapper._
import org.apache.cassandra.db.{Clustering, ClusteringPrefix, PartitionPosition}
import org.apache.cassandra.utils.FastByteOperations._
import org.apache.lucene.index.FilteredTermsEnum.AcceptStatus
import org.apache.lucene.index.{FilteredTermsEnum, Terms, TermsEnum}
import org.apache.lucene.search.MultiTermQuery
import org.apache.lucene.util.{AttributeSource, BytesRef}

/**
  * [[MultiTermQuery]] to get a range of clustering keys.
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class ClusteringQuery(val mapper: ClusteringMapper,
                      val position: PartitionPosition,
                      val start: ClusteringPrefix,
                      val stop: ClusteringPrefix) extends MultiTermQuery(fieldName) {

  val token = position.getToken
  val seek = ClusteringMapper.prefix(token)

  /** @inheritdoc */
  override def getTermsEnum(terms: Terms, attributes: AttributeSource): TermsEnum =
  new FullKeyDataRangeFilteredTermsEnum(terms.iterator)

  /** @inheritdoc
    *
    * Important to avoid collisions in Lucene's query cache.
    */
  override def equals(o: Any): Boolean = o match {
    case q: ClusteringQuery => token == q.token &&
        (start == null && q.start == null || start == q.start) &&
        (stop == null && q.stop == null || stop == q.stop)
    case _ => false
  }

  /** @inheritdoc
    *
    * Important to avoid collisions in Lucene's query cache.
    */
  override def hashCode: Int = {
    var result = super.hashCode
    result = 31 * result + token.hashCode
    result = 31 * result + (if (start != null) start.hashCode else 0)
    result = 31 * result + (if (stop != null) stop.hashCode else 0)
    result
  }

  /** @inheritdoc */
  override def toString(field: String): String =
  MoreObjects.toStringHelper(this)
    .add("field", field)
    .add("token", token)
    .add("start", mapper.toString(start))
    .add("stop", mapper.toString(stop))
    .toString

  private class FullKeyDataRangeFilteredTermsEnum(tenum: TermsEnum) extends FilteredTermsEnum(tenum) {

    setInitialSeekTerm(new BytesRef(seek))

    /** @inheritdoc */
    override def accept(term: BytesRef): FilteredTermsEnum.AcceptStatus = {
      // Check token range
      val comp:Int = compareUnsigned(term.bytes, 0, prefixBytes, seek, 0, prefixBytes)
      if (comp < 0) return AcceptStatus.NO
      if (comp > 0) return AcceptStatus.END
      // Check clustering range
      val bb: ByteBuffer = ByteBuffer.wrap(term.bytes, prefixBytes, term.length - prefixBytes)
      val clustering: Clustering = mapper.clustering(bb)
      if (start != null && mapper.comparator.compare(start, clustering) > 0) return AcceptStatus.NO
      if (stop != null && mapper.comparator.compare(stop, clustering) < 0) return AcceptStatus.NO
      AcceptStatus.YES
    }
  }

}
