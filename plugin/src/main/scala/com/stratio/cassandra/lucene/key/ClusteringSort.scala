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

import com.stratio.cassandra.lucene.key.ClusteringMapper._
import org.apache.cassandra.utils.FastByteOperations.compareUnsigned
import org.apache.lucene.search.{FieldComparator, FieldComparatorSource, SortField}
import org.apache.lucene.util.BytesRef

/**
  * [[SortField]] to sort by token and clustering key.
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class ClusteringSort(mapper: ClusteringMapper) extends SortField(fieldName, new FieldComparatorSource {
  override def newComparator(field: String, hits: Int, sortPos: Int, reversed: Boolean): FieldComparator[_] = {
    new FieldComparator.TermValComparator(hits, field, false) {
      override def compareValues(t1: BytesRef, t2: BytesRef): Int = {
        val comp = compareUnsigned(t1.bytes, 0, prefixBytes, t2.bytes, 0, prefixBytes)
        if (comp != 0) return comp
        val bb1 = ByteBuffer.wrap(t1.bytes, prefixBytes, t1.length - prefixBytes)
        val bb2 = ByteBuffer.wrap(t2.bytes, prefixBytes, t2.length - prefixBytes)
        val clustering1 = mapper.clustering(bb1)
        val clustering2 = mapper.clustering(bb2)
        mapper.comparator.compare(clustering1, clustering2)
      }
    }
  }
}) {

  /** @inheritdoc */
  override def toString: String = "<clustering>"

  /** @inheritdoc */
  override def equals(o: Any): Boolean = o match {
    case cs:ClusteringSort => toString == cs.toString // TODO: Check
    case _ => false
  }

}

