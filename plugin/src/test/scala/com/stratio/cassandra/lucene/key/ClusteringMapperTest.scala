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

import com.stratio.cassandra.lucene.BaseScalaTest
import org.apache.cassandra.dht.Murmur3Partitioner
import org.apache.lucene.util.BytesRef
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * Tests for [[ClusteringMapper]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class ClusteringMapperTest extends BaseScalaTest {

  test("collated prefix") {
    val values = List(Long.MinValue,-12345L, -123L, -2L, -1L, 0L, 1L, 2L, 123L, 12345L, Long.MaxValue)
    val tokens = values.map(new Murmur3Partitioner.LongToken(_))
    val l1 = tokens.map(ClusteringMapper.prefix(_)).map(x => new BytesRef(x))
    val l2 = tokens.map(ClusteringMapper.prefix(_)).map(x => new BytesRef(x)).reverse.sorted
    l1 shouldBe l2
  }
}
