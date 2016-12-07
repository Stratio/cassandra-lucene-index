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

import com.stratio.cassandra.lucene.BaseScalaTest
import org.apache.cassandra.dht.Murmur3Partitioner
import org.apache.lucene.util.BytesRef
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** Tests for [[ClusteringMapper]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class ClusteringMapperTest extends BaseScalaTest {

  test("collate prefix") {
    val values = List(Long.MinValue, -10L, -2L, -1L, 0L, 1L, 2L, 10L, Long.MaxValue)
    val tokens = values.map(new Murmur3Partitioner.LongToken(_))
    val bytes = tokens.map(ClusteringMapper.prefix(_)).map(new BytesRef(_))
    bytes shouldBe bytes.reverse.sorted
  }
}
