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
package com.stratio.cassandra.lucene.partitioning

import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.BaseScalaTest.int32
import org.apache.cassandra.db.DecoratedKey
import org.apache.cassandra.dht.Murmur3Partitioner

/** Tests for [[Partitioner]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class PartitionerTest extends BaseScalaTest {

  test("parse default") {
    Partitioner.fromJson(null, "{}") shouldBe PartitionerOnNone()
  }

  test("num partitions wiht none partitioner") {
    PartitionerOnNone().allPartitions shouldBe List(0)
  }

  test("num partitions with token partitioner") {
    PartitionerOnToken(4).allPartitions shouldBe List(0, 1, 2, 3)
  }

  def key(n: Int): DecoratedKey = Murmur3Partitioner.instance.decorateKey(int32.decompose(n))

}
