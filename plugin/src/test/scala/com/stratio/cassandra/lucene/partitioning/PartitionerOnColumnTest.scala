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

import com.stratio.cassandra.lucene.IndexException
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.stratio.cassandra.lucene.BaseScalaTest._
import org.apache.cassandra.db.marshal.CompositeType
import org.apache.cassandra.dht.Murmur3Partitioner

/** Tests for [[PartitionerOnColumn]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class PartitionerOnColumnTest extends PartitionerTest {

  test("build with zero partitions") {
    assertThrows[IndexException] {PartitionerOnColumn(0, "c", 0, int32)}
  }

  test("build with negative partitions") {
    assertThrows[IndexException] {PartitionerOnColumn(-1, "c", 0, int32)}
  }

  test("build with negative position") {
    assertThrows[IndexException] {PartitionerOnColumn(1, "c", -1, int32)}
  }

  test("build with null type") {
    assertThrows[IndexException] {PartitionerOnColumn(1, "c", 0, null)}
  }

  test("parse JSON") {
    val json = "{type:\"column\", partitions: 10, column:\"c\"}"
    Partitioner.fromJson(json) shouldBe PartitionerOnColumn.Builder(10, "c")
  }

  test("num partitions") {
    PartitionerOnColumn(4, "c", 0, int32).numPartitions shouldBe 4
  }

  test("key partition with 1 partition") {
    val partitioner = PartitionerOnColumn(1, "c", 0, int32)
    for (i <- 1 to 10) {
      partitioner.partition(key(i)) shouldBe 0
    }
  }

  test("key partition with n partitions") {
    val partitioner = PartitionerOnColumn(10, "c", 0, int32)
    partitioner.partition(key(0)) shouldBe 8
    partitioner.partition(key(1)) shouldBe 9
    partitioner.partition(key(2)) shouldBe 2
    partitioner.partition(key(3)) shouldBe 5
    partitioner.partition(key(4)) shouldBe 5
    partitioner.partition(key(5)) shouldBe 4
    partitioner.partition(key(6)) shouldBe 8
    partitioner.partition(key(7)) shouldBe 6
  }

  test("composite key") {
    val validator = CompositeType.getInstance(int32, utf8)
    val bb = validator.builder().add(int32.decompose(3)).add(utf8.decompose("v1")).build
    val key = Murmur3Partitioner.instance.decorateKey(bb)
    PartitionerOnColumn(10, "c", 0, validator).partition(key) shouldBe 5
    PartitionerOnColumn(10, "c", 1, validator).partition(key) shouldBe 3

  }

}
