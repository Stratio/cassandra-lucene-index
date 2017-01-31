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

import com.stratio.cassandra.lucene.BaseScalaTest._
import com.stratio.cassandra.lucene.IndexException
import org.apache.cassandra.db.marshal.CompositeType
import org.apache.cassandra.dht.Murmur3Partitioner
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** Tests for [[PartitionerOnColumn]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class PartitionerOnVirtualNodeTest extends PartitionerTest {

  test("build with zero vnodes_per_partition") {
    val longList = List(0, 1, 2, 3, 4, 5).map(new Murmur3Partitioner.LongToken(_))
    assertThrows[IndexException] {PartitionerOnVirtualNode(0, longList)}
  }

  test("build with negative partitions") {
    val longList = List(0, 1, 2, 3, 4, 5).map(new Murmur3Partitioner.LongToken(_))
    assertThrows[IndexException] {PartitionerOnVirtualNode(-1, longList)}
  }

  test("num partitions even division") {
    val longList = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9).map(new Murmur3Partitioner.LongToken(_))
    PartitionerOnVirtualNode(2, longList).numPartitions shouldBe 5
  }

  test("num partitions less than 1") {
    val longList = List(0, 1, 2, 3, 4).map(new Murmur3Partitioner.LongToken(_))
    PartitionerOnVirtualNode(7, longList).numPartitions shouldBe 1
  }

  test("num partitions odd") {
    val longList = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10).map(new Murmur3Partitioner.LongToken(_))
    PartitionerOnVirtualNode(2, longList).numPartitions shouldBe 6
  }

  test("key partition with 1 partition") {
    val tokens = List(-9223372036854775808l,
      -7378697629483820647l,
      -5534023222112865486l,
      -3689348814741910325l,
      -1844674407370955164l,
      -3l,
      1844674407370955158l,
      3689348814741910319l,
      5534023222112865480l,
      7378697629483820641l).map(new Murmur3Partitioner.LongToken(_))
    val partitioner = PartitionerOnVirtualNode(10, tokens)
    for (i <- 1 to 10) {
      partitioner.partition(key(i)) shouldBe 0
    }
  }

  test("key partition with n partitions") {
    val tokens = List(-9223372036854775808l,
      -7378697629483820647l,
      -5534023222112865486l,
      -3689348814741910325l,
      -1844674407370955164l,
      -3l,
      1844674407370955158l,
      3689348814741910319l,
      5534023222112865480l,
      7378697629483820641l).map(new Murmur3Partitioner.LongToken(_))
    val partitioner = PartitionerOnVirtualNode(2, tokens)
    partitioner.numPartitions shouldBe 5
    partitioner.partition(key(0)) shouldBe 1
    partitioner.partition(key(1)) shouldBe 1
    partitioner.partition(key(2)) shouldBe 1
    partitioner.partition(key(3)) shouldBe 2
    partitioner.partition(key(4)) shouldBe 1
    partitioner.partition(key(5)) shouldBe 0
    partitioner.partition(key(6)) shouldBe 2
    partitioner.partition(key(7)) shouldBe 2
  }

  test("composite key") {
    val validator = CompositeType.getInstance(int32, utf8)
    val bb = validator.builder().add(int32.decompose(3)).add(utf8.decompose("v1")).build
    val key = Murmur3Partitioner.instance.decorateKey(bb)

    val tokens = List(-9223372036854775808l,
      -7378697629483820647l,
      -5534023222112865486l,
      -3689348814741910325l,
      -1844674407370955164l,
      -3l,
      1844674407370955158l,
      3689348814741910319l,
      5534023222112865480l,
      7378697629483820641l).map(new Murmur3Partitioner.LongToken(_))

    PartitionerOnVirtualNode(2, tokens).partition(key) shouldBe 1
    PartitionerOnVirtualNode(5, tokens).partition(key) shouldBe 0
    PartitionerOnVirtualNode(10, tokens).partition(key) shouldBe 0
  }

}
