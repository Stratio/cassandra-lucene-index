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

import java.nio.file.Paths

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
    intercept [IndexException] {
      PartitionerOnColumn(0, "c", Array(), 0, int32)
    }.getMessage shouldBe "The number of partitions should be strictly positive but found 0"
  }

  test("build with negative partitions") {
    intercept [IndexException] {
      PartitionerOnColumn(-1, "c", Array(), 0, int32)
    }.getMessage shouldBe "The number of partitions should be strictly positive but found -1"
  }

  test("build with negative position") {
    intercept [IndexException] {
      PartitionerOnColumn(1, "c", Array("/home/a").map(Paths.get(_)), -1, int32)
    }.getMessage shouldBe "The column position in the partition key should be positive"
  }

  test("build with null type") {
    intercept [IndexException] {
      PartitionerOnColumn(1, "c", Array("/home/a").map(Paths.get(_)), 0, null)
    }.getMessage shouldBe "The partition key type should be specified"
  }

  test("build with size(paths) != partitions") {
    intercept [IndexException] {
      PartitionerOnColumn(1, "c", Array(), 0, int32)
    }.getMessage shouldBe "The paths size must be equal to number of partitions"
  }

  test("parse JSON") {
    val json = "{type:\"column\", partitions: 3, column:\"c\", paths : [\"/home/a\",\"/home/b\",\"/home/c\"]}"
    Partitioner.fromJson(json) shouldBe PartitionerOnColumn.Builder(3, "c", Array("/home/a","/home/b","/home/c"))
  }

  test("num partitions") {
    PartitionerOnColumn(4, "c", Array("/home/a","/home/b","/home/c","/home/d").map(Paths.get(_)), 0, int32).numPartitions shouldBe 4
  }

  test("key partition with 1 partition") {
    val partitioner = PartitionerOnColumn(1, "c", Array("/home/a").map(Paths.get(_)), 0, int32)
    for (i <- 1 to 10) {
      partitioner.partition(key(i)) shouldBe 0
    }
  }

  test("key partition with n partitions") {
    val partitioner = PartitionerOnColumn(10, "c", Array("/home/a","/home/b","/home/c","/home/d","/home/e","/home/f","/home/g","/home/h","/home/i","/home/j").map(Paths.get(_)), 0, int32)
    partitioner.partition(key(0)) shouldBe 8
    partitioner.partition(key(1)) shouldBe 9
    partitioner.partition(key(2)) shouldBe 2
    partitioner.partition(key(3)) shouldBe 5
    partitioner.partition(key(4)) shouldBe 5
    partitioner.partition(key(5)) shouldBe 4
    partitioner.partition(key(6)) shouldBe 8
    partitioner.partition(key(7)) shouldBe 6
  }

  test("test valid paths set get") {
    val partitioner = PartitionerOnColumn(10, "c", Array("/home/a","/home/b","/home/c","/home/d","/home/e","/home/f","/home/g","/home/h","/home/i","/home/j").map(Paths.get(_)), 0, int32)
    val pathForPartitions=partitioner.pathsForEachPartitions
    pathForPartitions(0) shouldBe Paths.get("/home/a")
    pathForPartitions(1) shouldBe Paths.get("/home/b")
    pathForPartitions(2) shouldBe Paths.get("/home/c")
    pathForPartitions(3) shouldBe Paths.get("/home/d")
    pathForPartitions(4) shouldBe Paths.get("/home/e")
    pathForPartitions(5) shouldBe Paths.get("/home/f")
    pathForPartitions(6) shouldBe Paths.get("/home/g")
    pathForPartitions(7) shouldBe Paths.get("/home/h")
    pathForPartitions(8) shouldBe Paths.get("/home/i")
    pathForPartitions(9) shouldBe Paths.get("/home/j")
  }

  test("composite key") {
    val validator = CompositeType.getInstance(int32, utf8)
    val bb = validator.builder().add(int32.decompose(3)).add(utf8.decompose("v1")).build
    val key = Murmur3Partitioner.instance.decorateKey(bb)
    PartitionerOnColumn(10, "c", Array("/home/a","/home/b","/home/c","/home/d","/home/e","/home/f","/home/g","/home/h","/home/i","/home/j").map(Paths.get(_)), 0, validator).partition(key) shouldBe 5
    PartitionerOnColumn(10, "c", Array("/home/a","/home/b","/home/c","/home/d","/home/e","/home/f","/home/g","/home/h","/home/i","/home/j").map(Paths.get(_)), 1, validator).partition(key) shouldBe 3
  }

}
