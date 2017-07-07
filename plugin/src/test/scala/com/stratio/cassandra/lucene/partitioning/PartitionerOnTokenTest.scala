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

/** Tests for [[PartitionerOnToken]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class PartitionerOnTokenTest extends PartitionerTest {

  test("build with zero partitions") {
    intercept[IndexException] {
      PartitionerOnToken(0, Array("/home/a"))
    }.getMessage shouldBe "The number of partitions should be strictly positive but found 0"
  }

  test("build with negative partitions") {
    intercept[IndexException] {
      PartitionerOnToken(-1, Array("/home/a"))
    }.getMessage shouldBe "The number of partitions should be strictly positive but found -1"
  }

  test("build with size(paths) != partitions") {
    intercept [IndexException] {
      PartitionerOnToken(1, Array())
    }.getMessage shouldBe "The paths size must be equal to number of partitions"
  }

  test("parse JSON") {
    val json = "{type:\"token\", partitions: 1, paths: [\"/home/a\"]}"
    Partitioner.fromJson(json) shouldBe PartitionerOnToken.Builder(1, Array("/home/a"))
  }

  test("num partitions") {
    PartitionerOnToken(4, Array("/home/a","/home/b","/home/c","/home/d")).numPartitions shouldBe 4
  }

  test("key partition with 1 partition") {
    for (i <- 1 to 10) {
      PartitionerOnToken(1, Array("/home/a")).partition(key(i)) shouldBe 0
    }
  }

  test("key partition with n partitions") {
    val partitioner = PartitionerOnToken(10, Array("/home/a","/home/b","/home/c","/home/d","/home/e","/home/f","/home/g","/home/h","/home/i","/home/j"))
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
    val partitioner = PartitionerOnToken(10, Array("/home/a","/home/b","/home/c","/home/d","/home/e","/home/f","/home/g","/home/h","/home/i","/home/j"))
    partitioner.pathForPartition(0) shouldBe "/home/a"
    partitioner.pathForPartition(1) shouldBe "/home/b"
    partitioner.pathForPartition(2) shouldBe "/home/c"
    partitioner.pathForPartition(3) shouldBe "/home/d"
    partitioner.pathForPartition(4) shouldBe "/home/e"
    partitioner.pathForPartition(5) shouldBe "/home/f"
    partitioner.pathForPartition(6) shouldBe "/home/g"
    partitioner.pathForPartition(7) shouldBe "/home/h"
    partitioner.pathForPartition(8) shouldBe "/home/i"
    partitioner.pathForPartition(9) shouldBe "/home/j"
  }

  test("testing invalid index in pathForPartition") {
    val partitioner = PartitionerOnToken(1, Array("/home/a"))
    intercept [IndexOutOfBoundsException] {
      partitioner.pathForPartition(1)
    }.getMessage shouldBe "partition must be [0,1)"
  }

  test("testing invalid index in pathForPartition with -1") {
    val partitioner = PartitionerOnToken(1, Array("/home/a"))
    intercept [IndexOutOfBoundsException] {
      partitioner.pathForPartition(-1)
    }.getMessage shouldBe "partition must be [0,1)"
  }
}
