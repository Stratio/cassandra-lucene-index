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

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** Tests for [[PartitionerOnNone]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class PartitionerOnNoneTest extends PartitionerTest {

  test("parse JSON") {
    val json = "{type:\"none\"}"
    Partitioner.fromJson(json) shouldBe PartitionerOnNone.Builder()
  }

  test("num partitions") {
    PartitionerOnNone().numPartitions shouldBe 1
  }

  test("key partition") {
    val partitioner = PartitionerOnNone()
    for (i <- 1 to 20) partitioner.partition(key(i)) shouldBe 0
  }

}
