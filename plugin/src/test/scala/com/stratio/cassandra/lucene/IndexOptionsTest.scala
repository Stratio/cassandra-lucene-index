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
package com.stratio.cassandra.lucene

import com.stratio.cassandra.lucene.IndexOptions._
import com.stratio.cassandra.lucene.partitioning.{PartitionerOnNone, PartitionerOnToken}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** Tests for [[IndexOptions]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class IndexOptionsTest extends BaseScalaTest {

  // Refresh seconds option tests

  test("parse refresh seconds option with default") {
    parseRefresh(Map()) shouldBe DEFAULT_REFRESH_SECONDS
  }

  test("parse refresh seconds option with integer") {
    parseRefresh(Map(REFRESH_SECONDS_OPTION -> "1")) shouldBe 1
  }

  test("parse refresh seconds option with decimal") {
    parseRefresh(Map(REFRESH_SECONDS_OPTION -> "0.1")) shouldBe 0.1
  }

  test("parse refresh seconds option with failing non numeric value") {
    intercept[IndexException] {
      parseRefresh(Map(REFRESH_SECONDS_OPTION -> "a"))
    }.getMessage shouldBe s"'$REFRESH_SECONDS_OPTION' must be a strictly positive decimal, found: a"
  }

  test("parse refresh seconds option with failing zero value") {
    intercept[IndexException] {
      parseRefresh(Map(REFRESH_SECONDS_OPTION -> "0"))
    }.getMessage shouldBe s"'$REFRESH_SECONDS_OPTION' must be strictly positive, found: 0.0"
  }

  test("parse refresh seconds option with failing negative value") {
    intercept[IndexException] {
      parseRefresh(Map(REFRESH_SECONDS_OPTION -> "-1"))
    }.getMessage shouldBe s"'$REFRESH_SECONDS_OPTION' must be strictly positive, found: -1.0"
  }

  // RAM buffer MB option tests

  test("parse RAM buffer MB option with default") {
    parseRamBufferMB(Map()) shouldBe DEFAULT_RAM_BUFFER_MB
  }

  test("parse RAM buffer MB option with integer") {
    parseRamBufferMB(Map(RAM_BUFFER_MB_OPTION -> "1")) shouldBe 1
  }

  test("parse RAM buffer MB option with failing decimal") {
    intercept[IndexException] {
      parseRamBufferMB(Map(RAM_BUFFER_MB_OPTION -> "0.1"))
    }.getMessage shouldBe s"'$RAM_BUFFER_MB_OPTION' must be a strictly positive integer, found: 0.1"
  }

  test("parse RAM buffer MB option with failing non numeric value") {
    intercept[IndexException] {
      parseRamBufferMB(Map(RAM_BUFFER_MB_OPTION -> "a"))
    }.getMessage shouldBe s"'$RAM_BUFFER_MB_OPTION' must be a strictly positive integer, found: a"
  }

  test("parse RAM buffer MB option with failing zero value") {
    intercept[IndexException] {
      parseRamBufferMB(Map(RAM_BUFFER_MB_OPTION -> "0"))
    }.getMessage shouldBe s"'$RAM_BUFFER_MB_OPTION' must be strictly positive, found: 0"
  }

  test("parse RAM buffer MB option with failing negative value") {
    intercept[IndexException] {
      parseRamBufferMB(Map(RAM_BUFFER_MB_OPTION -> "-1"))
    }.getMessage shouldBe s"'$RAM_BUFFER_MB_OPTION' must be strictly positive, found: -1"
  }

  // Max merge MB option tests

  test("parse max merge MB option with default") {
    parseMaxMergeMB(Map()) shouldBe DEFAULT_MAX_MERGE_MB
  }

  test("parse max merge MB option with integer") {
    parseMaxMergeMB(Map(MAX_MERGE_MB_OPTION -> "1")) shouldBe 1
  }

  test("parse max merge MB option with failing decimal") {
    intercept[IndexException] {
      parseMaxMergeMB(Map(MAX_MERGE_MB_OPTION -> "0.1"))
    }.getMessage shouldBe s"'$MAX_MERGE_MB_OPTION' must be a strictly positive integer, found: 0.1"
  }

  test("parse max merge MB option with failing non numeric value") {
    intercept[IndexException] {
      parseMaxMergeMB(Map(MAX_MERGE_MB_OPTION -> "a"))
    }.getMessage shouldBe s"'$MAX_MERGE_MB_OPTION' must be a strictly positive integer, found: a"
  }

  test("parse max merge MB option with failing zero value") {
    intercept[IndexException] {
      parseMaxMergeMB(Map(MAX_MERGE_MB_OPTION -> "0"))
    }.getMessage shouldBe s"'$MAX_MERGE_MB_OPTION' must be strictly positive, found: 0"
  }

  test("parse max merge MB option with failing negative value") {
    intercept[IndexException] {
      parseMaxMergeMB(Map(MAX_MERGE_MB_OPTION -> "-1"))
    }.getMessage shouldBe s"'$MAX_MERGE_MB_OPTION' must be strictly positive, found: -1"
  }

  // Max cached MB option tests

  test("parse max cached MB option with default") {
    parseMaxCachedMB(Map()) shouldBe DEFAULT_MAX_CACHED_MB
  }

  test("parse max cached MB option with integer") {
    parseMaxCachedMB(Map(MAX_CACHED_MB_OPTION -> "1")) shouldBe 1
  }

  test("parse max cached MB option with failing decimal") {
    intercept[IndexException] {
      parseMaxCachedMB(Map(MAX_CACHED_MB_OPTION -> "0.1"))
    }.getMessage shouldBe s"'$MAX_CACHED_MB_OPTION' must be a strictly positive integer, found: 0.1"
  }

  test("parse max cached MB option with failing non numeric value") {
    intercept[IndexException] {
      parseMaxCachedMB(Map(MAX_CACHED_MB_OPTION -> "a"))
    }.getMessage shouldBe s"'$MAX_CACHED_MB_OPTION' must be a strictly positive integer, found: a"
  }

  test("parse max cached MB option with failing zero value") {
    intercept[IndexException] {
      parseMaxCachedMB(Map(MAX_CACHED_MB_OPTION -> "0"))
    }.getMessage shouldBe s"'$MAX_CACHED_MB_OPTION' must be strictly positive, found: 0"
  }

  test("parse max cached MB option with failing negative value") {
    intercept[IndexException] {
      parseMaxCachedMB(Map(MAX_CACHED_MB_OPTION -> "-1"))
    }.getMessage shouldBe s"'$MAX_CACHED_MB_OPTION' must be strictly positive, found: -1"
  }

  // Indexing threads option tests

  test("parse indexing threads option with default") {
    parseIndexingThreads(Map()) shouldBe DEFAULT_INDEXING_THREADS
  }

  test("parse indexing threads option with integer") {
    parseIndexingThreads(Map(INDEXING_THREADS_OPTION -> "1")) shouldBe 1
  }

  test("parse indexing threads option with failing decimal") {
    intercept[IndexException] {
      parseIndexingThreads(Map(INDEXING_THREADS_OPTION -> "0.1"))
    }.getMessage shouldBe s"'$INDEXING_THREADS_OPTION' must be an integer, found: 0.1"
  }

  test("parse indexing threads option with failing non numeric value") {
    intercept[IndexException] {
      parseIndexingThreads(Map(INDEXING_THREADS_OPTION -> "a"))
    }.getMessage shouldBe s"'$INDEXING_THREADS_OPTION' must be an integer, found: a"
  }

  test("parse indexing threads option with zero value") {
    parseIndexingThreads(Map(INDEXING_THREADS_OPTION -> "-1")) shouldBe -1
  }

  test("parse indexing threads option with negative value") {
    parseIndexingThreads(Map(INDEXING_THREADS_OPTION -> "-1")) shouldBe -1
  }

  // Indexing queues size option tests

  test("parse indexing queues size option with default") {
    parseIndexingQueuesSize(Map()) shouldBe DEFAULT_INDEXING_QUEUES_SIZE
  }

  test("parse indexing queues size option with integer") {
    parseIndexingQueuesSize(Map(INDEXING_QUEUES_SIZE_OPTION -> "1")) shouldBe 1
  }

  test("parse indexing queues size option with failing decimal") {
    intercept[IndexException] {
      parseIndexingQueuesSize(Map(INDEXING_QUEUES_SIZE_OPTION -> "0.1"))
    }.getMessage shouldBe
      s"'$INDEXING_QUEUES_SIZE_OPTION' must be a strictly positive integer, found: 0.1"
  }

  test("parse indexing queues size option with failing non numeric value") {
    intercept[IndexException] {
      parseIndexingQueuesSize(Map(INDEXING_QUEUES_SIZE_OPTION -> "a"))
    }.getMessage shouldBe
      s"'$INDEXING_QUEUES_SIZE_OPTION' must be a strictly positive integer, found: a"
  }

  test("parse indexing queues size option with failing zero value") {
    intercept[IndexException] {
      parseIndexingQueuesSize(Map(INDEXING_QUEUES_SIZE_OPTION -> "0"))
    }.getMessage shouldBe s"'$INDEXING_QUEUES_SIZE_OPTION' must be strictly positive, found: 0"
  }

  test("parse indexing queues size option with failing negative value") {
    intercept[IndexException] {
      parseIndexingQueuesSize(Map(INDEXING_QUEUES_SIZE_OPTION -> "-1"))
    }.getMessage shouldBe s"'$INDEXING_QUEUES_SIZE_OPTION' must be strictly positive, found: -1"
  }

  // Excluded data centers size option tests

  test("parse excluded data centers option with default") {
    parseExcludedDataCenters(Map()) shouldBe DEFAULT_EXCLUDED_DATA_CENTERS
  }

  test("parse excluded data centers option with empty list") {
    parseExcludedDataCenters(Map(EXCLUDED_DATA_CENTERS_OPTION -> "")) shouldBe List()
  }

  test("parse excluded data centers option with singleton list") {
    parseExcludedDataCenters(Map(EXCLUDED_DATA_CENTERS_OPTION -> "dc1")) shouldBe List("dc1")
  }

  test("parse excluded data centers option with multiple list") {
    val options = Map(EXCLUDED_DATA_CENTERS_OPTION -> " dc1,dc2 ")
    parseExcludedDataCenters(options) shouldBe List("dc1", "dc2")
  }

  test("parse excluded data centers option with multiple list and spaces") {
    val options = Map(EXCLUDED_DATA_CENTERS_OPTION -> " dc1 , dc2 ")
    parseExcludedDataCenters(options) shouldBe List("dc1", "dc2")
  }

  // Partitioner option tests

  test("parse partitioner option with default") {
    parsePartitioner(Map(), null) shouldBe DEFAULT_PARTITIONER
  }

  test("parse partitioner with none partitioner") {
    val json = "{type:\"none\"}"
    parsePartitioner(Map(PARTITIONER_OPTION -> json), null) shouldBe PartitionerOnNone()
  }

  test("parse partitioner with token partitioner") {
    val json = "{type:\"token\", partitions: 10}"
    parsePartitioner(Map(PARTITIONER_OPTION -> json), null) shouldBe PartitionerOnToken(10)
  }

}
