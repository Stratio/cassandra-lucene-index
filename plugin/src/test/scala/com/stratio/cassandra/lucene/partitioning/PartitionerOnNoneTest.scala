package com.stratio.cassandra.lucene.partitioning

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** Tests for [[PartitionerOnNone]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class PartitionerOnNoneTest extends PartitionerTest {

  test("parse") {
    Partitioner.fromJson("{type:\"none\"}") shouldBe PartitionerOnNone()
  }

  test("num partitions") {
    PartitionerOnNone().numPartitions shouldBe 1
  }

  test("key partition") {
    for (i <- 1 to 10) PartitionerOnNone().partition(key(i)) shouldBe 0
  }

}
