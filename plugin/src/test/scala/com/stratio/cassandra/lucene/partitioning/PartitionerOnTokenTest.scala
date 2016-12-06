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
    assertThrows[IndexException] {PartitionerOnToken(0)}
  }

  test("build with negative partitions") {
    assertThrows[IndexException] {PartitionerOnToken(-1)}
  }

  test("parse") {
    Partitioner.fromJson("{type:\"token\", partitions: 10}") shouldBe PartitionerOnToken(10)
  }

  test("num partitions") {
    PartitionerOnToken(4).numPartitions shouldBe 4
  }

  test("key partition with 1 partition") {
    for (i <- 1 to 10) {
      PartitionerOnToken(1).partition(key(i)) shouldBe 0
    }
  }

  test("key partition with n partitions") {
    PartitionerOnToken(10).partition(key(0)) shouldBe 8
    PartitionerOnToken(10).partition(key(1)) shouldBe 9
    PartitionerOnToken(10).partition(key(2)) shouldBe 2
    PartitionerOnToken(10).partition(key(3)) shouldBe 5
    PartitionerOnToken(10).partition(key(4)) shouldBe 5
    PartitionerOnToken(10).partition(key(5)) shouldBe 4
    PartitionerOnToken(10).partition(key(6)) shouldBe 8
    PartitionerOnToken(10).partition(key(7)) shouldBe 6
  }

}
