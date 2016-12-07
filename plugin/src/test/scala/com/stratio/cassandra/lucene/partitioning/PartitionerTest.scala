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
    Partitioner.fromJson("{}") shouldBe PartitionerOnNone()
  }

  test("num partitions wiht none partitioner") {
    PartitionerOnNone().allPartitions shouldBe List(0)
  }

  test("num partitions with token partitioner") {
    PartitionerOnToken(4).allPartitions shouldBe List(0, 1, 2, 3)
  }

  def key(n: Int): DecoratedKey = Murmur3Partitioner.instance.decorateKey(int32.decompose(n))

}
