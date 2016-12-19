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
package com.stratio.cassandra.lucene.mapping

import java.lang.Long
import java.nio.ByteBuffer
import java.util

import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.util.ByteBufferUtils
import org.apache.cassandra.db.marshal._
import org.apache.lucene.util.BytesRef
import org.junit.Assert._

import scala.Ordering.comparatorToOrdering
import scala.collection.mutable.ListBuffer

/** Tests for [[ClusteringMapper]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */


class ClusteringMapperTest extends BaseScalaTest {

  def bytesRef(globalCType: CompositeType, values: ByteBuffer*): BytesRef = ByteBufferUtils.bytesRef(byteBuffer(globalCType, values: _*))

  def byteBuffer(globalCType: CompositeType, values: ByteBuffer*): ByteBuffer = (globalCType.builder /: values) (_ add _) build()


  test("long to bytebuffer tipe long ") {
    for (i <- -10000 until 10000) {
      org.junit.Assert.assertEquals(new Long(i), LongType.instance.compose(LongType.instance.decompose(new Long(i))))
    }
  }

  test("basic long sorting") {
    val longValues: scala.collection.mutable.ListBuffer[Long] = ListBuffer(2L, 5L, 6L, 3L, -45L, -105L, -278L, 46L, 28L)
    val sorted: scala.collection.mutable.ListBuffer[Long] = ListBuffer(-278L, -105L, -45L, 2L, 3L, 5L, 6L, 28L, 46L)
    assertLongListEquals(sorted, longValues.sorted(Ordering[Long]))
  }
  test("basic long sorting 2") {

    val longValues: scala.collection.mutable.ListBuffer[Long] = ListBuffer(2L, 5L, 6L, 3L, -45L, -105L, -278L, 46L, 28L)
    val sorted: scala.collection.mutable.ListBuffer[Long] = ListBuffer(-278L, -105L, -45L, 2L, 3L, 5L, 6L, 28L, 46L)

    val longValuesBB: scala.collection.mutable.ListBuffer[ByteBuffer] = longValues.map(LongType.instance decompose)
    val sortedValuesBB: scala.collection.mutable.ListBuffer[ByteBuffer] = sorted.map(LongType.instance decompose)

    sortedValuesBB.map((bb: ByteBuffer) => {
      System.out.println("ByteBuffer " + bb + " in Long is " + LongType.instance.compose(bb))
    })
    assertListEquals(sortedValuesBB, longValuesBB.sorted(comparatorToOrdering(LongType.instance)), List(LongType.instance))
  }

  test("Clustering comparator comparing Longs ") {
    val types: util.List[AbstractType[_]] = new util.ArrayList[AbstractType[_]]()
    types.add(LongType.instance)

    val clusteringComparator: ClusteringComparator = new ClusteringComparator(types)

    val longValues: Array[Long] = Array(2L, 5L, 6L, 3L, -45L, -105L, -278L, 46L, 28L)
    val sorted: Array[Long] = Array(-278L, -105L, -45L, 2L, 3L, 5L, 6L, 28L, 46L)

    val longValuesBB: Array[ByteBuffer] = longValues.map(LongType.instance decompose).map(ByteBufferUtils compose _)
    val sortedValuesBB: Array[ByteBuffer] = sorted.map(LongType.instance decompose).map(ByteBufferUtils compose _)

    sortedValuesBB.map((bb: ByteBuffer) => {
      System.out.println("ByteBuffer " + bb + " in Long is " + LongType.instance.compose(ByteBufferUtils.decompose(bb)(0)))
    })
    val orderingFromCmp: Ordering[ByteBuffer] = comparatorToOrdering(clusteringComparator)
    //java.util.Arrays.sort(longValuesBB, clusteringComparator)

    assertListEquals(sortedValuesBB, longValuesBB.sorted(comparatorToOrdering(clusteringComparator)),List(LongType.instance))
  }


  test("Clustering comparator comparing Long, Long ") {

    val types: util.List[AbstractType[_]] = new util.ArrayList[AbstractType[_]]()
    types.add(LongType.instance)
    types.add(LongType.instance)

    val clusteringComparator: ClusteringComparator = new ClusteringComparator(types)



    val longValuesBB: scala.collection.mutable.ListBuffer[ByteBuffer] = ListBuffer(2L, 5L, 6L, 3L, -45L, -105L, -278L, 46L, 28L)
      .map(LongType.instance decompose _)


    val longValuesBB2: scala.collection.mutable.ListBuffer[ByteBuffer] = ListBuffer(-50L, -41L, -74L, 8L, 4L, 5L, 6L, 3L, -504L)
      .map(LongType.instance decompose _)

    val zippedByteBufferValues = longValuesBB.zip(longValuesBB2)

    val composedByteBuffers: ListBuffer[ByteBuffer] = ListBuffer()
    for (i <- zippedByteBufferValues.indices) {
      composedByteBuffers += ByteBufferUtils.compose(zippedByteBufferValues(i)._1, zippedByteBufferValues(i)._2)
    }

    val sortedBB: scala.collection.mutable.ListBuffer[ByteBuffer] = ListBuffer(-278L, -105L, -45L, 2L, 3L, 5L, 6L, 28L, 46L)
      .map(LongType.instance decompose _)

    val sortedBB2: scala.collection.mutable.ListBuffer[ByteBuffer] = ListBuffer(6L, 5L, 4L, -50L, 8L, -41L, -74L, -504L, 3L)
      .map(LongType.instance decompose _)

    val zippedSorted = sortedBB.zip(sortedBB2)
    val composedSorted: ListBuffer[ByteBuffer] = ListBuffer()
    for (i <- zippedSorted.indices) {
      composedSorted += ByteBufferUtils.compose(zippedSorted(i)._1, zippedSorted(i)._2)
    }

    assertListEquals(composedSorted, composedByteBuffers.sorted(comparatorToOrdering(clusteringComparator)), List(LongType.instance, LongType.instance))

  }


  test("Clustering mapper comparing Ascii and reversed") {
    val types: util.List[AbstractType[_]] = new util.ArrayList[AbstractType[_]]()
    types.add(AsciiType.instance)

    val clusteringComparator: ClusteringComparator = new ClusteringComparator(types)



    val asciiValues: scala.collection.mutable.ListBuffer[String] = ListBuffer("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "40", "41", "42", "60", "205")
    val asciiValuesBB: scala.collection.mutable.ListBuffer[ByteBuffer]  =asciiValues.map(AsciiType.instance decompose _)


    val orderingFromCmp: Ordering[ByteBuffer] = comparatorToOrdering(AsciiType.instance)
    //java.util.Arrays.sort(longValuesBB, clusteringComparator)


    val sortedBB=asciiValuesBB.sorted(orderingFromCmp)
    val sortedValues : scala.collection.mutable.ListBuffer[String]= sortedBB.map(AsciiType.instance compose _)

    System.out.println("sorting "+asciiValues+ " resutls in: "+sortedValues)

  }



  def bbToString(bb: ByteBuffer, types: List[AbstractType[_]]): String = {
    val sb: StringBuilder = new StringBuilder
    val components: Array[ByteBuffer] = ByteBufferUtils.decompose(bb)
    val componentsType = types
    sb.append("[")
    for (i <- 0 until components.length) {
      val actual: ByteBuffer = components(i)
      val actualType = componentsType(i)
      sb.append(actualType.compose(actual))
      if (i != components.length - 1) {
        sb.append(", ")

      }
    }
    sb.append("]")
    sb.toString()
  }

  def assertListEquals(l1: ListBuffer[ByteBuffer], l2: ListBuffer[ByteBuffer], types: List[AbstractType[_]]) = {
    if (l1.length != l2.length) {
      assertTrue("List lenght differs", false)
    } else {
      var equals = true
      for (i <- 0 until l1.length) {
        if (!l1(i).equals(l2(i))) {
          equals = false
        }
      }
      if (equals != true) {
        val sb: StringBuilder = new StringBuilder
        sb.append("l1[ ")
        for (elem <- l1) {
          sb.append(bbToString(elem, types))
          sb.append(", ")
        }

        sb.append("]!= l2[")
        for (elem <- l2) {
          sb.append(bbToString(elem, types))
          sb.append(", ")
        }
        assertTrue(sb.toString, false)
      }
    }
  }
  def assertListEquals(l1: Array[ByteBuffer], l2: Array[ByteBuffer], types: List[AbstractType[_]]) = {
    if (l1.length != l2.length) {
      assertTrue("List lenght differs", false)
    } else {
      var equals = true
      for (i <- 0 until l1.length) {
        if (!l1(i).equals(l2(i))) {
          equals = false
        }
      }
      if (equals != true) {
        val sb: StringBuilder = new StringBuilder
        sb.append("l1[ ")
        for (elem <- l1) {
          sb.append(bbToString(elem, types))
          sb.append(", ")
        }

        sb.append("]!= l2[")
        for (elem <- l2) {
          sb.append(bbToString(elem, types))
          sb.append(", ")
        }
        assertTrue(sb.toString, false)
      }
    }
  }
  def assertLongListEquals(l1: ListBuffer[Long], l2: ListBuffer[Long]) = {
    if (l1.length != l2.length) {
      assertTrue("List lenght differs", false)
    } else {
      for (i <- 0 until l1.length) {
        assertEquals(l1(i), l2(i))
      }

    }
  }
}



