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

import java.nio.ByteBuffer
import java.util
import java.util.{Arrays, Collections}

import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.util.ByteBufferUtils
import org.apache.cassandra.config.{CFMetaData, DatabaseDescriptor}
import org.apache.cassandra.db.marshal.{AsciiType, CompositeType, Int32Type, LongType}
import org.apache.cassandra.dht.Murmur3Partitioner
import org.apache.lucene.util.BytesRef
import org.junit.Assert._
import org.junit.Ignore
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunnerDelegate
import org.scalatest.junit.JUnitRunner


/** Tests for [[ClusteringMapper]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */

@Ignore
@PowerMockRunnerDelegate(classOf[JUnitRunner])
@PrepareForTest(Array(classOf[DatabaseDescriptor]))
class ClusteringMapperTest extends BaseScalaTest {

  def byteBuffer(globalCType: CompositeType, values: ByteBuffer*): ByteBuffer = (globalCType.builder /: values) (_ add _) build()
  def bytesRef(globalCType: CompositeType, values: ByteBuffer*): BytesRef = ByteBufferUtils.bytesRef(byteBuffer(globalCType,values:_*))

  test("Clustering order test") {

    PowerMockito.mockStatic(classOf[DatabaseDescriptor])
    PowerMockito.when(DatabaseDescriptor.getPartitioner, Murmur3Partitioner.instance)

    val cfm: CFMetaData = CFMetaData.Builder.createDense("test_keyspace", "tets_table", false, false)
      .addPartitionKey("key", Int32Type.instance)
      .addClusteringColumn("c1", AsciiType.instance)
      .addRegularColumn("value", LongType.instance)
      .build
    val clusteringMapper: ClusteringMapper = new ClusteringMapper(cfm)

    val globalCType: CompositeType = clusteringMapper.globalCType

    val row1: BytesRef = bytesRef(globalCType, LongType.instance.decompose(-5000L), AsciiType.instance.decompose("aaa"))
    val row2: BytesRef = bytesRef(globalCType, LongType.instance.decompose(-4999L), AsciiType.instance.decompose("a"))
    val row3: BytesRef = bytesRef(globalCType, LongType.instance.decompose(-4999L), AsciiType.instance.decompose("b"))
    val row4: BytesRef = bytesRef(globalCType, LongType.instance.decompose(-4999L), AsciiType.instance.decompose("c"))
    val row5: BytesRef = bytesRef(globalCType, LongType.instance.decompose(-4999L), AsciiType.instance.decompose("d"))
    val row6: BytesRef = bytesRef(globalCType, LongType.instance.decompose(2L), AsciiType.instance.decompose("b"))
    val row7: BytesRef = bytesRef(globalCType, LongType.instance.decompose(2L), AsciiType.instance.decompose("a"))
    val row8: BytesRef = bytesRef(globalCType, LongType.instance.decompose(2L), AsciiType.instance.decompose("c"))
    val row9: BytesRef = bytesRef(globalCType, LongType.instance.decompose(2L), AsciiType.instance.decompose("b"))
    val row10: BytesRef = bytesRef(globalCType, LongType.instance.decompose(15L), AsciiType.instance.decompose("b"))
    val row11: BytesRef = bytesRef(globalCType, LongType.instance.decompose(16L), AsciiType.instance.decompose("b"))
    val row12: BytesRef = bytesRef(globalCType, LongType.instance.decompose(16L), AsciiType.instance.decompose("d"))
    val row13: BytesRef = bytesRef(globalCType, LongType.instance.decompose(16L), AsciiType.instance.decompose("a"))
    val bytesRefList: java.util.List[BytesRef] = new util.ArrayList[BytesRef](util.Arrays.asList(row1, row2, row3, row4, row5, row6, row7, row8, row9, row10, row11, row12, row13))
    val bytesRefList2: java.util.List[BytesRef] = new util.ArrayList[BytesRef](util.Arrays.asList(row1, row2, row3, row4, row5, row6, row7, row8, row9, row10, row11, row12, row13))
    Collections.reverse(bytesRefList)
    bytesRefList.sort(clusteringMapper)

    assertArrayEquals("Token collation is wrong", bytesRefList.toArray, bytesRefList2.toArray)
  }
}
