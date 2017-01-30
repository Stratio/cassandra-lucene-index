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
package com.stratio.cassandra.lucene.util

import java.nio.ByteBuffer

import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.BaseScalaTest._
import org.apache.cassandra.db.marshal.CompositeType
import org.apache.cassandra.utils.ByteBufferUtil

/** Class for testing [[ByteBufferUtils]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class BytBufferUtilsTest extends BaseScalaTest {

  test("test BytesRef") {
    val t = CompositeType.getInstance(utf8, int32)
    val in = t.builder().add(utf8.decompose("monkey")).add(int32.decompose(1)).build()
    val bytesRef = ByteBufferUtils.bytesRef(in)
    val out1 = ByteBufferUtils.byteBuffer(bytesRef)
    ByteBufferUtil.compareUnsigned(in, out1) shouldBe 0
    val out2 = ByteBufferUtils.byteBuffer(bytesRef)
    ByteBufferUtil.compareUnsigned(in, out2) shouldBe 0
  }

  test("isEmpty true") {
    val bb = ByteBuffer.allocate(0)
    ByteBufferUtils.isEmpty(bb) shouldBe true
  }

  test("isEmpty false") {
    val bb = ByteBuffer.allocate(10)
    ByteBufferUtils.isEmpty(bb) shouldBe false
  }

  test("split simple") {
    val bb = utf8.decompose("test")
    ByteBufferUtils.split(bb, utf8).length shouldBe 1
  }

  test("split composite") {
    val t = CompositeType.getInstance(utf8, int32)
    val bb = t.builder.add(utf8.decompose("1")).add(int32.decompose(1)).build
    ByteBufferUtils.split(bb, t).length shouldBe 2
  }

  test("compose-decompose") {
    val bbs = ByteBufferUtils.decompose(
      ByteBufferUtils.compose(
        utf8.decompose("test"),
        int32.decompose(999),
        boolean.decompose(true)))
    bbs.length shouldBe 3
    utf8.compose(bbs(0)) shouldBe "test"
    int32.compose(bbs(1)) shouldBe 999
    boolean.compose(bbs(2)) shouldBe true
  }

  test("compose-decompose empty") {
    ByteBufferUtils.decompose(ByteBufferUtils.compose()).length shouldBe 0
  }
}
