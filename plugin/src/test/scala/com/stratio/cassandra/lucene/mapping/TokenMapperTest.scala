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

import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.util.ByteBufferUtils.toHex
import org.apache.cassandra.dht.Murmur3Partitioner
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** Tests for [[TokenMapper]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class TokenMapperTest extends BaseScalaTest {

  test("long value") {
    val v1 = List(Long.MinValue, -12345L, -123L, -2L, -1L, 0L, 1L, 2L, 123L, 12345L, Long.MaxValue)
    val v2 = v1.map(new Murmur3Partitioner.LongToken(_)).map(TokenMapper.longValue)
    v1 shouldBe v2
  }

  test("bytes ref") {
    def hex(n: Long) = toHex(TokenMapper.bytesRef(new Murmur3Partitioner.LongToken(n)))
    hex(Long.MinValue) shouldBe "2000000000000000000000"
    hex(Long.MaxValue) shouldBe "20017f7f7f7f7f7f7f7f7f"
    hex(-1) shouldBe "20007f7f7f7f7f7f7f7f7f"
    hex(0) shouldBe "2001000000000000000000"
    hex(1) shouldBe "2001000000000000000001"
  }
}
