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

import java.math.{BigDecimal, BigInteger}
import java.util.{Date, UUID}

import com.google.common.collect.Lists
import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.BaseScalaTest._
import com.stratio.cassandra.lucene.column.{Column, Columns}
import com.stratio.cassandra.lucene.mapping.ColumnsMapper._
import org.apache.cassandra.config.ColumnDefinition
import org.apache.cassandra.db.marshal._
import org.apache.cassandra.db.rows.{BufferCell, Cell}
import org.apache.cassandra.utils.UUIDGen
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

/** Tests for [[ColumnsMapper]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class ColumnsMapperTest extends BaseScalaTest {

  test("columns from plain cells") {
    def test[A](abstractType: AbstractType[A], value: A) = {
      val column = Column("cell")
      columns(column, abstractType, abstractType.decompose(value)) shouldBe
        Columns(column.withValue(value))
    }
    test(ascii, "Ab")
    test(utf8, "Ab")
    test(int32, 7.asInstanceOf[Integer])
    test(float, 7.3f.asInstanceOf[java.lang.Float])
    test(long, 7l.asInstanceOf[java.lang.Long])
    test(double, 7.3d.asInstanceOf[java.lang.Double])
    test(integer, new BigInteger("7"))
    test(decimal, new BigDecimal("7.3"))
    test(uuid, UUID.randomUUID)
    test(lexicalUuid, UUID.randomUUID)
    test(timeUuid, UUIDGen.getTimeUUID)
    test(timestamp, new Date)
    test(boolean, true.asInstanceOf[java.lang.Boolean])
  }

  test("columns from frozen set") {
    val column = Column("cell")
    val `type` = set(utf8, false)
    val bb = `type`.decompose(Set("a", "b").asJava)
    columns(column, `type`, bb) shouldBe Columns(column.withValue("a"), column.withValue("b"))
  }

  test("columns from frozen list") {
    val column = Column("cell")
    val `type` = list(utf8, false)
    val bb = `type`.decompose(List("a", "b").asJava)
    columns(column, `type`, bb) shouldBe Columns(column.withValue("a"), column.withValue("b"))
  }

  test("columns from frozen map") {
    val column = Column("cell")
    val `type` = map(utf8, utf8, true)
    val bb = `type`.decompose(Map("k1" -> "v1", "k2" -> "v2").asJava)
    columns(column, `type`, bb) shouldBe
      Columns(column.withMapName("k1").withValue("v1"), column.withMapName("k2").withValue("v2"))
  }

  test("columns from tuple") {
    val column = Column("cell")
    val `type` = new TupleType(Lists.newArrayList(utf8, utf8))
    val bb = TupleType.buildValue(Array(utf8.decompose("a"), utf8.decompose("b")))
    columns(column, `type`, bb) shouldBe
      Columns(column.withUDTName("0").withValue("a"), column.withUDTName("1").withValue("b"))
  }

  test("columns from UDT") {
    val column = Column("cell")
    val `type` = udt(List("a", "b"), List(utf8, utf8))
    val bb = TupleType.buildValue(Array(utf8.decompose("1"), utf8.decompose("2")))
    columns(column, `type`, bb) shouldBe
      Columns(column.withUDTName("a").withValue("1"), column.withUDTName("b").withValue("2"))
  }

  test("columns from regular cell") {
    val columnDefinition = ColumnDefinition.regularDef("ks", "cf", "cell", utf8)
    val cell = new BufferCell(
      columnDefinition,
      System.currentTimeMillis(),
      Cell.NO_TTL,
      Cell.NO_DELETION_TIME,
      utf8.decompose("a"),
      null)
    columns(cell) shouldBe Columns(Column("cell").withValue("a"))
  }
}