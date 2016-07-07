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
package com.stratio.cassandra.lucene.core.column

import java.math.{BigDecimal, BigInteger}
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import com.google.common.collect.Lists
import com.stratio.cassandra.lucene.core.column.ColumnsMapper._
import org.apache.cassandra.config.ColumnDefinition
import org.apache.cassandra.db.marshal._
import org.apache.cassandra.db.rows.{BufferCell, Cell}
import org.apache.cassandra.utils.UUIDGen
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

/**
  * Tests [[Column]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class ColumnsMapperTest extends BaseTest {

  test("compose with basic types") {
    compose(AsciiType.instance.decompose("aB"), AsciiType.instance) shouldBe "aB"
    compose(UTF8Type.instance.decompose("aB"), UTF8Type.instance) shouldBe "aB"
    compose(ByteType.instance.decompose(2.toByte), ByteType.instance) shouldBe 2.toByte
    compose(ShortType.instance.decompose(2.toShort), ShortType.instance) shouldBe 2.toShort
    compose(Int32Type.instance.decompose(2), Int32Type.instance) shouldBe 2
    compose(LongType.instance.decompose(2l), LongType.instance) shouldBe 2l
    compose(FloatType.instance.decompose(2.1f), FloatType.instance) shouldBe 2.1f
    compose(DoubleType.instance.decompose(2.1d), DoubleType.instance) shouldBe 2.1d
  }

  test("compose with SimpleDateType") {
    val expected: Date = new SimpleDateFormat("yyyy-MM-dd").parse("1982-11-27")
    val bb = SimpleDateType.instance.fromTimeInMillis(expected.getTime)
    val actual = ColumnsMapper.compose(bb, SimpleDateType.instance)
    actual shouldBe a[Date]
    actual shouldBe expected
  }

  test("columns from plain cells") {
    def test[A](abstractType: AbstractType[A], value: A) = {
      val column = Column("cell")
      columns(isTombstone = false, column, abstractType, abstractType.decompose(value)) shouldBe
        Columns(column.withValue(value))
    }
    test(AsciiType.instance, "Ab")
    test(UTF8Type.instance, "Ab")
    test(Int32Type.instance, 7.asInstanceOf[Integer])
    test(FloatType.instance, 7.3f.asInstanceOf[java.lang.Float])
    test(LongType.instance, 7l.asInstanceOf[java.lang.Long])
    test(DoubleType.instance, 7.3d.asInstanceOf[java.lang.Double])
    test(IntegerType.instance, new BigInteger("7"))
    test(DecimalType.instance, new BigDecimal("7.3"))
    test(UUIDType.instance, UUID.randomUUID)
    test(LexicalUUIDType.instance, UUID.randomUUID)
    test(TimeUUIDType.instance, UUIDGen.getTimeUUID)
    test(TimestampType.instance, new Date)
    test(BooleanType.instance, true.asInstanceOf[java.lang.Boolean])
  }

  test("columns from frozen set") {
    val column = Column("cell")
    val `type` = SetType.getInstance(UTF8Type.instance, false)
    val bb = `type`.decompose(Set("a", "b").asJava)
    columns(isTombstone = false, column, `type`, bb) shouldBe Columns(column.withValue("a"), column.withValue("b"))
    columns(isTombstone = true, column, `type`, bb) shouldBe Columns(column)
  }

  test("columns from frozen list") {
    val column = Column("cell")
    val `type` = ListType.getInstance(UTF8Type.instance, false)
    val bb = `type`.decompose(List("a", "b").asJava)
    columns(isTombstone = false, column, `type`, bb) shouldBe
      Columns(column.withValue("a"), column.withValue("b"))
    columns(isTombstone = true, column, `type`, bb) shouldBe
      Columns(column)
  }

  test("columns from frozen map") {
    val column = Column("cell")
    val `type` = MapType.getInstance(UTF8Type.instance, UTF8Type.instance, true)
    val bb = `type`.decompose(Map("k1" -> "v1", "k2" -> "v2").asJava)
    columns(isTombstone = false, column, `type`, bb) shouldBe
      Columns(column.withMapName("k1").withValue("v1"), column.withMapName("k2").withValue("v2"))
    columns(isTombstone = true, column, `type`, bb) shouldBe
      Columns(column)
  }

  test("columns from tuple") {
    val column = Column("cell")
    val `type` = new TupleType(Lists.newArrayList(UTF8Type.instance, UTF8Type.instance))
    val bb = TupleType.buildValue(Array(UTF8Type.instance.decompose("a"), UTF8Type.instance.decompose("b")))
    columns(isTombstone = false, column, `type`, bb) shouldBe
      Columns(column.withUDTName("0").withValue("a"), column.withUDTName("1").withValue("b"))
    columns(isTombstone = true, column, `type`, bb) shouldBe
      Columns(column)
  }

  test("columns from UDT") {
    val column = Column("cell")
    val `type` = new UserType(
      "ks",
      UTF8Type.instance.decompose("cell"),
      Lists.newArrayList(UTF8Type.instance.decompose("a"), UTF8Type.instance.decompose("b")),
      Lists.newArrayList(UTF8Type.instance, UTF8Type.instance))
    val bb = TupleType.buildValue(Array(UTF8Type.instance.decompose("1"), UTF8Type.instance.decompose("2")))
    columns(isTombstone = false, column, `type`, bb) shouldBe
      Columns(column.withUDTName("a").withValue("1"), column.withUDTName("b").withValue("2"))
    columns(isTombstone = true, column, `type`, bb) shouldBe
      Columns(column)
  }

  test("columns from regular cell") {
    val columnDefinition = ColumnDefinition.regularDef("ks", "cf", "cell", UTF8Type.instance)
    val cell = new BufferCell(
      columnDefinition,
      System.currentTimeMillis(),
      Cell.NO_TTL,
      Cell.NO_DELETION_TIME,
      UTF8Type.instance.decompose("a"),
      null)
    columns(cell) shouldBe Columns(Column("cell").withValue("a").withDeletionTime(Cell.NO_DELETION_TIME))
  }
}
