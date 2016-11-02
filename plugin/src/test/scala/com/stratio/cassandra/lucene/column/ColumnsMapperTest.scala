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
package com.stratio.cassandra.lucene.column

import java.math.{BigDecimal, BigInteger}
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import com.google.common.collect.Lists
import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.column.ColumnsMapper._
import com.stratio.cassandra.lucene.column.ColumnsMapperTest._
import org.apache.cassandra.config.ColumnDefinition
import org.apache.cassandra.db.marshal.{DecimalType, _}
import org.apache.cassandra.db.rows.Cell.NO_DELETION_TIME
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

  test("compose with basic types") {
    compose(ascii.decompose("aB"), ascii) shouldBe "aB"
    compose(utf8.decompose("aB"), utf8) shouldBe "aB"
    compose(byte.decompose(2.toByte), byte) shouldBe 2.toByte
    compose(short.decompose(2.toShort), short) shouldBe 2.toShort
    compose(int32.decompose(2), int32) shouldBe 2
    compose(long.decompose(2l), long) shouldBe 2l
    compose(float.decompose(2.1f), float) shouldBe 2.1f
    compose(double.decompose(2.1d), double) shouldBe 2.1d
  }

  test("compose with SimpleDateType") {
    val expected: Date = new SimpleDateFormat("yyyy-MM-ddZ").parse("1982-11-27+0000")
    val bb = date.fromTimeInMillis(expected.getTime)
    val actual = ColumnsMapper.compose(bb, date)
    actual shouldBe a[Date]
    actual shouldBe expected
  }

  test("columns from plain cells") {
    def test[A](abstractType: AbstractType[A], value: A) = {
      val column = Column("cell")
      columns(isTombstone = false, column, abstractType, abstractType.decompose(value)) shouldBe
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
    columns(isTombstone = false, column, `type`, bb) shouldBe
      Columns(column.withValue("a"), column.withValue("b"))
    columns(isTombstone = true, column, `type`, bb) shouldBe Columns(column)
  }

  test("columns from frozen list") {
    val column = Column("cell")
    val `type` = list(utf8, false)
    val bb = `type`.decompose(List("a", "b").asJava)
    columns(isTombstone = false, column, `type`, bb) shouldBe
      Columns(column.withValue("a"), column.withValue("b"))
    columns(isTombstone = true, column, `type`, bb) shouldBe Columns(column)
  }

  test("columns from frozen map") {
    val column = Column("cell")
    val `type` = map(utf8, utf8, true)
    val bb = `type`.decompose(Map("k1" -> "v1", "k2" -> "v2").asJava)
    columns(isTombstone = false, column, `type`, bb) shouldBe
      Columns(column.withMapName("k1").withValue("v1"), column.withMapName("k2").withValue("v2"))
    columns(isTombstone = true, column, `type`, bb) shouldBe Columns(column)
  }

  test("columns from tuple") {
    val column = Column("cell")
    val `type` = new TupleType(Lists.newArrayList(utf8, utf8))
    val bb = TupleType.buildValue(Array(utf8.decompose("a"), utf8.decompose("b")))
    columns(isTombstone = false, column, `type`, bb) shouldBe
      Columns(column.withUDTName("0").withValue("a"), column.withUDTName("1").withValue("b"))
    columns(isTombstone = true, column, `type`, bb) shouldBe Columns(column)
  }

  test("columns from UDT") {
    val column = Column("cell")
    val `type` = udt(List("a", "b"), List(utf8, utf8))
    val bb = TupleType.buildValue(Array(utf8.decompose("1"), utf8.decompose("2")))
    columns(isTombstone = false, column, `type`, bb) shouldBe
      Columns(column.withUDTName("a").withValue("1"), column.withUDTName("b").withValue("2"))
    columns(isTombstone = true, column, `type`, bb) shouldBe Columns(column)
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
    columns(cell) shouldBe Columns(Column("cell").withValue("a").withDeletionTime(NO_DELETION_TIME))
  }

  test("supports regular") {
    supports(utf8, List(classOf[String])) shouldBe true
    supports(utf8, List(classOf[Number])) shouldBe false
    supports(utf8, List(classOf[String], classOf[Number])) shouldBe true
    supports(utf8, List(classOf[Number], classOf[String])) shouldBe true
  }

  test("supports list") {
    supports(list(utf8, false), List(classOf[String])) shouldBe true
    supports(list(utf8, true), List(classOf[String])) shouldBe true
    supports(list(int32, false), List(classOf[String])) shouldBe false
    supports(list(int32, true), List(classOf[String])) shouldBe false
  }

  test("supports set") {
    supports(set(utf8, false), List(classOf[String])) shouldBe true
    supports(set(utf8, true), List(classOf[String])) shouldBe true
    supports(set(int32, false), List(classOf[String])) shouldBe false
    supports(set(int32, true), List(classOf[String])) shouldBe false
  }

  test("supports map") {
    supports(map(int32, utf8, false), List(classOf[String])) shouldBe true
    supports(map(int32, utf8, true), List(classOf[String])) shouldBe true
    supports(map(utf8, int32, false), List(classOf[String])) shouldBe false
    supports(map(utf8, int32, true), List(classOf[String])) shouldBe false
  }

  test("supports reversed") {
    supports(reversed(utf8), List(classOf[String])) shouldBe true
    supports(reversed(int32), List(classOf[String])) shouldBe false
    supports(reversed(utf8), List(classOf[String], classOf[Number])) shouldBe true
    supports(reversed(utf8), List(classOf[Number], classOf[String])) shouldBe true
  }

  test("child regular") {
    childType(utf8, "") shouldBe None
  }

  test("child UDT") {
    val userType = udt(List("a", "b"), List(utf8, int32))
    childType(userType, "a") shouldBe Some(utf8)
    childType(userType, "b") shouldBe Some(int32)
    childType(userType, "c") shouldBe None
  }

  test("child regular set") {
    val setType = set(utf8, true)
    childType(setType, "a") shouldBe None
  }

  test("child UDT set") {
    val userType = udt(List("a", "b"), List(utf8, int32))
    val setType = set(userType, true)
    childType(setType, "a") shouldBe Some(utf8)
    childType(setType, "b") shouldBe Some(int32)
    childType(setType, "c") shouldBe None
  }

  test("child frozen UDT set") {
    val userType = udt(List("a", "b"), List(utf8, int32))
    val setType = set(userType, false)
    childType(setType, "a") shouldBe Some(utf8)
    childType(setType, "b") shouldBe Some(int32)
    childType(setType, "c") shouldBe None
  }

  test("child regular list") {
    val listType = list(utf8, true)
    childType(listType, "a") shouldBe None
  }

  test("child UDT list") {
    val userType = udt(List("a", "b"), List(utf8, int32))
    val listType = list(userType, true)
    childType(listType, "a") shouldBe Some(utf8)
    childType(listType, "b") shouldBe Some(int32)
    childType(listType, "c") shouldBe None
  }

  test("child frozen UDT list") {
    val userType = udt(List("a", "b"), List(utf8, int32))
    val listType = list(userType, false)
    childType(listType, "a") shouldBe Some(utf8)
    childType(listType, "b") shouldBe Some(int32)
    childType(listType, "c") shouldBe None
  }

  test("child regular map") {
    val mapType = map(utf8, utf8, true)
    childType(mapType, "a") shouldBe None
  }

  test("child UDT map") {
    val userType = udt(List("a", "b"), List(utf8, int32))
    val mapType = map(utf8, userType, true)
    childType(mapType, "a") shouldBe Some(utf8)
    childType(mapType, "b") shouldBe Some(int32)
    childType(mapType, "c") shouldBe None
  }

  test("child frozen UDT map") {
    val userType = udt(List("a", "b"), List(utf8, int32))
    val mapType = map(utf8, userType, false)
    childType(mapType, "a") shouldBe Some(utf8)
    childType(mapType, "b") shouldBe Some(int32)
    childType(mapType, "c") shouldBe None
  }
}

object ColumnsMapperTest {

  val utf8 = UTF8Type.instance
  val ascii = AsciiType.instance
  val int32 = Int32Type.instance
  val byte = ByteType.instance
  val short = ShortType.instance
  val long = LongType.instance
  val float = FloatType.instance
  val double = DoubleType.instance
  val date = SimpleDateType.instance
  val integer = IntegerType.instance
  val uuid = UUIDType.instance
  val lexicalUuid = LexicalUUIDType.instance
  val timeUuid = TimeUUIDType.instance
  val decimal = DecimalType.instance
  val timestamp = TimestampType.instance
  val boolean = BooleanType.instance

  def set[A](elements: AbstractType[A], multiCell: Boolean): SetType[A] =
    SetType.getInstance(elements, multiCell)

  def list[A](elements: AbstractType[A], multiCell: Boolean): ListType[A] =
    ListType.getInstance(elements, multiCell)

  def map[A, B](keys: AbstractType[A], values: AbstractType[B], multiCell: Boolean): MapType[A, B] =
    MapType.getInstance(keys, values, multiCell)

  def udt(names: List[String], types: List[AbstractType[_]]): UserType =
    new UserType(
      "ks",
      utf8.decompose("cell"),
      Lists.newArrayList(names.map(x => utf8.decompose(x)).asJava),
      Lists.newArrayList(types.asJava),false)

  def reversed[A](base: AbstractType[A]): ReversedType[A] = ReversedType.getInstance(base)
}
