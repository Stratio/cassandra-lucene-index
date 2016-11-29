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

import java.text.SimpleDateFormat
import java.util.Date

import com.stratio.cassandra.lucene.column.Column._
import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.BaseScalaTest._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** Tests for [[Column]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class ColumnTest extends BaseScalaTest {

  test("set default attributes") {
    val column = Column("cell")
    column.cellName shouldBe "cell"
    column.mapperName shouldBe "cell"
    column.mapperNames shouldBe List("cell")
    column.fieldName shouldBe "cell"
    column.value shouldBe None
  }

  test("set all attributes") {
    val column = Column("cell")
      .withUDTName("u1")
      .withUDTName("u2")
      .withMapName("m1")
      .withMapName("m2")
      .withValue(5)
    column.cellName shouldBe "cell"
    column.mapperName shouldBe "cell.u1.u2"
    column.mapperNames shouldBe List("cell", "u1", "u2")
    column.fieldName shouldBe "cell.u1.u2$m1$m2"
    column.value shouldBe Some(5)
  }

  test("fieldName") {
    Column("c").fieldName("f") shouldBe "f"
    Column("c").withUDTName("u").fieldName("f") shouldBe "f"
    Column("c").withMapName("m").fieldName("f") shouldBe "f$m"
    Column("c").withUDTName("u").withMapName("m").fieldName("f") shouldBe "f$m"
  }

  test("parse") {
    Column.parse("c") shouldBe Column("c")
    Column.parse("c.u") shouldBe Column("c").withUDTName("u")
    Column.parse("c$m") shouldBe Column("c").withMapName("m")
    Column.parse("c.u$m") shouldBe Column("c").withUDTName("u").withMapName("m")
    Column.parse("c.u1.u2$m1$m2") shouldBe Column("c")
      .withUDTName("u1")
      .withUDTName("u2")
      .withMapName("m1")
      .withMapName("m2")
  }

  test("add column") {
    Column("a") + Column("b") shouldBe Columns(Column("a"), Column("b"))
    Column("b") + Column("a") shouldBe Columns(Column("b"), Column("a"))
  }

  test("add columns") {
    Column("a") + Columns(Column("b"), Column("c")) shouldBe
      Columns(Column("a"), Column("b"), Column("c"))
  }

  test("toString with default attributes") {
    Column("cell").toString shouldBe
      s"Column{cell=cell, name=cell, value=None}"
  }

  test("toString with all attributes") {
    Column("cell")
      .withUDTName("u1")
      .withUDTName("u2")
      .withMapName("m1")
      .withMapName("m2")
      .withValue(5)
      .toString shouldBe
      "Column{cell=cell, name=cell.u1.u2$m1$m2, value=Some(5)}"
  }

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
    val actual = compose(bb, date)
    actual shouldBe a[Date]
    actual shouldBe expected
  }

  test("with composed value") {
    Column("c").withValue(ascii.decompose("aB"), ascii) shouldBe Column("c").withValue("aB")
    Column("c").withValue(utf8.decompose("aB"), utf8) shouldBe Column("c").withValue("aB")
    Column("c").withValue(byte.decompose(2.toByte), byte) shouldBe Column("c").withValue(2.toByte)
    Column("c").withValue(short.decompose(2.toShort), short) shouldBe Column("c").withValue(2.toShort)
    Column("c").withValue(int32.decompose(2), int32) shouldBe Column("c").withValue(2)
    Column("c").withValue(long.decompose(2l), long) shouldBe Column("c").withValue(2l)
    Column("c").withValue(float.decompose(2.1f), float) shouldBe Column("c").withValue(2.1f)
    Column("c").withValue(double.decompose(2.1d), double) shouldBe Column("c").withValue(2.1d)
  }

  test("with value") {
    Column("c").withValue(null) shouldBe Column("c")
    Column("c").withValue(3).withValue(null) shouldBe Column("c")
    Column("c").withValue(3).withValue(4) shouldBe Column("c").withValue(4)
    Column("c").withValue(null).withValue(3) shouldBe Column("c").withValue(3)
    Column("c").withValue(3).withValue(3) shouldBe Column("c").withValue(3)
  }
}
