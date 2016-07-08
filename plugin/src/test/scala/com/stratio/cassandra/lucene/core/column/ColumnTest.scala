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

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * Tests [[Column]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class ColumnTest extends BaseTest {

  test("set default attributes") {
    val column = Column("cell")
    column.cellName shouldBe "cell"
    column.mapperName shouldBe "cell"
    column.mapperNames shouldBe List("cell")
    column.fieldName shouldBe "cell"
    column.value shouldBe None
    column.deletionTime shouldBe Column.NO_DELETION_TIME
  }

  test("set all attributes") {
    val column = Column("cell")
      .withUDTName("u1")
      .withUDTName("u2")
      .withMapName("m1")
      .withMapName("m2")
      .withDeletionTime(10)
      .withValue(5)
    column.cellName shouldBe "cell"
    column.mapperName shouldBe "cell.u1.u2"
    column.mapperNames shouldBe List("cell", "u1", "u2")
    column.fieldName shouldBe "cell.u1.u2$m1$m2"
    column.value shouldBe Some(5)
    column.deletionTime shouldBe 10
  }

  test("isDeleted because of value") {
    val column = Column("cell")
    column.isDeleted(0) shouldBe true
    column.isDeleted(Int.MinValue) shouldBe true
    column.isDeleted(Int.MaxValue) shouldBe true
    column.withValue(7).isDeleted(0) shouldBe false
    column.withValue(7).isDeleted(Int.MinValue) shouldBe false
    column.withValue(7).isDeleted(Int.MaxValue) shouldBe true
  }

  test("isDeleted because of deletion time") {
    val column = Column("cell").withDeletionTime(10)
    column.isDeleted(9) shouldBe true
    column.isDeleted(10) shouldBe true
    column.isDeleted(11) shouldBe true
    column.withValue(7).isDeleted(9) shouldBe false
    column.withValue(7).isDeleted(10) shouldBe true
    column.withValue(7).isDeleted(11) shouldBe true
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
    Column("a") + Columns(Column("b"), Column("c")) shouldBe Columns(Column("a"), Column("b"), Column("c"))
  }

  test("toString with default attributes") {
    Column("cell").toString shouldBe
      s"Column{cell=cell, name=cell, value=null, deletionTime=${Column.NO_DELETION_TIME}}"
  }

  test("toString with all attributes") {
    Column("cell")
      .withUDTName("u1")
      .withUDTName("u2")
      .withMapName("m1")
      .withMapName("m2")
      .withDeletionTime(10)
      .withValue(5)
      .toString shouldBe
      "Column{cell=cell, name=cell.u1.u2$m1$m2, value=5, deletionTime=10}"
  }
}
