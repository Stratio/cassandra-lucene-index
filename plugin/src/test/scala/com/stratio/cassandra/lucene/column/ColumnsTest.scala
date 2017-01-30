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

import com.stratio.cassandra.lucene.BaseScalaTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** Tests for [[Column]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class ColumnsTest extends BaseScalaTest {

  test("build empty") {
    val columns = Columns()
    columns.size shouldBe 0
    columns.isEmpty shouldBe true
  }

  test("build with columns") {
    val columns = Columns(Column("c1"), Column("c2"))
    columns.size shouldBe 2
    columns.isEmpty shouldBe false
  }

  test("foreach with mapper") {
    val columns = Columns(
      Column("c1"),
      Column("c1").withUDTName("u1"),
      Column("c1").withMapName("m1"),
      Column("c1").withUDTName("u1").withMapName("m1"),
      Column("c2"),
      Column("c2").withUDTName("u1"),
      Column("c2").withMapName("m1"),
      Column("c2").withUDTName("u1").withMapName("m12"))

    var cols1 = Columns()
    columns.foreachWithMapper("c1")(c => cols1 += c)
    cols1 shouldBe Columns(Column("c1"), Column("c1").withMapName("m1"))

    var cols2 = Columns()
    columns.foreachWithMapper("c1.u1")(c => cols2 += c)
    cols2 shouldBe Columns(
      Column("c1").withUDTName("u1"),
      Column("c1").withUDTName("u1").withMapName("m1"))
  }

  test("value for field") {
    val columns = Columns() +
      Column("c1").withValue(1) +
      Column("c1").withUDTName("u1").withValue(2) +
      Column("c1").withMapName("m1").withValue(3) +
      Column("c1").withUDTName("u1").withMapName("m1").withValue(4) +
      Column("c2").withValue(5) +
      Column("c2").withUDTName("u1").withValue(6) +
      Column("c2").withMapName("m1").withValue(7) +
      Column("c2").withUDTName("u1").withMapName("m1").withValue(8)
    columns.valueForField("c1") shouldBe 1
    columns.valueForField("c1.u1") shouldBe 2
    columns.valueForField("c1$m1") shouldBe 3
    columns.valueForField("c1.u1$m1") shouldBe 4
    columns.valueForField("c2") shouldBe 5
    columns.valueForField("c2.u1") shouldBe 6
    columns.valueForField("c2$m1") shouldBe 7
    columns.valueForField("c2.u1$m1") shouldBe 8
  }

  test("prepend column") {
    Column("c1") :: Columns(Column("c2")) shouldBe Columns(Column("c1"), Column("c2"))
  }

  test("sum column") {
    Columns(Column("c1")) + Column("c2") shouldBe Columns(Column("c1"), Column("c2"))
  }

  test("sum columns") {
    Columns(Column("c1")) ++ Columns(Column("c2")) shouldBe Columns(Column("c1"), Column("c2"))
  }

  test("add column without value") {
    Columns(Column("c1")).add("c2") shouldBe Columns(Column("c1"), Column("c2"))
  }

  test("add column with value") {
    Columns(Column("c1")).add("c2", 1) shouldBe Columns(Column("c1"), Column("c2").withValue(1))
  }

  test("toString empty") {
    Columns().toString shouldBe "Columns{}"
  }

  test("toString with columns") {
    val columns = Columns(
      Column("c1"),
      Column("c2").withUDTName("u1").withMapName("m1").withValue(7))
    columns.toString shouldBe "Columns{c1=None, c2.u1$m1=Some(7)}"
  }
}
