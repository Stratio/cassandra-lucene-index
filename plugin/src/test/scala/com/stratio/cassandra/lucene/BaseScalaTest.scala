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
package com.stratio.cassandra.lucene

import java.text.{ParseException, SimpleDateFormat}
import java.util.Date

import com.google.common.collect.Lists
import com.stratio.cassandra.lucene.common.{DateParser, GeoShapes, JsonSerializer}
import org.apache.cassandra.db.marshal._
import org.junit.Assert
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConverters._

/** Base test.
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class BaseScalaTest extends FunSuite with Matchers {


}

object BaseScalaTest {

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
      Lists.newArrayList(types.asJava))

  def reversed[A](base: AbstractType[A]): ReversedType[A] = ReversedType.getInstance(base)

  def parse[T](json: String, valueType : Class[T]) : T  =
    JsonSerializer.fromString[T](json, valueType)

  def assertNull(pattern: String, value: Any): Unit = {
    val parser = new DateParser(pattern)
    Assert.assertNull(s"$parser for $value should return null", parser.parse(value))
  }

  def assertEquals(pattern: String, value: Any, expected: Date): Unit = {
    val parser = new DateParser(pattern)
    val date = parser.parse(value)
    Assert.assertEquals(s"$parser for $value should generate $expected but get $date",
      expected,
      date)
  }

  def assertFail(pattern: String, value: Any): Unit = {
    val parser = new DateParser(pattern)
    try {
      val date = parser.parse(value)
      Assert.fail(s"$parser for $value should throw an IndexException but returned $date")
    } catch {
      case _: Throwable => // Nothing to do here
    }
  }

  @throws[ParseException]
  def date(format: String, input: String): Date = {
    DateParser.formatter(format).get().parse(input)
  }

}
