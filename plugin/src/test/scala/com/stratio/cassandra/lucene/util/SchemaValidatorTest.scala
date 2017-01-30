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

import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.BaseScalaTest._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.stratio.cassandra.lucene.util.SchemaValidator._

/** Tests for [[SchemaValidator]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class SchemaValidatorTest extends BaseScalaTest {

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
