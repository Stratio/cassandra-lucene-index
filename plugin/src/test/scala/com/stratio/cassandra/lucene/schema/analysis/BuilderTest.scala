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
package com.stratio.cassandra.lucene.schema.analysis

import com.stratio.cassandra.lucene.BaseScalaTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


/** Tests for [[Builder]].
  *
  * @author Juan Pedro Gilaberte `jpgilaberte@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class BuilderTest extends BaseScalaTest {

  class TestFactory(){ def helloTest = "helloTest"}
  sealed abstract class TestBuilder[T](typeTest: String) extends Builder[T]{
    def buildFunction : () => T = () => {
        new TestFactory().asInstanceOf[T]
    }
  }
  case class TestBuilderChild(param1: String = "test", param2: Long = 1l, param3: Integer = 2) extends TestBuilder[TestFactory]("test")
  case class TestNullBuilderChild(param1: String = null, param2: Long = 1l, param3: Integer = 2) extends TestBuilder[TestFactory]("test")
  case class TestExceptionBuilderChild(param1: String = "testException", param2: Long = 1l, param3: Integer = 2) extends TestBuilder[TestFactory]("test"){
    override def buildFunction : () => TestFactory = () => {
      throw new RuntimeException("exception in Lucene's layer")
    }
  }

  //Exception
  test("exception build function") {
    val testBuilderChild = TestExceptionBuilderChild()
    assertThrows[RuntimeException](testBuilderChild.build)
  }

  //Null (Jackson doesn't inform value)
  test("null termSymbolsList function") {
    val testBuilderChild = TestNullBuilderChild()
    val termSymbol = testBuilderChild.termSymbolsList.toList
    assert(termSymbol.size == 3)
  }

  test("null reflectedFieldValue function") {
    val testBuilderChild = TestNullBuilderChild()
    val termSymbol = testBuilderChild.termSymbolsList
    val reflectFieldValueTest1 = testBuilderChild.reflectedFieldValue(termSymbol.toList.apply(0))
    val reflectFieldValueTest2 = testBuilderChild.reflectedFieldValue(termSymbol.toList.apply(1))
    val reflectFieldValueTest3 = testBuilderChild.reflectedFieldValue(termSymbol.toList.apply(2))
    assert(reflectFieldValueTest1 == 2)
    assert(reflectFieldValueTest2 == 1l)
    assert(reflectFieldValueTest3 == null)
  }

  test("null mapParsedFunction function") {
//    val testBuilderChild = TestNullBuilderChild()
//    val mapParsedTest = testBuilderChild.mapParsed
//    assert(mapParsedTest.size == 2)
//    assertThrows[NoSuchElementException](mapParsedTest.apply("param1") == "test")
//    assert(mapParsedTest.apply("param2") == "1")
//    assert(mapParsedTest.apply("param3") == "2")
  }

  test("null build function") {
    val testBuilderChild = TestNullBuilderChild()
    val factoryTest = testBuilderChild.build
    assert(factoryTest.helloTest == "helloTest")
  }


  // Right
  test("termSymbolsList function") {
    val testBuilderChild = TestBuilderChild()
    val termSymbol = testBuilderChild.termSymbolsList.toList
    assert(termSymbol.size == 3)
  }

  test("reflectedFieldValue function") {
    val testBuilderChild = TestBuilderChild()
    val termSymbol = testBuilderChild.termSymbolsList
    val reflectFieldValueTest1 = testBuilderChild.reflectedFieldValue(termSymbol.toList.apply(0))
    val reflectFieldValueTest2 = testBuilderChild.reflectedFieldValue(termSymbol.toList.apply(1))
    val reflectFieldValueTest3 = testBuilderChild.reflectedFieldValue(termSymbol.toList.apply(2))
    assert(reflectFieldValueTest1 == 2)
    assert(reflectFieldValueTest2 == 1l)
    assert(reflectFieldValueTest3 == "test")
  }

  test("mapParsed function") {
//    val testBuilderChild = TestBuilderChild()
//    val mapParsedTest = testBuilderChild.mapParsed
//    assert(mapParsedTest.size == 3)
//    assert(mapParsedTest.apply("param1") == "test")
//    assert(mapParsedTest.apply("param2") == "1")
//    assert(mapParsedTest.apply("param3") == "2")
  }

  test("build function") {
    val testBuilderChild = TestBuilderChild()
    val factoryTest: TestFactory = testBuilderChild.build
    assert(factoryTest.helloTest == "helloTest")
  }
}
