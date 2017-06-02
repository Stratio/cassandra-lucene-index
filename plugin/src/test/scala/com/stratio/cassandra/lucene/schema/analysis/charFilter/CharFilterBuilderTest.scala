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
package com.stratio.cassandra.lucene.schema.analysis.charFilter

import java.io.StringReader

import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.common.JsonSerializer
import org.apache.lucene.analysis.util.CharFilterFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


/** Tests for [[CharFilterBuilder]].
  *
  * @author Juan Pedro Gilaberte `jpgilaberte@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class CharFilterBuilderTest extends BaseScalaTest {

  test("available charFilter"){
    assert(CharFilterFactory.availableCharFilters().size() == 4)
  }

  type T = CharFilterBuilder[CharFilterFactory]

  def assertBuild(tokenizerBuilder: T, factoryClass: String, tokenizerClass: String) = {
    val tokenizerFactory = tokenizerBuilder.build
    val tokenizer = tokenizerFactory.create(new StringReader("hello test"))
    assert(tokenizerFactory.getClass.getSimpleName == factoryClass)
    assert(tokenizer.getClass.getSimpleName == tokenizerClass)
  }

  def assertBuildException (tokenizerBuilder: T) = assertThrows[RuntimeException](tokenizerBuilder.build.create(new StringReader("hello test")))

  test("HtmlStripCharFilterBuilder") {
    val jsonTest1 = """{"type":"htmlstrip"}"""
    val factoryName = "HTMLStripCharFilterFactory"
    val tokenizerName = "HTMLStripCharFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[HtmlStripCharFilterBuilder]), factoryName, tokenizerName)
  }

  test("MappingCharFilterBuilder") {
    val jsonTest1 = """{type:"mapping", mapping: "MappingCharFilter"}"""
    val factoryName = "MappingCharFilterFactory"
    val tokenizerName = "StringReader"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[MappingCharFilterBuilder]), factoryName, tokenizerName)
  }

  test("PatternReplaceCharFilterBuilder") {
    val jsonTest1 = """{"type":"patternreplace", "pattern":"/W+", "replacement":"test"}"""
    val jsonTest2 = """{"type":"patternreplace"}"""
    val factoryName = "PatternReplaceCharFilterFactory"
    val tokenizerName = "PatternReplaceCharFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[PatternReplaceCharFilterBuilder]), factoryName, tokenizerName)
    assertBuildException(JsonSerializer.fromString(jsonTest2, classOf[PatternReplaceCharFilterBuilder]))
  }

  test("PersianCharFilterBuilder") {
    val jsonTest1 = """{"type":"persian"}"""
    val factoryName = "PersianCharFilterFactory"
    val tokenizerName = "PersianCharFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[PersianCharFilterBuilder]), factoryName, tokenizerName)
  }
}