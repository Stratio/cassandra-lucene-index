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
package com.stratio.cassandra.lucene.schema.analysis.tokenFilter

import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.common.JsonSerializer
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.util.TokenFilterFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


/** Tests for [[TokenFilterBuilder]].
  *
  * @author Juan Pedro Gilaberte `jpgilaberte@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class TokenFilterBuilderTest extends BaseScalaTest {

  test("available charFilter"){
    assert(TokenFilterFactory.availableTokenFilters().size() == 90)
  }

  type T = TokenFilterBuilder[TokenFilterFactory]

  def assertBuild(tokenizerBuilder: T, factoryClass: String, tokenizerClass: String) = {
    val tokenizerFactory = tokenizerBuilder.build
    val tokenizer = tokenizerFactory.create(new TokenStream() {override def incrementToken(): Boolean = true})
    assert(tokenizerFactory.getClass.getSimpleName == factoryClass)
    assert(tokenizer.getClass.getSimpleName == tokenizerClass)
  }

  def assertBuildException (tokenizerBuilder: T) = assertThrows[RuntimeException](tokenizerBuilder.build.create(new TokenStream() {override def incrementToken(): Boolean = true}))

  test("StandardFilterBuilder") {
    val jsonTest1 = """{"type":"standard"}"""
    val factoryName = "StandardFilterFactory"
    val tokenizerName = "StandardFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[StandardTokenFilterBuilder]), factoryName, tokenizerName)
  }

  test("ApostropheFilterBuilder") {
    val jsonTest1 = """{"type":"apostrophe"}"""
    val factoryName = "ApostropheFilterFactory"
    val tokenizerName = "ApostropheFilter"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[ApostropheTokenFilterBuilder]), factoryName, tokenizerName)
  }
}
