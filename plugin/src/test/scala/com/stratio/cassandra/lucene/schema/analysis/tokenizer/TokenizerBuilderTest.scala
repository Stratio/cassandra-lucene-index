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
package com.stratio.cassandra.lucene.schema.analysis.tokenizer

import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.common.JsonSerializer
import org.apache.lucene.analysis.core._
import org.apache.lucene.analysis.ngram.{NGramTokenizer, EdgeNGramTokenizer}
import org.apache.lucene.analysis.path.{ReversePathHierarchyTokenizer, PathHierarchyTokenizer}
import org.apache.lucene.analysis.pattern.PatternTokenizer
import org.apache.lucene.analysis.standard.{UAX29URLEmailTokenizer, StandardTokenizer, ClassicTokenizer}
import org.apache.lucene.analysis.th.ThaiTokenizer
import org.apache.lucene.analysis.util.TokenizerFactory
import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.util.Try

/** Tests for [[TokenizerBuilder]].
  *
  * @author Juan Pedro Gilaberte `jpgilaberte@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class TokenizerBuilderTest extends BaseScalaTest{

  test("available tokenizer"){
    assert(TokenizerFactory.availableTokenizers().size() == 13)
  }

  type T = TokenizerBuilder[TokenizerFactory]

  def assertBuild(tokenizerBuilder: T, factoryClass: String, tokenizerClass: String) = {
    val tokenizerFactory = tokenizerBuilder.build
    val tokenizer = tokenizerFactory.create()
    assert(tokenizerFactory.getClass.getSimpleName == factoryClass)
    assert(tokenizer.getClass.getSimpleName == tokenizerClass)
  }

  def assertBuildException (tokenizerBuilder: T) = assertThrows[RuntimeException](tokenizerBuilder.build.create())

  test("ClassicTokenizerBuilder") {
    val jsonTest1 = """{"type":"classic", "max_token_length": 25}"""
    val jsonTest2 = """{"type":"classic"}"""
    val jsonTest3 = """{"type":"classic", "max_token_length": -25}"""
    val factoryName = "ClassicTokenizerFactory"
    val tokenizerName = "ClassicTokenizer"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[ClassicTokenizerBuilder]), factoryName, tokenizerName)
    assertBuild(JsonSerializer.fromString(jsonTest2, classOf[ClassicTokenizerBuilder]), factoryName, tokenizerName)
    assertBuildException(JsonSerializer.fromString(jsonTest3, classOf[ClassicTokenizerBuilder]))
  }

  test("EdgeNGramTokenizerBuilder") {
    val jsonTest1 = """{"type":"edge_ngram", "max_gram_size": 25, "min_gram_size": 10}"""
    val jsonTest2 = """{"type":"edge_ngram"}"""
    val jsonTest3 = """{"type":"edge_ngram", "max_gram_size": 25, "min_gram_size": -10}"""
    val factoryName = "EdgeNGramTokenizerFactory"
    val tokenizerName = "EdgeNGramTokenizer"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[EdgeNGramTokenizerBuilder]), factoryName, tokenizerName)
    assertBuild(JsonSerializer.fromString(jsonTest2, classOf[EdgeNGramTokenizerBuilder]), factoryName, tokenizerName)
    assertBuildException(JsonSerializer.fromString(jsonTest3, classOf[EdgeNGramTokenizerBuilder]))
  }

  test("KeywordTokenizerBuilder") {
    val jsonTest1 = """{"type":"keyword"}"""
    val factoryName = "KeywordTokenizerFactory"
    val tokenizerName = "KeywordTokenizer"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[KeywordTokenizerBuilder]), factoryName, tokenizerName)
  }

  test("LetterTokenizerBuilder") {
    val jsonTest1 = """{"type":"letter"}"""
    val factoryName = "LetterTokenizerFactory"
    val tokenizerName = "LetterTokenizer"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[LetterTokenizerBuilder]), factoryName, tokenizerName)
  }

  test("LowerCaseTokenizerBuilder") {
    val jsonTest1 = """{"type":"lower_case"}"""
    val factoryName = "LowerCaseTokenizerFactory"
    val tokenizerName = "LowerCaseTokenizer"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[LowerCaseTokenizerBuilder]), factoryName, tokenizerName)
  }

  test("NGramTokenizerBuilder") {
    val jsonTest1 = """{"type":"ngram", "max_gram_size": 25, "min_gram_size": 10}"""
    val jsonTest2 = """{"type":"ngram"}"""
    val jsonTest3 = """{"type":"ngram", "max_gram_size": 25, "min_gram_size": -10}"""
    val factoryName = "NGramTokenizerFactory"
    val tokenizerName = "NGramTokenizer"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[NGramTokenizerBuilder]), factoryName, tokenizerName)
    assertBuild(JsonSerializer.fromString(jsonTest2, classOf[NGramTokenizerBuilder]), factoryName, tokenizerName)
    assertBuildException(JsonSerializer.fromString(jsonTest3, classOf[NGramTokenizerBuilder]))
  }

  test("PathHierarchyTokenizerBuilder") {
    val jsonTest1 = """{"type":"path_hierarchy", "reverse": false, "delimiter": "/", "replace": "%", skip: 3}"""
    val jsonTest2 = """{"type":"path_hierarchy"}"""
    val jsonTest3 = """{"type":"path_hierarchy", "reverse": false, "delimiter": "/", "replace": "%", skip: -3}"""
    val factoryName = "PathHierarchyTokenizerFactory"
    val tokenizerName = "PathHierarchyTokenizer"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[PathHierarchyTokenizerBuilder]), factoryName, tokenizerName)
    assertBuild(JsonSerializer.fromString(jsonTest2, classOf[PathHierarchyTokenizerBuilder]), factoryName, tokenizerName)
    assertBuildException(JsonSerializer.fromString(jsonTest3, classOf[PathHierarchyTokenizerBuilder]))

    val jsonTest4 = """{"type":"path_hierarchy", "reverse": true, "delimiter": "/", "replace": "%", skip: 3}"""
    val jsonTest5 = """{"type":"path_hierarchy", "reverse": true, "delimiter": "/", "replace": "%", skip: -3}"""
    val tokenizerNameReverse = "ReversePathHierarchyTokenizer"
    assertBuild(JsonSerializer.fromString(jsonTest4, classOf[PathHierarchyTokenizerBuilder]), factoryName, tokenizerNameReverse)
    assertBuildException(JsonSerializer.fromString(jsonTest5, classOf[PathHierarchyTokenizerBuilder]))
  }

  test("PatternTokenizerBuilder") {
    val jsonTest1 = """{"type":"pattern", pattern: "[a-z]", group: 0}"""
    val jsonTest2 = """{"type":"pattern", pattern: "[a-z]", group: 0}"""
    val jsonTest3 = """{"type":"pattern"}"""
    val factoryName = "PatternTokenizerFactory"
    val tokenizerName = "PatternTokenizer"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[PatternTokenizerBuilder]), factoryName, tokenizerName)
    assertBuild(JsonSerializer.fromString(jsonTest2, classOf[PatternTokenizerBuilder]), factoryName, tokenizerName)
    assertBuildException(JsonSerializer.fromString(jsonTest3, classOf[PatternTokenizerBuilder]))
  }

  test("StandardTokenizerBuilder") {
    val jsonTest1 = """{"type":"standard", "max_token_length": 25}"""
    val jsonTest2 = """{"type":"standard"}"""
    val jsonTest3 = """{"type":"standard", "max_token_length": -25}"""
    val factoryName = "StandardTokenizerFactory"
    val tokenizerName = "StandardTokenizer"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[StandardTokenizerBuilder]), factoryName, tokenizerName)
    assertBuild(JsonSerializer.fromString(jsonTest2, classOf[StandardTokenizerBuilder]), factoryName, tokenizerName)
    assertBuildException(JsonSerializer.fromString(jsonTest3, classOf[StandardTokenizerBuilder]))
  }

  test("ThaiTokenizerBuilder") {
    val jsonTest1 = """{"type":"thai"}"""
    val factoryName = "ThaiTokenizerFactory"
    val tokenizerName = "ThaiTokenizer"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[ThaiTokenizerBuilder]), factoryName, tokenizerName)
  }

  test("UAX29URLEmailTokenizerBuilder") {
    val jsonTest1 = """{"type":"uax29_url_email", "max_token_length": 25}"""
    val jsonTest2 = """{"type":"uax29_url_email"}"""
    val jsonTest3 = """{"type":"uax29_url_email", "max_token_length": -25}"""
    val factoryName = "UAX29URLEmailTokenizerFactory"
    val tokenizerName = "UAX29URLEmailTokenizer"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[UAX29URLEmailTokenizerBuilder]), factoryName, tokenizerName)
    assertBuild(JsonSerializer.fromString(jsonTest2, classOf[UAX29URLEmailTokenizerBuilder]), factoryName, tokenizerName)
    assertBuildException(JsonSerializer.fromString(jsonTest3, classOf[UAX29URLEmailTokenizerBuilder]))
  }

  test("WhiteSpaceTokenizerBuilder") {
    val jsonTest1 = """{"type":"whitespace", "rule": "java"}"""
    val jsonTest2 = """{"type":"whitespace"}"""
    val jsonTest3 = """{"type":"whitespace", "rule": "failure"}"""
    val factoryName = "WhitespaceTokenizerFactory"
    val tokenizerName = "WhitespaceTokenizer"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[WhitespaceTokenizerBuilder]), factoryName, tokenizerName)
    assertBuild(JsonSerializer.fromString(jsonTest2, classOf[WhitespaceTokenizerBuilder]), factoryName, tokenizerName)
    assertBuildException(JsonSerializer.fromString(jsonTest3, classOf[WhitespaceTokenizerBuilder]))

    val jsonTest4 = """{"type":"whitespace", "rule": "unicode"}"""
    val tokenizerNameUnicode = "UnicodeWhitespaceTokenizer"
    assertBuild(JsonSerializer.fromString(jsonTest4, classOf[WhitespaceTokenizerBuilder]), factoryName, tokenizerNameUnicode)
  }

  test("WikipediaTokenizerBuilder") {
    val jsonTest1 = """{"type":"wikipedia"}"""
    val factoryName = "WikipediaTokenizerFactory"
    val tokenizerName = "WikipediaTokenizer"
    assertBuild(JsonSerializer.fromString(jsonTest1, classOf[WikipediaTokenizerBuilder]), factoryName, tokenizerName)
  }
}
