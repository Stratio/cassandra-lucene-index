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
package com.stratio.cassandra.lucene.schema.analysis.analyzer

import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.common.JsonSerializer
import com.stratio.cassandra.lucene.schema.analysis.AnalyzerBuilder
import org.apache.lucene.analysis.tokenattributes.{OffsetAttribute, CharTermAttribute}
import org.apache.lucene.analysis.Analyzer
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


/**
  * Created by jpgilaberte on 24/05/17.
  */
@RunWith(classOf[JUnitRunner])
class CustomAnalyzerTest extends BaseScalaTest {

  def assertBuildException (custom: String, source: String, result: Array[String], count: Array[Int]) = {
    assertThrows[Throwable](assertCustomContents(custom, source, result, count))
  }
  def assertCustomContents(custom: String, source: String, result: Array[String], count: Array[Int]) = {

    val analyzerBuilder: AnalyzerBuilder = JsonSerializer.fromString(custom, classOf[AnalyzerBuilder])
    val customAnalyzer: Analyzer = analyzerBuilder.analyzer()
    val ts = customAnalyzer.tokenStream("dumy", source)

    ts.reset()
    count.map(x => {
        ts.incrementToken()
        val res = ts.getAttribute(classOf[CharTermAttribute])
        assert(new String(res.buffer(), 0, res.length()).equals(result(x)))
      })
    ts.end()
    ts.close()
  }

  test("CustomAnalyzer deserialize json exception") {
    val custom = """{type:"custo"}""".stripMargin
    assertBuildException(custom, "föó bär FÖÖ BAR",  Array("föó", "bär", "FÖÖ", "BAR"), Array(0, 1, 2, 3))
  }

  test("CustomAnalyzer only tokenizer") {
    val custom = """{type:"custom", tokenizer: {type:"whitespace"}}""".stripMargin
    assertCustomContents(custom, "föó bär FÖÖ BAR",  Array("föó", "bär", "FÖÖ", "BAR"), Array(0, 1, 2, 3))
  }

  test("CustomAnalyzer tokenizer mandatory exception") {
    val custom = """{type:"custom"}""".stripMargin
    assertBuildException(custom, "föó bär FÖÖ BAR",  Array("föó", "bär", "FÖÖ", "BAR"), Array(0, 1, 2, 3))
  }

  test("CustomAnalyzer token_filter") {
    val custom = """{type:"custom", tokenizer: {type:"whitespace"}, token_filter: [{type:"asciifolding"}, {type:"lowercase"}]}""".stripMargin
    assertCustomContents(custom, "föó bär FÖÖ BAR",  Array("foo", "bar", "foo", "bar"), Array(0, 1, 2, 3))
  }

  test("CustomAnalyzer token_filter empty") {
    val custom = """{type:"custom", tokenizer: {type:"whitespace"}, token_filter: []}""".stripMargin
    assertCustomContents(custom, "föó bär FÖÖ BAR",  Array("föó", "bär", "FÖÖ", "BAR"), Array(0, 1, 2, 3))
  }

  test("CustomAnalyzer token_filter json exception") {
    val custom = """{type:"custom", tokenizer: {type:"whitespace"}, token_filter: }""".stripMargin
    assertBuildException(custom, "föó bär FÖÖ BAR",  Array("föó", "bär", "FÖÖ", "BAR"), Array(0, 1, 2, 3))
  }

  test("CustomAnalyzer char_filter") {
    val custom = """{type:"custom", tokenizer: {type:"whitespace"}, char_filter: [{type:"mapping", mapping:"MappingCharFilter"}, {type:"patternreplace", pattern:"(a)\\s+(b)", replacement:"$1#$2"}]}""".stripMargin
    assertCustomContents(custom, "aa  bb aa bb", Array("a#b", "a#b"), Array(0, 1))
  }

  test("CustomAnalyzer char_filter empty") {
    val custom = """{type:"custom", tokenizer: {type:"whitespace"}, char_filter: []}""".stripMargin
    assertCustomContents(custom, "a f ff aa", Array("a", "f", "ff", "aa"), Array(0, 1, 2, 3))
  }

  test("CustomAnalyzer full") {
    val custom = """{type:"custom", tokenizer: {type:"whitespace"}, char_filter: [{type:"mapping", mapping:"MappingCharFilter"}, {type:"patternreplace", pattern:"(zöó)\\s+(zöó)", replacement:"$1#$2"}], token_filter: [{type:"asciifolding"}, {type:"lowercase"}]}""".stripMargin
    assertCustomContents(custom, "föó föó bär FÖÖ BAR",  Array("zoo#zoo", "bar", "zoo", "bar"), Array(0, 1, 2, 3))
  }

  test("CustomAnalyzer validate before 'cassandra index build'") {
    val custom = """{type:"custom", tokenizer: {type:"ngram", "max_gram_size": 1, "min_gram_size": 2}}""".stripMargin
    assertBuildException(custom, "aabb", Array("aa", "ab", "bb"), Array(0, 1))
  }
}
