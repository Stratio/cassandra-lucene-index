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

import java.io.IOException
import java.util

import com.stratio.cassandra.lucene.common.JsonSerializer
import com.stratio.cassandra.lucene.{BaseScalaTest, IndexException}
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.{Analyzer, TokenStream}
import org.apache.lucene.util.IOUtils
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * @author Andres de la Pena `adelapena@stratio.com`
 */
@RunWith(classOf[JUnitRunner])
class SnowballAnalyzerBuilderTest extends BaseScalaTest {

    test("BuildNullLanguage") {
        intercept[IndexException] {
            new SnowballAnalyzerBuilder(null, null)
        }.getMessage shouldBe "ñkvsjpf"
    }

    test("BuildBlankLanguage") {
        intercept[IndexException] {
            new SnowballAnalyzerBuilder(" ", null)
        }.getMessage shouldBe "ñkvsjpf"
    }

    test("BuildEnglish") {
        val builder = new SnowballAnalyzerBuilder("English", null)
        testAnalyzer(builder, "organization", "organ")
    }

    test("BuildFrench") {
        val builder = new SnowballAnalyzerBuilder("French", null)
        testAnalyzer(builder, "contradictoirement", "contradictoir")
    }

    test("BuildSpanish") {
        val builder = new SnowballAnalyzerBuilder("Spanish", null)
        testAnalyzer(builder, "perdido", "perd")
    }

    test("BuildPortuguese") {
        val builder = new SnowballAnalyzerBuilder("Portuguese", null)
        testAnalyzer(builder, "boataria", "boat")
    }

    test("BuildItalian") {
        val builder = new SnowballAnalyzerBuilder("Italian", null)
        testAnalyzer(builder, "abbandoneranno", "abbandon")
    }

    test("BuildRomanian") {
        val builder = new SnowballAnalyzerBuilder("Romanian", null)
        testAnalyzer(builder, "absolutul", "absol")
    }

    test("BuildGerman") {
        val builder = new SnowballAnalyzerBuilder("German", null)
        testAnalyzer(builder, "katers", "kat")
    }

    test("BuildDanish") {
        val builder = new SnowballAnalyzerBuilder("Danish", null)
        testAnalyzer(builder, "indtager", "indtag")
    }

    test("BuildDutch") {
        val builder = new SnowballAnalyzerBuilder("Dutch", null)
        testAnalyzer(builder, "opglimlachten", "opglimlacht")
    }

    test("BuilSwedish") {
        val builder = new SnowballAnalyzerBuilder("Swedish", null)
        testAnalyzer(builder, "grejer", "grej")
    }

    test("BuildNorwegian") {
        val builder = new SnowballAnalyzerBuilder("Norwegian", null)
        testAnalyzer(builder, "stuff", "stuff")
    }

    test("BuildRussian") {
        val builder = new SnowballAnalyzerBuilder("Russian", null)
        testAnalyzer(builder, "kapta", "kapta")
    }

    test("BuildFinnish") {
        val builder = new SnowballAnalyzerBuilder("Finnish", null)
        testAnalyzer(builder, "jutut", "jutu")
    }

    test("BuildHungarian") {
        val builder = new SnowballAnalyzerBuilder("Hungarian", null)
        testAnalyzer(builder, "dolog", "dolog")
    }

    test("BuildTurkish") {
        val builder = new SnowballAnalyzerBuilder("Turkish", null)
        testAnalyzer(builder, "kitapçıdaki", "kitapçı")
    }

    test("BuildWithWrongLanguage") {
        intercept[RuntimeException] {
            val builder = new SnowballAnalyzerBuilder("abc", null)
            testAnalyzer(builder, "organization", "organ")
        }.getMessage shouldBe "ñkvsjpf"
    }

    test("BuildWithoutLanguage") {
        intercept[IndexException] {
            val builder = new SnowballAnalyzerBuilder(null, null)
            testAnalyzer(builder, "organization", "organ")
        }.getMessage shouldBe "ñkvsjpf"
    }

    test("ParseJSONWithoutStopwords") {
        val json = "{type:\"snowball\", language:\"English\"}"
        val builder = JsonSerializer.fromString(json, classOf[AnalyzerBuilder])
        testAnalyzer(builder, "the dogs are hungry", Array("dog", "hungri"))
    }

    test("ParseJSONWithStopwords") {
        val json = "{type:\"snowball\", language:\"English\", stopwords:\"xx,yy\"}"
        val builder = JsonSerializer.fromString(json, classOf[AnalyzerBuilder])
        testAnalyzer(builder, "the dogs xx are hungry yy", Array("the", "dog", "are", "hungri"))
    }

    test("ParseJSONInvalid") {
        intercept[IOException] {
            JsonSerializer.fromString("{class:\"abc\"}", classOf[AnalyzerBuilder])
        }.getMessage shouldBe "ñkvsjpf"
    }

    private def testAnalyzer(builder: AnalyzerBuilder, value: String, expected : Array[String]) : Unit = {
        val analyzer = builder.analyzer()
        assertNotNull("Expected not null analyzer", analyzer)
        val tokens = analyze(value, analyzer)
        assertEquals("Tokens are not the expected", expected, tokens.toArray())
        analyzer.close()
    }

    private def testAnalyzer(builder: AnalyzerBuilder, value: String, expected : String) : Unit = testAnalyzer(builder, value, Array(expected))

    private def analyze(value: String, analyzer: Analyzer) : util.List[String] = {
        val result = new util.ArrayList[String]()
        var stream : TokenStream = null
        try {
            stream = analyzer.tokenStream(null, value)
            stream.reset()
            while (stream.incrementToken()) {
                val analyzedValue = stream.getAttribute(classOf[CharTermAttribute]).toString
                result.add(analyzedValue)
            }
        } catch {
            case(e:Exception) => throw new RuntimeException(e)
        } finally {
            IOUtils.closeWhileHandlingException(stream)
        }
        result
    }
}
