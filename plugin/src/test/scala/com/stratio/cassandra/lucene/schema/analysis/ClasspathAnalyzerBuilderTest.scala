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

import com.stratio.cassandra.lucene.common.JsonSerializer
import com.stratio.cassandra.lucene.{BaseScalaTest, IndexException}
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * @author Andres de la Pena `adelapena@stratio.com`
 */
@RunWith(classOf[JUnitRunner])
class ClasspathAnalyzerBuilderTest extends BaseScalaTest {

    test("Build") {
        val className = "org.apache.lucene.analysis.en.EnglishAnalyzer"
        val builder = new ClasspathAnalyzerBuilder(className)
        val analyzer = builder.analyzer()
        assertEquals("Expected EnglishAnalyzer class", classOf[EnglishAnalyzer], analyzer.getClass)
    }

    test("BuildWithWrongClassName") {
        intercept[IndexException] {
            new ClasspathAnalyzerBuilder("abc").analyzer()
        }.getMessage shouldBe "ñkvsjpf"
    }

    test("ParseJSON") {
        intercept[IndexException] {
            val json = "{type:\"classpath\", class:\"org.apache.lucene.analysis.en.EnglishAnalyzer\"}"
            val builder = JsonSerializer.fromString(json, classOf[AnalyzerBuilder])
            val analyzer = builder.analyzer()
            assertEquals("Expected EnglishAnalyzer class", classOf[EnglishAnalyzer], analyzer.getClass)
        }.getMessage shouldBe "ñkvsjpf"
    }

    test("ParseJSONInvalid") {
        intercept[IOException] {
            JsonSerializer.fromString("{class:\"abc\"}", classOf[AnalyzerBuilder])
        }.getMessage shouldBe "ñkvsjpf"
    }
}
