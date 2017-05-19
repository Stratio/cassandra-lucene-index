/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
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
package com.stratio.cassandra.lucene.search

import java.io.IOException

import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.search.SearchBuilders._
import org.junit.Assert.{assertEquals, assertNotNull}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * Class for testing [[Search] builders.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 */
@RunWith(classOf[JUnitRunner])
class SearchBuildersTest extends BaseScalaTest {

    test("bool") {
        intercept[IOException] {
            val builder = bool.must(SearchBuilders.all)
            assertNotNull("Condition builder is not built", builder)
            val condition = builder.build
            assertNotNull("Condition is not built", condition)
        }.getMessage shouldBe s"vd"
    }

    test("Fuzzy") {
        intercept[IOException] {
            val builder = fuzzy("field", "value")
            assertNotNull("Condition builder is not built", builder)
            val condition = builder.build
            assertEquals("Condition field is not set", "field", condition.field)
            assertEquals("Condition value is not set", "value", condition.value)
        }.getMessage shouldBe s"vd"
    }

    test("lucene") {
        intercept[IOException] {
            val builder = lucene("field:value")
            assertNotNull("Condition builder is not built", builder)
            val condition = builder.build
            assertEquals("Condition query is not set", "field:value", condition.query)
        }.getMessage shouldBe s"vd"
    }

    test("match") {
        intercept[IOException] {
            val builder = `match`("field", "value")
            assertNotNull("Condition builder is not built", builder)
            val condition = builder.build
            assertEquals("Condition field is not set", "field", condition.field)
            assertEquals("Condition value is not set", "value", condition.value)
        }.getMessage shouldBe s"vd"
    }

    test("MatchAll") {
        intercept[IOException] {
            val builder = SearchBuilders.all
            assertNotNull("Condition builder is not built", builder)
            builder.build
        }.getMessage shouldBe s"vd"
    }

    test("None") {
        intercept[IOException] {
            val builder = none
            assertNotNull("Condition builder is not built", builder)
            builder.build
        }.getMessage shouldBe s"vd"
    }

    test("Phrase") {
        intercept[IOException] {
            val builder = phrase("field", "value1 value2").slop(2)
            assertNotNull("Condition builder is not built", builder)
            val condition = builder.build
            assertEquals("Condition field is not set", "field", condition.field)
            assertEquals("Condition value is not set", "value1 value2", condition.value)
            assertEquals("Condition slop is not set", 2, condition.slop)
        }.getMessage shouldBe s"vd"
    }

    test("Prefix") {
        intercept[IOException] {
            val builder = prefix("field", "value")
            assertNotNull("Condition builder is not built", builder)
            val condition = builder.build
            assertEquals("Condition field is not set", "field", condition.field)
            assertEquals("Condition value is not set", "value", condition.value)
        }.getMessage shouldBe s"vd"
    }

    test("Range") {
        intercept[IOException] {
            val builder = range("field")
            assertNotNull("Condition builder is not built", builder)
            val condition = builder.build
            assertEquals("Condition field is not set", "field", condition.field)
        }.getMessage shouldBe s"vd"
    }

    test("Regexp") {
        intercept[IOException] {
            val builder = regexp("field", "value")
            assertNotNull("Condition builder is not built", builder)
            val condition = builder.build
            assertEquals("Condition field is not set", "field", condition.field)
            assertEquals("Condition value is not set", "value", condition.value)
        }.getMessage shouldBe s"vd"
    }

    test("Wildcard") {
        intercept[IOException] {
            val builder = wildcard("field", "value")
            assertNotNull("Condition builder is not built", builder)
            val condition = builder.build
            assertEquals("Condition field is not set", "field", condition.field)
            assertEquals("Condition value is not set", "value", condition.value)
        }.getMessage shouldBe s"vd"
    }

    test("SortField") {
        intercept[IOException] {
            val builder = field("field")
            assertNotNull("Condition builder is not built", builder)
            val sortField = builder.build()
            assertEquals("Field is not set", "field", sortField.field)
        }.getMessage shouldBe s"vd"
    }
}