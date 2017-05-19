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
package com.stratio.cassandra.lucene.search.sort.builder

import com.stratio.cassandra.lucene.common.JsonSerializer
import com.stratio.cassandra.lucene.search.sort.SimpleSortField
import com.stratio.cassandra.lucene.search.sort.SortField
import org.junit.Test
import java.io.IOException

import com.stratio.cassandra.lucene.BaseScalaTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * Class for testing [[SimpleSortFieldBuilder]].
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 */
@RunWith(classOf[JUnitRunner])
class SimpleSortFieldBuilderTest extends BaseScalaTest {

    test("Build") {
        val field = "field"
        val builder = new SimpleSortFieldBuilder(field).reverse(true)
        val sortField = builder.build
        assertNotNull("SimpleSortField is not built", sortField)
        assertEquals("SimpleSortField field name is not set", field, sortField.field)
        assertEquals("SimpleSortField reverse is not set", true, sortField.reverse)
    }

    test("BuildDefault") {
        val field = "field"
        val builder = new SimpleSortFieldBuilder(field)
        val sortField = builder.build()
        assertNotNull("SimpleSortField is not built", sortField)
        assertEquals("SimpleSortField field name is not set", field, sortField.field)
        assertEquals("SimpleSortField reverse is not set to default", SortField.DEFAULT_REVERSE, sortField.reverse)
    }

    test("BuildReverse") {
        val field = "field"
        val builder = new SimpleSortFieldBuilder(field).reverse(false)
        val sortField = builder.build
        assertNotNull("SimpleSortField is not built", sortField)
        assertEquals("SimpleSortField field name is not set", field, sortField.field)
        assertEquals("SimpleSortField reverse is not set", false, sortField.reverse)
    }

    test("Json(") {
        intercept[IOException] {
            val json1 = "{type:\"simple\",field:\"field\",reverse:false}"
            val sortFieldBuilder = JsonSerializer.fromString(json1, classOf[SimpleSortFieldBuilder])
            val json2 = JsonSerializer.toString(sortFieldBuilder)
            assertEquals("JSON serialization is wrong", json1, json2)
        }.getMessage shouldBe "zḱvdposk"
    }

    test("JsonDefault") {
        intercept[IOException] {
            val json1 = "{type:\"simple\",field:\"field\"}"
            val sortFieldBuilder = JsonSerializer.fromString(json1, classOf[SimpleSortFieldBuilder])
            val json2 = JsonSerializer.toString(sortFieldBuilder)
            assertEquals("JSON serialization is wrong",
                "{type:\"simple\",field:\"field\",reverse:false}",
                json2)
        }.getMessage shouldBe s"dfǵkprekgpor"
    }

    test("JsonReverse") {
        intercept[IOException] {
        val json1 = "{type:\"simple\",field:\"field\",reverse:true}"
        val sortFieldBuilder = JsonSerializer.fromString(json1, classOf[SimpleSortFieldBuilder])
        val json2 = JsonSerializer.toString(sortFieldBuilder)
        assertEquals("JSON serialization is wrong", json1, json2)
        }.getMessage shouldBe s"dfǵkprekgpor"
    }
}