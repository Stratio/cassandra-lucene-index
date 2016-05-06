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
package com.stratio.cassandra.lucene.search.sort.builder;

import com.stratio.cassandra.lucene.search.sort.SimpleSortField;
import com.stratio.cassandra.lucene.search.sort.SortField;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Class for testing {@link SimpleSortFieldBuilder}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SimpleSortFieldBuilderTest {

    @Test
    public void testBuild() {
        String field = "field";
        SimpleSortFieldBuilder builder = new SimpleSortFieldBuilder(field).reverse(true);
        SimpleSortField sortField = builder.build();
        assertNotNull("SimpleSortField is not built", sortField);
        assertEquals("SimpleSortField field name is not set", field, sortField.getField());
        assertEquals("SimpleSortField reverse is not set", true, sortField.reverse);
    }

    @Test
    public void testBuildDefault() {
        String field = "field";
        SimpleSortFieldBuilder builder = new SimpleSortFieldBuilder(field);
        SimpleSortField sortField = builder.build();
        assertNotNull("SimpleSortField is not built", sortField);
        assertEquals("SimpleSortField field name is not set", field, sortField.getField());
        assertEquals("SimpleSortField reverse is not set to default", SortField.DEFAULT_REVERSE, sortField.reverse);
    }

    @Test
    public void testBuildReverse() {
        String field = "field";
        SimpleSortFieldBuilder builder = new SimpleSortFieldBuilder(field).reverse(false);
        SimpleSortField sortField = builder.build();
        assertNotNull("SimpleSortField is not built", sortField);
        assertEquals("SimpleSortField field name is not set", field, sortField.getField());
        assertEquals("SimpleSortField reverse is not set", false, sortField.reverse);
    }

    @Test
    public void testJson() throws IOException {
        String json1 = "{type:\"simple\",field:\"field\",reverse:false}";
        SimpleSortFieldBuilder sortFieldBuilder = JsonSerializer.fromString(json1, SimpleSortFieldBuilder.class);
        String json2 = JsonSerializer.toString(sortFieldBuilder);
        assertEquals("JSON serialization is wrong", json1, json2);
    }

    @Test
    public void testJsonDefault() throws IOException {
        String json1 = "{type:\"simple\",field:\"field\"}";
        SimpleSortFieldBuilder sortFieldBuilder = JsonSerializer.fromString(json1, SimpleSortFieldBuilder.class);
        String json2 = JsonSerializer.toString(sortFieldBuilder);
        assertEquals("JSON serialization is wrong", "{type:\"simple\",field:\"field\",reverse:false}", json2);
    }

    @Test
    public void testJsonReverse() throws IOException {
        String json1 = "{type:\"simple\",field:\"field\",reverse:true}";
        SimpleSortFieldBuilder sortFieldBuilder = JsonSerializer.fromString(json1, SimpleSortFieldBuilder.class);
        String json2 = JsonSerializer.toString(sortFieldBuilder);
        assertEquals("JSON serialization is wrong", json1, json2);
    }
}