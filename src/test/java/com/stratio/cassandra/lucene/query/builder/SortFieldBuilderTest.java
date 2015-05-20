/*
 * Copyright 2015, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.query.builder;

import com.stratio.cassandra.lucene.query.SortField;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SortFieldBuilderTest {

    @Test
    public void testBuild() {
        String field = "field";
        SortFieldBuilder builder = new SortFieldBuilder(field);
        builder.reverse(true);
        SortField sortField = builder.build();
        assertNotNull(sortField);
        assertEquals(field, sortField.getField());
        assertEquals(true, sortField.isReverse());
    }

    @Test
    public void testBuildWithDefaultReverse() {
        String field = "field";
        SortFieldBuilder builder = new SortFieldBuilder(field);
        SortField sortField = builder.build();
        assertNotNull(sortField);
        assertEquals(field, sortField.getField());
        assertEquals(SortField.DEFAULT_REVERSE, sortField.isReverse());
    }
}
