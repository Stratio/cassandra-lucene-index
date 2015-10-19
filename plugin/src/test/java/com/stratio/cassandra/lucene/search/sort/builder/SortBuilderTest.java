/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.cassandra.lucene.search.sort.builder;

import com.stratio.cassandra.lucene.search.sort.Sort;
import com.stratio.cassandra.lucene.search.sort.SortField;
import com.stratio.cassandra.lucene.util.Builder;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Class for testing {@link SortFieldBuilder}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SortBuilderTest {

    @Test
    public void testBuildWithArray() {
        SortFieldBuilder sortFieldBuilder1 = new SortFieldBuilder("field1").reverse(true);
        SortFieldBuilder sortFieldBuilder2 = new SortFieldBuilder("field2").reverse(false);
        SortBuilder sortBuilder = new SortBuilder(sortFieldBuilder1, sortFieldBuilder2);
        Sort sort = sortBuilder.build();
        assertNotNull("Sort is not built", sort);
        assertArrayEquals("Array based builder is wrong",
                          new SortField[]{sortFieldBuilder1.build(), sortFieldBuilder2.build()},
                          sort.getSortFields().toArray());

        assertNotNull("SortBuilder is mnot instance of Builder<Sort>", sortBuilder instanceof Builder);

    }

    @Test
    public void testBuildWithList() {
        SortFieldBuilder sortFieldBuilder1 = new SortFieldBuilder("field1").reverse(true);
        SortFieldBuilder sortFieldBuilder2 = new SortFieldBuilder("field2").reverse(false);
        SortBuilder sortBuilder = new SortBuilder(Arrays.asList(sortFieldBuilder1, sortFieldBuilder2));
        Sort sort = sortBuilder.build();
        assertNotNull("Sort is not built", sort);
        assertArrayEquals("List based builder is wrong",
                          new SortField[]{sortFieldBuilder1.build(), sortFieldBuilder2.build()},
                          sort.getSortFields().toArray());
    }

    @Test
    public void testJson() throws IOException {
        SortFieldBuilder sortFieldBuilder1 = new SortFieldBuilder("field1").reverse(true);
        SortFieldBuilder sortFieldBuilder2 = new SortFieldBuilder("field2").reverse(false);
        SortFieldBuilder sortFieldBuilder3 = new SortFieldBuilder("field3");
        SortBuilder sortBuilder = new SortBuilder(sortFieldBuilder1, sortFieldBuilder2, sortFieldBuilder3);
        String json = JsonSerializer.toString(sortBuilder);
        assertEquals("Method #toString is wrong", "{fields:[{field:\"field1\",reverse:true}," +
                                                  "{field:\"field2\"," +
                                                  "reverse:false}," +
                                                  "{field:\"field3\",reverse:false}]}", json);

    }
}
