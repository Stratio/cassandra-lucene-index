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
        SimpleSortFieldBuilder sortFieldBuilder1 = new SimpleSortFieldBuilder("field1").reverse(true);
        SimpleSortFieldBuilder simpleSortFieldBuilder2 = new SimpleSortFieldBuilder("field2").reverse(false);
        SortBuilder sortBuilder = new SortBuilder(sortFieldBuilder1, simpleSortFieldBuilder2);
        Sort sort = sortBuilder.build();
        assertNotNull("Sort is not built", sort);
        assertArrayEquals("Array based builder is wrong",
                          new SortField[]{sortFieldBuilder1.build(), simpleSortFieldBuilder2.build()},
                          sort.getSortFields().toArray());

        assertNotNull("SortBuilder is mnot instance of Builder<Sort>", sortBuilder instanceof Builder);

    }

    @Test
    public void testBuildWithList() {
        SimpleSortFieldBuilder sortFieldBuilder1 = new SimpleSortFieldBuilder("field1").reverse(true);
        SimpleSortFieldBuilder sortFieldBuilder2 = new SimpleSortFieldBuilder("field2").reverse(false);
        SortBuilder sortBuilder = new SortBuilder(Arrays.asList((SortFieldBuilder)sortFieldBuilder1,(SortFieldBuilder)sortFieldBuilder2));
        Sort sort = sortBuilder.build();
        assertNotNull("Sort is not built", sort);
        assertArrayEquals("List based builder is wrong",
                          new SortField[]{sortFieldBuilder1.build(), sortFieldBuilder2.build()},
                          sort.getSortFields().toArray());
    }

    @Test
    public void testJson() throws IOException {
        SimpleSortFieldBuilder sortFieldBuilder1 = new SimpleSortFieldBuilder("field1").reverse(true);
        SimpleSortFieldBuilder sortFieldBuilder2 = new SimpleSortFieldBuilder("field2").reverse(false);
        SimpleSortFieldBuilder sortFieldBuilder3 = new SimpleSortFieldBuilder("field3");
        SortBuilder sortBuilder = new SortBuilder(sortFieldBuilder1, sortFieldBuilder2, sortFieldBuilder3);
        String json = JsonSerializer.toString(sortBuilder);
        assertEquals("Method #toString is wrong", "{fields:[{type:\"simple\",field:\"field1\",reverse:true}," +
                                                  "{type:\"simple\",field:\"field2\",reverse:false}," +
                                                  "{type:\"simple\",field:\"field3\",reverse:false}]}", json);

    }
}
