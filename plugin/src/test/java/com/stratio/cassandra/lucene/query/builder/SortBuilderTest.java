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

import com.stratio.cassandra.lucene.query.Sort;
import com.stratio.cassandra.lucene.query.SortField;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SortBuilderTest {

    @Test
    public void testBuildWithArray() {
        SortFieldBuilder sortFieldBuilder1 = new SortFieldBuilder("field1").reverse(true);
        SortFieldBuilder sortFieldBuilder2 = new SortFieldBuilder("field2").reverse(false);
        SortBuilder sortBuilder = new SortBuilder(sortFieldBuilder1, sortFieldBuilder2);
        Sort sort = sortBuilder.build();
        assertNotNull(sort);
        assertArrayEquals(new SortField[]{sortFieldBuilder1.build(), sortFieldBuilder2.build()},
                          sort.getSortFields().toArray());

    }

    @Test
    public void testBuildWithList() {
        SortFieldBuilder sortFieldBuilder1 = new SortFieldBuilder("field1").reverse(true);
        SortFieldBuilder sortFieldBuilder2 = new SortFieldBuilder("field2").reverse(false);
        SortBuilder sortBuilder = new SortBuilder(Arrays.asList(sortFieldBuilder1, sortFieldBuilder2));
        Sort sort = sortBuilder.build();
        assertNotNull(sort);
        assertArrayEquals(new SortField[]{sortFieldBuilder1.build(), sortFieldBuilder2.build()},
                          sort.getSortFields().toArray());

    }
}
