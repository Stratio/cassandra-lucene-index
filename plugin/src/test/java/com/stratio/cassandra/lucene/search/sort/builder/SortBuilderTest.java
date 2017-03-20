/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
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

import com.stratio.cassandra.lucene.search.sort.Sort;
import com.stratio.cassandra.lucene.search.sort.SortField;
import com.stratio.cassandra.lucene.util.Builder;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
        GeoDistanceSortFieldBuilder sortFieldBuilder2 = new GeoDistanceSortFieldBuilder("field2",
                                                                                        0.0,
                                                                                        0.0).reverse(true);
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
        SimpleSortFieldBuilder sortFieldBuilder1 = new SimpleSortFieldBuilder("field1").reverse(true);
        GeoDistanceSortFieldBuilder sortFieldBuilder2 = new GeoDistanceSortFieldBuilder("field2",
                                                                                        0.0,
                                                                                        0.0).reverse(true);
        List<SortFieldBuilder> sortFieldBuilderList = new ArrayList<>();
        sortFieldBuilderList.add(sortFieldBuilder1);
        sortFieldBuilderList.add(sortFieldBuilder2);
        SortBuilder sortBuilder = new SortBuilder(sortFieldBuilderList);

        Sort sort = sortBuilder.build();
        assertNotNull("Sort is not built", sort);
        assertArrayEquals("List based builder is wrong",
                          new SortField[]{sortFieldBuilder1.build(), sortFieldBuilder2.build()},
                          sort.getSortFields().toArray());
    }

    @Test
    public void testJson() throws IOException {
        SimpleSortFieldBuilder sortFieldBuilder1 = new SimpleSortFieldBuilder("field1").reverse(true);
        GeoDistanceSortFieldBuilder sortFieldBuilder2 = new GeoDistanceSortFieldBuilder("mapper2", 0.0, 0.0).reverse(
                true);
        SimpleSortFieldBuilder sortFieldBuilder3 = new SimpleSortFieldBuilder("field3");
        SortBuilder sortBuilder = new SortBuilder(sortFieldBuilder1, sortFieldBuilder2, sortFieldBuilder3);
        String json = JsonSerializer.toString(sortBuilder);
        assertEquals("Method #toString is wrong", "{fields:[{type:\"simple\",field:\"field1\",reverse:true}," +
                                                  "{type:\"geo_distance\",mapper:\"mapper2\",longitude:0.0,latitude:0.0,reverse:true}," +
                                                  "{type:\"simple\",field:\"field3\",reverse:false}]}", json);

    }

    @Test
    public void testDeserializeDefaultSort() {
        String json1 = "{field:\"field1\",reverse:true}";

        SortFieldBuilder sortFieldBuilder = null;
        try {
            sortFieldBuilder = JsonSerializer.fromString(json1, SortFieldBuilder.class);
            assertEquals("JSON serialization is wrong", sortFieldBuilder.getClass(), SimpleSortFieldBuilder.class);
            String json2 = JsonSerializer.toString(sortFieldBuilder);
            assertEquals("JSON serialization is wrong", "{type:\"simple\",field:\"field1\",reverse:true}", json2);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}