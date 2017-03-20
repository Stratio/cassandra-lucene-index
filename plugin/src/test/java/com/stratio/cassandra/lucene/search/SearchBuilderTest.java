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
package com.stratio.cassandra.lucene.search;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.search.condition.builder.ConditionBuilder;
import com.stratio.cassandra.lucene.search.sort.builder.SimpleSortFieldBuilder;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.junit.Test;

import java.io.IOException;

import static com.stratio.cassandra.lucene.search.SearchBuilders.*;
import static org.junit.Assert.assertEquals;

/**
 * Class for testing {@link SearchBuilder}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SearchBuilderTest {

    @Test
    public void testBuild() throws IOException {
        ConditionBuilder<?, ?> query = match("field1", "value2");
        ConditionBuilder<?, ?> filter = match("field2", "value2");
        SimpleSortFieldBuilder sort1 = field("field3");
        SimpleSortFieldBuilder sort2 = field("field4");
        SearchBuilder searchBuilder = new SearchBuilder().query(query).filter(filter).sort(sort1, sort2);
        String json = searchBuilder.toJson();
        assertEquals("JSON serialization is wrong", json, JsonSerializer.toString(searchBuilder));
    }

    @Test
    public void testJson() {
        SearchBuilder searchBuilder = search().query(match("field", "value"))
                                              .filter(match("field", "value"))
                                              .sort(field("field"));
        String json = searchBuilder.toJson();
        assertEquals("JSON serialization is wrong", json, SearchBuilder.fromJson(json).toJson());
    }

    @Test(expected = IndexException.class)
    public void testFromJsonInvalid() {
        SearchBuilder.fromJson("error");
    }
}