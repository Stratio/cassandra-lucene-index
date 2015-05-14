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

import com.stratio.cassandra.lucene.query.Search;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.match;
import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.sortField;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SearchBuilderTest {

    @Test
    public void testBuild() throws IOException {
        ConditionBuilder query = match("field1", "value2");
        ConditionBuilder filter = match("field2", "value2");
        SortFieldBuilder sort1 = sortField("field3");
        SortFieldBuilder sort2 = sortField("field4");
        SearchBuilder searchBuilder = new SearchBuilder().query(query).filter(filter).sort(sort1, sort2);
        Search search = searchBuilder.build();
        String json = searchBuilder.toJson();
        Assert.assertEquals(json, JsonSerializer.toString(search));
    }
}
