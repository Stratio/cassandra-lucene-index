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
        ConditionBuilder<?, ?> filter = match("field1", "value2");
        ConditionBuilder<?, ?> query = match("field2", "value2");
        SimpleSortFieldBuilder sort1 = field("field3");
        SimpleSortFieldBuilder sort2 = field("field4");
        SearchBuilder builder = new SearchBuilder().filter(filter)
                                                   .query(query)
                                                   .sort(sort1, sort2);
        String json = builder.toJson();
        assertEquals("JSON serialization is wrong", json, JsonSerializer.toString(builder));
    }

    @Test
    public void testJson() {
        SearchBuilder searchBuilder = search().filter(match("field1", "value1"))
                                              .query(match("field2", "value2"))
                                              .sort(field("field"));
        String json = searchBuilder.toJson();
        assertEquals("JSON serialization is wrong", json, SearchBuilder.fromJson(json).toJson());
    }

    @Test(expected = IndexException.class)
    public void testFromJsonInvalid() {
        SearchBuilder.fromJson("error");
    }

    @Test
    public void testFromLegacyJsonWithOnlyQuery() {
        assertEquals("Legacy syntax parsing fails",
                     "Search{" +
                     "filter=[], " +
                     "query=[MatchCondition{boost=null, field=f, value=1, docValues=false}], sort=[], " +
                     "refresh=false, " +
                     "paging=null}",
                     SearchBuilder.fromJson("{query:{type: \"match\", field: \"f\", value:1}}").build().toString());
    }

    @Test
    public void testFromLegacyJsonWithOnlySort() {
        assertEquals("Legacy syntax parsing fails",
                     "Search{" +
                     "filter=[], " +
                     "query=[], " +
                     "sort=[SimpleSortField{field=f, reverse=false}], " +
                     "refresh=false, " +
                     "paging=null}",
                     SearchBuilder.fromJson("{sort:{fields:[{field:\"f\"}]}}").build().toString());
    }

    @Test
    public void testFromLegacyJsonWithAll() {
        assertEquals("Legacy syntax parsing fails",
                     "Search{" +
                     "filter=[MatchCondition{boost=null, field=f1, value=1, docValues=false}], " +
                     "query=[MatchCondition{boost=null, field=f2, value=2, docValues=false}], " +
                     "sort=[SimpleSortField{field=f, reverse=false}], " +
                     "refresh=true, " +
                     "paging=null}",
                     SearchBuilder.fromJson("{filter:{type: \"match\", field: \"f1\", value:1}, " +
                                            "query:{type: \"match\", field: \"f2\", value:2}, " +
                                            "sort:{fields:[{field:\"f\"}]}, " +
                                            "refresh:true}}").build().toString());
    }
}