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
package com.stratio.cassandra.lucene.search.condition;

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.search.condition.builder.AllConditionBuilder;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.schema;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class AllConditionTest extends AbstractConditionTest {

    @Test
    public void testBuild() {
        AllConditionBuilder builder = new AllConditionBuilder().boost(0.7f);
        AllCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
    }

    @Test
    public void testBuildDefaults() {
        AllConditionBuilder builder = new AllConditionBuilder();
        AllCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertNull("Boost is not set to default", condition.boost);
    }

    @Test
    public void testJsonSerialization() {
        AllConditionBuilder builder = new AllConditionBuilder().boost(0.7);
        testJsonSerialization(builder, "{type:\"all\",boost:0.7}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        AllConditionBuilder builder = new AllConditionBuilder();
        testJsonSerialization(builder, "{type:\"all\"}");
    }

    @Test
    public void testQuery() {
        AllCondition condition = new AllCondition(0.7f);
        Schema schema = schema().build();
        Query query = condition.doQuery(schema);
        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", MatchAllDocsQuery.class, query.getClass());
    }

    @Test
    public void testToString() {
        AllCondition condition = new AllCondition(0.7f);
        assertEquals("Method #toString is wrong", "AllCondition{boost=0.7}", condition.toString());
    }

}
