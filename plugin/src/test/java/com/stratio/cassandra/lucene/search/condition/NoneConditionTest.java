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
import com.stratio.cassandra.lucene.search.condition.builder.NoneConditionBuilder;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.schema;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class NoneConditionTest extends AbstractConditionTest {

    @Test
    public void testBuild() {
        NoneConditionBuilder builder = new NoneConditionBuilder().boost(0.7f);
        NoneCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
    }

    @Test
    public void testBuildDefaults() {
        NoneConditionBuilder builder = new NoneConditionBuilder();
        NoneCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertNull("Boost is not set to default", condition.boost);
    }

    @Test
    public void testJsonSerialization() {
        NoneConditionBuilder builder = new NoneConditionBuilder().boost(0.7);
        testJsonSerialization(builder, "{type:\"none\",boost:0.7}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        NoneConditionBuilder builder = new NoneConditionBuilder();
        testJsonSerialization(builder, "{type:\"none\"}");
    }

    @Test
    public void testQuery() {
        Schema schema = schema().build();
        NoneCondition condition = new NoneCondition(0.7f);
        Query query = condition.doQuery(schema);
        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", BooleanQuery.class, query.getClass());
    }

    @Test
    public void testToString() {
        NoneCondition condition = new NoneCondition(0.7f);
        assertEquals("Method #toString is wrong", "NoneCondition{boost=0.7}", condition.toString());
    }

}
