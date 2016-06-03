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
import com.stratio.cassandra.lucene.search.condition.builder.PrefixConditionBuilder;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class PrefixConditionTest extends AbstractConditionTest {

    @Test
    public void testBuild() {
        PrefixConditionBuilder builder = new PrefixConditionBuilder("field", "value").boost(0.7);
        PrefixCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Value is not set", "value", condition.value);
    }

    @Test
    public void testBuildDefaults() {
        PrefixConditionBuilder builder = new PrefixConditionBuilder("field", "value");
        PrefixCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertNull("Boost is not set to default", condition.boost);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Value is not set", "value", condition.value);
    }

    @Test
    public void testJsonSerialization() {
        PrefixConditionBuilder builder = new PrefixConditionBuilder("field", "value").boost(0.7);
        testJsonSerialization(builder, "{type:\"prefix\",field:\"field\",value:\"value\",boost:0.7}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        PrefixConditionBuilder builder = new PrefixConditionBuilder("field", "value");
        testJsonSerialization(builder, "{type:\"prefix\",field:\"field\",value:\"value\"}");
    }

    @Test
    public void testStringValue() {

        Schema schema = schema().mapper("name", stringMapper()).build();

        PrefixCondition prefixCondition = new PrefixCondition(0.5f, "name", "tr");
        Query query = prefixCondition.doQuery(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", PrefixQuery.class, query.getClass());

        PrefixQuery luceneQuery = (PrefixQuery) query;
        assertEquals("Query field is wrong", "name", luceneQuery.getField());
        assertEquals("Query prefix is wrong", "tr", luceneQuery.getPrefix().text());
    }

    @Test
    public void testInetV4Value() {

        Schema schema = schema().mapper("name", inetMapper()).build();

        PrefixCondition wildcardCondition = new PrefixCondition(0.5f, "name", "192.168.");
        Query query = wildcardCondition.doQuery(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", PrefixQuery.class, query.getClass());

        PrefixQuery luceneQuery = (PrefixQuery) query;
        assertEquals("Query field is wrong", "name", luceneQuery.getField());
        assertEquals("Query prefix is wrong", "192.168.", luceneQuery.getPrefix().text());
    }

    @Test
    public void testInetV6Value() {

        Schema schema = schema().mapper("name", inetMapper()).build();

        PrefixCondition wildcardCondition = new PrefixCondition(0.5f, "name", "2001:db8:2de:0:0:0:0:e");
        Query query = wildcardCondition.doQuery(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", PrefixQuery.class, query.getClass());

        PrefixQuery luceneQuery = (PrefixQuery) query;
        assertEquals("Query field is wrong", "name", luceneQuery.getField());
        assertEquals("Query prefix is wrong", "2001:db8:2de:0:0:0:0:e", luceneQuery.getPrefix().text());
    }

    @Test
    public void testToString() {
        PrefixCondition condition = new PrefixCondition(0.5f, "name", "tr");
        assertEquals("Method #toString is wrong",
                     "PrefixCondition{boost=0.5, field=name, value=tr}",
                     condition.toString());
    }

}
