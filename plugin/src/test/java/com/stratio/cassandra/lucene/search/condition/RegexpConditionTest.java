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

package com.stratio.cassandra.lucene.search.condition;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.search.condition.builder.RegexpConditionBuilder;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RegexpConditionTest extends AbstractConditionTest {

    @Test
    public void testBuild() {
        RegexpConditionBuilder builder = new RegexpConditionBuilder("field", "value").boost(0.7f);
        RegexpCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Value is not set", "value", condition.value);
    }

    @Test
    public void testBuildDefaults() {
        RegexpConditionBuilder builder = new RegexpConditionBuilder("field", "value");
        RegexpCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set to default", Condition.DEFAULT_BOOST, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Value is not set", "value", condition.value);
    }

    @Test(expected = IndexException.class)
    public void testBuildNullValue() {
        new RegexpConditionBuilder("field", null).build();
    }

    @Test
    public void testJsonSerialization() {
        RegexpConditionBuilder builder = new RegexpConditionBuilder("field", "value").boost(0.7f);
        testJsonSerialization(builder, "{type:\"regexp\",field:\"field\",value:\"value\",boost:0.7}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        RegexpConditionBuilder builder = new RegexpConditionBuilder("field", "value");
        testJsonSerialization(builder, "{type:\"regexp\",field:\"field\",value:\"value\"}");
    }

    @Test
    public void testBlankValue() {

        Schema schema = schema().mapper("name", stringMapper().indexed(true).sorted(true)).build();

        RegexpCondition condition = new RegexpCondition(0.5f, "name", " ");
        Query query = condition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", RegexpQuery.class, query.getClass());
        RegexpQuery regexQuery = (RegexpQuery) query;
        assertEquals("Query field is wrong", "name", regexQuery.getField());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testString() {

        Schema schema = schema().mapper("name", stringMapper().indexed(true).sorted(true)).build();

        RegexpCondition condition = new RegexpCondition(0.5f, "name", "tr*");
        Query query = condition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", RegexpQuery.class, query.getClass());
        RegexpQuery regexQuery = (RegexpQuery) query;
        assertEquals("Query field is wrong", "name", regexQuery.getField());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test(expected = IndexException.class)
    public void testInteger() {

        Schema schema = schema().mapper("name", integerMapper()).build();

        RegexpCondition condition = new RegexpCondition(0.5f, "name", "22*");
        condition.query(schema);
    }

    @Test
    public void testInetV4() {

        Schema schema = schema().mapper("name", inetMapper()).build();

        RegexpCondition condition = new RegexpCondition(0.5f, "name", "192.168.*");
        Query query = condition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", RegexpQuery.class, query.getClass());
        RegexpQuery regexQuery = (RegexpQuery) query;
        assertEquals("Query field is wrong", "name", regexQuery.getField());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV6() {

        Schema schema = schema().mapper("name", inetMapper()).build();

        RegexpCondition regexpCondition = new RegexpCondition(0.5f, "name", "2001:db8:2de:0:0:0:0:e*");
        Query query = regexpCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", RegexpQuery.class, query.getClass());
        RegexpQuery regexQuery = (RegexpQuery) query;
        assertEquals("Query field is wrong", "name", regexQuery.getField());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testToString() {
        RegexpCondition condition = new RegexpCondition(0.5f, "name", "2001:db8:2de:0:0:0:0:e*");
        assertEquals("Method #toString is wrong",
                     "RegexpCondition{boost=0.5, field=name, value=2001:db8:2de:0:0:0:0:e*}",
                     condition.toString());
    }

}
