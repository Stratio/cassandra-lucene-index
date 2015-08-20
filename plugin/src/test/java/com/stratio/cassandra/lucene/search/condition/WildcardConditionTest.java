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
import com.stratio.cassandra.lucene.search.SearchBuilders;
import com.stratio.cassandra.lucene.search.condition.builder.WildcardConditionBuilder;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class WildcardConditionTest extends AbstractConditionTest {

    @Test
    public void testBuild() {
        WildcardConditionBuilder builder = new WildcardConditionBuilder("field", "value").boost(0.7f);
        WildcardCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Value is not set", "value", condition.value);
    }

    @Test
    public void testBuildDefaults() {
        WildcardConditionBuilder builder = new WildcardConditionBuilder("field", "value");
        WildcardCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", Condition.DEFAULT_BOOST, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Value is not set", "value", condition.value);
    }

    @Test
    public void testJsonSerialization() {
        WildcardConditionBuilder builder = new WildcardConditionBuilder("field", "value").boost(0.7f);
        testJsonSerialization(builder, "{type:\"wildcard\",field:\"field\",value:\"value\",boost:0.7}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        WildcardConditionBuilder builder = new WildcardConditionBuilder("field", "value");
        testJsonSerialization(builder, "{type:\"wildcard\",field:\"field\",value:\"value\"}");
    }

    @Test(expected = IndexException.class)
    public void testNullValue() {
        new WildcardCondition(0.1f, "field", null);
    }

    @Test
    public void testBlankValue() {

        Schema schema = schema().mapper("name", stringMapper().indexed(true).sorted(true)).build();

        WildcardCondition wildcardCondition = new WildcardCondition(0.5f, "name", " ");
        Query query = wildcardCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Expected wildcard query", WildcardQuery.class, query.getClass());
        WildcardQuery wildcardQuery = (WildcardQuery) query;
        assertEquals("Field name is not properly set", "name", wildcardQuery.getField());
        assertEquals("Term text is not properly set", " ", wildcardQuery.getTerm().text());
        assertEquals("Boost is not properly set", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testStringValue() {

        Schema schema = schema().mapper("name", stringMapper().indexed(true).sorted(true)).build();

        WildcardCondition wildcardCondition = new WildcardCondition(0.5f, "name", "tr*");
        Query query = wildcardCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Expected wildcard query", WildcardQuery.class, query.getClass());
        WildcardQuery wildcardQuery = (WildcardQuery) query;
        assertEquals("Field name is not properly set", "name", wildcardQuery.getField());
        assertEquals("Term text is not properly set", "tr*", wildcardQuery.getTerm().text());
        assertEquals("Boost is not properly set", 0.5f, query.getBoost(), 0);
    }

    @Test(expected = IndexException.class)
    public void testIntegerValue() {

        Schema schema = schema().mapper("name", integerMapper()).build();

        WildcardCondition wildcardCondition = new WildcardCondition(0.5f, "name", "22*");
        wildcardCondition.query(schema);
    }

    @Test
    public void testInetV4Value() {

        Schema schema = schema().mapper("name", inetMapper()).build();

        WildcardCondition wildcardCondition = new WildcardCondition(0.5f, "name", "192.168.*");
        Query query = wildcardCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Expected wildcard query", WildcardQuery.class, query.getClass());
        WildcardQuery wildcardQuery = (WildcardQuery) query;
        assertEquals("Field name is not properly set", "name", wildcardQuery.getField());
        assertEquals("Term text is not properly set", "192.168.*", wildcardQuery.getTerm().text());
        assertEquals("Boost is not properly set", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV6Value() {

        Schema schema = schema().mapper("name", inetMapper()).build();

        WildcardCondition condition = new WildcardCondition(0.5f, "name", "2001:db8:2de:0:0:0:0:e*");
        Query query = condition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Expected wildcard query", WildcardQuery.class, query.getClass());
        WildcardQuery wildcardQuery = (WildcardQuery) query;
        assertEquals("Field name is not properly set", "name", wildcardQuery.getField());
        assertEquals("Term text is not properly set", "2001:db8:2de:0:0:0:0:e*", wildcardQuery.getTerm().text());
        assertEquals("Boost is not properly set", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testToString() {
        WildcardCondition condition = SearchBuilders.wildcard("name", "aaa*").boost(0.5f).build();
        assertEquals("Method #toString is wrong",
                     "WildcardCondition{boost=0.5, field=name, value=aaa*}",
                     condition.toString());
    }

}
