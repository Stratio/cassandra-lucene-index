/*
 * Copyright 2014, Stratio.
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
package com.stratio.cassandra.lucene.search.condition;

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.InetMapper;
import com.stratio.cassandra.lucene.schema.mapping.IntegerMapper;
import com.stratio.cassandra.lucene.schema.mapping.StringMapper;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RegexpConditionTest extends AbstractConditionTest {

    @Test
    public void testBuild() {
        RegexpCondition condition = new RegexpCondition(0.5f, "field", "value");
        assertEquals(0.5f, condition.boost, 0);
        assertEquals("field", condition.field);
        assertEquals("value", condition.value);
    }

    @Test
    public void testBuildDefaults() {
        RegexpCondition condition = new RegexpCondition(null, "field", "value");
        assertEquals(Condition.DEFAULT_BOOST, condition.boost, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildNullValue() {
        new RegexpCondition(null, "field", null);
    }

    @Test
    public void testBuildBlankValue() {
        Schema schema = mockSchema("name", new StringMapper("name", true, true, null));

        RegexpCondition condition = new RegexpCondition(0.5f, "name", " ");
        Query query = condition.query(schema);

        assertNotNull(query);
        assertEquals(RegexpQuery.class, query.getClass());
        RegexpQuery luceneQuery = (RegexpQuery) query;
        assertEquals("name", luceneQuery.getField());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testString() {

        Schema schema = mockSchema("name", new StringMapper("name", true, true, null));

        RegexpCondition condition = new RegexpCondition(0.5f, "name", "tr*");
        Query query = condition.query(schema);

        assertNotNull(query);
        assertEquals(RegexpQuery.class, query.getClass());
        RegexpQuery luceneQuery = (RegexpQuery) query;
        assertEquals("name", luceneQuery.getField());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInteger() {

        Schema schema = mockSchema("name", new IntegerMapper("name", null, null, 1f));

        RegexpCondition condition = new RegexpCondition(0.5f, "name", "22*");
        condition.query(schema);
    }

    @Test
    public void testInetV4() {

        Schema schema = mockSchema("name", new InetMapper("name", null, null));

        RegexpCondition condition = new RegexpCondition(0.5f, "name", "192.168.*");
        Query query = condition.query(schema);

        assertNotNull(query);
        assertEquals(RegexpQuery.class, query.getClass());
        RegexpQuery luceneQuery = (RegexpQuery) query;
        assertEquals("name", luceneQuery.getField());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV6() {

        Schema schema = mockSchema("name", new InetMapper("name", null, null));

        RegexpCondition regexpCondition = new RegexpCondition(0.5f, "name", "2001:db8:2de:0:0:0:0:e*");
        Query query = regexpCondition.query(schema);

        assertNotNull(query);
        assertEquals(RegexpQuery.class, query.getClass());
        RegexpQuery luceneQuery = (RegexpQuery) query;
        assertEquals("name", luceneQuery.getField());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testToString() {
        RegexpCondition condition = new RegexpCondition(0.5f, "name", "2001:db8:2de:0:0:0:0:e*");
        assertEquals("RegexpCondition{boost=0.5, field=name, value=2001:db8:2de:0:0:0:0:e*}", condition.toString());
    }

}
