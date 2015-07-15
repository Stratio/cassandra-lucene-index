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
package com.stratio.cassandra.lucene.search.condition.builder;

import com.stratio.cassandra.lucene.search.condition.BitemporalCondition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Eduardo Alonso  {@literal <eduardoalonso@stratio.com>}
 */
public class BitemporalConditionBuilderTest extends AbstractConditionBuilderTest {

    @Test
    public void testBuildLong() {
        BitemporalConditionBuilder builder = new BitemporalConditionBuilder("field");
        builder.ttFrom((long) 1);
        builder.ttTo((long) 2);
        builder.vtFrom((long) 3);
        builder.vtTo((long) 4);
        builder.operation("intersects");
        BitemporalCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals("field", condition.field);
        assertEquals((long) 1, condition.ttFrom);
        assertEquals((long) 2, condition.ttTo);
        assertEquals((long) 3, condition.vtFrom);
        assertEquals((long) 4, condition.vtTo);
        assertEquals("intersects", condition.operation);
    }

    @Test
    public void testBuildString() {
        BitemporalConditionBuilder builder = new BitemporalConditionBuilder("field");
        builder.ttFrom("2015/03/20 11:45:32.333");
        builder.ttTo("2013/03/20 11:45:32.333");
        builder.vtFrom("2012/03/20 11:45:32.333");
        builder.vtTo("2011/03/20 11:45:32.333");
        builder.operation("intersects");
        BitemporalCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals("field", condition.field);
        assertEquals("2015/03/20 11:45:32.333", condition.ttFrom);
        assertEquals("2013/03/20 11:45:32.333", condition.ttTo);
        assertEquals("2012/03/20 11:45:32.333", condition.vtFrom);
        assertEquals("2011/03/20 11:45:32.333", condition.vtTo);
        assertEquals("intersects", condition.operation);
    }

    @Test
    public void testBuildDefaults() {
        BitemporalConditionBuilder builder = new BitemporalConditionBuilder("field");
        BitemporalCondition condition = builder.build();
        assertNotNull(condition);
        assertEquals("field", condition.field);
        assertEquals(null, condition.ttFrom);
        assertEquals(null, condition.ttTo);
        assertEquals(null, condition.vtFrom);
        assertEquals(null, condition.vtTo);
        assertEquals(BitemporalCondition.DEFAULT_OPERATION, condition.operation);
    }

    @Test
    public void testJsonSerialization() {
        BitemporalConditionBuilder builder = new BitemporalConditionBuilder("field").boost(0.7f);
        testJsonSerialization(builder, "{type:\"bitemporal\",field:\"field\",boost:0.7}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        BitemporalConditionBuilder builder = new BitemporalConditionBuilder("field");
        testJsonSerialization(builder, "{type:\"bitemporal\",field:\"field\"}");
    }
}
