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
import com.stratio.cassandra.lucene.schema.mapping.builder.MapperBuilder;
import com.stratio.cassandra.lucene.search.condition.builder.BitemporalConditionBuilder;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static com.stratio.cassandra.lucene.search.SearchBuilders.bitemporal;
import static com.stratio.cassandra.lucene.search.condition.Condition.DEFAULT_BOOST;
import static org.junit.Assert.*;

/**
 * @author Eduardo Alonso  {@literal <eduardoalonso@stratio.com>}
 */
public class BitemporalConditionTest extends AbstractConditionTest {

    private static final String TIMESTAMP_PATTERN = "timestamp";

    @Test
    public void testBuildLong() {
        BitemporalCondition condition = new BitemporalConditionBuilder("field").boost(0.7)
                                                                               .ttFrom(1L)
                                                                               .ttTo(2L)
                                                                               .vtFrom(3L)
                                                                               .vtTo(4L)
                                                                               .build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("tt_from is not set", 1l, condition.ttFrom);
        assertEquals("tt_to is not set", 2l, condition.ttTo);
        assertEquals("vt_from is not set", 3l, condition.vtFrom);
        assertEquals("vt_to is not set", 4l, condition.vtTo);
    }

    @Test
    public void testBuildString() {
        BitemporalCondition condition = new BitemporalConditionBuilder("field").boost(0.7)
                                                                               .ttFrom("2015/03/20 11:45:32.333")
                                                                               .ttTo("2013/03/20 11:45:32.333")
                                                                               .vtFrom("2012/03/20 11:45:32.333")
                                                                               .vtTo("2011/03/20 11:45:32.333")
                                                                               .build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("tt_from is not set", "2015/03/20 11:45:32.333", condition.ttFrom);
        assertEquals("tt_to is not set", "2013/03/20 11:45:32.333", condition.ttTo);
        assertEquals("vt_from is not set", "2012/03/20 11:45:32.333", condition.vtFrom);
        assertEquals("vt_to is not set", "2011/03/20 11:45:32.333", condition.vtTo);
    }

    @Test
    public void testBuildDefaults() {
        BitemporalCondition condition = new BitemporalConditionBuilder("field").build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set to default", DEFAULT_BOOST, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertNull("tt_from is not set to default", condition.ttFrom);
        assertNull("tt_to is not set to default", condition.ttTo);
        assertNull("vt_from is not set to default", condition.vtFrom);
        assertNull("vt_to is not set to default", condition.vtTo);
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

    @Test
    public void testQuery() {

        MapperBuilder<?, ?> mapperBuilder = bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern(
                TIMESTAMP_PATTERN);
        Schema schema = schema().mapper("name", mapperBuilder).build();
        BitemporalCondition condition = new BitemporalCondition(0.5f, "name", 1, 2, 3, 4);

        Query query = condition.query(schema);
        assertNotNull("Query is not built", query);
        assertTrue("Query type is wrong", query instanceof BooleanQuery);
    }

    @Test(expected = IndexException.class)
    public void testQueryWithoutValidMapper() {
        Schema schema = schema().mapper("name", uuidMapper()).build();
        BitemporalCondition condition = new BitemporalCondition(null, "name", 1, 2, 3, 4);
        condition.query(schema);
    }

    @Test
    public void testToString() {
        BitemporalCondition condition = bitemporal("name").vtFrom(1).vtTo(2).ttFrom(3).ttTo(4).boost(0.3).build();
        assertEquals("Method #toString is wrong",
                     "BitemporalCondition{boost=0.3, field=name, vtFrom=1, vtTo=2, ttFrom=3, ttTo=4}",
                     condition.toString());
    }

}
