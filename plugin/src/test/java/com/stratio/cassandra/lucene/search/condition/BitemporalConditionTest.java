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
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static com.stratio.cassandra.lucene.search.SearchBuilders.bitemporalSearch;
import static org.junit.Assert.*;

/**
 * @author Eduardo Alonso  {@literal <eduardoalonso@stratio.com>}
 */
public class BitemporalConditionTest {

    private static final String TIMESTAMP_PATTERN = "timestamp";

    @Test
    public void testConstructorWithDefaults() {
        BitemporalCondition condition = new BitemporalCondition(null, "name", 1, 2, 3, 4, null);

        assertEquals(BitemporalCondition.DEFAULT_BOOST, condition.boost, 0);
        assertEquals("name", condition.field);
        assertEquals(1, condition.vtFrom);
        assertEquals(2, condition.vtTo);
        assertEquals(3, condition.ttFrom);
        assertEquals(4, condition.ttTo);
        assertEquals(BitemporalCondition.DEFAULT_OPERATION, condition.operation);
    }

    @Test
    public void testConstructorWithAllArgs() {
        BitemporalCondition condition = new BitemporalCondition(0.5f, "name", 1, 2, 3, 4, "intersects");
        assertEquals(0.5, condition.boost, 0);
        assertEquals("name", condition.field);
        assertEquals(1, condition.vtFrom);
        assertEquals(2, condition.vtTo);
        assertEquals(3, condition.ttFrom);
        assertEquals(4, condition.ttTo);
        assertEquals("intersects", condition.operation);
    }

    @Test
    public void testQuery() {

        Schema schema = schema().mapper("name",
                                        bitemporalMapper("vtFrom", "vtTo", "ttFrom", "ttTo").pattern(TIMESTAMP_PATTERN))
                                .build();
        BitemporalCondition condition = new BitemporalCondition(0.5f, "name", 1, 2, 3, 4, null);

        Query query = condition.query(schema);
        assertNotNull(query);
        assertTrue(query instanceof BooleanQuery);
    }

    @Test(expected = IndexException.class)
    public void testQueryWithoutValidMapper() {
        Schema schema = schema().mapper("name", uuidMapper()).build();
        BitemporalCondition condition = new BitemporalCondition(null, "name", 1, 2, 3, 4, null);
        condition.query(schema);
    }

    @Test
    public void testToString() {
        BitemporalCondition condition = bitemporalSearch("name").vtFrom(1).vtTo(2).ttFrom(3).ttTo(4).boost(0.3).build();
        assertEquals("BitemporalCondition{boost=0.3, field=name, vtFrom=1, vtTo=2, ttFrom=3, ttTo=4}",
                     condition.toString());
    }
}
