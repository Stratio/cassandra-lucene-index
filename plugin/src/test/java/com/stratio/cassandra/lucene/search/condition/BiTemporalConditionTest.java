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
import com.stratio.cassandra.lucene.schema.mapping.BiTemporalMapper;
import com.stratio.cassandra.lucene.schema.mapping.UUIDMapper;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static com.stratio.cassandra.lucene.search.SearchBuilders.biTemporalSearch;
import static org.junit.Assert.*;

/**
 * @author Eduardo Alonso <eduardoalonso@stratio.com>
 */

public class BiTemporalConditionTest extends AbstractConditionTest {

    @Test
    public void testConstructorWithDefaults() {
        BiTemporalCondition condition = new BiTemporalCondition(null, "name", 1, 2, 3, 4, null);

        assertEquals(BiTemporalCondition.DEFAULT_BOOST, condition.boost, 0);
        assertEquals("name", condition.field);
        assertEquals(1, condition.vt_from);
        assertEquals(2, condition.vt_to);
        assertEquals(3, condition.tt_from);
        assertEquals(4, condition.tt_to);
        assertEquals(BiTemporalCondition.DEFAULT_OPERATION, condition.operation);
    }

    @Test
    public void testConstructorWithAllArgs() {
        BiTemporalCondition condition = new BiTemporalCondition(0.5f, "name", 1, 2, 3, 4, "intersects");
        assertEquals(0.5, condition.boost, 0);
        assertEquals("name", condition.field);
        assertEquals(1, condition.vt_from);
        assertEquals(2, condition.vt_to);
        assertEquals(3, condition.tt_from);
        assertEquals(4, condition.tt_to);
        assertEquals("intersects", condition.operation);
    }

    @Test
    public void testQuery() {
        Schema schema = mockSchema("name", new BiTemporalMapper("name", "vt_from", "vt_to", "tt_from", "tt_to", null));
        BiTemporalCondition condition = new BiTemporalCondition(0.5f, "name", 1, 2, 3, 4, null);

        Query query = condition.query(schema);
        assertNotNull(query);
        assertTrue(query instanceof BooleanQuery);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryWithoutValidMapper() {
        Schema schema = mockSchema("name", new UUIDMapper("name", null, null));
        BiTemporalCondition condition = new BiTemporalCondition(null, "name", 1, 2, 3, 4, null);
        condition.query(schema);
    }

    @Test
    public void testToString() {
        BiTemporalCondition condition = biTemporalSearch("name").setVt_from(1)
                                                                .setVt_to(2)
                                                                .setTt_from(3)
                                                                .setTt_to(4)
                                                                .boost(0.3)
                                                                .build();
        assertEquals("BiTemporalCondition{boost=0.3, field=name, vt_from=1, vt_to=2, tt_from=3, tt_to=4}",
                     condition.toString());
    }
}
