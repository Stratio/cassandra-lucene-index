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
package com.stratio.cassandra.lucene.query;

import com.stratio.cassandra.lucene.query.builder.DateRangeConditionBuilder;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.DateRangeMapper;
import com.stratio.cassandra.lucene.schema.mapping.UUIDMapper;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.IntersectsPrefixTreeFilter;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.junit.Test;

import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.dateRange;
import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.query;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class DateRangeConditionTest extends AbstractConditionTest {

    @Test
    public void testConstructorWithDefaults() {
        DateRangeCondition condition = new DateRangeCondition(null, "name", 1, 2, null);
        assertEquals(DateRangeCondition.DEFAULT_BOOST, condition.getBoost(), 0);
        assertEquals("name", condition.getField());
        assertEquals(1, condition.getStart());
        assertEquals(2, condition.getStop());
        assertEquals(SpatialOperation.Intersects, condition.getSpatialOperation());
    }

    @Test
    public void testConstructorWithIntersects() {
        DateRangeCondition condition = new DateRangeCondition(0.5f, "name", 1, 2, "intersects");
        assertEquals(0.5, condition.getBoost(), 0);
        assertEquals("name", condition.getField());
        assertEquals(1, condition.getStart());
        assertEquals(2, condition.getStop());
        assertEquals(SpatialOperation.Intersects, condition.getSpatialOperation());
    }

    @Test
    public void testConstructorWithContains() {
        DateRangeCondition condition = new DateRangeCondition(0.5f, "name", 1, 2, "contains");
        assertEquals(0.5, condition.getBoost(), 0);
        assertEquals("name", condition.getField());
        assertEquals(1, condition.getStart());
        assertEquals(2, condition.getStop());
        assertEquals(SpatialOperation.Contains, condition.getSpatialOperation());
    }

    @Test
    public void testConstructorWithIsWithin() {
        DateRangeCondition condition = new DateRangeCondition(0.5f, "name", 1, 2, "is_within");
        assertEquals(0.5, condition.getBoost(), 0);
        assertEquals("name", condition.getField());
        assertEquals(1, condition.getStart());
        assertEquals(2, condition.getStop());
        assertEquals(SpatialOperation.IsWithin, condition.getSpatialOperation());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithIsBadOperation() {
        new DateRangeCondition(0.5f, "name", 1, 2, "abc");
    }

    @Test
    public void testQuery() {
        Schema schema = mockSchema("name", new DateRangeMapper("field", "to", "from", null));
        DateRangeCondition condition = new DateRangeCondition(null, "name", 1, 2, null);
        Query query = condition.query(schema);
        assertNotNull(query);
        assertTrue(query instanceof ConstantScoreQuery);
        query = ((ConstantScoreQuery) query).getQuery();
        assertTrue(query instanceof IntersectsPrefixTreeFilter);
        IntersectsPrefixTreeFilter filter = (IntersectsPrefixTreeFilter) query;
        assertEquals(
                "IntersectsPrefixTreeFilter(fieldName=field,queryShape=[1970-01-01T00:00:00.001 TO 1970-01-01T00:00:00.002],detailLevel=9,prefixGridScanLevel=7)",
                filter.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryWithoutValidMapper() {
        Schema schema = mockSchema("name", new UUIDMapper("name", null, null));
        GeoBBoxCondition condition = new GeoBBoxCondition(0.5f, "name", -180D, 180D, -90D, 90D);
        condition.query(schema);
    }

    @Test
    public void testJson() {
        DateRangeConditionBuilder condition = dateRange("name").setStart(1)
                                                               .setStop(2)
                                                               .setOperation("contains")
                                                               .boost(0.3);
        testJsonCondition(query(condition));
    }

    @Test
    public void testToString() {
        DateRangeCondition condition = dateRange("name").setStart(1)
                                                        .setStop(2)
                                                        .setOperation("contains")
                                                        .boost(0.3)
                                                        .build();
        assertEquals("DateRangeCondition{boost=0.3, field=name, start=1, stop=2, operation=contains}",
                     condition.toString());
    }
}
