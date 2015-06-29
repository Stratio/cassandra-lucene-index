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

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.DateRangeMapper;
import com.stratio.cassandra.lucene.schema.mapping.UUIDMapper;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.IntersectsPrefixTreeFilter;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.junit.Test;

import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.dateRange;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class DateRangeConditionTest extends AbstractConditionTest {

    @Test
    public void testConstructorWithDefaults() {
        DateRangeCondition condition = new DateRangeCondition(null, "name", 1, 2, null);
        assertEquals(DateRangeCondition.DEFAULT_BOOST, condition.boost, 0);
        assertEquals("name", condition.field);
        assertEquals(1, condition.start);
        assertEquals(2, condition.stop);
        assertEquals(DateRangeCondition.DEFAULT_OPERATION, condition.operation);
    }

    @Test
    public void testConstructorWithAllArgs() {
        DateRangeCondition condition = new DateRangeCondition(0.5f, "name", 1, 2, "contains");
        assertEquals(0.5, condition.boost, 0);
        assertEquals("name", condition.field);
        assertEquals(1, condition.start);
        assertEquals(2, condition.stop);
        assertEquals("contains", condition.operation);
    }

    @Test
    public void testParseSpatialOperationIntersectsLowerCase() {
        assertEquals(SpatialOperation.Intersects, DateRangeCondition.parseSpatialOperation("intersects"));
    }

    @Test
    public void testParseSpatialOperationIntersectsUpperCase() {
        assertEquals(SpatialOperation.Intersects, DateRangeCondition.parseSpatialOperation("INTERSECTS"));
    }

    @Test
    public void testParseSpatialOperationIsWithinLowerCase() {
        assertEquals(SpatialOperation.IsWithin, DateRangeCondition.parseSpatialOperation("is_within"));
    }

    @Test
    public void testParseSpatialOperationIsWithinUpperCase() {
        assertEquals(SpatialOperation.IsWithin, DateRangeCondition.parseSpatialOperation("IS_WITHIN"));
    }

    @Test
    public void testParseSpatialOperationIContainsLowerCase() {
        assertEquals(SpatialOperation.Contains, DateRangeCondition.parseSpatialOperation("contains"));
    }

    @Test
    public void testParseSpatialOperationContainsUpperCase() {
        assertEquals(SpatialOperation.Contains, DateRangeCondition.parseSpatialOperation("CONTAINS"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseSpatialOperationNull() {
        DateRangeCondition.parseSpatialOperation(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseSpatialOperationEmpty() {
        DateRangeCondition.parseSpatialOperation("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseSpatialOperationBlank() {
        DateRangeCondition.parseSpatialOperation(" ");
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
        DateRangeCondition condition = new DateRangeCondition(null, "name", 1, 2, null);
        condition.query(schema);
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
