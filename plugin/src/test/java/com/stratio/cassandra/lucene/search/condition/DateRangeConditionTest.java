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
import com.stratio.cassandra.lucene.search.condition.builder.DateRangeConditionBuilder;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.IntersectsPrefixTreeFilter;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static com.stratio.cassandra.lucene.search.SearchBuilders.dateRange;
import static com.stratio.cassandra.lucene.search.condition.DateRangeCondition.*;
import static org.apache.lucene.spatial.query.SpatialOperation.*;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DateRangeConditionTest extends AbstractConditionTest {

    private static final String TIMESTAMP_PATTERN = "timestamp";

    @Test
    public void testBuildString() {
        DateRangeConditionBuilder builder = new DateRangeConditionBuilder("field").boost(0.4)
                                                                                  .from("2015/01/05")
                                                                                  .to("2015/01/08")
                                                                                  .operation("intersects");
        DateRangeCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.4f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("From is not set", "2015/01/05", condition.from);
        assertEquals("To is not set", "2015/01/08", condition.to);
        assertEquals("Operation is not set", "intersects", condition.operation);
    }

    @Test
    public void testBuildNumber() {
        DateRangeConditionBuilder builder = new DateRangeConditionBuilder("field").boost(0.4)
                                                                                  .from(1)
                                                                                  .to(2)
                                                                                  .operation("is_within");
        DateRangeCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.4f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("From is not set", 1, condition.from);
        assertEquals("To is not set", 2, condition.to);
        assertEquals("Operation is not set", "is_within", condition.operation);
    }

    @Test
    public void testBuildDefaults() {
        DateRangeConditionBuilder builder = new DateRangeConditionBuilder("field");
        DateRangeCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set to default", DEFAULT_BOOST, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("From is not set to default", DEFAULT_FROM, condition.from);
        assertEquals("To is not set to default", DEFAULT_TO, condition.to);
        assertEquals("Operation is not set to default", DEFAULT_OPERATION, condition.operation);
    }

    @Test
    public void testJsonSerializationString() {
        DateRangeConditionBuilder builder = new DateRangeConditionBuilder("field").boost(0.4)
                                                                                  .from("from")
                                                                                  .to("to")
                                                                                  .operation("intersects");
        testJsonSerialization(builder,
                              "{type:\"date_range\",field:\"field\",boost:0.4," +
                              "from:\"from\",to:\"to\",operation:\"intersects\"}");
    }

    @Test
    public void testJsonSerializationNumber() {
        DateRangeConditionBuilder builder = new DateRangeConditionBuilder("field").boost(0.4)
                                                                                  .from("2015/01/05")
                                                                                  .to("2015/01/08")
                                                                                  .operation("contains");
        testJsonSerialization(builder,
                              "{type:\"date_range\",field:\"field\",boost:0.4," +
                              "from:\"2015/01/05\",to:\"2015/01/08\",operation:\"contains\"}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        DateRangeConditionBuilder builder = new DateRangeConditionBuilder("field");
        testJsonSerialization(builder, "{type:\"date_range\",field:\"field\"}");
    }

    @Test
    public void testParseSpatialOperationIntersectsLowerCase() {
        assertEquals("Operation is not properly parsed", Intersects, parseSpatialOperation("intersects"));
    }

    @Test
    public void testParseSpatialOperationIntersectsUpperCase() {
        assertEquals("Operation is not properly parsed", Intersects, parseSpatialOperation("INTERSECTS"));
    }

    @Test
    public void testParseSpatialOperationIsWithinLowerCase() {
        assertEquals("Operation is not properly parsed", IsWithin, parseSpatialOperation("is_within"));
    }

    @Test
    public void testParseSpatialOperationIsWithinUpperCase() {
        assertEquals("Operation is not properly parsed", IsWithin, parseSpatialOperation("IS_WITHIN"));
    }

    @Test
    public void testParseSpatialOperationIContainsLowerCase() {
        assertEquals("Operation is not properly parsed", Contains, parseSpatialOperation("contains"));
    }

    @Test
    public void testParseSpatialOperationContainsUpperCase() {
        assertEquals("Operation is not properly parsed", Contains, parseSpatialOperation("CONTAINS"));
    }

    @Test(expected = IndexException.class)
    public void testParseSpatialOperationNull() {
        parseSpatialOperation(null);
    }

    @Test(expected = IndexException.class)
    public void testParseSpatialOperationEmpty() {
        parseSpatialOperation("");
    }

    @Test(expected = IndexException.class)
    public void testParseSpatialOperationBlank() {
        parseSpatialOperation(" ");
    }

    @Test
    public void testQuery() {

        Schema schema = schema().mapper("name", dateRangeMapper("to", "from").pattern(TIMESTAMP_PATTERN)).build();

        DateRangeCondition condition = new DateRangeCondition(null, "name", 1L, 2L, null);
        Query query = condition.query(schema);
        assertNotNull("Query is not built", query);
        assertTrue("Query type is wrong", query instanceof ConstantScoreQuery);
        query = ((ConstantScoreQuery) query).getQuery();
        assertTrue("Query type is wrong", query instanceof IntersectsPrefixTreeFilter);
        IntersectsPrefixTreeFilter filter = (IntersectsPrefixTreeFilter) query;
        assertEquals("Query is wrong",
                     "IntersectsPrefixTreeFilter(fieldName=name,queryShape=" +
                     "[1970-01-01T00:00:00.001 TO 1970-01-01T00:00:00.002],detailLevel=9,prefixGridScanLevel=7)",
                     filter.toString());
    }

    @Test(expected = IndexException.class)
    public void testQueryWithoutValidMapper() {
        Schema schema = schema().mapper("name", uuidMapper()).build();
        DateRangeCondition condition = new DateRangeCondition(null, "name", 1, 2, null);
        condition.query(schema);
    }

    @Test
    public void testToString() {
        DateRangeCondition condition = dateRange("name").from(1).to(2).operation("contains").boost(0.3).build();
        assertEquals("Method #toString is wrong",
                     "DateRangeCondition{boost=0.3, field=name, from=1, to=2, operation=contains}",
                     condition.toString());
    }
}
