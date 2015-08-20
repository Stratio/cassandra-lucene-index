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

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.search.condition.builder.PrefixConditionBuilder;
import com.stratio.cassandra.lucene.search.condition.builder.RangeConditionBuilder;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static com.stratio.cassandra.lucene.search.SearchBuilders.range;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RangeConditionTest extends AbstractConditionTest {

    @Test
    public void testBuildString() {
        RangeConditionBuilder builder = new RangeConditionBuilder("field").boost(0.4)
                                                                          .lower("lower")
                                                                          .upper("upper")
                                                                          .includeLower(false)
                                                                          .includeUpper(true);
        RangeCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Boost is not set", 0.4f, condition.boost, 0);
        assertEquals("Lower is not set", "lower", condition.lower);
        assertEquals("Upper is not set", "upper", condition.upper);
        assertEquals("Include lower is not set", false, condition.includeLower);
        assertEquals("Include upper is not set", true, condition.includeUpper);
    }

    @Test
    public void testBuildNumber() {
        RangeConditionBuilder builder = new RangeConditionBuilder("field").boost(0.4)
                                                                          .lower(1)
                                                                          .upper(2)
                                                                          .includeLower(false)
                                                                          .includeUpper(true);
        RangeCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Boost is not set", 0.4f, condition.boost, 0);
        assertEquals("Lower is not set", 1, condition.lower);
        assertEquals("Upper is not set", 2, condition.upper);
        assertEquals("Include lower is not set", false, condition.includeLower);
        assertEquals("Include upper is not set", true, condition.includeUpper);
    }

    @Test
    public void testBuildDefaults() {
        RangeConditionBuilder builder = new RangeConditionBuilder("field");
        RangeCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Boost is not set to default", Condition.DEFAULT_BOOST, condition.boost, 0);
        assertNull("Lower is not set to default", condition.lower);
        assertNull("Upper is not set to default", condition.upper);
        assertEquals("Include Lower is not set to default",
                     RangeCondition.DEFAULT_INCLUDE_LOWER,
                     condition.includeLower);
        assertEquals("Include upper is not set to default",
                     RangeCondition.DEFAULT_INCLUDE_UPPER,
                     condition.includeUpper);
    }

    @Test
    public void testJsonSerializationString() {
        RangeConditionBuilder builder = new RangeConditionBuilder("field").boost(0.4)
                                                                          .lower("lower")
                                                                          .upper("upper")
                                                                          .includeLower(false)
                                                                          .includeUpper(true);
        testJsonSerialization(builder,
                              "{type:\"range\",field:\"field\",boost:0.4,lower:\"lower\",upper:\"upper\"," +
                              "include_lower:false,include_upper:true}");
    }

    @Test
    public void testJsonSerializationNumber() {
        RangeConditionBuilder builder = new RangeConditionBuilder("field").boost(0.4)
                                                                          .lower(1)
                                                                          .upper(2)
                                                                          .includeLower(false)
                                                                          .includeUpper(true);
        testJsonSerialization(builder,
                              "{type:\"range\",field:\"field\",boost:0.4,lower:1,upper:2," +
                              "include_lower:false,include_upper:true}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        PrefixConditionBuilder builder = new PrefixConditionBuilder("field", "value");
        testJsonSerialization(builder, "{type:\"prefix\",field:\"field\",value:\"value\"}");
    }

    @Test
    public void testStringClose() {

        Schema schema = schema().mapper("name", stringMapper()).build();

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", "alpha", "beta", true, true);
        Query query = rangeCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", TermRangeQuery.class, query.getClass());

        TermRangeQuery termRangeQuery = (TermRangeQuery) query;
        assertEquals("Query field is wrong", "name", termRangeQuery.getField());
        assertEquals("Query lower is wrong", "alpha", termRangeQuery.getLowerTerm().utf8ToString());
        assertEquals("Query upper is wrong", "beta", termRangeQuery.getUpperTerm().utf8ToString());
        assertEquals("Query include lower is wrong", true, termRangeQuery.includesLower());
        assertEquals("Query include upper is wrong", true, termRangeQuery.includesUpper());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testStringOpen() {

        Schema schema = schema().mapper("name", stringMapper()).build();

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", "alpha", null, true, false);
        Query query = rangeCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", TermRangeQuery.class, query.getClass());

        TermRangeQuery termRangeQuery = (TermRangeQuery) query;
        assertEquals("Query field is wrong", "name", termRangeQuery.getField());
        assertEquals("Query lower is wrong", "alpha", termRangeQuery.getLowerTerm().utf8ToString());
        assertEquals("Query upper is wrong", null, termRangeQuery.getUpperTerm());
        assertEquals("Query include lower is wrong", true, termRangeQuery.includesLower());
        assertEquals("Query include upper is wrong", false, termRangeQuery.includesUpper());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testIntegerClose() {

        Schema schema = schema().mapper("name", integerMapper().boost(1f)).build();

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42, 43, false, false);
        Query query = rangeCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", NumericRangeQuery.class, query.getClass());

        NumericRangeQuery<?> numericRangeQuery = (NumericRangeQuery<?>) query;
        assertEquals("Query field is wrong", "name", numericRangeQuery.getField());
        assertEquals("Query lower is wrong", 42, numericRangeQuery.getMin());
        assertEquals("Query upper is wrong", 43, numericRangeQuery.getMax());
        assertEquals("Query include lower is wrong", false, numericRangeQuery.includesMin());
        assertEquals("Query include upper is wrong", false, numericRangeQuery.includesMax());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testIntegerOpen() {

        Schema schema = schema().mapper("name", integerMapper().boost(1f)).build();

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42, null, true, false);
        Query query = rangeCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", NumericRangeQuery.class, query.getClass());

        NumericRangeQuery<?> numericRangeQuery = (NumericRangeQuery<?>) query;
        assertEquals("Query field is wrong", "name", numericRangeQuery.getField());
        assertEquals("Query lower is wrong", 42, numericRangeQuery.getMin());
        assertEquals("Query upper is wrong", null, numericRangeQuery.getMax());
        assertEquals("Query include lower is wrong", true, numericRangeQuery.includesMin());
        assertEquals("Query include upper is wrong", false, numericRangeQuery.includesMax());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testLongClose() {

        Schema schema = schema().mapper("name", longMapper().boost(1f)).build();

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42L, 43, false, false);
        Query query = rangeCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", NumericRangeQuery.class, query.getClass());

        NumericRangeQuery<?> numericRangeQuery = (NumericRangeQuery<?>) query;
        assertEquals("Query field is wrong", "name", numericRangeQuery.getField());
        assertEquals("Query lower is wrong", 42L, numericRangeQuery.getMin());
        assertEquals("Query upper is wrong", 43L, numericRangeQuery.getMax());
        assertEquals("Query include lower is wrong", false, numericRangeQuery.includesMin());
        assertEquals("Query include upper is wrong", false, numericRangeQuery.includesMax());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testLongOpen() {

        Schema schema = schema().mapper("name", longMapper().boost(1f)).build();

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42f, null, true, false);
        Query query = rangeCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", NumericRangeQuery.class, query.getClass());

        NumericRangeQuery<?> numericRangeQuery = (NumericRangeQuery<?>) query;
        assertEquals("Query field is wrong", "name", numericRangeQuery.getField());
        assertEquals("Query lower is wrong", 42L, numericRangeQuery.getMin());
        assertEquals("Query upper is wrong", null, numericRangeQuery.getMax());
        assertEquals("Query include lower is wrong", true, numericRangeQuery.includesMin());
        assertEquals("Query include upper is wrong", false, numericRangeQuery.includesMax());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testFloatClose() {

        Schema schema = schema().mapper("name", floatMapper().boost(1f)).build();

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42D, 43.42F, false, false);
        Query query = rangeCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", NumericRangeQuery.class, query.getClass());

        NumericRangeQuery<?> numericRangeQuery = (NumericRangeQuery<?>) query;
        assertEquals("Query field is wrong", "name", numericRangeQuery.getField());
        assertEquals("Query lower is wrong", 42.42F, numericRangeQuery.getMin());
        assertEquals("Query upper is wrong", 43.42f, numericRangeQuery.getMax());
        assertEquals("Query include lower is wrong", false, numericRangeQuery.includesMin());
        assertEquals("Query include upper is wrong", false, numericRangeQuery.includesMax());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testFloatOpen() {

        Schema schema = schema().mapper("name", floatMapper().boost(1f)).build();

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42f, null, true, false);
        Query query = rangeCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", NumericRangeQuery.class, query.getClass());

        NumericRangeQuery<?> numericRangeQuery = (NumericRangeQuery<?>) query;
        assertEquals("Query field is wrong", "name", numericRangeQuery.getField());
        assertEquals("Query lower is wrong", 42.42f, numericRangeQuery.getMin());
        assertEquals("Query upper is wrong", null, numericRangeQuery.getMax());
        assertEquals("Query include lower is wrong", true, numericRangeQuery.includesMin());
        assertEquals("Query include upper is wrong", false, numericRangeQuery.includesMax());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testDoubleClose() {

        Schema schema = schema().mapper("name", doubleMapper().boost(1f)).build();

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42D, 43.42D, false, false);
        Query query = rangeCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", NumericRangeQuery.class, query.getClass());

        NumericRangeQuery<?> numericRangeQuery = (NumericRangeQuery<?>) query;
        assertEquals("Query field is wrong", "name", numericRangeQuery.getField());
        assertEquals("Query lower is wrong", 42.42D, numericRangeQuery.getMin());
        assertEquals("Query upper is wrong", 43.42D, numericRangeQuery.getMax());
        assertEquals("Query include lower is wrong", false, numericRangeQuery.includesMin());
        assertEquals("Query include upper is wrong", false, numericRangeQuery.includesMax());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testDoubleOpen() {

        Schema schema = schema().mapper("name", doubleMapper().boost(1f)).build();

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42D, null, true, false);
        Query query = rangeCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", NumericRangeQuery.class, query.getClass());

        NumericRangeQuery<?> numericRangeQuery = (NumericRangeQuery<?>) query;
        assertEquals("Query field is wrong", "name", numericRangeQuery.getField());
        assertEquals("Query lower is wrong", 42.42D, numericRangeQuery.getMin());
        assertEquals("Query upper is wrong", null, numericRangeQuery.getMax());
        assertEquals("Query include lower is wrong", true, numericRangeQuery.includesMin());
        assertEquals("Query include upper is wrong", false, numericRangeQuery.includesMax());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV4() {

        Schema schema = schema().mapper("name", inetMapper()).build();

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", "192.168.0.01", "192.168.0.045", true, true);
        Query query = rangeCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", TermRangeQuery.class, query.getClass());

        TermRangeQuery termRangeQuery = (TermRangeQuery) query;
        assertEquals("Query field is wrong", "name", termRangeQuery.getField());
        assertEquals("Query lower is wrong", "192.168.0.1", termRangeQuery.getLowerTerm().utf8ToString());
        assertEquals("Query upper is wrong", "192.168.0.45", termRangeQuery.getUpperTerm().utf8ToString());
        assertEquals("Query include lower is wrong", true, termRangeQuery.includesLower());
        assertEquals("Query include upper is wrong", true, termRangeQuery.includesUpper());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV6() {

        Schema schema = schema().mapper("name", inetMapper()).build();

        RangeCondition rangeCondition = range("name").boost(0.5f)
                                                     .lower("2001:DB8:2de::e13")
                                                     .upper("2001:DB8:02de::e23")
                                                     .includeLower(true)
                                                     .includeUpper(true)
                                                     .build();
        Query query = rangeCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", TermRangeQuery.class, query.getClass());

        TermRangeQuery termRangeQuery = (TermRangeQuery) query;
        assertEquals("Query field is wrong", "name", termRangeQuery.getField());
        assertEquals("Query lower is wrong", "2001:db8:2de:0:0:0:0:e13", termRangeQuery.getLowerTerm().utf8ToString());
        assertEquals("Query upper is wrong", "2001:db8:2de:0:0:0:0:e23", termRangeQuery.getUpperTerm().utf8ToString());
        assertEquals("Query include lower is wrong", true, termRangeQuery.includesLower());
        assertEquals("Query include upper is wrong", true, termRangeQuery.includesUpper());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testToString() {
        RangeCondition condition = range("name").boost(0.5f)
                                                .lower("2001:DB8:2de::e13")
                                                .upper("2001:DB8:02de::e23")
                                                .includeLower(true)
                                                .includeUpper(true)
                                                .build();
        assertEquals("Method #toString is wrong",
                     "RangeCondition{boost=0.5, field=name, lower=2001:DB8:2de::e13, " +
                     "upper=2001:DB8:02de::e23, includeLower=true, includeUpper=true}",
                     condition.toString());
    }

}
