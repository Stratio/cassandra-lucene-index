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

import com.stratio.cassandra.lucene.query.builder.RangeConditionBuilder;
import com.stratio.cassandra.lucene.query.builder.SearchBuilder;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapper;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperDouble;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperFloat;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperInet;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperInteger;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperLong;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperString;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.*;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RangeConditionTest extends AbstractConditionTest {

    @Test
    public void testStringClose() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperString(null, null, null));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", "alpha", "beta", true, true);
        Query query = rangeCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(TermRangeQuery.class, query.getClass());
        Assert.assertEquals("name", ((TermRangeQuery) query).getField());
        Assert.assertEquals("alpha", ((TermRangeQuery) query).getLowerTerm().utf8ToString());
        Assert.assertEquals("beta", ((TermRangeQuery) query).getUpperTerm().utf8ToString());
        Assert.assertEquals(true, ((TermRangeQuery) query).includesLower());
        Assert.assertEquals(true, ((TermRangeQuery) query).includesUpper());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testStringOpen() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperString(null, null, null));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", "alpha", null, true, false);
        Query query = rangeCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(TermRangeQuery.class, query.getClass());
        Assert.assertEquals("name", ((TermRangeQuery) query).getField());
        Assert.assertEquals("alpha", ((TermRangeQuery) query).getLowerTerm().utf8ToString());
        Assert.assertEquals(null, ((TermRangeQuery) query).getUpperTerm());
        Assert.assertNull(((TermRangeQuery) query).getUpperTerm());
        Assert.assertEquals(true, ((TermRangeQuery) query).includesLower());
        Assert.assertEquals(false, ((TermRangeQuery) query).includesUpper());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testIntegerClose() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperInteger(null, null, 1f));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42, 43, false, false);
        Query query = rangeCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(NumericRangeQuery.class, query.getClass());
        Assert.assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        Assert.assertEquals(42, ((NumericRangeQuery<?>) query).getMin());
        Assert.assertEquals(43, ((NumericRangeQuery<?>) query).getMax());
        Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMin());
        Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testIntegerOpen() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperInteger(null, null, 1f));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42, null, true, false);
        Query query = rangeCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(NumericRangeQuery.class, query.getClass());
        Assert.assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        Assert.assertEquals(42, ((NumericRangeQuery<?>) query).getMin());
        Assert.assertEquals(null, ((NumericRangeQuery<?>) query).getMax());
        Assert.assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testLongClose() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperLong(true, true, 1f));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42L, 43, false, false);
        Query query = rangeCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(NumericRangeQuery.class, query.getClass());
        Assert.assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        Assert.assertEquals(42L, ((NumericRangeQuery<?>) query).getMin());
        Assert.assertEquals(43L, ((NumericRangeQuery<?>) query).getMax());
        Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMin());
        Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testLongOpen() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperLong(true, true, 1f));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42f, null, true, false);
        Query query = rangeCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(NumericRangeQuery.class, query.getClass());
        Assert.assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        Assert.assertEquals(42L, ((NumericRangeQuery<?>) query).getMin());
        Assert.assertEquals(null, ((NumericRangeQuery<?>) query).getMax());
        Assert.assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testFloatClose() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperFloat(null, null, 1f));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42D, 43.42F, false, false);
        Query query = rangeCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(NumericRangeQuery.class, query.getClass());
        Assert.assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        Assert.assertEquals(42.42F, ((NumericRangeQuery<?>) query).getMin());
        Assert.assertEquals(43.42f, ((NumericRangeQuery<?>) query).getMax());
        Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMin());
        Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testFloatOpen() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperFloat(null, null, 1f));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42f, null, true, false);
        Query query = rangeCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(NumericRangeQuery.class, query.getClass());
        Assert.assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        Assert.assertEquals(42.42f, ((NumericRangeQuery<?>) query).getMin());
        Assert.assertEquals(null, ((NumericRangeQuery<?>) query).getMax());
        Assert.assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testDoubleClose() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperDouble(null, null, 1f));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42D, 43.42D, false, false);
        Query query = rangeCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(NumericRangeQuery.class, query.getClass());
        Assert.assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        Assert.assertEquals(42.42D, ((NumericRangeQuery<?>) query).getMin());
        Assert.assertEquals(43.42D, ((NumericRangeQuery<?>) query).getMax());
        Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMin());
        Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testDoubleOpen() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperDouble(null, null, 1f));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42D, null, true, false);
        Query query = rangeCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(NumericRangeQuery.class, query.getClass());
        Assert.assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        Assert.assertEquals(42.42D, ((NumericRangeQuery<?>) query).getMin());
        Assert.assertEquals(null, ((NumericRangeQuery<?>) query).getMax());
        Assert.assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV4() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperInet(null, null));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", "192.168.0.01", "192.168.0.045", true, true);
        Query query = rangeCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(TermRangeQuery.class, query.getClass());
        Assert.assertEquals("name", ((TermRangeQuery) query).getField());
        Assert.assertEquals("192.168.0.1", ((TermRangeQuery) query).getLowerTerm().utf8ToString());
        Assert.assertEquals("192.168.0.45", ((TermRangeQuery) query).getUpperTerm().utf8ToString());
        Assert.assertEquals(true, ((TermRangeQuery) query).includesLower());
        Assert.assertEquals(true, ((TermRangeQuery) query).includesUpper());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV6() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperInet(null, null));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        RangeCondition rangeCondition = range("name").boost(0.5f)
                                                     .lower("2001:DB8:2de::e13")
                                                     .upper("2001:DB8:02de::e23")
                                                     .includeLower(true)
                                                     .includeUpper(true)
                                                     .build();
        Query query = rangeCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(TermRangeQuery.class, query.getClass());
        Assert.assertEquals("name", ((TermRangeQuery) query).getField());
        Assert.assertEquals("2001:db8:2de:0:0:0:0:e13", ((TermRangeQuery) query).getLowerTerm().utf8ToString());
        Assert.assertEquals("2001:db8:2de:0:0:0:0:e23", ((TermRangeQuery) query).getUpperTerm().utf8ToString());
        Assert.assertEquals(true, ((TermRangeQuery) query).includesLower());
        Assert.assertEquals(true, ((TermRangeQuery) query).includesUpper());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testJsonInteger() {
        RangeConditionBuilder rangeCondition = range("name").lower(1)
                                                            .upper(2)
                                                            .includeLower(true)
                                                            .includeUpper(false)
                                                            .boost(0.5f);
        SearchBuilder searchBuilder = query(rangeCondition);

        testJsonCondition(searchBuilder);
    }

    @Test
    public void testJsonDouble() {
        testJsonCondition(query(range("name").lower(1.6)
                                             .upper(2.5)
                                             .includeLower(true)
                                             .includeUpper(false)
                                             .boost(0.5f)));
    }

    @Test
    public void testJsonString() {
        testJsonCondition(query(range("name").lower("a")
                                             .upper("b")
                                             .includeLower(true)
                                             .includeUpper(false)
                                             .boost(0.5f)).filter(bool().must(match("f1", "v1").boost(2),
                                                                              match("f2", "v2"))
                                                                        .should(match("f3", "v3"))
                                                                        .boost(0.5)).build());
    }

}
