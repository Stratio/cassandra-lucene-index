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

import com.stratio.cassandra.lucene.query.builder.SearchBuilders;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapper;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperBlob;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperDouble;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperFloat;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperInet;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperInteger;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperLong;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperString;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class MatchConditionTest extends AbstractConditionTest {

    @Test
    public void testString() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperString(true, true, null));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", "casa");
        Query query = matchCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(TermQuery.class, query.getClass());
        Assert.assertEquals("casa", ((TermQuery) query).getTerm().bytes().utf8ToString());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInteger() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperInteger(null, null, 1f));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", 42);
        Query query = matchCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(NumericRangeQuery.class, query.getClass());
        Assert.assertEquals(42, ((NumericRangeQuery<?>) query).getMin());
        Assert.assertEquals(42, ((NumericRangeQuery<?>) query).getMax());
        Assert.assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        Assert.assertEquals(true, ((NumericRangeQuery<?>) query).includesMax());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testLong() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperLong(true, true, 1f));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", 42L);
        Query query = matchCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(NumericRangeQuery.class, query.getClass());
        Assert.assertEquals(42L, ((NumericRangeQuery<?>) query).getMin());
        Assert.assertEquals(42L, ((NumericRangeQuery<?>) query).getMax());
        Assert.assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        Assert.assertEquals(true, ((NumericRangeQuery<?>) query).includesMax());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testFloat() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperFloat(null, null, 1f));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", 42.42F);
        Query query = matchCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(NumericRangeQuery.class, query.getClass());
        Assert.assertEquals(42.42F, ((NumericRangeQuery<?>) query).getMin());
        Assert.assertEquals(42.42F, ((NumericRangeQuery<?>) query).getMax());
        Assert.assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        Assert.assertEquals(true, ((NumericRangeQuery<?>) query).includesMax());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testDouble() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperDouble(null, null, 1f));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", 42.42D);
        Query query = matchCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(NumericRangeQuery.class, query.getClass());
        Assert.assertEquals(42.42D, ((NumericRangeQuery<?>) query).getMin());
        Assert.assertEquals(42.42D, ((NumericRangeQuery<?>) query).getMax());
        Assert.assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        Assert.assertEquals(true, ((NumericRangeQuery<?>) query).includesMax());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testBlob() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperBlob(null, null));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", "0Fa1");
        Query query = matchCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(TermQuery.class, query.getClass());
        Assert.assertEquals("0fa1", ((TermQuery) query).getTerm().bytes().utf8ToString());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV4() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperInet(null, null));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", "192.168.0.01");
        Query query = matchCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(TermQuery.class, query.getClass());
        Assert.assertEquals("192.168.0.1", ((TermQuery) query).getTerm().bytes().utf8ToString());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV6() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperInet(null, null));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", "2001:DB8:2de::0e13");
        Query query = matchCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(TermQuery.class, query.getClass());
        Assert.assertEquals("2001:db8:2de:0:0:0:0:e13", ((TermQuery) query).getTerm().bytes().utf8ToString());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testJson() {
        testJsonCondition(SearchBuilders.filter(SearchBuilders.match("name", 42).boost(0.5f)));
    }

}
