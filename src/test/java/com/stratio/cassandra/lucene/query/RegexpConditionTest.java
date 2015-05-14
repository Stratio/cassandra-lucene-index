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
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperInet;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperInteger;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperString;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RegexpConditionTest extends AbstractConditionTest {

    @Test
    public void testString() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperString(true, true, null));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        RegexpCondition condition = new RegexpCondition(0.5f, "name", "tr*");
        Query query = condition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(RegexpQuery.class, query.getClass());
        RegexpQuery luceneQuery = (RegexpQuery) query;
        Assert.assertEquals("name", luceneQuery.getField());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInteger() {
        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperInteger(null, null, 1f));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        RegexpCondition condition = new RegexpCondition(0.5f, "name", "22*");
        condition.query(mappers);
    }

    @Test
    public void testInetV4() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperInet(null, null));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        RegexpCondition condition = new RegexpCondition(0.5f, "name", "192.168.*");
        Query query = condition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(RegexpQuery.class, query.getClass());
        RegexpQuery luceneQuery = (RegexpQuery) query;
        Assert.assertEquals("name", luceneQuery.getField());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV6() {

        Map<String, ColumnMapper> map = new HashMap<>();
        map.put("name", new ColumnMapperInet(null, null));
        Schema mappers = new Schema(map, null, EnglishAnalyzer.class.getName());

        RegexpCondition regexpCondition = new RegexpCondition(0.5f, "name", "2001:db8:2de:0:0:0:0:e*");
        Query query = regexpCondition.query(mappers);

        Assert.assertNotNull(query);
        Assert.assertEquals(RegexpQuery.class, query.getClass());
        RegexpQuery luceneQuery = (RegexpQuery) query;
        Assert.assertEquals("name", luceneQuery.getField());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testJson() {
        testJsonCondition(SearchBuilders.query(SearchBuilders.regexp("name", "aaa*").boost(0.5f)));
    }

}
