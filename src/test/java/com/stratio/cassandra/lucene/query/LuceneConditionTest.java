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
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class LuceneConditionTest extends AbstractConditionTest {

    @Test
    public void testBuild() {
        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(new EnglishAnalyzer());
        Float boost = 0.7f;
        String defaultField = "field_1";
        String query = "field_2:houses";
        LuceneCondition condition = new LuceneCondition(boost, defaultField, query);
        assertEquals(boost, condition.getBoost(), 0);
        assertEquals(defaultField, condition.getDefaultField());
        assertEquals(query, condition.getQuery());
    }

    @Test
    public void testBuildWithDefaults() {
        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(new EnglishAnalyzer());
        String query = "field_2:houses";
        LuceneCondition condition = new LuceneCondition(null, null, query);
        assertEquals(Condition.DEFAULT_BOOST, condition.getBoost(), 0);
        assertEquals(LuceneCondition.DEFAULT_FIELD, condition.getDefaultField());
        assertEquals(query, condition.getQuery());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildWithoutQuery() {
        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(new EnglishAnalyzer());
        new LuceneCondition(null, null, null);
    }

    @Test
    public void testQuery() {

        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(new EnglishAnalyzer());
        LuceneCondition condition = new LuceneCondition(0.7f, "field_1", "field_2:houses");
        Query query = condition.query(schema);
        assertNotNull(query);

        assertEquals(TermQuery.class, query.getClass());
        assertEquals("hous", ((TermQuery) query).getTerm().bytes().utf8ToString());
        assertEquals(0.7f, query.getBoost(), 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryInvalid() {
        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(new EnglishAnalyzer());
        LuceneCondition condition = new LuceneCondition(0.7f, "field_1", ":");
        condition.query(schema);
    }

    @Test
    public void testJson() throws IOException {
        String in = "{type:\"lucene\",boost:0.7,default_field:\"field_1\",query:\"field_2:houses\"}";
        LuceneCondition condition = JsonSerializer.fromString(in, LuceneCondition.class);
        String out = JsonSerializer.toString(condition);
        assertEquals(in, out);
    }

    @Test
    public void testToString() {
        LuceneCondition condition = new LuceneCondition(0.7f, "field_1", "field_2:houses");
        assertEquals("LuceneCondition{query=field_2:houses, defaultField=field_1}", condition.toString());
    }

}
