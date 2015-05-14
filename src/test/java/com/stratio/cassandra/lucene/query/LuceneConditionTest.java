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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

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
        Assert.assertEquals(boost, condition.getBoost(), 0);
        Assert.assertEquals(defaultField, condition.getDefaultField());
        Assert.assertEquals(query, condition.getQuery());
    }

    @Test
    public void testBuildWithDefaults() {
        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(new EnglishAnalyzer());
        String query = "field_2:houses";
        LuceneCondition condition = new LuceneCondition(null, null, query);
        Assert.assertEquals(Condition.DEFAULT_BOOST, condition.getBoost(), 0);
        Assert.assertEquals(LuceneCondition.DEFAULT_FIELD, condition.getDefaultField());
        Assert.assertEquals(query, condition.getQuery());
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
        Assert.assertNotNull(query);

        Assert.assertEquals(TermQuery.class, query.getClass());
        Assert.assertEquals("hous", ((TermQuery) query).getTerm().bytes().utf8ToString());
        Assert.assertEquals(0.7f, query.getBoost(), 0);
    }

    @Test
    public void testJson() throws IOException {
        String in = "{type:\"lucene\",boost:0.7,default_field:\"field_1\",query:\"field_2:houses\"}";
        LuceneCondition condition = JsonSerializer.fromString(in, LuceneCondition.class);
        String out = JsonSerializer.toString(condition);
        Assert.assertEquals(in, out);
    }

}
