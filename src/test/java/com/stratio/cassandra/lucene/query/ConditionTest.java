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
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ConditionTest extends AbstractConditionTest {

    @Test
    public void testConstructorWithBoost() {
        Condition condition = new Condition(0.7f) {
            @Override
            public Query query(Schema schema) {
                return null;
            }
        };
        Assert.assertEquals(0.7f, condition.boost, 0.0f);
    }

    @Test
    public void testConstructor() {
        Condition condition = new Condition(null) {
            @Override
            public Query query(Schema schema) {
                return null;
            }
        };
        Assert.assertEquals(Condition.DEFAULT_BOOST, condition.boost, 0.0f);
    }

    @Test
    public void testFilter() {
        Schema schema = mock(Schema.class);
        Condition condition = new Condition(null) {
            @Override
            public Query query(Schema schema) {
                return new MatchAllDocsQuery();
            }
        };
        Filter filter = condition.filter(schema);
        Assert.assertNotNull(filter);
        Assert.assertEquals(QueryWrapperFilter.class, filter.getClass());
        QueryWrapperFilter queryWrapperFilter = (QueryWrapperFilter) filter;
        Query query = queryWrapperFilter.getQuery();
        Assert.assertNotNull(query);
        Assert.assertEquals(MatchAllDocsQuery.class, query.getClass());
    }

    @Test
    public void testAnalyze() {
        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(new EnglishAnalyzer());
        Condition condition = new Condition(null) {
            @Override
            public Query query(Schema schema) {
                return null;
            }
        };
        Assert.assertEquals("test", condition.analyze("f", "tests", schema));
        schema.close();
    }

}
