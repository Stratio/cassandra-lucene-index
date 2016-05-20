/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.search.condition;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.schema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ConditionTest {
    private static class MockCondition extends Condition {

        MockCondition(Float boost) {
            super(boost);
        }

        @Override
        public Query doQuery(Schema schema) {
            return new MatchAllDocsQuery();
        }

        public Objects.ToStringHelper toStringHelper() {
            return toStringHelper(this);
        }
    }
    @Test
    public void testConstructorWithBoost() {
        Condition condition = new MockCondition(0.7f) {
            @Override
            public Query query(Schema schema) {
                return null;
            }
        };
        assertEquals("Query boost is wrong", 0.7f, condition.boost, 0);
    }

    @Test
    public void testConstructor() {
        Condition condition = new MockCondition(null) {
            @Override
            public Query query(Schema schema) {
                return null;
            }
        };
        assertEquals("Query boost is wrong", Condition.DEFAULT_BOOST, condition.boost, 0);
    }

    @Test
    public void testFilter() {
        Schema schema = schema().build();
        Condition condition = new MockCondition(null) {
            @Override
            public Query query(Schema schema) {
                return new MatchAllDocsQuery();
            }
        };
        Filter filter = condition.filter(schema);
        assertNotNull("Filter is not built", filter);
        assertEquals("Filter type is wrong", QueryWrapperFilter.class, filter.getClass());
        QueryWrapperFilter queryWrapperFilter = (QueryWrapperFilter) filter;
        Query query = queryWrapperFilter.getQuery();
        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", MatchAllDocsQuery.class, query.getClass());
    }

}
