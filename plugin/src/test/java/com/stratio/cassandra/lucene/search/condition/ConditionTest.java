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
import org.apache.lucene.search.*;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.schema;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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
            public Query doQuery(Schema schema) {
                return null;
            }
        };
        assertEquals("Query boost is wrong", 0.7f, condition.boost, 0);
    }

    @Test
    public void testConstructor() {
        Condition condition = new MockCondition(null) {
            @Override
            public Query doQuery(Schema schema) {
                return null;
            }
        };
        assertNull("Query boost is wrong", condition.boost);
    }

    @Test
    public void testQueryWithoutBoost() {
        Condition condition = new MockCondition(null);
        Schema schema = schema().build();
        Query query = condition.query(schema);
        assertEquals("Query type is wrong", MatchAllDocsQuery.class, query.getClass());
    }

    @Test
    public void testQueryWithBoost() {
        Condition condition = new MockCondition(0.7f);
        Schema schema = schema().build();
        Query query = condition.query(schema);
        assertTrue("Query is not boosted", query instanceof BoostQuery);
        BoostQuery boostQuery = (BoostQuery) query;
        assertEquals("Query boost is wrong", 0.7f, boostQuery.getBoost(), 0);
    }
}
