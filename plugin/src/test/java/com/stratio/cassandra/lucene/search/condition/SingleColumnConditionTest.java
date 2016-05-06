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

import com.google.common.base.MoreObjects;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.schema;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.stringMapper;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SingleColumnConditionTest {

    private static class MockCondition extends SingleColumnCondition {

        MockCondition(Float boost, String field) {
            super(boost, field);
        }

        @Override
        public Query doQuery(SingleColumnMapper<?> mapper, Analyzer analyzer) {
            return new MatchAllDocsQuery();
        }

        @Override
        public MoreObjects.ToStringHelper toStringHelper() {
            return toStringHelper(this);
        }
    }

    @Test
    public void testBuild() {
        SingleColumnCondition condition = new MockCondition(0.5f, "field");
        assertEquals("Boost is not properly set", 0.5f, condition.boost, 0);
        assertEquals("Field name is not properly set", "field", condition.field);
    }

    @Test
    public void testBuildDefaults() {
        SingleColumnCondition condition = new MockCondition(null, "field");
        assertNull("Boost is not set to default", condition.boost);
    }

    @Test(expected = IndexException.class)
    public void testBuildNullField() {
        new MockCondition(null, null);
    }

    @Test(expected = IndexException.class)
    public void testBuildBlankField() {
        new MockCondition(null, " ");
    }

    @Test
    public void testGetMapper() {
        Schema schema = schema().mapper("field", stringMapper()).build();
        SingleColumnCondition condition = new MockCondition(null, "field");
        Query query = condition.query(schema);
        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", MatchAllDocsQuery.class, query.getClass());

    }

    @Test(expected = IndexException.class)
    public void testGetMapperNotFound() {
        Schema schema = schema().build();
        SingleColumnCondition condition = new MockCondition(null, "field");
        condition.query(schema);
    }

}
