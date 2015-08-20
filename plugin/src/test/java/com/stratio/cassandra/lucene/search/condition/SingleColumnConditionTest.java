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

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.schema;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.stringMapper;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SingleColumnConditionTest {

    @Test
    public void testBuild() {
        SingleColumnCondition condition = new SingleColumnCondition(0.5f, "field") {
            @Override
            public Query query(SingleColumnMapper<?> mapper, Analyzer analyzer) {
                return null;
            }
        };
        assertEquals("Boost is not properly set", 0.5f, condition.boost, 0);
        assertEquals("Field name is not properly set", "field", condition.field);
    }

    @Test
    public void testBuildDefaults() {
        SingleColumnCondition condition = new SingleColumnCondition(null, "field") {
            @Override
            public Query query(SingleColumnMapper<?> mapper, Analyzer analyzer) {
                return null;
            }
        };
        assertEquals("Boost is not set to default value", Condition.DEFAULT_BOOST, condition.boost, 0);
    }

    @Test(expected = IndexException.class)
    public void testBuildNullField() {
        new SingleColumnCondition(null, null) {
            @Override
            public Query query(SingleColumnMapper<?> mapper, Analyzer analyzer) {
                return null;
            }
        };
    }

    @Test(expected = IndexException.class)
    public void testBuildBlankField() {
        new SingleColumnCondition(null, " ") {
            @Override
            public Query query(SingleColumnMapper<?> mapper, Analyzer analyzer) {
                return null;
            }
        };
    }

    @Test
    public void testGetMapper() {
        Schema schema = schema().mapper("field", stringMapper()).build();
        SingleColumnCondition condition = new SingleColumnCondition(null, "field") {
            @Override
            public Query query(SingleColumnMapper<?> mapper, Analyzer analyzer) {
                return new MatchAllDocsQuery();
            }
        };
        Query query = condition.query(schema);
        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", MatchAllDocsQuery.class, query.getClass());

    }

    @Test(expected = IndexException.class)
    public void testGetMapperNotFound() {
        Schema schema = schema().build();
        SingleColumnCondition condition = new SingleColumnCondition(null, "field") {
            @Override
            public Query query(SingleColumnMapper<?> mapper, Analyzer analyzer) {
                return new MatchAllDocsQuery();
            }
        };
        condition.query(schema);
    }

}
