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
import com.stratio.cassandra.lucene.search.condition.builder.LuceneConditionBuilder;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.schema;
import static com.stratio.cassandra.lucene.search.condition.LuceneCondition.DEFAULT_BOOST;
import static com.stratio.cassandra.lucene.search.condition.LuceneCondition.DEFAULT_FIELD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class LuceneConditionTest extends AbstractConditionTest {

    @Test
    public void testBuild() {
        LuceneConditionBuilder builder = new LuceneConditionBuilder("field:value").boost(0.7f).defaultField("field");
        LuceneCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.defaultField);
        assertEquals("Query is not set", "field:value", condition.query);
    }

    @Test
    public void testBuildDefaults() {
        LuceneConditionBuilder builder = new LuceneConditionBuilder("field:value");
        LuceneCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set to default", DEFAULT_BOOST, condition.boost, 0);
        assertEquals("Field is not set to default", DEFAULT_FIELD, condition.defaultField);
        assertEquals("Query is not set", "field:value", condition.query);
    }

    @Test(expected = IndexException.class)
    public void testBuildWithoutQuery() {
        new LuceneConditionBuilder(null).build();
    }

    @Test
    public void testJsonSerialization() {
        LuceneConditionBuilder builder = new LuceneConditionBuilder("field:value").boost(0.7f).defaultField("field");
        testJsonSerialization(builder, "{type:\"lucene\",query:\"field:value\",boost:0.7,default_field:\"field\"}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        LuceneConditionBuilder builder = new LuceneConditionBuilder("field:value");
        testJsonSerialization(builder, "{type:\"lucene\",query:\"field:value\"}");
    }

    @Test
    public void testQuery() {

        Schema schema = schema().defaultAnalyzer("english").build();
        LuceneCondition condition = new LuceneCondition(0.7f, "field_1", "field_2:houses");

        Query query = condition.query(schema);
        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", TermQuery.class, query.getClass());

        TermQuery termQuery = (TermQuery) query;
        assertEquals("Query term is wrong", "hous", termQuery.getTerm().bytes().utf8ToString());
        assertEquals("Query boost is wrong", 0.7f, query.getBoost(), 0);
    }

    @Test(expected = IndexException.class)
    public void testQueryInvalid() {
        Schema schema = schema().defaultAnalyzer("english").build();
        LuceneCondition condition = new LuceneCondition(0.7f, "field_1", ":");
        condition.query(schema);
    }

    @Test
    public void testToString() {
        LuceneCondition condition = new LuceneCondition(0.7f, "field_1", "field_2:houses");
        assertEquals("Method #toString is wrong",
                     "LuceneCondition{query=field_2:houses, defaultField=field_1}",
                     condition.toString());
    }

}
