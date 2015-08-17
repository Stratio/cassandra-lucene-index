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
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.schema;
import static com.stratio.cassandra.lucene.schema.SchemaBuilders.textMapper;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class PhraseConditionTest {

    @Test
    public void testBuild() {
        String value = "hello adios";
        PhraseCondition condition = new PhraseCondition(0.5f, "name", value, 2);
        assertEquals(0.5f, condition.boost, 0);
        assertEquals("name", condition.field);
        assertEquals(value, condition.value);
        assertEquals(2, condition.slop);
    }

    @Test
    public void testBuildDefaults() {
        String value = "hello adios";
        PhraseCondition condition = new PhraseCondition(null, "name", value, null);
        assertEquals(PhraseCondition.DEFAULT_BOOST, condition.boost, 0);
        assertEquals("name", condition.field);
        assertEquals(value, condition.value);
        assertEquals(PhraseCondition.DEFAULT_SLOP, condition.slop);
    }

    @Test(expected = IndexException.class)
    public void testBuildNullValues() {
        new PhraseCondition(null, "name", null, null);
    }

    @Test(expected = IndexException.class)
    public void testBuildNegativeSlop() {
        String value = "hello adios";
        new PhraseCondition(null, "name", value, -1);
    }

    @Test
    public void testPhraseQuery() {

        Schema schema = schema().mapper("name", textMapper().analyzer("spanish")).build();

        String value = "hola adios  the    a";
        PhraseCondition condition = new PhraseCondition(0.5f, "name", value, 2);
        Query query = condition.query(schema);
        assertNotNull(query);
        assertEquals(PhraseQuery.class, query.getClass());
        PhraseQuery luceneQuery = (PhraseQuery) query;
        assertEquals(3, luceneQuery.getTerms().length);
        assertEquals(2, luceneQuery.getSlop());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testToString() {
        PhraseCondition condition = new PhraseCondition(0.5f, "name", "hola adios", 2);
        assertEquals("PhraseCondition{boost=0.5, field=name, value=hola adios, slop=2}", condition.toString());
    }

}
