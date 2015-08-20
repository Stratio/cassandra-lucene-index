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
import com.stratio.cassandra.lucene.search.condition.builder.PhraseConditionBuilder;
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
public class PhraseConditionTest extends AbstractConditionTest {

    @Test
    public void testBuild() {
        PhraseConditionBuilder builder = new PhraseConditionBuilder("field", "value1 value2").slop(2).boost(0.7f);
        PhraseCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Value is not set", "value1 value2", condition.value);
        assertEquals("Slop is not set", 2, condition.slop);
    }

    @Test
    public void testBuildDefaults() {
        PhraseConditionBuilder builder = new PhraseConditionBuilder("field", "value1 value2");
        PhraseCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set to default", Condition.DEFAULT_BOOST, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Value is not set", "value1 value2", condition.value);
        assertEquals("Slop is not set to default", PhraseCondition.DEFAULT_SLOP, condition.slop);
    }

    @Test(expected = IndexException.class)
    public void testBuildNullValues() {
        new PhraseConditionBuilder("field", null).build();
    }

    @Test(expected = IndexException.class)
    public void testBuildNegativeSlop() {
        new PhraseConditionBuilder("field", "value1 value2").slop(-1).build();
    }

    @Test
    public void testJsonSerialization() {
        PhraseConditionBuilder builder = new PhraseConditionBuilder("field", "value1 value2").slop(2).boost(0.7f);
        testJsonSerialization(builder, "{type:\"phrase\",field:\"field\",value:\"value1 value2\",boost:0.7,slop:2}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        PhraseConditionBuilder builder = new PhraseConditionBuilder("field", "value1 value2");
        testJsonSerialization(builder, "{type:\"phrase\",field:\"field\",value:\"value1 value2\"}");
    }

    @Test
    public void testPhraseQuery() {

        Schema schema = schema().mapper("name", textMapper().analyzer("spanish")).build();

        String value = "hola adios  the    a";
        PhraseCondition condition = new PhraseCondition(0.5f, "name", value, 2);
        Query query = condition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", PhraseQuery.class, query.getClass());

        PhraseQuery luceneQuery = (PhraseQuery) query;
        assertEquals("Query terms are wrong", 3, luceneQuery.getTerms().length);
        assertEquals("Query slop is wrong", 2, luceneQuery.getSlop());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testToString() {
        PhraseCondition condition = new PhraseCondition(0.5f, "name", "hola adios", 2);
        assertEquals("Method #toString is wrong",
                     "PhraseCondition{boost=0.5, field=name, value=hola adios, slop=2}",
                     condition.toString());
    }

}
