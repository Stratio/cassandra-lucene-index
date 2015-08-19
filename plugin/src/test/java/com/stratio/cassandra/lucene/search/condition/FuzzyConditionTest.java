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
import com.stratio.cassandra.lucene.search.condition.builder.FuzzyConditionBuilder;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static com.stratio.cassandra.lucene.search.condition.FuzzyCondition.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class FuzzyConditionTest extends AbstractConditionTest {

    @Test
    public void testBuild() {
        FuzzyConditionBuilder builder = new FuzzyConditionBuilder("field", "value").boost(0.7f)
                                                                                   .maxEdits(2)
                                                                                   .prefixLength(2)
                                                                                   .maxExpansions(49)
                                                                                   .transpositions(true);
        FuzzyCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Value is not set", "value", condition.value);
        assertEquals("Max edits is not set", 2, condition.maxEdits);
        assertEquals("Prefix length is not set", 2, condition.prefixLength);
        assertEquals("Max defaults is not set", 49, condition.maxExpansions);
        assertEquals("Transpositions is not set", true, condition.transpositions);
    }

    @Test
    public void testBuildDefaults() {
        FuzzyConditionBuilder builder = new FuzzyConditionBuilder("field", "value");
        FuzzyCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set to default", DEFAULT_BOOST, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Value is not set", "value", condition.value);
        assertEquals("Max edits is not set to default", DEFAULT_MAX_EDITS, condition.maxEdits);
        assertEquals("Prefix length is not set to default", DEFAULT_PREFIX_LENGTH, condition.prefixLength);
        assertEquals("Max defaults is not set to default", DEFAULT_MAX_EXPANSIONS, condition.maxExpansions);
        assertEquals("Transpositions is not set to default", DEFAULT_TRANSPOSITIONS, condition.transpositions);
    }

    @Test(expected = IndexException.class)
    public void testBuildValueNull() {
        new FuzzyCondition(0.5f, "name", null, 1, 2, 49, true);
    }

    @Test(expected = IndexException.class)
    public void testBuildValueBlank() {
        new FuzzyCondition(0.5f, "name", " ", 1, 2, 49, true);
    }

    @Test(expected = IndexException.class)
    public void testBuildMaxEditsTooSmall() {
        new FuzzyCondition(0.5f, "name", "tr", 1, -2, 49, true);
    }

    @Test(expected = IndexException.class)
    public void testBuildMaxEditsTooLarge() {
        new FuzzyCondition(0.5f, "name", "tr", 100, 2, 49, true);
    }

    @Test(expected = IndexException.class)
    public void testBuildPrefixLengthInvalid() {
        new FuzzyCondition(0.5f, "name", "tr", -2, 2, 49, true);
    }

    @Test(expected = IndexException.class)
    public void testBuildMaxExpansionsInvalid() {
        new FuzzyCondition(0.5f, "name", "tr", 1, 2, -1, true);
    }

    @Test
    public void testJsonSerialization() {
        FuzzyConditionBuilder builder = new FuzzyConditionBuilder("field", "value").boost(0.7f)
                                                                                   .maxEdits(2)
                                                                                   .prefixLength(2)
                                                                                   .maxExpansions(49)
                                                                                   .transpositions(true);
        testJsonSerialization(builder,
                              "{type:\"fuzzy\",field:\"field\",value:\"value\",boost:0.7," +
                              "transpositions:true,max_edits:2,prefix_length:2,max_expansions:49}");
    }

    @Test
    public void testJsonSerializationDefaults() {
        FuzzyConditionBuilder builder = new FuzzyConditionBuilder("field", "value");
        testJsonSerialization(builder, "{type:\"fuzzy\",field:\"field\",value:\"value\"}");
    }

    @Test
    public void testQuery() {

        Schema schema = schema().mapper("name", stringMapper()).build();

        FuzzyCondition condition = new FuzzyCondition(0.5f, "name", "tr", 1, 2, 49, true);
        Query query = condition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", FuzzyQuery.class, query.getClass());

        FuzzyQuery fuzzyQuery = (FuzzyQuery) query;
        assertEquals("Query field is wrong", "name", fuzzyQuery.getField());
        assertEquals("Query term is wrong", "tr", fuzzyQuery.getTerm().text());
        assertEquals("Query max edits is wrong", 1, fuzzyQuery.getMaxEdits());
        assertEquals("Query prefix length is wrong", 2, fuzzyQuery.getPrefixLength());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test(expected = IndexException.class)
    public void testQueryInvalid() {

        Schema schema = schema().mapper("name", integerMapper()).build();

        FuzzyCondition condition = new FuzzyCondition(0.5f, "name", "tr", 1, 2, 49, true);
        condition.query(schema);
    }

    @Test
    public void testToString() {
        FuzzyCondition condition = new FuzzyCondition(0.5f, "name", "tr", 1, 2, 49, true);
        assertEquals("Method #toString is wrong",
                     "FuzzyCondition{boost=0.5, field=name, value=tr, " +
                     "maxEdits=1, prefixLength=2, maxExpansions=49, transpositions=true}",
                     condition.toString());
    }

}
