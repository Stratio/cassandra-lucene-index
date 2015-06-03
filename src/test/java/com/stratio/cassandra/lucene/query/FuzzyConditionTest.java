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
import com.stratio.cassandra.lucene.schema.analysis.PreBuiltAnalyzers;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperInteger;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperString;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.filter;
import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.fuzzy;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class FuzzyConditionTest extends AbstractConditionTest {

    @Test
    public void testBuilder() {
        FuzzyCondition condition = new FuzzyCondition(0.5f, "name", "tr", 1, 2, 49, true);
        assertEquals("name", condition.getField());
        assertEquals("tr", condition.getValue());
        assertEquals(1, condition.getMaxEdits());
        assertEquals(2, condition.getPrefixLength());
        assertEquals(49, condition.getMaxExpansions());
        assertTrue(condition.getTranspositions());
    }

    @Test
    public void testBuilderDefaults() {
        FuzzyCondition condition = new FuzzyCondition(0.5f, "name", "tr", null, null, null, null);
        assertEquals("name", condition.getField());
        assertEquals("tr", condition.getValue());
        assertEquals(FuzzyCondition.DEFAULT_MAX_EDITS, condition.getMaxEdits());
        assertEquals(FuzzyCondition.DEFAULT_PREFIX_LENGTH, condition.getPrefixLength());
        assertEquals(FuzzyCondition.DEFAULT_MAX_EXPANSIONS, condition.getMaxExpansions());
        assertEquals(FuzzyCondition.DEFAULT_TRANSPOSITIONS, condition.getTranspositions());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderValueNull() {
        new FuzzyCondition(0.5f, "name", null, 1, 2, 49, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderValueBlank() {
        new FuzzyCondition(0.5f, "name", " ", 1, 2, 49, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderMaxEditsTooSmall() {
        new FuzzyCondition(0.5f, "name", "tr", 1, -2, 49, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderMaxEditsTooLarge() {
        new FuzzyCondition(0.5f, "name", "tr", 100, 2, 49, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderPrefixLengthInvalid() {
        new FuzzyCondition(0.5f, "name", "tr", -2, 2, 49, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderMaxExpansionsInvalid() {
        new FuzzyCondition(0.5f, "name", "tr", 1, 2, -1, true);
    }

    @Test
    public void testQuery() {

        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(PreBuiltAnalyzers.STANDARD.get());
        when(schema.getMapper("name")).thenReturn(new ColumnMapperString("name", null, null, null));

        FuzzyCondition condition = new FuzzyCondition(0.5f, "name", "tr", 1, 2, 49, true);
        Query query = condition.query(schema);

        assertNotNull(query);
        assertEquals(FuzzyQuery.class, query.getClass());
        FuzzyQuery luceneQuery = (FuzzyQuery) query;
        assertEquals("name", luceneQuery.getField());
        assertEquals("tr", luceneQuery.getTerm().text());
        assertEquals(1, luceneQuery.getMaxEdits());
        assertEquals(2, luceneQuery.getPrefixLength());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testQueryInvalid() {

        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(PreBuiltAnalyzers.STANDARD.get());
        when(schema.getMapper("name")).thenReturn(new ColumnMapperInteger("name", null, null, null));

        FuzzyCondition condition = new FuzzyCondition(0.5f, "name", "tr", 1, 2, 49, true);
        condition.query(schema);
    }

    @Test
    public void testJson() {
        testJsonCondition(filter(fuzzy("name", "tr").maxEdits(1)
                                                    .maxExpansions(1)
                                                    .prefixLength(40)
                                                    .transpositions(true)
                                                    .boost(0.5f)));
    }

    @Test
    public void testToString() {
        FuzzyCondition condition = new FuzzyCondition(0.5f, "name", "tr", 1, 2, 49, true);
        assertEquals(
                "FuzzyCondition{boost=0.5, field=name, value=tr, maxEdits=1, prefixLength=2, maxExpansions=49, transpositions=true}",
                condition.toString());
    }

}
