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

package com.stratio.cassandra.lucene.search;

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.search.condition.BooleanCondition;
import com.stratio.cassandra.lucene.search.condition.FuzzyCondition;
import com.stratio.cassandra.lucene.search.condition.LuceneCondition;
import com.stratio.cassandra.lucene.search.condition.MatchCondition;
import com.stratio.cassandra.lucene.search.condition.PhraseCondition;
import com.stratio.cassandra.lucene.search.condition.PrefixCondition;
import com.stratio.cassandra.lucene.search.condition.RangeCondition;
import com.stratio.cassandra.lucene.search.condition.RegexpCondition;
import com.stratio.cassandra.lucene.search.condition.WildcardCondition;
import com.stratio.cassandra.lucene.search.condition.builder.*;
import com.stratio.cassandra.lucene.search.sort.SortField;
import com.stratio.cassandra.lucene.search.sort.builder.SortFieldBuilder;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.junit.Test;

import java.io.IOException;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.schema;
import static com.stratio.cassandra.lucene.search.SearchBuilders.*;
import static org.junit.Assert.*;

/**
 * Class for testing {@link Search} builders.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SearchBuildersTest {

    @Test
    public void testBool() throws IOException {
        BooleanConditionBuilder builder = bool().must(all());
        assertNotNull("Condition builder is not built", builder);
        BooleanCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
    }

    @Test
    public void testFuzzy() throws IOException {
        FuzzyConditionBuilder builder = fuzzy("field", "value");
        assertNotNull("Condition builder is not built", builder);
        FuzzyCondition condition = builder.build();
        assertEquals("Condition field is not set", "field", condition.field);
        assertEquals("Condition value is not set", "value", condition.value);
    }

    @Test
    public void testLucene() throws IOException {
        LuceneConditionBuilder builder = lucene("field:value");
        assertNotNull("Condition builder is not built", builder);
        LuceneCondition condition = builder.build();
        assertEquals("Condition query is not set", "field:value", condition.query);
    }

    @Test
    public void testMatch() throws IOException {
        MatchConditionBuilder builder = match("field", "value");
        assertNotNull("Condition builder is not built", builder);
        MatchCondition condition = builder.build();
        assertEquals("Condition field is not set", "field", condition.field);
        assertEquals("Condition value is not set", "value", condition.value);
    }

    @Test
    public void testMatchAll() throws IOException {
        AllConditionBuilder builder = all();
        assertNotNull("Condition builder is not built", builder);
        builder.build();
    }

    @Test
    public void testNone() throws IOException {
        NoneConditionBuilder builder = none();
        assertNotNull("Condition builder is not built", builder);
        builder.build();
    }

    @Test
    public void testPhrase() throws IOException {
        PhraseConditionBuilder builder = phrase("field", "value1 value2");
        assertNotNull("Condition builder is not built", builder);
        PhraseCondition condition = builder.build();
        assertEquals("Condition field is not set", "field", condition.field);
        assertEquals("Condition value is not set", "value1 value2", condition.value);
    }

    @Test
    public void testPrefix() throws IOException {
        PrefixConditionBuilder builder = prefix("field", "value");
        assertNotNull("Condition builder is not built", builder);
        PrefixCondition condition = builder.build();
        assertEquals("Condition field is not set", "field", condition.field);
        assertEquals("Condition value is not set", "value", condition.value);
    }

    @Test
    public void testRange() throws IOException {
        RangeConditionBuilder builder = range("field");
        assertNotNull("Condition builder is not built", builder);
        RangeCondition condition = builder.build();
        assertEquals("Condition field is not set", "field", condition.field);
    }

    @Test
    public void testRegexp() throws IOException {
        RegexpConditionBuilder builder = regexp("field", "value");
        assertNotNull("Condition builder is not built", builder);
        RegexpCondition condition = builder.build();
        assertEquals("Condition field is not set", "field", condition.field);
        assertEquals("Condition value is not set", "value", condition.value);
    }

    @Test
    public void testWildcard() throws IOException {
        WildcardConditionBuilder builder = wildcard("field", "value");
        assertNotNull("Condition builder is not built", builder);
        WildcardCondition condition = builder.build();
        assertEquals("Condition field is not set", "field", condition.field);
        assertEquals("Condition value is not set", "value", condition.value);
    }

    @Test
    public void testSortField() throws IOException {
        SortFieldBuilder builder = sortField("field");
        assertNotNull("Condition builder is not built", builder);
        SortField sortField = builder.build();
        assertEquals("Field is not set", "field", sortField.field);
    }

    @Test
    public void testSort() throws IOException {
        SearchBuilder builder = sort(sortField("field"));
        assertNotNull("Condition builder is not built", builder);
        Search search = builder.build();
        assertEquals("Field is not set", "field", search.getSort().getSortFields().iterator().next().field);
    }

    @Test
    public void testQuery() throws IOException {
        SearchBuilder builder = query(all());
        assertNotNull("Condition builder is not built", builder);
        Search search = builder.build();
        Schema schema = schema().build();
        assertEquals("Query type is wrong", MatchAllDocsQuery.class, search.query(schema).getClass());
        assertNull("Filter must be null", search.filter(schema));
    }

    @Test
    public void testFilter() throws IOException {
        SearchBuilder builder = filter(all());
        assertNotNull("Condition builder is not built", builder);
        Search search = builder.build();
        Schema schema = schema().build();
        assertNull("Query type is not built", search.query(schema));
        assertEquals("Query type is wrong", MatchAllDocsQuery.class, search.filter(schema).getClass());
    }

    @Test
    public void testSearch() throws IOException {
        SearchBuilder builder = search();
        Search search = builder.build();
        Schema schema = schema().build();
        assertNull("Query must be null", search.query(schema));
        assertNull("Filter must be null", search.filter(schema));
    }
}
