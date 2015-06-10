/*
 * Copyright 2015, Stratio.
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
package com.stratio.cassandra.lucene.query.builder;

import com.stratio.cassandra.lucene.query.*;
import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static com.stratio.cassandra.lucene.query.builder.SearchBuilders.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SearchBuildersTest {

    @Test
    public void testBool() throws IOException {
        BooleanConditionBuilder builder = bool();
        assertNotNull(builder);
        BooleanCondition condition = builder.build();
        assertNotNull(condition);
    }

    @Test
    public void testFuzzy() throws IOException {
        FuzzyConditionBuilder builder = fuzzy("field", "value");
        assertNotNull(builder);
        FuzzyCondition condition = builder.build();
        assertEquals("field", condition.getField());
        assertEquals("value", condition.getValue());
    }

    @Test
    public void testLucene() throws IOException {
        LuceneConditionBuilder builder = lucene("field:value");
        assertNotNull(builder);
        LuceneCondition condition = builder.build();
        assertEquals("field:value", condition.getQuery());
    }

    @Test
    public void testMatchAll() throws IOException {
        MatchAllConditionBuilder builder = matchAll();
        assertNotNull(builder);
        builder.build();
    }

    @Test
    public void testMatch() throws IOException {
        MatchConditionBuilder builder = match("field", "value");
        assertNotNull(builder);
        MatchCondition condition = builder.build();
        assertEquals("field", condition.getField());
        assertEquals("value", condition.getValue());
    }

    @Test
    public void testPhrase() throws IOException {
        PhraseConditionBuilder builder = phrase("field", "value1 value2");
        assertNotNull(builder);
        PhraseCondition condition = builder.build();
        assertEquals("field", condition.getField());
        assertEquals("value1 value2", condition.getValue());
    }

    @Test
    public void testPrefix() throws IOException {
        PrefixConditionBuilder builder = prefix("field", "value");
        assertNotNull(builder);
        PrefixCondition condition = builder.build();
        assertEquals("field", condition.getField());
        assertEquals("value", condition.getValue());
    }

    @Test
    public void testRange() throws IOException {
        RangeConditionBuilder builder = range("field");
        assertNotNull(builder);
        RangeCondition condition = builder.build();
        assertEquals("field", condition.getField());
    }

    @Test
    public void testRegexp() throws IOException {
        RegexpConditionBuilder builder = regexp("field", "value");
        assertNotNull(builder);
        RegexpCondition condition = builder.build();
        assertEquals("field", condition.getField());
        assertEquals("value", condition.getValue());
    }

    @Test
    public void testWildcard() throws IOException {
        WildcardConditionBuilder builder = wildcard("field", "value");
        assertNotNull(builder);
        WildcardCondition condition = builder.build();
        assertEquals("field", condition.getField());
        assertEquals("value", condition.getValue());
    }

    @Test
    public void testSortField() throws IOException {
        SortFieldBuilder builder = sortField("field");
        assertNotNull(builder);
        SortField sortField = builder.build();
        assertEquals("field", sortField.getField());
    }

    @Test
    public void testSort() throws IOException {
        SearchBuilder builder = sort(sortField("field"));
        assertNotNull(builder);
        Search search = builder.build();
        assertEquals("field", search.getSort().getSortFields().iterator().next().getField());
    }

    @Test
    public void testQuery() throws IOException {
        SearchBuilder builder = query(matchAll());
        assertNotNull(builder);
        Search search = builder.build();
        Query query = search.query(mock(Schema.class), null);
        assertEquals(BooleanQuery.class, query.getClass());
        List<BooleanClause> clauses = ((BooleanQuery) query).clauses();
        assertEquals(1, clauses.size());
        assertEquals(MatchAllDocsQuery.class, clauses.get(0).getQuery().getClass());
    }

    @Test
    public void testFilter() throws IOException {
        SearchBuilder builder = filter(matchAll());
        assertNotNull(builder);
        Search search = builder.build();
        Query query = search.query(mock(Schema.class), null);
        assertEquals(BooleanQuery.class, query.getClass());
        List<BooleanClause> clauses = ((BooleanQuery) query).clauses();
        assertEquals(1, clauses.size());
        assertEquals(ConstantScoreQuery.class, clauses.get(0).getQuery().getClass());
    }

    @Test
    public void testSearch() throws IOException {
        SearchBuilder builder = search();
        Search search = builder.build();
        Query query = search.query(mock(Schema.class), null);
        assertEquals(MatchAllDocsQuery.class, query.getClass());
    }
}
