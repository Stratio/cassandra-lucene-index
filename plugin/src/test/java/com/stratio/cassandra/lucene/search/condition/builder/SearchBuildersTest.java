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
package com.stratio.cassandra.lucene.search.condition.builder;

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.search.Search;
import com.stratio.cassandra.lucene.search.SearchBuilder;
import com.stratio.cassandra.lucene.search.SearchBuilders;
import com.stratio.cassandra.lucene.search.condition.BooleanCondition;
import com.stratio.cassandra.lucene.search.condition.FuzzyCondition;
import com.stratio.cassandra.lucene.search.condition.LuceneCondition;
import com.stratio.cassandra.lucene.search.condition.MatchCondition;
import com.stratio.cassandra.lucene.search.condition.PhraseCondition;
import com.stratio.cassandra.lucene.search.condition.PrefixCondition;
import com.stratio.cassandra.lucene.search.condition.RangeCondition;
import com.stratio.cassandra.lucene.search.condition.RegexpCondition;
import com.stratio.cassandra.lucene.search.condition.WildcardCondition;
import com.stratio.cassandra.lucene.search.sort.SortField;
import com.stratio.cassandra.lucene.search.sort.builder.SortFieldBuilder;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.junit.Test;

import java.io.IOException;

import static com.stratio.cassandra.lucene.search.SearchBuilders.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * Class for testing {@link SearchBuilders}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SearchBuildersTest {

    @Test
    public void testBool() throws IOException {
        BooleanConditionBuilder builder = bool().must(matchAll());
        assertNotNull(builder);
        BooleanCondition condition = builder.build();
        assertNotNull(condition);
    }

    @Test
    public void testFuzzy() throws IOException {
        FuzzyConditionBuilder builder = fuzzy("field", "value");
        assertNotNull(builder);
        FuzzyCondition condition = builder.build();
        assertEquals("field", condition.field);
        assertEquals("value", condition.value);
    }

    @Test
    public void testLucene() throws IOException {
        LuceneConditionBuilder builder = lucene("field:value");
        assertNotNull(builder);
        LuceneCondition condition = builder.build();
        assertEquals("field:value", condition.query);
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
        assertEquals("field", condition.field);
        assertEquals("value", condition.value);
    }

    @Test
    public void testPhrase() throws IOException {
        PhraseConditionBuilder builder = phrase("field", "value1 value2");
        assertNotNull(builder);
        PhraseCondition condition = builder.build();
        assertEquals("field", condition.field);
        assertEquals("value1 value2", condition.value);
    }

    @Test
    public void testPrefix() throws IOException {
        PrefixConditionBuilder builder = prefix("field", "value");
        assertNotNull(builder);
        PrefixCondition condition = builder.build();
        assertEquals("field", condition.field);
        assertEquals("value", condition.value);
    }

    @Test
    public void testRange() throws IOException {
        RangeConditionBuilder builder = range("field");
        assertNotNull(builder);
        RangeCondition condition = builder.build();
        assertEquals("field", condition.field);
    }

    @Test
    public void testRegexp() throws IOException {
        RegexpConditionBuilder builder = regexp("field", "value");
        assertNotNull(builder);
        RegexpCondition condition = builder.build();
        assertEquals("field", condition.field);
        assertEquals("value", condition.value);
    }

    @Test
    public void testWildcard() throws IOException {
        WildcardConditionBuilder builder = wildcard("field", "value");
        assertNotNull(builder);
        WildcardCondition condition = builder.build();
        assertEquals("field", condition.field);
        assertEquals("value", condition.value);
    }

    @Test
    public void testSortField() throws IOException {
        SortFieldBuilder builder = sortField("field");
        assertNotNull(builder);
        SortField sortField = builder.build();
        assertEquals("field", sortField.field);
    }

    @Test
    public void testSort() throws IOException {
        SearchBuilder builder = sort(sortField("field"));
        assertNotNull(builder);
        Search search = builder.build();
        assertEquals("field", search.getSort().getSortFields().iterator().next().field);
    }

    @Test
    public void testQuery() throws IOException {
        SearchBuilder builder = query(matchAll());
        assertNotNull(builder);
        Search search = builder.build();
        Schema schema = mock(Schema.class);
        assertEquals(MatchAllDocsQuery.class, search.query(schema).getClass());
        assertNull(search.filter(schema));
    }

    @Test
    public void testFilter() throws IOException {
        SearchBuilder builder = filter(matchAll());
        assertNotNull(builder);
        Search search = builder.build();
        Schema schema = mock(Schema.class);
        assertNull(search.query(schema));
        assertEquals(MatchAllDocsQuery.class, search.filter(schema).getClass());
    }

    @Test
    public void testSearch() throws IOException {
        SearchBuilder builder = search();
        Search search = builder.build();
        Schema schema = mock(Schema.class);
        assertNull(search.query(schema));
        assertNull(search.filter(schema));
    }
}
