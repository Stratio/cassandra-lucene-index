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
package com.stratio.cassandra.lucene.search;

import com.stratio.cassandra.lucene.search.condition.*;
import com.stratio.cassandra.lucene.search.condition.builder.*;
import com.stratio.cassandra.lucene.search.sort.SimpleSortField;
import com.stratio.cassandra.lucene.search.sort.builder.SimpleSortFieldBuilder;
import org.junit.Test;

import java.io.IOException;

import static com.stratio.cassandra.lucene.search.SearchBuilders.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
        PhraseConditionBuilder builder = phrase("field", "value1 value2").slop(2);
        assertNotNull("Condition builder is not built", builder);
        PhraseCondition condition = builder.build();
        assertEquals("Condition field is not set", "field", condition.field);
        assertEquals("Condition value is not set", "value1 value2", condition.value);
        assertEquals("Condition slop is not set", 2, condition.slop);
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
        SimpleSortFieldBuilder builder = field("field");
        assertNotNull("Condition builder is not built", builder);
        SimpleSortField sortField = builder.build();
        assertEquals("Field is not set", "field", sortField.field);
    }
}