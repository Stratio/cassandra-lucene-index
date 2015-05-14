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
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperInteger;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperString;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ContainsConditionTest extends AbstractConditionTest {

    @Test
    public void testBuild() {
        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(new EnglishAnalyzer());
        Float boost = 0.7f;
        String field = "test";
        Object[] values = new Object[]{1, 2, 3};
        ContainsCondition condition = new ContainsCondition(boost, field, values);
        Assert.assertEquals(boost, condition.getBoost(), 0);
        Assert.assertEquals(field, condition.getField());
        Assert.assertArrayEquals(values, condition.getValues());
    }

    @Test
    public void testBuildWithDefaults() {
        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(new EnglishAnalyzer());
        String field = "test";
        Object[] values = new Object[]{1, 2, 3};
        ContainsCondition condition = new ContainsCondition(null, field, values);
        Assert.assertEquals(Condition.DEFAULT_BOOST, condition.getBoost(), 0);
        Assert.assertEquals(field, condition.getField());
        Assert.assertArrayEquals(values, condition.getValues());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildWithNullField() {
        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(new EnglishAnalyzer());
        new ContainsCondition(0.7f, null, new Object[]{1, 2, 3});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildWithBlankField() {
        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(new EnglishAnalyzer());
        new ContainsCondition(0.7f, " ", new Object[]{1, 2, 3});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildWithNullValues() {
        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(new EnglishAnalyzer());
        new ContainsCondition(0.7f, "values", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildWithEmptyValues() {
        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(new EnglishAnalyzer());
        new ContainsCondition(0.7f, "values", new Object[]{});
    }

    @Test
    public void testQueryNumeric() {

        Float boost = 0.7f;
        String field = "test";
        Object[] values = new Object[]{1, 2, 3};

        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(new EnglishAnalyzer());
        when(schema.getMapperSingle(field)).thenReturn(new ColumnMapperInteger(null, null, null));

        ContainsCondition condition = new ContainsCondition(boost, field, values);
        Query query = condition.query(schema);
        Assert.assertNotNull(query);

        Assert.assertEquals(BooleanQuery.class, query.getClass());
        Assert.assertEquals(0.7f, query.getBoost(), 0);
        BooleanClause[] booleanClauses = ((BooleanQuery) query).getClauses();
        Assert.assertEquals(values.length, booleanClauses.length);
        for (int i = 0; i < values.length; i++) {
            NumericRangeQuery numericRangeQuery = (NumericRangeQuery) booleanClauses[i].getQuery();
            Assert.assertEquals(values[i], numericRangeQuery.getMin());
            Assert.assertEquals(values[i], numericRangeQuery.getMax());
        }
    }

    @Test
    public void testQueryString() {

        Float boost = 0.7f;
        String field = "test";
        Object[] values = new Object[]{"houses", "cats"};

        Schema schema = mock(Schema.class);
        when(schema.getAnalyzer()).thenReturn(new EnglishAnalyzer());
        when(schema.getMapperSingle(field)).thenReturn(new ColumnMapperString(null, null, null));

        ContainsCondition condition = new ContainsCondition(boost, field, values);
        Query query = condition.query(schema);
        Assert.assertNotNull(query);

        Assert.assertEquals(BooleanQuery.class, query.getClass());
        Assert.assertEquals(0.7f, query.getBoost(), 0);
        BooleanClause[] booleanClauses = ((BooleanQuery) query).getClauses();
        Assert.assertEquals("hous", ((TermQuery) booleanClauses[0].getQuery()).getTerm().bytes().utf8ToString());
        Assert.assertEquals("cat", ((TermQuery) booleanClauses[1].getQuery()).getTerm().bytes().utf8ToString());
    }

    @Test
    public void testJsonNumbers() throws IOException {
        String in = "{type:\"contains\",boost:0.7,field:\"test\",values:[1,2,3]}";
        ContainsCondition condition = JsonSerializer.fromString(in, ContainsCondition.class);
        Assert.assertEquals(0.7f, condition.getBoost(), 0f);
        Assert.assertEquals("test", condition.getField());
        Assert.assertArrayEquals(new Object[]{1, 2, 3}, condition.getValues());
        String out = JsonSerializer.toString(condition);
        Assert.assertEquals(in, out);
    }

    @Test
    public void testJsonStrings() throws IOException {
        String in = "{type:\"contains\",boost:0.7,field:\"test\",values:[\"a\",\"b\"]}";
        ContainsCondition condition = JsonSerializer.fromString(in, ContainsCondition.class);
        Assert.assertEquals(0.7f, condition.getBoost(), 0f);
        Assert.assertEquals("test", condition.getField());
        Assert.assertArrayEquals(new Object[]{"a", "b"}, condition.getValues());
        String out = JsonSerializer.toString(condition);
        Assert.assertEquals(in, out);
    }

}
