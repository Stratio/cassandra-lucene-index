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
import com.stratio.cassandra.lucene.search.condition.builder.ContainsConditionBuilder;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

import java.util.List;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ContainsConditionTest extends AbstractConditionTest {

    @Test
    public void testBuildDefaults() {
        Object[] values = new Object[]{"a", "b"};
        ContainsCondition condition = new ContainsConditionBuilder("field", values).build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set to default", Condition.DEFAULT_BOOST, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertArrayEquals("Values is not set", values, condition.values);
    }

    @Test
    public void testBuildStrings() {
        Object[] values = new Object[]{"a", "b"};
        ContainsCondition condition = new ContainsConditionBuilder("field", values).boost(0.7).build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertArrayEquals("Values is not set", values, condition.values);
    }

    @Test
    public void testBuildNumbers() {
        Object[] values = new Object[]{1, 2, -3};
        ContainsCondition condition = new ContainsConditionBuilder("field", values).boost(0.7).build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertArrayEquals("Values is not set", values, condition.values);
    }

    @Test(expected = IndexException.class)
    public void testBuildWithNullField() {
        new ContainsCondition(0.7f, null, 1, 2, 3);
    }

    @Test(expected = IndexException.class)
    public void testBuildWithBlankField() {
        new ContainsCondition(0.7f, " ", 1, 2, 3);
    }

    @Test(expected = IndexException.class)
    public void testBuildWithNullValues() {
        new ContainsCondition(0.7f, "values");
    }

    @Test(expected = IndexException.class)
    public void testBuildWithEmptyValues() {
        new ContainsCondition(0.7f, "values");
    }

    @Test
    public void testJsonSerializationStrings() {
        ContainsConditionBuilder builder = new ContainsConditionBuilder("field", "a", "b").boost(0.7);
        testJsonSerialization(builder, "{type:\"contains\",field:\"field\",values:[\"a\",\"b\"],boost:0.7}");
    }

    @Test
    public void testJsonSerializationNumbers() {
        ContainsConditionBuilder builder = new ContainsConditionBuilder("field", 1, 2, -3).boost(0.7);
        testJsonSerialization(builder, "{type:\"contains\",field:\"field\",values:[1,2,-3],boost:0.7}");
    }

    @Test
    public void testQueryNumeric() {

        Float boost = 0.7f;
        Object[] values = new Object[]{1, 2, 3};

        Schema schema = schema().mapper("name", integerMapper()).build();

        ContainsCondition condition = new ContainsCondition(boost, "name", values);
        Query query = condition.query(schema);
        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", BooleanQuery.class, query.getClass());

        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertEquals("Boost is not set", 0.7f, query.getBoost(), 0);
        List<BooleanClause> clauses = booleanQuery.clauses();
        assertEquals("Query is wrong", values.length, clauses.size());
        for (int i = 0; i < values.length; i++) {
            NumericRangeQuery<?> numericRangeQuery = (NumericRangeQuery<?>) clauses.get(i).getQuery();
            assertEquals("Query is wrong", values[i], numericRangeQuery.getMin());
            assertEquals("Query is wrong", values[i], numericRangeQuery.getMax());
        }
    }

    @Test
    public void testQueryString() {

        Float boost = 0.7f;
        Object[] values = new Object[]{"houses", "cats"};

        Schema schema = schema().mapper("name", stringMapper()).build();

        ContainsCondition condition = new ContainsCondition(boost, "name", values);
        Query query = condition.query(schema);
        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", BooleanQuery.class, query.getClass());

        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertEquals("Query boost is wrong", 0.7f, query.getBoost(), 0);
        List<BooleanClause> clauses = booleanQuery.clauses();
        TermQuery termQuery1 = (TermQuery) clauses.get(0).getQuery();
        TermQuery termQuery2 = (TermQuery) clauses.get(1).getQuery();
        assertEquals("Query is wrong", "houses", termQuery1.getTerm().bytes().utf8ToString());
        assertEquals("Query is wrong", "cats", termQuery2.getTerm().bytes().utf8ToString());
    }

    @Test
    public void testQueryText() {

        Float boost = 0.7f;
        Object[] values = new Object[]{"houses", "cats"};

        Schema schema = schema().mapper("name", textMapper()).defaultAnalyzer("english").build();

        ContainsCondition condition = new ContainsCondition(boost, "name", values);
        Query query = condition.query(schema);
        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", BooleanQuery.class, query.getClass());

        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertEquals("Query boost is wrong", 0.7f, query.getBoost(), 0);
        List<BooleanClause> clauses = booleanQuery.clauses();
        TermQuery termQuery1 = (TermQuery) clauses.get(0).getQuery();
        TermQuery termQuery2 = (TermQuery) clauses.get(1).getQuery();
        assertEquals("Query is wrong", "hous", termQuery1.getTerm().bytes().utf8ToString());
        assertEquals("Query is wrong", "cat", termQuery2.getTerm().bytes().utf8ToString());
    }

    @Test
    public void testToString() {
        ContainsCondition condition = new ContainsCondition(0.7f, "field", "value1", "value2");
        assertEquals("Method #toString is wrong",
                     "ContainsCondition{boost=0.7, field=field, values=[value1, value2]}",
                     condition.toString());
    }

}
