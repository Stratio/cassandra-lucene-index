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
package com.stratio.cassandra.lucene.search.condition;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.search.condition.builder.ContainsConditionBuilder;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

import java.util.List;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static com.stratio.cassandra.lucene.search.SearchBuilders.contains;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ContainsConditionTest extends AbstractConditionTest {

    @Test
    public void testBuildDefaults() {
        Object[] values = new Object[]{"a", "b"};
        ContainsCondition condition = contains("field", values).build();
        assertNotNull("Condition is not built", condition);
        assertNull("Boost is not set to default", condition.boost);
        assertEquals("Field is not set", "field", condition.field);
        assertArrayEquals("Values is not set", values, condition.values);
        assertEquals("Doc values is not set", MatchCondition.DEFAULT_DOC_VALUES, condition.docValues);
    }

    @Test
    public void testBuildStrings() {
        Object[] values = new Object[]{"a", "b"};
        ContainsCondition condition = contains("field", values).boost(0.7).docValues(true).build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertArrayEquals("Values is not set", values, condition.values);
        assertTrue("Doc values is not set", condition.docValues);
    }

    @Test
    public void testBuildNumbers() {
        Object[] values = new Object[]{1, 2, -3};
        ContainsCondition condition = contains("field", values).boost(0.7).docValues(true).build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertArrayEquals("Values is not set", values, condition.values);
        assertTrue("Doc values is not set", condition.docValues);
    }

    @Test(expected = IndexException.class)
    public void testBuildWithNullField() {
        contains(null, 1, 2, 3).build();
    }

    @Test(expected = IndexException.class)
    public void testBuildWithBlankField() {
        contains(" ", 1, 2, 3).build();
    }

    @Test(expected = IndexException.class)
    public void testBuildWithNullValues() {
        contains("values").build();
    }

    @Test
    public void testJsonSerializationStrings() {
        ContainsConditionBuilder builder = contains("field", "a", "b").boost(0.7).docValues(true);
        testJsonSerialization(builder,
                              "{type:\"contains\",field:\"field\",values:[\"a\",\"b\"],boost:0.7,doc_values:true}");
    }

    @Test
    public void testJsonSerializationNumbers() {
        ContainsConditionBuilder builder = contains("field", 1, 2, -3).boost(0.7).docValues(true);
        testJsonSerialization(builder, "{type:\"contains\",field:\"field\",values:[1,2,-3],boost:0.7,doc_values:true}");
    }

    @Test
    public void testQueryNumeric() {

        Object[] values = new Object[]{0, 1, 2};

        Schema schema = schema().mapper("name", integerMapper()).build();

        ContainsCondition condition = contains("name", values).build();
        Query query = condition.doQuery(schema);
        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", BooleanQuery.class, query.getClass());

        BooleanQuery booleanQuery = (BooleanQuery) query;
        List<BooleanClause> clauses = booleanQuery.clauses();
        assertEquals("Query is wrong", values.length, clauses.size());
        TermQuery query0 = (TermQuery) clauses.get(0).getQuery();
        TermQuery query1 = (TermQuery) clauses.get(1).getQuery();
        TermQuery query2 = (TermQuery) clauses.get(2).getQuery();
        assertEquals("Query value is wrong", "600800000000", ByteBufferUtils.toHex(query0.getTerm().bytes()));
        assertEquals("Query value is wrong", "600800000001", ByteBufferUtils.toHex(query1.getTerm().bytes()));
        assertEquals("Query value is wrong", "600800000002", ByteBufferUtils.toHex(query2.getTerm().bytes()));
    }

    @Test
    public void testQueryString() {

        Object[] values = new Object[]{"houses", "cats"};

        Schema schema = schema().mapper("name", stringMapper()).build();

        ContainsCondition condition = contains("name", values).build();
        Query query = condition.doQuery(schema);
        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", BooleanQuery.class, query.getClass());

        BooleanQuery booleanQuery = (BooleanQuery) query;
        List<BooleanClause> clauses = booleanQuery.clauses();
        TermQuery query1 = (TermQuery) clauses.get(0).getQuery();
        TermQuery query2 = (TermQuery) clauses.get(1).getQuery();
        assertEquals("Query is wrong", "houses", query1.getTerm().bytes().utf8ToString());
        assertEquals("Query is wrong", "cats", query2.getTerm().bytes().utf8ToString());
    }

    @Test
    public void testQueryText() {

        Object[] values = new Object[]{"houses", "cats"};

        Schema schema = schema().mapper("name", textMapper()).defaultAnalyzer("english").build();

        ContainsCondition condition = contains("name", values).build();
        Query query = condition.doQuery(schema);
        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", BooleanQuery.class, query.getClass());

        BooleanQuery booleanQuery = (BooleanQuery) query;
        List<BooleanClause> clauses = booleanQuery.clauses();
        TermQuery termQuery1 = (TermQuery) clauses.get(0).getQuery();
        TermQuery termQuery2 = (TermQuery) clauses.get(1).getQuery();
        assertEquals("Query is wrong", "hous", termQuery1.getTerm().bytes().utf8ToString());
        assertEquals("Query is wrong", "cat", termQuery2.getTerm().bytes().utf8ToString());
    }

    @Test
    public void testToString() {
        ContainsCondition condition = contains("field", "value1", "value2").boost(0.7f).docValues(true).build();
        assertEquals("Method #toString is wrong",
                     "ContainsCondition{boost=0.7, field=field, values=[value1, value2], docValues=true}",
                     condition.toString());
    }

}
