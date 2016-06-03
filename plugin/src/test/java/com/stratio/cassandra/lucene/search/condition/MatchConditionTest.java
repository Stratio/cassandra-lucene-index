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
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper;
import com.stratio.cassandra.lucene.schema.mapping.builder.MapperBuilder;
import com.stratio.cassandra.lucene.search.condition.builder.MatchConditionBuilder;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

import java.util.Optional;
import java.util.UUID;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static com.stratio.cassandra.lucene.search.SearchBuilders.match;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class MatchConditionTest extends AbstractConditionTest {

    @Test
    public void testBuildDefaults() {
        MatchCondition condition = match("field", "value").build();
        assertNotNull("Condition is not built", condition);
        assertNull("Boost is not set to default", condition.boost);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Value is not set", "value", condition.value);
        assertEquals("Doc values is not set", MatchCondition.DEFAULT_DOC_VALUES, condition.docValues);
    }

    @Test
    public void testBuildString() {
        MatchCondition condition = match("field", "value").boost(0.7).docValues(true).build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Value is not set", "value", condition.value);
        assertTrue("Doc values is not set", condition.docValues);
    }

    @Test
    public void testBuildNumber() {
        MatchCondition condition = match("field", 3).boost(0.7).docValues(true).build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Value is not set", 3, condition.value);
        assertTrue("Doc values is not set", condition.docValues);
    }

    @Test
    public void testBlankValue() {
        MatchCondition condition = match("field", " ").boost(0.7).build();
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Value is not set", " ", condition.value);
    }

    @Test
    public void testJsonSerializationDefaults() {
        MatchConditionBuilder builder = match("field", "value");
        testJsonSerialization(builder, "{type:\"match\",field:\"field\",value:\"value\"}");
    }

    @Test
    public void testJsonSerializationString() {
        MatchConditionBuilder builder = match("field", "value").boost(0.7).docValues(true);
        testJsonSerialization(builder, "{type:\"match\",field:\"field\",value:\"value\",boost:0.7,doc_values:true}");
    }

    @Test
    public void testJsonSerializationNumber() {
        MatchConditionBuilder builder = match("field", 3).boost(0.7).docValues(true);
        testJsonSerialization(builder, "{type:\"match\",field:\"field\",value:3,boost:0.7,doc_values:true}");
    }

    @Test
    public void testString() {

        Schema schema = schema().mapper("name", stringMapper()).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", "value", false);
        Query query = matchCondition.doQuery(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", TermQuery.class, query.getClass());

        TermQuery termQuery = (TermQuery) query;
        assertEquals("Query value is wrong", "value", termQuery.getTerm().bytes().utf8ToString());
    }

    @Test
    public void testStringStopwords() {

        Schema schema = schema().mapper("name", textMapper().analyzer("english")).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", "the", false);
        Query query = matchCondition.doQuery(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", BooleanQuery.class, query.getClass());
    }

    @Test
    public void testInteger() {

        Schema schema = schema().mapper("name", integerMapper()).build();

        MatchCondition matchCondition = match("name", 42).boost(0.5f).build();
        Query query = matchCondition.doQuery(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", TermQuery.class, query.getClass());
        Term term = ((TermQuery) query).getTerm();
        assertEquals("Query value is wrong", "60080000002a", ByteBufferUtils.toHex(term.bytes()));
    }

    @Test
    public void testLong() {

        Schema schema = schema().mapper("name", longMapper()).build();

        MatchCondition matchCondition = match("name", 42L).build();
        Query query = matchCondition.doQuery(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", TermQuery.class, query.getClass());
        Term term = ((TermQuery) query).getTerm();
        assertEquals("Query value is wrong", "200100000000000000002a", ByteBufferUtils.toHex(term.bytes()));
    }

    @Test
    public void testFloat() {

        Schema schema = schema().mapper("name", floatMapper()).build();

        MatchCondition matchCondition = match("name", 42.42F).build();
        Query query = matchCondition.doQuery(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", NumericRangeQuery.class, query.getClass());

        NumericRangeQuery<?> numericRangeQuery = (NumericRangeQuery<?>) query;
        assertEquals("Query value is wrong", 42.42F, numericRangeQuery.getMin());
        assertEquals("Query value is wrong", 42.42F, numericRangeQuery.getMax());
        assertEquals("Query value is wrong", true, numericRangeQuery.includesMin());
        assertEquals("Query value is wrong", true, numericRangeQuery.includesMax());
    }

    @Test
    public void testDouble() {

        Schema schema = schema().mapper("name", doubleMapper()).build();

        MatchCondition matchCondition = match("name", 42.42D).build();
        Query query = matchCondition.doQuery(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", NumericRangeQuery.class, query.getClass());

        NumericRangeQuery<?> numericRangeQuery = (NumericRangeQuery<?>) query;
        assertEquals("Query value is wrong", 42.42D, numericRangeQuery.getMin());
        assertEquals("Query value is wrong", 42.42D, numericRangeQuery.getMax());
        assertEquals("Query value is wrong", true, numericRangeQuery.includesMin());
        assertEquals("Query value is wrong", true, numericRangeQuery.includesMax());
    }

    @Test
    public void testBlob() {

        Schema schema = schema().mapper("name", blobMapper()).build();

        MatchCondition matchCondition = match("name", "0Fa1").build();
        Query query = matchCondition.doQuery(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", TermQuery.class, query.getClass());

        TermQuery termQuery = (TermQuery) query;
        assertEquals("Query value is wrong", "0fa1", termQuery.getTerm().bytes().utf8ToString());
    }

    @Test
    public void testInetV4() {

        Schema schema = schema().mapper("name", inetMapper()).build();

        MatchCondition matchCondition = match("name", "192.168.0.01").build();
        Query query = matchCondition.doQuery(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", TermQuery.class, query.getClass());

        TermQuery termQuery = (TermQuery) query;
        assertEquals("Query value is wrong", "192.168.0.1", termQuery.getTerm().bytes().utf8ToString());
    }

    @Test
    public void testInetV6() {

        Schema schema = schema().mapper("name", inetMapper()).build();

        MatchCondition matchCondition = match("name", "2001:DB8:2de::0e13").build();
        Query query = matchCondition.doQuery(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", TermQuery.class, query.getClass());

        TermQuery termQuery = (TermQuery) query;
        assertEquals("Query value is wrong", "2001:db8:2de:0:0:0:0:e13", termQuery.getTerm().bytes().utf8ToString());
    }

    @Test(expected = IndexException.class)
    public void testUnsupportedMapper() {

        final MockedMapper mapper = new MockedMapper();

        Schema schema = schema().mapper("field", new MockedMapperBuilder(mapper)).build();

        MatchCondition matchCondition = match("field", "2001:DB8:2de::0e13").build();
        matchCondition.doQuery(schema);
    }

    private class MockedMapper extends SingleColumnMapper.SingleFieldMapper<UUID> {

        MockedMapper() {
            super("field", null, true, true, null, UUID.class, UUIDType.instance);
        }

        @Override
        public Optional<Field> indexedField(String name, UUID value) {
            return null;
        }

        @Override
        public Optional<Field> sortedField(String name, UUID value) {
            return null;
        }

        @Override
        protected UUID doBase(String name, Object value) {
            return null;
        }

        @Override
        public org.apache.lucene.search.SortField sortField(String name, boolean reverse) {
            return null;
        }
    }

    private class MockedMapperBuilder extends MapperBuilder<MockedMapper, MockedMapperBuilder> {

        private MockedMapper mapper;

        MockedMapperBuilder(MockedMapper mapper) {
            this.mapper = mapper;
        }

        @Override
        public MockedMapper build(String field) {
            return mapper;
        }
    }

    @Test
    public void testToString() {
        MatchCondition condition = match("name", "2001:DB8:2de::0e13").boost(0.5f).docValues(true).build();
        assertEquals("Method #toString is wrong",
                     "MatchCondition{boost=0.5, field=name, value=2001:DB8:2de::0e13, docValues=true}",
                     condition.toString());
    }

}
