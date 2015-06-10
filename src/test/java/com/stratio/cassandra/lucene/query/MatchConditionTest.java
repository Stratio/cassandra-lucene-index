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

import com.stratio.cassandra.lucene.query.builder.SearchBuilders;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperBlob;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperDouble;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperFloat;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperInet;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperInteger;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperLong;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperSingle;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperString;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperText;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class MatchConditionTest extends AbstractConditionTest {

    @Test
    public void testBuild() {
        MatchCondition condition = new MatchCondition(0.5f, "field", "value");
        assertEquals(0.5f, condition.getBoost(), 0);
        assertEquals("field", condition.getField());
        assertEquals("value", condition.getValue());
    }

    @Test
    public void testBuildDefaults() {
        MatchCondition condition = new MatchCondition(null, "field", "value");
        assertEquals(Condition.DEFAULT_BOOST, condition.getBoost(), 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildNullValue() {
        new MatchCondition(null, "field", null);
    }

    @Test
    public void testBuildBlankValue() {
        MatchCondition condition = new MatchCondition(0.5f, "field", " ");
        assertEquals(0.5f, condition.getBoost(), 0);
        assertEquals("field", condition.getField());
        assertEquals(" ", condition.getValue());
    }

    @Test
    public void testString() {

        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(new ColumnMapperString("field", null, null, null));
        when(schema.getAnalyzer()).thenReturn(new KeywordAnalyzer());

        MatchCondition matchCondition = new MatchCondition(0.5f, "field", "value");
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermQuery.class, query.getClass());
        assertEquals("value", ((TermQuery) query).getTerm().bytes().utf8ToString());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testStringStopwords() {

        Schema schema = mockSchema("name", new ColumnMapperText("name", null, null, "english"), "english");

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", "the");
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermQuery.class, query.getClass());
        assertEquals("", ((TermQuery) query).getTerm().bytes().utf8ToString());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInteger() {

        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(new ColumnMapperInteger("field", null, null, null));

        MatchCondition matchCondition = new MatchCondition(0.5f, "field", 42);
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals(42, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(42, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testLong() {

        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(new ColumnMapperLong("field", null, null, 1f));

        MatchCondition matchCondition = new MatchCondition(0.5f, "field", 42L);
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals(42L, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(42L, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testFloat() {

        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(new ColumnMapperFloat("field", null, null, 1f));

        MatchCondition matchCondition = new MatchCondition(0.5f, "field", 42.42F);
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals(42.42F, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(42.42F, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testDouble() {

        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(new ColumnMapperDouble("field", null, null, 1f));

        MatchCondition matchCondition = new MatchCondition(0.5f, "field", 42.42D);
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals(42.42D, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(42.42D, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testBlob() {

        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(new ColumnMapperBlob("field", null, null));
        when(schema.getAnalyzer()).thenReturn(new KeywordAnalyzer());

        MatchCondition matchCondition = new MatchCondition(0.5f, "field", "0Fa1");
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermQuery.class, query.getClass());
        assertEquals("0fa1", ((TermQuery) query).getTerm().bytes().utf8ToString());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV4() {

        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(new ColumnMapperInet("field", null, null));
        when(schema.getAnalyzer()).thenReturn(new KeywordAnalyzer());

        MatchCondition matchCondition = new MatchCondition(0.5f, "field", "192.168.0.01");
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermQuery.class, query.getClass());
        assertEquals("192.168.0.1", ((TermQuery) query).getTerm().bytes().utf8ToString());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV6() {

        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(new ColumnMapperInet("field", null, null));
        when(schema.getAnalyzer()).thenReturn(new KeywordAnalyzer());

        MatchCondition matchCondition = new MatchCondition(0.5f, "field", "2001:DB8:2de::0e13");
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermQuery.class, query.getClass());
        assertEquals("2001:db8:2de:0:0:0:0:e13", ((TermQuery) query).getTerm().bytes().utf8ToString());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedMapper() {

        ColumnMapperSingle<UUID> mapper = new ColumnMapperSingle<UUID>("field", null, null, UUIDType.instance) {
            @Override
            public Field indexedField(String name, UUID value) {
                return null;
            }

            @Override
            public Field sortedField(String name, UUID value, boolean isCollection) {
                return null;
            }

            @Override
            public Class<UUID> baseClass() {
                return UUID.class;
            }

            @Override
            public UUID base(String field, Object value) {
                return null;
            }

            @Override
            public org.apache.lucene.search.SortField sortField(boolean reverse) {
                return null;
            }
        };

        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(mapper);
        when(schema.getAnalyzer()).thenReturn(new KeywordAnalyzer());

        MatchCondition matchCondition = new MatchCondition(0.5f, "field", "2001:DB8:2de::0e13");
        matchCondition.query(schema);
    }

    @Test
    public void testJson() {
        testJsonCondition(SearchBuilders.filter(SearchBuilders.match("name", 42).boost(0.5f)));
    }

    @Test
    public void testToString() {
        MatchCondition condition = new MatchCondition(0.5f, "name", "2001:DB8:2de::0e13");
        assertEquals("MatchCondition{boost=0.5, field=name, value=2001:DB8:2de::0e13}", condition.toString());
    }

}
