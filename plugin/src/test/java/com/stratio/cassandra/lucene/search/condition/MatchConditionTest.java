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
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper;
import com.stratio.cassandra.lucene.schema.mapping.builder.MapperBuilder;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

import java.util.UUID;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class MatchConditionTest {

    @Test
    public void testBuild() {
        MatchCondition condition = new MatchCondition(0.5f, "field", "value");
        assertEquals(0.5f, condition.boost, 0);
        assertEquals("field", condition.field);
        assertEquals("value", condition.value);
    }

    @Test
    public void testBuildDefaults() {
        MatchCondition condition = new MatchCondition(null, "field", "value");
        assertEquals(Condition.DEFAULT_BOOST, condition.boost, 0);
    }

    @Test(expected = IndexException.class)
    public void testBuildNullValue() {
        new MatchCondition(null, "field", null);
    }

    @Test
    public void testBuildBlankValue() {
        MatchCondition condition = new MatchCondition(0.5f, "field", " ");
        assertEquals(0.5f, condition.boost, 0);
        assertEquals("field", condition.field);
        assertEquals(" ", condition.value);
    }

    @Test
    public void testString() {

        Schema schema = schema().mapper("name", stringMapper()).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", "value");
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermQuery.class, query.getClass());
        assertEquals("value", ((TermQuery) query).getTerm().bytes().utf8ToString());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testStringStopwords() {

        Schema schema = schema().mapper("name", textMapper().analyzer("english")).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", "the");
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(BooleanQuery.class, query.getClass());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInteger() {

        Schema schema = schema().mapper("name", integerMapper()).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", 42);
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

        Schema schema = schema().mapper("name", longMapper()).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", 42L);
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

        Schema schema = schema().mapper("name", floatMapper()).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", 42.42F);
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

        Schema schema = schema().mapper("name", doubleMapper()).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", 42.42D);
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

        Schema schema = schema().mapper("name", blobMapper()).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", "0Fa1");
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermQuery.class, query.getClass());
        assertEquals("0fa1", ((TermQuery) query).getTerm().bytes().utf8ToString());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV4() {

        Schema schema = schema().mapper("name", inetMapper()).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", "192.168.0.01");
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermQuery.class, query.getClass());
        assertEquals("192.168.0.1", ((TermQuery) query).getTerm().bytes().utf8ToString());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV6() {

        Schema schema = schema().mapper("name", inetMapper()).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", "2001:DB8:2de::0e13");
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermQuery.class, query.getClass());
        assertEquals("2001:db8:2de:0:0:0:0:e13", ((TermQuery) query).getTerm().bytes().utf8ToString());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test(expected = IndexException.class)
    public void testUnsupportedMapper() {

        final SingleColumnMapper<UUID> mapper = new SingleColumnMapper<UUID>("field",
                                                                             null,
                                                                             null,
                                                                             null,
                                                                             null,
                                                                             UUID.class,
                                                                             UUIDType.instance) {
            @Override
            public Field indexedField(String name, UUID value) {
                return null;
            }

            @Override
            public Field sortedField(String name, UUID value) {
                return null;
            }

            @Override
            public UUID base(String field, Object value) {
                return null;
            }

            @Override
            public org.apache.lucene.search.SortField sortField(String name, boolean reverse) {
                return null;
            }
        };

        Schema schema = schema().mapper("field", new MapperBuilder<Mapper>() {
            @Override
            public Mapper build(String field) {
                return mapper;
            }
        }).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "field", "2001:DB8:2de::0e13");
        matchCondition.query(schema);
    }

    @Test
    public void testToString() {
        MatchCondition condition = new MatchCondition(0.5f, "name", "2001:DB8:2de::0e13");
        assertEquals("MatchCondition{boost=0.5, field=name, value=2001:DB8:2de::0e13}", condition.toString());
    }

}
