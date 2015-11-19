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
import com.stratio.cassandra.lucene.search.condition.builder.MatchConditionBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
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
public class MatchConditionTest extends AbstractConditionTest {

    @Test
    public void testBuildDefaults() {
        MatchConditionBuilder builder = new MatchConditionBuilder("field", "value");
        MatchCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set to default", Condition.DEFAULT_BOOST, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Value is not set", "value", condition.value);
    }

    @Test
    public void testBuildString() {
        MatchConditionBuilder builder = new MatchConditionBuilder("field", "value").boost(0.7);
        MatchCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Value is not set", "value", condition.value);
    }

    @Test
    public void testBuildNumber() {
        MatchConditionBuilder builder = new MatchConditionBuilder("field", 3).boost(0.7);
        MatchCondition condition = builder.build();
        assertNotNull("Condition is not built", condition);
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Value is not set", 3, condition.value);
    }

    @Test
    public void testBlankValue() {
        MatchConditionBuilder builder = new MatchConditionBuilder("field", " ").boost(0.7);
        MatchCondition condition = builder.build();
        assertEquals("Boost is not set", 0.7f, condition.boost, 0);
        assertEquals("Field is not set", "field", condition.field);
        assertEquals("Value is not set", " ", condition.value);
    }

    @Test
    public void testJsonSerializationDefaults() {
        MatchConditionBuilder builder = new MatchConditionBuilder("field", "value");
        testJsonSerialization(builder, "{type:\"match\",field:\"field\",value:\"value\"}");
    }

    @Test
    public void testJsonSerializationString() {
        MatchConditionBuilder builder = new MatchConditionBuilder("field", "value").boost(0.7);
        testJsonSerialization(builder, "{type:\"match\",field:\"field\",value:\"value\",boost:0.7}");
    }

    @Test
    public void testJsonSerializationNumber() {
        MatchConditionBuilder builder = new MatchConditionBuilder("field", 3).boost(0.7);
        testJsonSerialization(builder, "{type:\"match\",field:\"field\",value:3,boost:0.7}");
    }

    @Test
    public void testString() {

        Schema schema = schema().mapper("name", stringMapper()).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", "value");
        Query query = matchCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", TermQuery.class, query.getClass());

        TermQuery termQuery = (TermQuery) query;
        assertEquals("Query value is wrong", "value", termQuery.getTerm().bytes().utf8ToString());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testStringStopwords() {

        Schema schema = schema().mapper("name", textMapper().analyzer("english")).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", "the");
        Query query = matchCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", BooleanQuery.class, query.getClass());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInteger() {

        Schema schema = schema().mapper("name", integerMapper()).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", 42);
        Query query = matchCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", NumericRangeQuery.class, query.getClass());

        NumericRangeQuery<?> numericRangeQuery = (NumericRangeQuery<?>) query;
        assertEquals("Query value is wrong", 42, numericRangeQuery.getMin());
        assertEquals("Query value is wrong", 42, numericRangeQuery.getMax());
        assertEquals("Query value is wrong", true, numericRangeQuery.includesMin());
        assertEquals("Query value is wrong", true, numericRangeQuery.includesMax());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testLong() {

        Schema schema = schema().mapper("name", longMapper()).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", 42L);
        Query query = matchCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", NumericRangeQuery.class, query.getClass());

        NumericRangeQuery<?> numericRangeQuery = (NumericRangeQuery<?>) query;
        assertEquals("Query value is wrong", 42L, numericRangeQuery.getMin());
        assertEquals("Query value is wrong", 42L, numericRangeQuery.getMax());
        assertEquals("Query value is wrong", true, numericRangeQuery.includesMin());
        assertEquals("Query value is wrong", true, numericRangeQuery.includesMax());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testFloat() {

        Schema schema = schema().mapper("name", floatMapper()).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", 42.42F);
        Query query = matchCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", NumericRangeQuery.class, query.getClass());

        NumericRangeQuery<?> numericRangeQuery = (NumericRangeQuery<?>) query;
        assertEquals("Query value is wrong", 42.42F, numericRangeQuery.getMin());
        assertEquals("Query value is wrong", 42.42F, numericRangeQuery.getMax());
        assertEquals("Query value is wrong", true, numericRangeQuery.includesMin());
        assertEquals("Query value is wrong", true, numericRangeQuery.includesMax());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testDouble() {

        Schema schema = schema().mapper("name", doubleMapper()).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", 42.42D);
        Query query = matchCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", NumericRangeQuery.class, query.getClass());

        NumericRangeQuery<?> numericRangeQuery = (NumericRangeQuery<?>) query;
        assertEquals("Query value is wrong", 42.42D, numericRangeQuery.getMin());
        assertEquals("Query value is wrong", 42.42D, numericRangeQuery.getMax());
        assertEquals("Query value is wrong", true, numericRangeQuery.includesMin());
        assertEquals("Query value is wrong", true, numericRangeQuery.includesMax());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testBlob() {

        Schema schema = schema().mapper("name", blobMapper()).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", "0Fa1");
        Query query = matchCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", TermQuery.class, query.getClass());

        TermQuery termQuery = (TermQuery) query;
        assertEquals("Query value is wrong", "0fa1", termQuery.getTerm().bytes().utf8ToString());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV4() {

        Schema schema = schema().mapper("name", inetMapper()).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", "192.168.0.01");
        Query query = matchCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", TermQuery.class, query.getClass());

        TermQuery termQuery = (TermQuery) query;
        assertEquals("Query value is wrong", "192.168.0.1", termQuery.getTerm().bytes().utf8ToString());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV6() {

        Schema schema = schema().mapper("name", inetMapper()).build();

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", "2001:DB8:2de::0e13");
        Query query = matchCondition.query(schema);

        assertNotNull("Query is not built", query);
        assertEquals("Query type is wrong", TermQuery.class, query.getClass());

        TermQuery termQuery = (TermQuery) query;
        assertEquals("Query value is wrong", "2001:db8:2de:0:0:0:0:e13", termQuery.getTerm().bytes().utf8ToString());
        assertEquals("Query boost is wrong", 0.5f, query.getBoost(), 0);
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
            protected UUID doBase(String name, Object value) {
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
        assertEquals("Method #toString is wrong",
                     "MatchCondition{boost=0.5, field=name, value=2001:DB8:2de::0e13}",
                     condition.toString());
    }

}
