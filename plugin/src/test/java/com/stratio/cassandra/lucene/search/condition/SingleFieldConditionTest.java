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

package com.stratio.cassandra.lucene.search.condition;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper;
import com.stratio.cassandra.lucene.schema.mapping.StringMapper;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SingleFieldConditionTest {

    @Test
    public void testBuild() {
        SingleFieldCondition condition = new SingleFieldCondition(0.5f, "field") {
            @Override
            public Query query(Schema schema) {
                return null;
            }
        };
        assertEquals(0.5f, condition.boost, 0);
        assertEquals("field", condition.field);
    }

    @Test
    public void testBuildDefaults() {
        SingleFieldCondition condition = new SingleFieldCondition(null, "field") {
            @Override
            public Query query(Schema schema) {
                return null;
            }
        };
        assertEquals(Condition.DEFAULT_BOOST, condition.boost, 0);
    }

    @Test(expected = IndexException.class)
    public void testBuildNullField() {
        new SingleFieldCondition(null, null) {
            @Override
            public Query query(Schema schema) {
                return null;
            }
        };
    }

    @Test(expected = IndexException.class)
    public void testBuildBlankField() {
        new SingleFieldCondition(null, " ") {
            @Override
            public Query query(Schema schema) {
                return null;
            }
        };
    }

    @Test
    public void testGetMapper() {

        SingleColumnMapper mapper = new StringMapper("field", null, null, null, null);
        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(mapper);

        SingleFieldCondition condition = new SingleFieldCondition(null, "field") {
            @Override
            public Query query(Schema schema) {
                return null;
            }
        };
        Mapper out = condition.getMapper(schema, "field");
        assertNotNull(out);
        assertEquals(mapper, out);

    }

    @Test(expected = IndexException.class)
    public void testGetMapperNotFound() {

        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(null);

        SingleFieldCondition condition = new SingleFieldCondition(null, "field") {
            @Override
            public Query query(Schema schema) {
                return null;
            }
        };
        condition.getMapper(schema, "field2");

    }

}
