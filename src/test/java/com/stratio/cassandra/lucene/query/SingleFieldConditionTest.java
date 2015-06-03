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
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapper;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperSingle;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperString;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SingleFieldConditionTest extends AbstractConditionTest {

    @Test
    public void testBuild() {
        SingleFieldCondition condition = new SingleFieldCondition(0.5f, "field") {
            @Override
            public Query query(Schema schema) {
                return null;
            }
        };
        assertEquals(0.5f, condition.getBoost(), 0);
        assertEquals("field", condition.getField());
    }

    @Test
    public void testBuildDefaults() {
        SingleFieldCondition condition = new SingleFieldCondition(null, "field") {
            @Override
            public Query query(Schema schema) {
                return null;
            }
        };
        assertEquals(Condition.DEFAULT_BOOST, condition.getBoost(), 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildNullField() {
        new SingleFieldCondition(null, null) {
            @Override
            public Query query(Schema schema) {
                return null;
            }
        };
    }

    @Test(expected = IllegalArgumentException.class)
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

        ColumnMapperSingle mapper = new ColumnMapperString("field", null, null, null);
        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(mapper);

        SingleFieldCondition condition = new SingleFieldCondition(null, "field") {
            @Override
            public Query query(Schema schema) {
                return null;
            }
        };
        ColumnMapper out = condition.getMapper(schema, "field");
        assertNotNull(out);
        assertEquals(mapper, out);

    }

    @Test(expected = IllegalArgumentException.class)
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
