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

import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.schema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class NoneConditionTest {

    @Test
    public void testBuild() {
        Float boost = 0.7f;
        NoneCondition condition = new NoneCondition(boost);
        assertEquals(boost, condition.boost, 0);
    }

    @Test
    public void testBuildWithDefaults() {
        NoneCondition condition = new NoneCondition(null);
        assertEquals(Condition.DEFAULT_BOOST, condition.boost, 0);
    }

    @Test
    public void testQuery() {
        Schema schema = schema().build();
        NoneCondition condition = new NoneCondition(0.7f);
        Query query = condition.query(schema);
        assertNotNull(query);
        assertEquals(BooleanQuery.class, query.getClass());
        assertEquals(0.7f, query.getBoost(), 0);
    }

    @Test
    public void testToString() {
        NoneCondition condition = new NoneCondition(0.7f);
        assertEquals("NoneCondition{boost=0.7}", condition.toString());
    }

}
